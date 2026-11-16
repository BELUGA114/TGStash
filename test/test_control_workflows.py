from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from support import ARCHIVE, RECEIVE, Harness, record
from tgstash.bot.commands import BotContext, handle_callback, handle_message
from tgstash.bot.operations import AdminOperations
from tgstash.bot.pending import PendingStore
from tgstash.runtime import bootstrap, listener
from tgstash.runtime.config import load_config
from tgstash.storage.db import ArchiveDB


class BotMessage:
    def __init__(self, text, user=111, mid=1000):
        self.text = text
        self.from_user = SimpleNamespace(id=user)
        self.id = mid
        self.sent = []

    async def reply_text(self, text, **kwargs):
        self.sent.append(text)
        return SimpleNamespace(id=self.id + 1)

    async def edit_text(self, text, **kwargs):
        self.sent.append(text)


class Callback:
    def __init__(self, action, mid, user=111):
        self.data = action
        self.message = BotMessage("", mid=mid)
        self.from_user = SimpleNamespace(id=user)
        self.answers = []

    async def answer(self, text="", **kwargs):
        self.answers.append(text)


def test_bot_authorization_confirmation_and_execution_scope(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        record(h.db, 100)
        h.db.set_checkpoint(RECEIVE, 140)
        lock = asyncio.Lock()

        async def backup():
            return "snapshot.db"

        ctx = BotContext(AdminOperations(h.db, lock), frozenset({111, 222}), backup,
                         PendingStore(), PendingStore())
        stranger = BotMessage("/delete 100", user=999)
        await handle_message(ctx, stranger)
        assert stranger.sent == []
        preview = BotMessage("/delete 100 999 rollback")
        await handle_message(ctx, preview)
        assert "未找到" in preview.sent[0]
        await handle_callback(ctx, Callback("del:ok", 1001, user=222))
        assert len(h.db.find_messages_by_source_ids([100])) == 1

        confirmation = Callback("del:ok", 1001)
        async with lock:
            task = asyncio.create_task(handle_callback(ctx, confirmation))
            await asyncio.sleep(0)
            assert not task.done()
            record(h.db, 999)
            h.db.set_checkpoint(RECEIVE, 500)
        await task
        assert "已取消" in confirmation.message.sent[0]
        assert len(h.db.find_messages_by_source_ids([100, 999])) == 2

        preview = BotMessage("/delete 100 888", mid=2000)
        await handle_message(ctx, preview)
        record(h.db, 888)
        await handle_callback(ctx, Callback("del:ok", 2001))
        assert h.db.find_messages_by_source_ids([100]) == []
        assert len(h.db.find_messages_by_source_ids([888, 999])) == 2
        assert h.db.get_checkpoint(RECEIVE) == 500
        replay = Callback("del:ok", 2001)
        await handle_callback(ctx, replay)
        assert "失效" in replay.answers[0]

        h.db.increment_failure(RECEIVE, 888, "upload", "failed")
        clear = BotMessage("/failures clear", mid=3000)
        await handle_message(ctx, clear)
        h.db.increment_failure(RECEIVE, 999, "upload", "new failure")
        await handle_callback(ctx, Callback("fclr:ok", 3001))
        assert h.db.list_failures() == []
        assert h.db.get_checkpoint(RECEIVE) == 500
        assert len(h.db.find_messages_by_source_ids([888, 999])) == 2

        record(h.db, 200)
        rollback = BotMessage("/delete 200 rollback", mid=4000)
        await handle_message(ctx, rollback)
        await handle_callback(ctx, Callback("del:ok", 4001))
        assert h.db.find_messages_by_source_ids([200]) == []
        assert h.db.get_checkpoint(RECEIVE) == 199
        assert len(h.db.find_messages_by_source_ids([888, 999])) == 2

    asyncio.run(scenario())


def test_startup_wiring_and_backup_restore_without_import_side_effects(tmp_path, monkeypatch):
    service = Path(__file__).resolve().parent.parent / "stash-listener"
    env = {key: value for key, value in os.environ.items()
           if key not in {"TG_API_ID", "TG_API_HASH", "RECEIVE_CHAT_ID", "ARCHIVE_CHAT_ID"}}
    env.update(DATA_DIR=str(tmp_path / "never-created"), BATCH_SIZE="invalid",
               PYTHONPATH=str(service))
    subprocess.run([sys.executable, "-c", "import listener"], env=env,
                   check=True, capture_output=True, timeout=30)
    assert not (tmp_path / "never-created").exists()
    for key, value in {
        "TG_API_ID": "12345", "TG_API_HASH": "hash", "RECEIVE_CHAT_ID": str(RECEIVE),
        "ARCHIVE_CHAT_ID": str(ARCHIVE), "DATA_DIR": str(tmp_path), "TG_BOT_TOKEN": "123:abc",
        "BOT_ADMIN_IDS": "111", "HTTP_PROXY": "http://localhost:7890",
        "BATCH_SIZE": "10", "SCAN_INTERVAL_SECONDS": "300", "UPLOAD_COOLDOWN_SECONDS": "5",
    }.items():
        monkeypatch.setenv(key, value)
    config = load_config()
    assert config.receive_chat == RECEIVE and config.archive_chat == ARCHIVE
    assert (config.scan.batch_size, config.scan.interval_seconds,
            config.pipeline.upload_cooldown_seconds) == (10, 300, 5)
    assert config.admin_ids == frozenset({111})
    clients = []

    def client(name, **kwargs):
        clients.append((name, kwargs))
        return SimpleNamespace()

    monkeypatch.setattr(bootstrap, "Client", client)
    async def mark(client, message, duplicate):
        pass
    config = replace(config, backup=replace(config.backup, interval_seconds=100, keep=2, upload=False))
    runtime = bootstrap.build_runtime(config, mark)
    assert [name for name, _ in clients] == ["listener", "bot"]
    assert clients[1][1]["api_id"] == 12345 and clients[1][1]["bot_token"] == "123:abc"
    assert all(kwargs["proxy"]["hostname"] == "localhost" for _, kwargs in clients)
    assert bootstrap.build_client(replace(config, bot_token=""), bot=True) is None

    async def wiring():
        lock = asyncio.Lock()
        registered = []
        ctx = listener.ListenerContext(runtime.client, runtime.db, runtime.pipeline, RECEIVE, config.scan)

        class Bot:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        async def menu(bot):
            pass

        async def scan(context):
            assert lock.locked()
            assert registered[0].operations.lock is lock
            assert registered[0].operations.db is runtime.db
            raise asyncio.CancelledError

        monkeypatch.setattr(listener, "register_handlers", lambda bot, context: registered.append(context))
        monkeypatch.setattr(listener, "register_command_menu", menu)
        monkeypatch.setattr(listener, "scan_once", scan)
        with pytest.raises(asyncio.CancelledError):
            await listener._serve_bot(Bot(), ctx, lock, runtime.backup, config.admin_ids)

    asyncio.run(wiring())
    record(runtime.db, 100)
    last = 0.0
    for now in [100.0, 200.0, 300.0]:
        last = asyncio.run(runtime.backup.maybe_run(now, last))
    assert asyncio.run(runtime.backup.maybe_run(350.0, last)) == last
    snapshots = sorted(Path(config.backup.directory).glob("*.db"))
    assert len(snapshots) == 2
    assert len(ArchiveDB(str(snapshots[-1])).search("归档核心内容")) == 1
    assert asyncio.run(runtime.backup.run(400.0)).endswith("archive-19700101-000640.db")
    attempts = []

    def broken_backup(dest):
        attempts.append(dest)
        raise OSError("disk full")

    monkeypatch.setattr(runtime.db, "backup_to", broken_backup)
    assert asyncio.run(runtime.backup.maybe_run(500.0, 400.0)) == 500.0
    assert asyncio.run(runtime.backup.maybe_run(550.0, 500.0)) == 500.0
    assert len(attempts) == 1
