from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import bootstrap
import listener
import pytest
from runtime_config import ScanConfig


class TestBuildBotClient:
    def test_no_token_means_bot_disabled(self, runtime_config):
        assert bootstrap.build_client(runtime_config, bot=True) is None

    def test_builds_bot_session_with_token(self, monkeypatch, runtime_config):
        made = {}

        def fake_client(name, **kwargs):
            made["name"] = name
            made.update(kwargs)
            return SimpleNamespace(name=name)

        monkeypatch.setattr(bootstrap, "Client", fake_client)
        client = bootstrap.build_client(replace(runtime_config, bot_token="123:abc"), bot=True)

        assert client is not None
        assert made["name"] == "bot"
        assert made["bot_token"] == "123:abc"
        assert made["workdir"] == runtime_config.session_dir
        # 新授权的 API 凭证缺失直到 start 时才暴露，构造桩必须明确检查它们。
        assert made["api_id"] == 12345
        assert made["api_hash"] == "hash"


def test_scan_and_backup_run_inside_the_lock(monkeypatch):
    lock = asyncio.Lock()
    seen = []
    ctx = listener.ListenerContext(
        client=SimpleNamespace(), db=SimpleNamespace(), pipeline=SimpleNamespace(),
        receive_chat=-1, scan=ScanConfig(0, 10, 3, ""))

    async def scan(_ctx):
        seen.append(("scan", lock.locked()))
        return 0

    async def maybe_backup(now, last):
        seen.append(("backup", lock.locked()))
        raise asyncio.CancelledError

    monkeypatch.setattr(listener, "scan_once", scan)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(listener.scan_loop(ctx, lock, SimpleNamespace(maybe_run=maybe_backup)))
    assert seen == [("scan", True), ("backup", True)]


def test_bot_and_scan_receive_the_same_lock(monkeypatch):
    lock = asyncio.Lock()
    registered = []
    scanned = []
    ctx = listener.ListenerContext(
        client=SimpleNamespace(), db=SimpleNamespace(), pipeline=SimpleNamespace(),
        receive_chat=-1, scan=ScanConfig(0, 10, 3, ""))
    backup = SimpleNamespace()

    class Bot:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    async def menu(bot):
        pass

    async def scan(context, shared_lock, service):
        scanned.append((context, shared_lock, service))

    monkeypatch.setattr(listener, "register_handlers", lambda bot, context: registered.append(context))
    monkeypatch.setattr(listener, "register_command_menu", menu)
    monkeypatch.setattr(listener, "scan_loop", scan)
    asyncio.run(listener._serve_bot(Bot(), ctx, lock, backup, frozenset({111})))

    assert registered[0].operations.lock is lock
    assert registered[0].operations.db is ctx.db
    assert registered[0].admin_ids == frozenset({111})
    assert scanned == [(ctx, lock, backup)]
