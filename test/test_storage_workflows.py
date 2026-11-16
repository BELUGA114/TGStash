from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from legacy_schema import OLD_SCHEMA
from support import RECEIVE, message, record
from tgstash.media.downloader import TDLDownloader
from tgstash.storage.db import SCHEMA_VERSION, ArchiveDB


def test_migration_search_and_management_writes_preserve_atomicity(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as con:
        con.executescript(OLD_SCHEMA)
        con.execute("INSERT INTO messages(caption, file_unique_id) VALUES (?,?)", ("历史归档内容", "old"))
    db = ArchiveDB(str(path))
    assert len(db.search("历史归档内容")) == 1
    assert len(ArchiveDB(str(path)).search("历史归档内容")) == 1
    with db._connect() as con:
        assert con.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        con.execute("CREATE TRIGGER reject_message BEFORE INSERT ON messages "
                    "BEGIN SELECT RAISE(ABORT, 'write failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        record(db, 100)
    assert db.find_by_unique_id("U100") is None
    assert db.find_messages_by_source_ids([100]) == []

    with db._connect() as con:
        con.execute("DROP TRIGGER reject_message")
    record(db, 100, caption="报告-2026.pdf 归档核心内容")
    assert db.search("报告-2026.pdf")[0]["source_message_id"] == 100
    db.ensure_channel(RECEIVE, "manual_forward")
    db.set_checkpoint(RECEIVE, 250)
    db.increment_failure(RECEIVE, 100, "upload", "failed")
    db.mark_failure_skipped(RECEIVE, 100, "skip")
    with db._connect() as con:
        con.execute("CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON channels "
                    "BEGIN SELECT RAISE(ABORT, 'checkpoint failure'); END")
    for operation in [db.purge_messages, db.retry_skipped]:
        with pytest.raises(sqlite3.IntegrityError):
            operation([100])
        assert db.find_by_unique_id("U100") is not None
        assert len(db.search("归档核心内容")) == 1
        assert db.get_failure(RECEIVE, 100)["status"] == "skipped"
        assert db.get_checkpoint(RECEIVE) == 250
    with db._connect() as con:
        con.execute("DROP TRIGGER reject_checkpoint")
    db.retry_skipped([100])
    assert db.get_checkpoint(RECEIVE) == 99
    assert db.get_failure(RECEIVE, 100)["status"] == "retrying"
    db.set_checkpoint(RECEIVE, 250)
    record(db, 101, uid="U100")
    db.purge_messages([100])
    assert db.get_checkpoint(RECEIVE) == 99
    assert db.find_by_unique_id("U100") is not None
    db.purge_messages([101])
    assert db.find_by_unique_id("U100") is None
    assert db.search("归档核心内容") == []
    assert db.list_failures() == []
    assert len(db.search("历史归档内容")) == 1


def test_downloader_preserves_partial_success_and_falls_back_on_failure(tmp_path):
    calls = []
    fallbacks = []
    fail = False

    async def runner(cmd):
        calls.append(cmd)
        if fail:
            raise OSError("tdl unavailable")
        (Path(cmd[cmd.index("-d") + 1]) / "1234567890_41_photo.jpg").write_bytes(b"tdl")

    async def fallback(msg, path):
        fallbacks.append(msg.id)
        Path(path).write_bytes(f"fallback-{msg.id}".encode())
        return path

    downloader = TDLDownloader(runner=runner)
    messages = [message(41), message(42)]
    paths = asyncio.run(downloader.download(
        messages, str(tmp_path), fallback, links={41: "https://t.me/channel/41"}))
    assert "--group" in calls[0]
    assert Path(paths[41]).read_bytes() == b"tdl"
    assert Path(paths[42]).read_bytes() == b"fallback-42"
    assert fallbacks == [42]
    for path in paths.values():
        Path(path).unlink()
    fail = True
    paths = asyncio.run(downloader.download(messages, str(tmp_path), fallback))
    assert {mid: Path(path).read_bytes() for mid, path in paths.items()} == {
        41: b"fallback-41", 42: b"fallback-42"}
