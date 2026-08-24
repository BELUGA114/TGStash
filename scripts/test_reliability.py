"""
可靠性补课单元测试
覆盖：archive_failures 表的记录/累计/跳过/自愈/待处理查询
"""
import os
import sqlite3
import tempfile

import pytest

from db import ArchiveDB, SCHEMA


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = ArchiveDB(path)
    yield db
    try:
        os.remove(path)
        os.remove(path + "-wal")
    except OSError:
        pass
    try:
        os.remove(path + "-shm")
    except OSError:
        pass


class TestFailureSchema:
    def test_archive_failures_table_exists(self, db: ArchiveDB):
        with db._connect() as con:
            tables = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "archive_failures" in tables

    def test_failures_table_idempotent(self, db: ArchiveDB):
        with db._connect() as con:
            con.executescript(SCHEMA)  # 重复执行不报错

    def test_failure_index_exists(self, db: ArchiveDB):
        with db._connect() as con:
            indexes = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                ).fetchall()
            }
        assert "idx_archive_failures_status" in indexes


class TestFailures:
    def test_get_failure_unknown_returns_none(self, db: ArchiveDB):
        assert db.get_failure("-100123", 42) is None

    def test_increment_failure_first_creates(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "tdl 失败")
        row = db.get_failure("-100123", 42)
        assert row is not None
        assert row["attempt_count"] == 1
        assert row["status"] == "retrying"
        assert row["failure_stage"] == "download"
        assert row["first_failed_at"] is not None

    def test_increment_failure_counts_up(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err1")
        db.increment_failure("-100123", 42, "download", "err2")
        row = db.get_failure("-100123", 42)
        assert row["attempt_count"] == 2
        assert row["last_error"] == "err2"

    def test_mark_failure_skipped(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err")
        db.mark_failure_skipped("-100123", 42, "重试 3 次仍失败")
        row = db.get_failure("-100123", 42)
        assert row["status"] == "skipped"
        assert row["skipped_at"] is not None
        assert row["skipped_reason"] == "重试 3 次仍失败"

    def test_delete_failure(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err")
        db.delete_failure("-100123", 42)
        assert db.get_failure("-100123", 42) is None

    def test_pending_failures_only_retrying(self, db: ArchiveDB):
        db.increment_failure("-100123", 41, "download", "e")
        db.increment_failure("-100123", 42, "download", "e")
        db.mark_failure_skipped("-100123", 42, "skip")
        pending = db.pending_failures()
        assert pending == ["-100123:41"]


import asyncio
import os
import sys
from types import SimpleNamespace

# 让测试能 import listener 模块（与 test_db 同一运行方式）
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stash-listener"))

import listener
from db import ArchiveDB as RealDB


def _stub_message(message_id, chat_id=-1001234567890, caption=None, text=None):
    return SimpleNamespace(
        id=message_id,
        chat=SimpleNamespace(id=chat_id, title="接收频道"),
        caption=caption,
        text=text,
        media_group_id=None,
    )


class TestRecordFailure:
    def test_record_failure_first_alert_and_retry(self, db: ArchiveDB, monkeypatch):
        """首次失败：告警一次，返回 retry（不推进 checkpoint）"""
        sent = []
        async def fake_send_message(chat_id, text, reply_parameters=None):
            sent.append(text)
            return SimpleNamespace(id=1)
        monkeypatch.setattr(listener, "app", SimpleNamespace(send_message=fake_send_message))
        monkeypatch.setattr(listener, "db", db)

        msg = _stub_message(42)
        result = asyncio.run(listener._record_failure(msg, "download", "tdl 失败"))

        assert result == "retry"
        assert len(sent) == 1
        assert "归档失败" in sent[0]
        row = db.get_failure("-1001234567890", 42)
        assert row["attempt_count"] == 1
        assert row["status"] == "retrying"

    def test_record_failure_at_max_skips(self, db: ArchiveDB, monkeypatch):
        """满 RETRY_MAX_ATTEMPTS：标记 skipped + 告警，返回 skip"""
        sent = []
        async def fake_send_message(chat_id, text, reply_parameters=None):
            sent.append(text)
            return SimpleNamespace(id=1)
        monkeypatch.setattr(listener, "app", SimpleNamespace(send_message=fake_send_message))
        monkeypatch.setattr(listener, "db", db)
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 3)

        msg = _stub_message(42)
        for _ in range(2):
            asyncio.run(listener._record_failure(msg, "download", "err"))
        sent.clear()
        result = asyncio.run(listener._record_failure(msg, "download", "err"))

        assert result == "skip"
        row = db.get_failure("-1001234567890", 42)
        assert row["status"] == "skipped"
        assert len(sent) == 1
        assert "已跳过" in sent[0]

    def test_record_failure_midway_no_extra_alert(self, db: ArchiveDB, monkeypatch):
        """中间轮次不再告警（只首次和满 N 轮各一条）"""
        sent = []
        async def fake_send_message(chat_id, text, reply_parameters=None):
            sent.append(text)
            return SimpleNamespace(id=1)
        monkeypatch.setattr(listener, "app", SimpleNamespace(send_message=fake_send_message))
        monkeypatch.setattr(listener, "db", db)
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 5)

        msg = _stub_message(42)
        asyncio.run(listener._record_failure(msg, "download", "err"))
        asyncio.run(listener._record_failure(msg, "download", "err"))
        asyncio.run(listener._record_failure(msg, "download", "err"))

        assert len(sent) == 1  # 只有首次那一条

    def test_alert_failure_send_fails_does_not_raise(self, db: ArchiveDB, monkeypatch):
        """告警发送失败不影响归档流程（尽力而为）"""
        async def fake_send_message(chat_id, text, reply_parameters=None):
            raise RuntimeError("network down")
        monkeypatch.setattr(listener, "app", SimpleNamespace(send_message=fake_send_message))

        msg = _stub_message(42)
        # 不应抛异常
        asyncio.run(listener.alert_failure(msg, "测试告警"))
        asyncio.run(listener.alert_failure(None, "无 chat 的告警"))  # chat None 也安全


class TestArchiveSingleFailure:
    def test_single_download_failure_reports_retry(self, monkeypatch):
        """下载失败：record_failure → 返回 False（不推进 checkpoint）"""
        from tdl_downloader import TDLDownloader

        captured = {}

        class FakeTDLSingle:
            async def download(self, messages, dir, fallback=None, links=None, fallback_paths=None):
                captured["called"] = True
                return {}

        monkeypatch.setattr(listener, "tdl_downloader", FakeTDLSingle())
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(listener, "db", SimpleNamespace(
            find_by_unique_id=lambda x: None,
            increment_failure=lambda c, m, s, e: 1,
            mark_failure_skipped=lambda *a: None,
            delete_failure=lambda *a: None,
        ))
        sent = []
        async def fake_send_message(chat_id, text, reply_parameters=None):
            sent.append(text)
            return SimpleNamespace(id=1)
        monkeypatch.setattr(listener, "app", SimpleNamespace(send_message=fake_send_message))

        msg = _stub_message(42)
        msg.photo = SimpleNamespace(file_unique_id="uniq-42")  # 无媒体会提前 return True，到不了下载分支
        result = asyncio.run(listener.archive_single(msg))

        assert result is False
        assert captured["called"] is True
        assert len(sent) == 1


class TestArchiveGroupCheckpoint:
    def test_scan_once_not_advance_when_pending_failure(self, monkeypatch):
        """组内有未满 N 轮的失败：不推进 checkpoint"""
        monkeypatch.setattr(listener, "db", SimpleNamespace(
            get_checkpoint=lambda c: 0,
            pending_failures=lambda: ["-1001234567890:41"],
            set_checkpoint=lambda c, m: set_calls.append(m),
            ensure_channel=lambda *a: None,
        ))
        set_calls = []

        msgs = [_stub_message(41), _stub_message(42)]
        result = asyncio.run(listener._advance_group_checkpoint(msgs))

        assert result is False
        assert set_calls == []

    def test_scan_once_advance_when_no_pending_failure(self, monkeypatch):
        """组内无未满 N 轮的失败：推进 checkpoint"""
        set_calls = []
        monkeypatch.setattr(listener, "db", SimpleNamespace(
            get_checkpoint=lambda c: 0,
            pending_failures=lambda: [],
            set_checkpoint=lambda c, m: set_calls.append(m),
            ensure_channel=lambda *a: None,
        ))

        msgs = [_stub_message(41), _stub_message(42)]
        result = asyncio.run(listener._advance_group_checkpoint(msgs))

        assert result is True
        assert set_calls == [42]  # 推进到组内最大消息 id

    def test_scan_once_advance_after_self_heal(self, monkeypatch):
        """失败成员后来成功（自愈清除失败记录）：不再阻塞推进"""
        set_calls = []
        # 模拟：失败已通过 _clear_failure 清除，pending_failures 为空
        monkeypatch.setattr(listener, "db", SimpleNamespace(
            get_checkpoint=lambda c: 0,
            pending_failures=lambda: [],
            set_checkpoint=lambda c, m: set_calls.append(m),
            ensure_channel=lambda *a: None,
        ))

        msgs = [_stub_message(41), _stub_message(42)]
        result = asyncio.run(listener._advance_group_checkpoint(msgs))

        assert result is True
        assert set_calls == [42]
