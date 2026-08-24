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
