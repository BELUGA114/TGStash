"""
db.py 单元测试
覆盖：schema 创建、channel checkpoint、文件去重、消息元数据、FTS5 全文搜索
"""
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

# 把项目根目录加入 sys.path，让 test 能 import db
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stash-listener"))
from db import ArchiveDB, SCHEMA


@pytest.fixture
def db():
    """每次测试用独立临时文件，测试结束自动清理"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = ArchiveDB(path)
    yield db
    # 关闭所有连接后再删文件，避免 WAL 残留
    try:
        os.remove(path)
        os.remove(path + "-wal")
    except OSError:
        pass
    try:
        os.remove(path + "-shm")
    except OSError:
        pass


# ═══════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════


class TestSchema:
    def test_tables_exist(self, db: ArchiveDB):
        """三张核心表 + FTS 虚拟表"""
        with db._connect() as con:
            tables = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "channels" in tables
        assert "files" in tables
        assert "messages" in tables
        assert "messages_fts" in tables

    def test_sha256_index_exists(self, db: ArchiveDB):
        """sha256 索引——去重查询的关键路径"""
        with db._connect() as con:
            indexes = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                ).fetchall()
            }
        assert "idx_files_sha256" in indexes

    def test_fts_triggers_exist(self, db: ArchiveDB):
        """FTS 的 INSERT/DELETE/UPDATE 触发器"""
        with db._connect() as con:
            triggers = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                ).fetchall()
            }
        assert "messages_ai" in triggers  # after insert
        assert "messages_ad" in triggers  # after delete
        assert "messages_au" in triggers  # after update

    def test_wal_mode_enabled(self, db: ArchiveDB):
        """WAL 模式：两个服务同时读写不阻塞"""
        with db._connect() as con:
            row = con.execute("PRAGMA journal_mode").fetchone()
        assert row[0].upper() == "WAL"

    def test_idempotent_schema(self, db: ArchiveDB):
        """重复执行 SCHEMA 不报错（服务重启时 __init__ 会重新执行）"""
        with db._connect() as con:
            con.executescript(SCHEMA)  # 第一次：CREATE TABLE IF NOT EXISTS
            con.executescript(SCHEMA)  # 第二次：应无错误
        # 能走到这里就是通过


# ═══════════════════════════════════════════
# Channels / Checkpoint
# ═══════════════════════════════════════════


class TestChannels:
    def test_ensure_channel_creates(self, db: ArchiveDB):
        db.ensure_channel("-1001234567890", "manual_forward")
        checkpoint = db.get_checkpoint("-1001234567890")
        assert checkpoint == 0  # 新频道的 checkpoint 从 0 开始

    def test_ensure_channel_idempotent(self, db: ArchiveDB):
        """重复 ensure 不报错、不改变已有数据"""
        db.ensure_channel("-100123", "manual_forward")
        db.set_checkpoint("-100123", 42)
        db.ensure_channel("-100123", "manual_forward")
        assert db.get_checkpoint("-100123") == 42

    def test_get_checkpoint_unknown_returns_zero(self, db: ArchiveDB):
        assert db.get_checkpoint("nonexistent") == 0

    def test_set_and_get_checkpoint(self, db: ArchiveDB):
        db.ensure_channel("-100123", "manual_forward")
        db.set_checkpoint("-100123", 999)
        assert db.get_checkpoint("-100123") == 999

    def test_checkpoint_updates_last_run_at(self, db: ArchiveDB):
        """set_checkpoint 同时更新 last_run_at"""
        db.ensure_channel("-100123", "tdl_bulk")
        db.set_checkpoint("-100123", 10)
        last_run = db.get_last_run("-100123")
        assert last_run is not None  # 应有 ISO 时间戳

    def test_get_last_run_unknown_returns_none(self, db: ArchiveDB):
        assert db.get_last_run("nonexistent") is None

    def test_set_last_run(self, db: ArchiveDB):
        db.ensure_channel("-100123", "tdl_bulk")
        db.set_last_run("-100123", "2026-01-01T00:00:00+00:00")
        assert db.get_last_run("-100123") == "2026-01-01T00:00:00+00:00"


# ═══════════════════════════════════════════
# Files / 去重
# ═══════════════════════════════════════════


class TestFiles:
    def test_find_by_unique_id_hit(self, db: ArchiveDB):
        db.record_file(
            file_unique_id="AQADBQAD",
            sha256="a" * 64,
            size=1024,
            archived_chat_id="-100456",
            archived_message_id=10,
            source="manual_forward",
            source_channel="-100123",
        )
        row = db.find_by_unique_id("AQADBQAD")
        assert row is not None
        assert row["sha256"] == "a" * 64
        assert row["size"] == 1024

    def test_find_by_unique_id_miss(self, db: ArchiveDB):
        assert db.find_by_unique_id("nonexistent") is None

    def test_find_by_sha256_hit(self, db: ArchiveDB):
        db.record_file(
            file_unique_id="AAA",
            sha256="b" * 64,
            size=2048,
            archived_chat_id="-100456",
            archived_message_id=11,
            source="tdl_bulk",
            source_channel="@test_channel",
        )
        row = db.find_by_sha256("b" * 64)
        assert row is not None
        assert row["file_unique_id"] == "AAA"

    def test_find_by_sha256_miss(self, db: ArchiveDB):
        assert db.find_by_sha256("c" * 64) is None

    def test_record_file_insert_or_ignore(self, db: ArchiveDB):
        """file_unique_id 是主键，重复插入不报错、不覆盖"""
        db.record_file(
            file_unique_id="UNIQUE_ID",
            sha256="first",
            size=100,
            archived_chat_id="-1001",
            archived_message_id=1,
            source="manual_forward",
            source_channel="-100123",
        )
        db.record_file(
            file_unique_id="UNIQUE_ID",
            sha256="second",
            size=200,
            archived_chat_id="-1002",
            archived_message_id=2,
            source="tdl_bulk",
            source_channel="@other",
        )
        row = db.find_by_unique_id("UNIQUE_ID")
        assert row["sha256"] == "first"  # 第一条保留
        assert row["size"] == 100

    def test_record_file_nullable_archived_ids(self, db: ArchiveDB):
        """archived_chat_id 和 archived_message_id 允许为 None（路径二场景）"""
        db.record_file(
            file_unique_id="SYNTHETIC",
            sha256="d" * 64,
            size=512,
            archived_chat_id=None,
            archived_message_id=None,
            source="tdl_bulk",
            source_channel="@priority_ch",
        )
        row = db.find_by_unique_id("SYNTHETIC")
        assert row is not None
        assert row["archived_chat_id"] is None
        assert row["archived_message_id"] is None

    def test_record_file_first_seen_at_auto_set(self, db: ArchiveDB):
        db.record_file(
            file_unique_id="TIMED",
            sha256="e" * 64,
            size=256,
            archived_chat_id="-1001",
            archived_message_id=5,
            source="manual_forward",
            source_channel="-100123",
        )
        row = db.find_by_unique_id("TIMED")
        assert row["first_seen_at"] is not None


# ═══════════════════════════════════════════
# Messages / 元数据
# ═══════════════════════════════════════════


class TestMessages:
    def test_record_message_basic(self, db: ArchiveDB):
        db.record_message(
            source_chat_id="-100123",
            source_message_id=42,
            source_channel_title="测试频道",
            sender="张三",
            sent_at="2026-01-01T12:00:00+00:00",
            caption="这是一条测试消息",
            file_unique_id="FILE001",
            media_group_id="12345678901234567",
            archived_chat_id="-100456",
            archived_message_id=100,
        )
        with db._connect() as con:
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM messages WHERE file_unique_id=?", ("FILE001",)).fetchone()
        assert row is not None
        assert row["sender"] == "张三"
        assert row["caption"] == "这是一条测试消息"
        assert row["media_group_id"] == "12345678901234567"

    def test_record_message_nullable_fields(self, db: ArchiveDB):
        """大部分字段允许 None"""
        db.record_message(
            source_chat_id="@channel",
            source_message_id=1,
        )
        with db._connect() as con:
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM messages WHERE source_chat_id='@channel'").fetchone()
        assert row is not None
        assert row["caption"] is None
        assert row["sender"] is None

    def test_created_at_auto_set(self, db: ArchiveDB):
        db.record_message(
            source_chat_id="-100123",
            source_message_id=1,
            caption="时间测试",
        )
        with db._connect() as con:
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT created_at FROM messages WHERE source_message_id=1"
            ).fetchone()
        assert row["created_at"] is not None


# ═══════════════════════════════════════════
# FTS5 全文搜索
# ═══════════════════════════════════════════


class TestFTS:
    def _seed_messages(self, db: ArchiveDB):
        """插入几条中文消息用于搜索测试"""
        db.record_message(
            source_chat_id="-1001",
            source_message_id=1,
            source_channel_title="归档频道",
            sender="张三丰",
            caption="今天天气不错，适合出去玩",
        )
        db.record_message(
            source_chat_id="-1001",
            source_message_id=2,
            source_channel_title="归档频道",
            sender="李四方",
            caption="明天可能有雨，记得带伞",
        )
        db.record_message(
            source_chat_id="-1001",
            source_message_id=3,
            source_channel_title="归档频道",
            sender="王五六",
            caption="Python 异步编程最佳实践",
        )

    def test_search_chinese_exact(self, db: ArchiveDB):
        self._seed_messages(db)
        results = db.search("出去玩", limit=10)
        assert len(results) == 1
        assert results[0]["sender"] == "张三丰"

    def test_search_chinese_trigram_min_length(self, db: ArchiveDB):
        """trigram 分词器：搜索词至少 3 个字符"""
        self._seed_messages(db)
        # "带伞" 只有 2 个字符——trigram 分词器下可能搜不到
        results = db.search("带伞", limit=10)
        # trigram 按 3 字符切分，"带伞" 只有 2 字，FTS5 会报错或返回空
        # 这个测试验证的是"不会崩溃"，结果可以为空
        assert isinstance(results, list)

    def test_search_by_sender(self, db: ArchiveDB):
        """sender 字段也在 FTS 索引中（注意 trigram 要求 ≥3 字符）"""
        self._seed_messages(db)
        results = db.search("李四方", limit=10)
        assert len(results) >= 1
        assert any(r["sender"] == "李四方" for r in results)

    def test_search_by_channel_title(self, db: ArchiveDB):
        """source_channel_title 在 FTS 索引中"""
        self._seed_messages(db)
        results = db.search("归档频道", limit=10)
        assert len(results) >= 1

    def test_search_no_match(self, db: ArchiveDB):
        self._seed_messages(db)
        results = db.search("完全不存在的内容XYZ", limit=10)
        assert len(results) == 0

    def test_search_limit(self, db: ArchiveDB):
        """limit 参数生效"""
        for i in range(10):
            db.record_message(
                source_chat_id="-1001",
                source_message_id=i,
                caption=f"测试消息 编号 {i}",
            )
        results = db.search("测试消息", limit=3)
        assert len(results) <= 3

    def test_fts_syncs_on_insert(self, db: ArchiveDB):
        """触发器自动同步：INSERT 后立即可搜"""
        db.record_message(
            source_chat_id="-1001",
            source_message_id=1,
            caption="立即同步测试内容",
        )
        results = db.search("同步测试", limit=5)
        assert len(results) == 1


# ═══════════════════════════════════════════
# 并发场景（两个服务共享同一 DB 文件）
# ═══════════════════════════════════════════


class TestConcurrency:
    def test_two_instances_same_file(self, tmp_path: Path):
        """listener 和 archiver 各自创建 ArchiveDB 实例，操作同一个文件"""
        db_path = str(tmp_path / "shared.db")

        listener_db = ArchiveDB(db_path)
        archiver_db = ArchiveDB(db_path)

        # listener 写入
        listener_db.ensure_channel("-100_LISTENER", "manual_forward")
        listener_db.set_checkpoint("-100_LISTENER", 100)
        listener_db.record_file(
            file_unique_id="L001",
            sha256="l" * 64,
            size=1000,
            archived_chat_id="-100_ARCHIVE",
            archived_message_id=1,
            source="manual_forward",
            source_channel="-100_LISTENER",
        )

        # archiver 写入
        archiver_db.ensure_channel("@priority_ch", "tdl_bulk")
        archiver_db.set_last_run("@priority_ch", "2026-01-01T00:00:00+00:00")
        archiver_db.record_file(
            file_unique_id="A001",
            sha256="a" * 64,
            size=2000,
            archived_chat_id="-100_ARCHIVE",
            archived_message_id=2,
            source="tdl_bulk",
            source_channel="@priority_ch",
        )

        # listener 应能看到 archiver 的数据（同一文件）
        assert listener_db.get_checkpoint("-100_LISTENER") == 100
        assert listener_db.get_last_run("@priority_ch") == "2026-01-01T00:00:00+00:00"
        assert listener_db.find_by_unique_id("A001") is not None

        # archiver 应能看到 listener 的数据
        assert archiver_db.find_by_unique_id("L001") is not None

    def test_cross_service_dedup(self, tmp_path: Path):
        """路径一写入的文件，路径二的 sha256 判重应命中"""
        db_path = str(tmp_path / "shared.db")

        listener_db = ArchiveDB(db_path)
        archiver_db = ArchiveDB(db_path)

        listener_db.record_file(
            file_unique_id="REAL_FUID",
            sha256="dup_sha256",
            size=999,
            archived_chat_id="-100_ARCHIVE",
            archived_message_id=10,
            source="manual_forward",
            source_channel="-100_RECEIVE",
        )

        # archiver 用同一个 sha256 判重——应命中
        dup = archiver_db.find_by_sha256("dup_sha256")
        assert dup is not None
        assert dup["file_unique_id"] == "REAL_FUID"

    def test_no_deadlock_on_concurrent_writes(self, tmp_path: Path):
        """WAL 模式下两个连接同时写入不应死锁"""
        import threading

        db_path = str(tmp_path / "shared.db")
        errors = []

        def writer(source: str):
            try:
                db = ArchiveDB(db_path)
                for i in range(20):
                    db.record_file(
                        file_unique_id=f"{source}_{i}",
                        sha256=f"sha_{source}_{i}",
                        size=i,
                        archived_chat_id="-100_ARCHIVE",
                        archived_message_id=i,
                        source=source,
                        source_channel="-100_TEST",
                    )
            except Exception as e:
                errors.append((source, str(e)))

        t1 = threading.Thread(target=writer, args=("service_a",))
        t2 = threading.Thread(target=writer, args=("service_b",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors, f"并发写入出错：{errors}"

        # 验证所有记录都写入了
        db = ArchiveDB(db_path)
        for i in range(20):
            assert db.find_by_unique_id(f"service_a_{i}") is not None
            assert db.find_by_unique_id(f"service_b_{i}") is not None
