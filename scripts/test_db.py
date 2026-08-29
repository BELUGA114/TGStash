"""
db.py 单元测试
覆盖：schema 创建、channel checkpoint、文件去重、消息元数据、FTS5 全文搜索
"""
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest
from db import SCHEMA, SCHEMA_VERSION, ArchiveDB, build_match_query


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
            origin_title="某个来源频道",
            sender="张三丰",
            caption="今天天气不错，适合出去玩",
        )
        db.record_message(
            source_chat_id="-1001",
            source_message_id=2,
            source_channel_title="归档频道",
            origin_title="某个来源频道",
            sender="李四方",
            caption="明天可能有雨，记得带伞",
        )
        db.record_message(
            source_chat_id="-1001",
            source_message_id=3,
            source_channel_title="归档频道",
            origin_title="某个来源频道",
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

    def test_search_by_origin_title(self, db: ArchiveDB):
        """origin_title（真实来源）在 FTS 索引中"""
        self._seed_messages(db)
        results = db.search("某个来源频道", limit=10)
        assert len(results) == 3

    def test_entry_channel_title_not_indexed(self, db: ArchiveDB):
        """source_channel_title 是入口频道标题，对所有行恒定，进索引纯属噪音"""
        self._seed_messages(db)
        assert db.search("归档频道", limit=10) == []

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


# ═══════════════════════════════════════════
# Schema 迁移
# ═══════════════════════════════════════════

OLD_SCHEMA = """
CREATE TABLE files (
    file_unique_id TEXT PRIMARY KEY, sha256 TEXT, size INTEGER,
    archived_chat_id TEXT, archived_message_id INTEGER,
    source TEXT, source_channel TEXT, first_seen_at TEXT
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_chat_id TEXT, source_message_id INTEGER, source_channel_title TEXT,
    sender TEXT, sent_at TEXT, caption TEXT, file_unique_id TEXT,
    media_group_id TEXT, archived_chat_id TEXT, archived_message_id INTEGER,
    created_at TEXT
);
CREATE VIRTUAL TABLE messages_fts USING fts5(
    caption, source_channel_title, sender,
    content='messages', content_rowid='id', tokenize='trigram');
CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, caption, source_channel_title, sender)
    VALUES (new.id, new.caption, new.source_channel_title, new.sender);
END;
"""


def _make_old_db(path):
    """造一个迁移前的库并塞一条数据，模拟线上已有的 archive.db。"""
    con = sqlite3.connect(path)
    con.executescript(OLD_SCHEMA)
    con.execute(
        "INSERT INTO messages(source_chat_id, source_message_id, source_channel_title,"
        " sender, caption, file_unique_id) VALUES(?,?,?,?,?,?)",
        ("-1001234567890", 100, "接收频道", "阿猫", "一只很胖的橘猫", "FUID_OLD"),
    )
    con.execute(
        "INSERT INTO files(file_unique_id, sha256, size, source) VALUES(?,?,?,?)",
        ("FUID_OLD", "a" * 64, 2048, "manual_forward"),
    )
    con.commit()
    con.close()


class TestMigration:
    def test_old_db_gains_new_columns(self, tmp_path):
        path = str(tmp_path / "old.db")
        _make_old_db(path)
        ArchiveDB(path)
        with sqlite3.connect(path) as con:
            files_cols = {r[1] for r in con.execute("PRAGMA table_info(files)")}
            msg_cols = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
        assert {"file_name", "mime_type", "media_kind"} <= files_cols
        assert {"origin_chat_id", "origin_message_id", "origin_title",
                "origin_type", "file_name", "media_kind"} <= msg_cols

    def test_migration_preserves_existing_rows(self, tmp_path):
        """迁移不能动到已有数据——archive.db 是唯一真相"""
        path = str(tmp_path / "old.db")
        _make_old_db(path)
        ArchiveDB(path)
        with sqlite3.connect(path) as con:
            row = con.execute(
                "SELECT caption, file_unique_id, source_message_id FROM messages"
            ).fetchone()
            size = con.execute("SELECT size FROM files").fetchone()[0]
        assert row == ("一只很胖的橘猫", "FUID_OLD", 100)
        assert size == 2048

    def test_migration_sets_user_version(self, tmp_path):
        path = str(tmp_path / "old.db")
        _make_old_db(path)
        ArchiveDB(path)
        with sqlite3.connect(path) as con:
            assert con.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    def test_migration_is_idempotent(self, tmp_path):
        """服务每次启动都跑 __init__，重复执行必须无副作用"""
        path = str(tmp_path / "old.db")
        _make_old_db(path)
        ArchiveDB(path)
        ArchiveDB(path)
        ArchiveDB(path)
        with sqlite3.connect(path) as con:
            assert con.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            assert con.execute("SELECT count(*) FROM messages").fetchone()[0] == 1

    def test_fresh_db_also_at_current_version(self, tmp_path):
        """全新库由 SCHEMA 一次建全，迁移在其上是 no-op，版本号同样要落定"""
        path = str(tmp_path / "fresh.db")
        ArchiveDB(path)
        with sqlite3.connect(path) as con:
            assert con.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION

    def test_migrated_db_searches_new_columns(self, tmp_path):
        """重建后 origin_title / file_name 可搜，旧的入口标题不再进索引"""
        path = str(tmp_path / "old.db")
        _make_old_db(path)
        db = ArchiveDB(path)
        with sqlite3.connect(path) as con:
            con.execute(
                "UPDATE messages SET origin_title=?, file_name=? WHERE source_message_id=100",
                ("某个公开频道", "fat_cat.mp4"),
            )
            con.commit()
            con.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
            con.commit()
        assert len(db.search("某个公开频道")) == 1
        assert len(db.search("fat_cat.mp4")) == 1
        assert len(db.search("接收频道")) == 0


# ═══════════════════════════════════════════
# FTS 查询转义
# ═══════════════════════════════════════════


class TestQueryEscaping:
    @pytest.fixture
    def seeded(self, db: ArchiveDB):
        db.record_message(caption="一只很胖的橘猫在睡觉", origin_title="猫咪频道",
                          sender="阿猫", file_name="fat_cat.mp4")
        db.record_message(caption="很胖的狗在跑步", origin_title="宠物频道",
                          sender="小李", file_name="repair-guide (1).mkv")
        return db

    def test_build_match_query_quotes_each_term(self):
        assert build_match_query("很胖的 在睡觉") == '"很胖的" "在睡觉"'

    def test_build_match_query_escapes_inner_quote(self):
        assert build_match_query('a"b') == '"a""b"'

    def test_build_match_query_blank_is_none(self):
        assert build_match_query("   ") is None

    def test_dotted_filename_does_not_raise(self, seeded: ArchiveDB):
        """未转义时这个查询会抛 OperationalError: syntax error near '.'"""
        assert len(seeded.search("fat_cat.mp4")) == 1

    def test_hyphen_query_does_not_raise(self, seeded: ArchiveDB):
        """未转义时 repair-guide 会被解析成列名，抛 no such column: guide"""
        assert len(seeded.search("repair-guide")) == 1

    def test_paren_query_does_not_raise(self, seeded: ArchiveDB):
        assert seeded.search("(汽车") == []

    def test_multi_term_keeps_and_semantics(self, seeded: ArchiveDB):
        """两个 3 字以上的词之间是 AND，不能退化成 OR"""
        assert len(seeded.search("很胖的 在睡觉")) == 1
        assert len(seeded.search("很胖的")) == 2

    def test_blank_query_returns_empty(self, seeded: ArchiveDB):
        assert seeded.search("   ") == []

    def test_concurrent_cold_start_does_not_crash(self, tmp_path: Path):
        """
        两个服务同时对着一个还不存在的库启动。

        回归测试：迁移放进 BEGIN IMMEDIATE 事务后，另一个连接的
        PRAGMA journal_mode=WAL 会撞上独占锁——而切换 journal_mode 不吃
        busy_timeout，会直接抛 database is locked。_ensure_wal 先读后判断，
        已是 WAL 就不再发写操作。重复多轮以覆盖时序抖动。
        """
        import threading

        for round_no in range(5):
            db_path = str(tmp_path / f"cold_{round_no}.db")
            errors = []

            def starter():
                try:
                    ArchiveDB(db_path)  # noqa: B023
                except Exception as e:
                    errors.append(str(e))  # noqa: B023

            threads = [threading.Thread(target=starter) for _ in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors, f"第 {round_no} 轮冷启动竞争出错：{errors}"


# ═══════════════════════════════════════════
# 回填支持
# ═══════════════════════════════════════════


class TestBackfillQueries:
    def test_rows_missing_origin_selects_only_null(self, db: ArchiveDB):
        db.record_message(source_message_id=1, file_unique_id="F1", caption="待回填")
        db.record_message(source_message_id=2, file_unique_id="F2", caption="已原创",
                          origin_type="original")
        db.record_message(source_message_id=3, file_unique_id="F3", caption="已放弃",
                          origin_type="unknown")
        rows = db.rows_missing_origin()
        assert [r["source_message_id"] for r in rows] == [1]

    def test_rows_missing_origin_respects_limit(self, db: ArchiveDB):
        for i in range(5):
            db.record_message(source_message_id=i, file_unique_id=f"F{i}")
        assert len(db.rows_missing_origin(limit=2)) == 2

    def test_update_message_metadata_writes_origin(self, db: ArchiveDB):
        db.record_message(source_message_id=10, file_unique_id="F10", caption="内容正文")
        row_id = db.rows_missing_origin()[0]["id"]
        db.update_message_metadata(
            row_id,
            origin_chat_id="-1001111111111", origin_message_id=42,
            origin_title="某个公开频道", origin_type="channel",
            file_name="cat.mp4", media_kind="video",
        )
        assert db.rows_missing_origin() == []
        row = db.search("某个公开频道")[0]
        assert row["origin_message_id"] == 42
        assert row["file_name"] == "cat.mp4"

    def test_update_file_metadata_writes_identity(self, db: ArchiveDB):
        db.record_file(file_unique_id="F20", sha256="d" * 64, size=8,
                       archived_chat_id=None, archived_message_id=None,
                       source="manual_forward", source_channel=None)
        db.update_file_metadata("F20", file_name="dog.jpg",
                                mime_type="image/jpeg", media_kind="photo")
        row = db.find_by_unique_id("F20")
        assert row["file_name"] == "dog.jpg"
        assert row["media_kind"] == "photo"

    def test_update_file_metadata_does_not_clobber_existing(self, db: ArchiveDB):
        """回填不能把已有的好数据覆盖成 None"""
        db.record_file(file_unique_id="F21", sha256="e" * 64, size=8,
                       archived_chat_id=None, archived_message_id=None,
                       source="manual_forward", source_channel=None,
                       file_name="原始名.mp4", mime_type="video/mp4", media_kind="video")
        db.update_file_metadata("F21", file_name=None, mime_type=None, media_kind=None)
        row = db.find_by_unique_id("F21")
        assert row["file_name"] == "原始名.mp4"
        assert row["media_kind"] == "video"

    def test_mark_origin_unknown_stops_reselection(self, db: ArchiveDB):
        """查不到原消息的行标 unknown，下次不再被选中"""
        db.record_message(source_message_id=30, file_unique_id="F30")
        row_id = db.rows_missing_origin()[0]["id"]
        db.mark_origin_unknown(row_id)
        assert db.rows_missing_origin() == []

    def test_update_reindexes_via_trigger(self, db: ArchiveDB):
        """回填走 UPDATE，messages_au 触发器逐行同步 FTS，不需要整表 rebuild"""
        db.record_message(source_message_id=40, file_unique_id="F40", caption="内容正文")
        row_id = db.rows_missing_origin()[0]["id"]
        db.update_message_metadata(row_id, origin_title="某个公开频道",
                                   origin_type="channel")
        assert len(db.search("某个公开频道")) == 1

    def test_two_char_term_alone_returns_nothing(self, seeded: ArchiveDB):
        """trigram 下 2 字词不产生 token：单独搜是空结果，不是「全部结果」"""
        assert seeded.search("橘猫") == []

    def test_two_char_term_adds_no_constraint_when_combined(self, seeded: ArchiveDB):
        """与 ≥3 字的词一起用时，短词不起约束作用"""
        assert len(seeded.search("橘猫 很胖的")) == len(seeded.search("很胖的")) == 2

    def test_trailing_star_is_literal_not_prefix_wildcard(self, seeded: ArchiveDB):
        """加引号的代价：`*` 变字面字符。trigram 直接搜子串即可"""
        assert seeded.search("fat_cat*") == []
        assert len(seeded.search("fat_cat")) == 1
