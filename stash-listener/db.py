"""
SQLite + FTS5 数据层，供 stash-listener 主服务与离线运维脚本共用。

messages.source_* 与 archive_failures.source_* 存入口，origin_* 存真实来源；
files.source 存归档路径，channels 按频道保存 checkpoint，不能混用这些语义。
SCHEMA 保持目标形态，旧库通过 user_version + MIGRATIONS 有序迁移。
关键约束见 docs/db-invariants.md。
"""

import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import NamedTuple

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# FTS 需在迁移时重建，因此与表定义分开。
TABLES_SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    chat_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,          -- 'manual_forward' | 'link' | 'tdl_bulk'
    last_message_id INTEGER NOT NULL DEFAULT 0,
    last_run_at TEXT
);

CREATE TABLE IF NOT EXISTS files (
    file_unique_id TEXT PRIMARY KEY,
    sha256 TEXT,
    size INTEGER,
    archived_chat_id TEXT,
    archived_message_id INTEGER,
    source TEXT,                        -- 'manual_forward' | 'link' | 'tdl_bulk'
    source_channel TEXT,
    first_seen_at TEXT,
    file_name TEXT,                     -- 原始文件名；Photo 类型没有这个字段，为 NULL
    mime_type TEXT,
    media_kind TEXT                     -- document/video/photo/audio/animation/voice/video_note
);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- source_* 三列是「入口」：两条路径一致地指向接收频道里那条消息。
    -- 真实来源在 origin_* 四列，两者不要混用
    source_chat_id TEXT,
    source_message_id INTEGER,
    source_channel_title TEXT,          -- 入口频道标题；值对所有行恒定，故不进 FTS
    sender TEXT,
    sent_at TEXT,
    caption TEXT,
    file_unique_id TEXT,
    media_group_id TEXT,
    archived_chat_id TEXT,
    archived_message_id INTEGER,
    created_at TEXT,
    origin_chat_id TEXT,
    origin_message_id INTEGER,
    origin_title TEXT,
    -- 'channel'|'chat'|'user'|'hidden_user'|'import' 来自 forward_origin 五变体，
    -- 'link' 路径二直读，'original' 原创直发无转发来源，'unknown' 回填不到，
    -- NULL 表示这行还没回填过。'original' 与 NULL 必须区分，否则回填脚本的
    -- origin_type IS NULL 选择器会永久反复选中原创消息
    origin_type TEXT,
    file_name TEXT,                     -- 冗余自 files：FTS 是 messages 的外部内容，跨表索引不了
    media_kind TEXT                     -- 冗余自 files：便于按类型过滤而不必 JOIN
);

CREATE TABLE IF NOT EXISTS archive_failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- 这两列是「入口」：接收频道里那条消息，两条路径一致。
    -- 路径二若写成源频道的 id，pending_failures 与 delete_message.py 都读不到，
    -- 那条失败既不重试也不阻塞 checkpoint
    source_chat_id TEXT NOT NULL,
    source_message_id INTEGER NOT NULL,
    failure_stage TEXT NOT NULL,   -- 'download' | 'verify' | 'convert' | 'upload' | 'process' | 'unknown'
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'retrying',  -- 'retrying' | 'skipped'
    first_failed_at TEXT,
    last_failed_at TEXT,
    skipped_at TEXT,
    skipped_reason TEXT,
    UNIQUE(source_chat_id, source_message_id)
);
CREATE INDEX IF NOT EXISTS idx_archive_failures_status
    ON archive_failures(status);
"""

# trigram 支持中文子串搜索，代价是搜索词至少 3 个字符。
# 索引真实来源标题，避免入口频道的固定标题污染搜索。
# 逐条 execute 保住迁移事务；executescript 会隐式提交。
FTS_CREATE_STATEMENTS = [
    """CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    caption, origin_title, sender, file_name,
    content='messages', content_rowid='id',
    tokenize='trigram'
)""",
    """CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, caption, origin_title, sender, file_name)
    VALUES (new.id, new.caption, new.origin_title, new.sender, new.file_name);
END""",
    """CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, caption, origin_title, sender, file_name)
    VALUES('delete', old.id, old.caption, old.origin_title, old.sender, old.file_name);
END""",
    """CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, caption, origin_title, sender, file_name)
    VALUES('delete', old.id, old.caption, old.origin_title, old.sender, old.file_name);
    INSERT INTO messages_fts(rowid, caption, origin_title, sender, file_name)
    VALUES (new.id, new.caption, new.origin_title, new.sender, new.file_name);
END""",
]

# 触发器写死索引列名，FTS 换列时必须一并重建。
FTS_DROP_STATEMENTS = [
    "DROP TRIGGER IF EXISTS messages_ai",
    "DROP TRIGGER IF EXISTS messages_ad",
    "DROP TRIGGER IF EXISTS messages_au",
    "DROP TABLE IF EXISTS messages_fts",
]

# 初始化与迁移共用同一份 FTS 定义。
FTS_SCHEMA = ";\n".join(FTS_CREATE_STATEMENTS) + ";\n"

SCHEMA = TABLES_SCHEMA + FTS_SCHEMA

# 旧库需补列；新库的目标形态已在 SCHEMA 中定义。
MIGRATION_COLUMNS = {
    "files": [("file_name", "TEXT"), ("mime_type", "TEXT"), ("media_kind", "TEXT")],
    "messages": [
        ("origin_chat_id", "TEXT"), ("origin_message_id", "INTEGER"),
        ("origin_title", "TEXT"), ("origin_type", "TEXT"),
        ("file_name", "TEXT"), ("media_kind", "TEXT"),
    ],
}


# journal_mode 切换不受 busy_timeout 保护，因此另设有限重试。
WAL_SWITCH_ATTEMPTS = 5
WAL_SWITCH_DELAY_SECONDS = 0.2


def _ensure_wal(con) -> None:
    """确保库处于 WAL 模式。已是 WAL 则不发写操作，避免和别的连接抢独占锁。"""
    if str(con.execute("PRAGMA journal_mode").fetchone()[0]).upper() == "WAL":
        return
    for attempt in range(WAL_SWITCH_ATTEMPTS):
        try:
            con.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError:
            # 并发建库时，另一个连接可能已切换成功。
            if str(con.execute("PRAGMA journal_mode").fetchone()[0]).upper() == "WAL":
                return
            if attempt == WAL_SWITCH_ATTEMPTS - 1:
                raise
            time.sleep(WAL_SWITCH_DELAY_SECONDS)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def build_match_query(raw: str) -> str | None:
    """
    逐词引用并转义双引号，避免文件名标点被 MATCH 当成语法，保留词间 AND。
    空白返回 None，避免 MATCH '' 报错；* 按字面处理，trigram 已支持子串搜索。
    """
    terms = [t for t in raw.split() if t]
    if not terms:
        return None
    return " ".join('"' + t.replace('"', '""') + '"' for t in terms)


def _add_missing_columns(con) -> None:
    """先检查缺列再 ALTER，使新旧库都可安全执行。"""
    for table, columns in MIGRATION_COLUMNS.items():
        existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
        for name, decl in columns:
            if name not in existing:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
                logger.info("迁移：%s 新增列 %s", table, name)


def _migrate_v1(con) -> None:
    """补齐元数据列，并把 FTS 索引列从 source_channel_title 换成 origin_title + file_name。"""
    _add_missing_columns(con)
    for statement in FTS_DROP_STATEMENTS + FTS_CREATE_STATEMENTS:
        con.execute(statement)
    # 外部内容仍在 messages 中，重建索引后需回填已有行。
    con.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
    logger.info("迁移：messages_fts 索引列已切换并重建")


# 按版本升序迁移，避免每次启动都重建 FTS。
MIGRATIONS = [(1, _migrate_v1)]


# 独立写入与原子归档共用 SQL 和参数装配，避免增列时漏改某条路径。
_FILES_INSERT = """INSERT OR IGNORE INTO files
       (file_unique_id, sha256, size, archived_chat_id, archived_message_id,
        source, source_channel, first_seen_at, file_name, mime_type, media_kind)
       VALUES (?,?,?,?,?,?,?,?,?,?,?)"""

_MESSAGES_INSERT = """INSERT INTO messages
       (source_chat_id, source_message_id, source_channel_title, sender, sent_at,
        caption, file_unique_id, media_group_id, archived_chat_id, archived_message_id,
        created_at, origin_chat_id, origin_message_id, origin_title, origin_type,
        file_name, media_kind)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""


def _as_text(value) -> str | None:
    """chat_id 一类的列统一存字符串；None 要保持 None，不能变成 'None'。"""
    return str(value) if value is not None else None


def _files_params(*, file_unique_id, sha256, size, archived_chat_id, archived_message_id,
                  source, source_channel, file_name, mime_type, media_kind) -> tuple:
    return (
        file_unique_id, sha256, size, _as_text(archived_chat_id), archived_message_id,
        source, _as_text(source_channel), _now(), file_name, mime_type, media_kind,
    )


def _messages_params(*, source_chat_id, source_message_id, source_channel_title, sender,
                     sent_at, caption, file_unique_id, media_group_id, archived_chat_id,
                     archived_message_id, origin_chat_id, origin_message_id, origin_title,
                     origin_type, file_name, media_kind) -> tuple:
    return (
        _as_text(source_chat_id), source_message_id, source_channel_title, sender, sent_at,
        caption, file_unique_id, _as_text(media_group_id), _as_text(archived_chat_id),
        archived_message_id, _now(), _as_text(origin_chat_id), origin_message_id,
        origin_title, origin_type, file_name, media_kind,
    )


# 预览与删除共用列清单；事务内操作复用 SQL，不能调用另开连接的公开写方法。
_MESSAGES_BY_SOURCE_COLUMNS = ("id, source_message_id, file_unique_id, "
                               "source_chat_id, caption, sender, sent_at")
_FAILURE_DELETE = "DELETE FROM archive_failures WHERE source_chat_id=? AND source_message_id=?"
_CHECKPOINT_UPDATE = "UPDATE channels SET last_message_id=?, last_run_at=? WHERE chat_id=?"


class PurgeSummary(NamedTuple):
    deleted_messages: int
    deleted_files: list[str]                    # file_unique_id（无其他消息引用才删）
    cleared_failures: list[tuple[str, int]]     # (入口 chat_id, 入口 message_id)
    rollback: dict[str, tuple[int, int]]        # chat_id → (旧 checkpoint, 新 checkpoint)


class Stats(NamedTuple):
    total_files: int              # files 行数 = 登记过的唯一文件数（含去重命中）
    message_count: int            # messages 行数 = 归档的消息条目数
    dedup_hits: int               # 命中 SHA-256 去重、只登记文件没上传的条数
    by_kind: dict[str, int]       # media_kind → 文件数；NULL 归到 'unknown'
    failures: dict[str, int]      # archive_failures.status → 行数


class RetrySummary(NamedTuple):
    reset_entries: list[tuple[str, int]]     # 被重置的 (入口 chat_id, 入口 message_id)
    rollback: dict[str, tuple[int, int]]     # 实际回退的 chat_id → (旧 checkpoint, 新 checkpoint)


class ClearSummary(NamedTuple):
    deleted: int                 # 实删行数；跨 chat 的同一入口 id 可对应多行
    matched_ids: list[int]       # 指定 id 时实际命中的入口 id；清全部时为空


def _fetch_messages_by_source(con, source_message_ids: list[int]) -> list[sqlite3.Row]:
    placeholders = ",".join("?" * len(source_message_ids))
    return con.execute(
        f"SELECT {_MESSAGES_BY_SOURCE_COLUMNS} FROM messages "
        f"WHERE source_message_id IN ({placeholders})",
        source_message_ids,
    ).fetchall()


class ArchiveDB:
    def __init__(self, path: str):
        self._path = path
        with self._connect() as con:
            con.executescript(SCHEMA)
            self._migrate(con)

    def _migrate(self, con) -> None:
        if con.execute("PRAGMA user_version").fetchone()[0] >= SCHEMA_VERSION:
            return
        # 先拿写锁再读版本，避免并发迁移时读事务升级失败。
        con.execute("BEGIN IMMEDIATE")
        try:
            current = con.execute("PRAGMA user_version").fetchone()[0]
            for version, migrate in MIGRATIONS:
                if current < version:
                    logger.info("archive.db 迁移 v%s → v%s", current, version)
                    migrate(con)
                    # PRAGMA 不支持参数绑定；version 来自本模块常量，不是外部输入
                    con.execute(f"PRAGMA user_version={version}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(self._path, timeout=30)
        # 容忍主服务与运维脚本的短暂写锁竞争。
        con.execute("PRAGMA busy_timeout=30000")
        # WAL 允许查询与写入并行。
        _ensure_wal(con)
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def ensure_channel(self, chat_id, source_type: str):
        with self._connect() as con:
            con.execute(
                "INSERT OR IGNORE INTO channels(chat_id, source_type, last_message_id) VALUES (?,?,0)",
                (str(chat_id), source_type),
            )

    def get_checkpoint(self, chat_id) -> int:
        with self._connect() as con:
            row = con.execute(
                "SELECT last_message_id FROM channels WHERE chat_id=?", (str(chat_id),)
            ).fetchone()
            return row[0] if row else 0

    def set_checkpoint(self, chat_id, message_id: int):
        with self._connect() as con:
            con.execute(_CHECKPOINT_UPDATE, (message_id, _now(), str(chat_id)))

    def get_last_run(self, chat_id):
        with self._connect() as con:
            row = con.execute(
                "SELECT last_run_at FROM channels WHERE chat_id=?", (str(chat_id),)
            ).fetchone()
            return row[0] if row else None

    def set_last_run(self, chat_id, iso_ts: str):
        with self._connect() as con:
            con.execute(
                "UPDATE channels SET last_run_at=? WHERE chat_id=?", (iso_ts, str(chat_id))
            )

    def find_by_unique_id(self, file_unique_id: str):
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(
                "SELECT * FROM files WHERE file_unique_id=?", (file_unique_id,)
            ).fetchone()

    def find_by_sha256(self, sha256: str):
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(
                "SELECT * FROM files WHERE sha256=? LIMIT 1", (sha256,)
            ).fetchone()

    def record_file(
        self,
        file_unique_id: str,
        sha256: str,
        size: int,
        archived_chat_id,
        archived_message_id,
        source: str,
        source_channel,
        file_name: str | None = None,
        mime_type: str | None = None,
        media_kind: str | None = None,
    ):
        with self._connect() as con:
            con.execute(_FILES_INSERT, _files_params(
                file_unique_id=file_unique_id, sha256=sha256, size=size,
                archived_chat_id=archived_chat_id, archived_message_id=archived_message_id,
                source=source, source_channel=source_channel, file_name=file_name,
                mime_type=mime_type, media_kind=media_kind,
            ))

    def record_message(
        self,
        source_chat_id=None,
        source_message_id=None,
        source_channel_title=None,
        sender=None,
        sent_at=None,
        caption=None,
        file_unique_id=None,
        media_group_id=None,
        archived_chat_id=None,
        archived_message_id=None,
        origin_chat_id=None,
        origin_message_id=None,
        origin_title=None,
        origin_type=None,
        file_name=None,
        media_kind=None,
    ):
        with self._connect() as con:
            con.execute(_MESSAGES_INSERT, _messages_params(
                source_chat_id=source_chat_id, source_message_id=source_message_id,
                source_channel_title=source_channel_title, sender=sender, sent_at=sent_at,
                caption=caption, file_unique_id=file_unique_id,
                media_group_id=media_group_id, archived_chat_id=archived_chat_id,
                archived_message_id=archived_message_id, origin_chat_id=origin_chat_id,
                origin_message_id=origin_message_id, origin_title=origin_title,
                origin_type=origin_type, file_name=file_name, media_kind=media_kind,
            ))

    def record_archived(
        self,
        *,
        # 文件身份与去重（files）
        file_unique_id: str,
        sha256: str,
        size: int,
        source: str,
        source_channel=None,
        file_name: str | None = None,
        mime_type: str | None = None,
        media_kind: str | None = None,
        # 归档落点（两张表都记）
        archived_chat_id=None,
        archived_message_id=None,
        # 入口与来源（messages）
        source_chat_id=None,
        source_message_id=None,
        source_channel_title=None,
        sender=None,
        sent_at=None,
        caption=None,
        media_group_id=None,
        origin_chat_id=None,
        origin_message_id=None,
        origin_title=None,
        origin_type=None,
    ):
        """
        归档成功只走此入口，files 与 messages 同一事务提交。
        分开写会让 files 去重命中阻止补写失败的 messages，搜索与回填都无法恢复。
        写失败时整笔回滚；接受重传副本，以保住完整记录。
        """
        with self._connect() as con:
            con.execute(_FILES_INSERT, _files_params(
                file_unique_id=file_unique_id, sha256=sha256, size=size,
                archived_chat_id=archived_chat_id, archived_message_id=archived_message_id,
                source=source, source_channel=source_channel, file_name=file_name,
                mime_type=mime_type, media_kind=media_kind,
            ))
            con.execute(_MESSAGES_INSERT, _messages_params(
                source_chat_id=source_chat_id, source_message_id=source_message_id,
                source_channel_title=source_channel_title, sender=sender, sent_at=sent_at,
                caption=caption, file_unique_id=file_unique_id,
                media_group_id=media_group_id, archived_chat_id=archived_chat_id,
                archived_message_id=archived_message_id, origin_chat_id=origin_chat_id,
                origin_message_id=origin_message_id, origin_title=origin_title,
                origin_type=origin_type, file_name=file_name, media_kind=media_kind,
            ))

    def rows_missing_origin(self, limit: int | None = None):
        """仅 NULL 表示未回填；original/unknown 是终态，避免反复拉取。"""
        sql = ("SELECT id, source_chat_id, source_message_id, file_unique_id "
               "FROM messages WHERE origin_type IS NULL ORDER BY id")
        params: tuple = ()
        if limit is not None:
            sql += " LIMIT ?"
            params = (limit,)
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(sql, params).fetchall()

    def update_message_metadata(
        self,
        row_id: int,
        *,
        origin_chat_id=None,
        origin_message_id=None,
        origin_title=None,
        origin_type=None,
        file_name=None,
        media_kind=None,
    ):
        with self._connect() as con:
            con.execute(
                """UPDATE messages SET
                       origin_chat_id=?, origin_message_id=?, origin_title=?,
                       origin_type=?, file_name=?, media_kind=?
                   WHERE id=?""",
                (
                    str(origin_chat_id) if origin_chat_id is not None else None,
                    origin_message_id,
                    origin_title,
                    origin_type,
                    file_name,
                    media_kind,
                    row_id,
                ),
            )

    def update_file_metadata(
        self,
        file_unique_id: str,
        *,
        file_name=None,
        mime_type=None,
        media_kind=None,
    ):
        """COALESCE 保留已有非空值，避免回填覆盖有效文件身份。"""
        with self._connect() as con:
            con.execute(
                """UPDATE files SET
                       file_name=COALESCE(file_name, ?),
                       mime_type=COALESCE(mime_type, ?),
                       media_kind=COALESCE(media_kind, ?)
                   WHERE file_unique_id=?""",
                (file_name, mime_type, media_kind, file_unique_id),
            )

    def mark_origin_unknown(self, row_id: int):
        """原消息查不到或校验不通过：标为终态，下轮不再选中。"""
        with self._connect() as con:
            con.execute(
                "UPDATE messages SET origin_type='unknown' WHERE id=? AND origin_type IS NULL",
                (row_id,),
            )

    def stats(self) -> Stats:
        """
        dedup_hits 按 files - messages 计：SHA-256 命中只写 files，快速去重不入库。
        旧库两表可能失衡，差值钳到 0。
        """
        with self._connect() as con:
            total_files = con.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            message_count = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            by_kind = {row[0]: row[1] for row in con.execute(
                "SELECT COALESCE(media_kind, 'unknown'), COUNT(*) FROM files GROUP BY 1")}
            failures = {row[0]: row[1] for row in con.execute(
                "SELECT status, COUNT(*) FROM archive_failures GROUP BY 1")}
        return Stats(
            total_files=total_files,
            message_count=message_count,
            dedup_hits=max(total_files - message_count, 0),
            by_kind=by_kind,
            failures=failures,
        )

    def list_failures(self, status: str | None = None):
        """按入口 id 排序以便对照 checkpoint；WAL 下只读查询无需扫描锁。"""
        sql = ("SELECT source_chat_id, source_message_id, failure_stage, last_error, "
               "attempt_count, status, last_failed_at FROM archive_failures")
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            if status is None:
                return con.execute(sql + " ORDER BY source_message_id").fetchall()
            return con.execute(
                sql + " WHERE status=? ORDER BY source_message_id", (status,)).fetchall()

    def search(self, query: str, limit: int = 20):
        match = build_match_query(query)
        if match is None:
            return []
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(
                """SELECT m.* FROM messages_fts f
                   JOIN messages m ON m.id = f.rowid
                   WHERE messages_fts MATCH ?
                   ORDER BY rank LIMIT ?""",
                (match, limit),
            ).fetchall()

    def get_failure(self, source_chat_id, source_message_id: int):
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(
                "SELECT * FROM archive_failures WHERE source_chat_id=? AND source_message_id=?",
                (str(source_chat_id), source_message_id),
            ).fetchone()

    def increment_failure(self, source_chat_id, source_message_id: int, failure_stage: str, last_error: str):
        """按入口累计失败，返回供跳过阈值判断的次数。"""
        with self._connect() as con:
            con.execute(
                """INSERT INTO archive_failures
                   (source_chat_id, source_message_id, failure_stage, last_error,
                    attempt_count, status, first_failed_at, last_failed_at)
                   VALUES (?,?,?,?,1,'retrying',?,?)
                   ON CONFLICT(source_chat_id, source_message_id) DO UPDATE SET
                       attempt_count = attempt_count + 1,
                       last_error = excluded.last_error,
                       failure_stage = excluded.failure_stage,
                       last_failed_at = excluded.last_failed_at,
                       status = 'retrying'""",
                (str(source_chat_id), source_message_id, failure_stage, last_error, _now(), _now()),
            )
            row = con.execute(
                "SELECT attempt_count FROM archive_failures WHERE source_chat_id=? AND source_message_id=?",
                (str(source_chat_id), source_message_id),
            ).fetchone()
            return row[0]

    def mark_failure_skipped(self, source_chat_id, source_message_id: int, reason: str):
        with self._connect() as con:
            con.execute(
                """UPDATE archive_failures
                   SET status='skipped', skipped_at=?, skipped_reason=?
                   WHERE source_chat_id=? AND source_message_id=?""",
                (_now(), reason, str(source_chat_id), source_message_id),
            )

    def retry_skipped(self, source_message_ids: list[int] | None = None) -> RetrySummary:
        """
        重置 skipped 与回退 checkpoint 必须原子提交，否则重置行可能永远不再扫描。
        每个入口 chat 退至最小入口 id - 1，只退不进；清零次数避免重试后立即跳过。
        None 处理全部 skipped，列表只取 skipped 交集，空列表不操作。
        bot 写操作由调用方与扫描共用 asyncio.Lock；离线脚本不受该锁保护。
        """
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            if source_message_ids is None:
                rows = con.execute(
                    "SELECT source_chat_id, source_message_id FROM archive_failures "
                    "WHERE status='skipped'"
                ).fetchall()
            else:
                if not source_message_ids:
                    return RetrySummary([], {})
                placeholders = ",".join("?" * len(source_message_ids))
                rows = con.execute(
                    f"SELECT source_chat_id, source_message_id FROM archive_failures "
                    f"WHERE status='skipped' AND source_message_id IN ({placeholders})",
                    source_message_ids,
                ).fetchall()

            if not rows:
                return RetrySummary([], {})

            entries = sorted((str(r["source_chat_id"]), r["source_message_id"])
                             for r in rows)
            now = _now()
            for chat_id, msg_id in entries:
                con.execute(
                    """UPDATE archive_failures
                       SET status='retrying', attempt_count=0,
                           last_failed_at=?, skipped_at=NULL, skipped_reason=NULL
                       WHERE source_chat_id=? AND source_message_id=?""",
                    (now, chat_id, msg_id),
                )

            chat_min: dict[str, int] = {}
            for chat_id, msg_id in entries:
                chat_min[chat_id] = min(chat_min.get(chat_id, msg_id), msg_id)

            rollback: dict[str, tuple[int, int]] = {}
            for chat_id, min_id in chat_min.items():
                new_cp = min_id - 1
                cp_row = con.execute(
                    "SELECT last_message_id FROM channels WHERE chat_id=?", (chat_id,)
                ).fetchone()
                # 未登记的频道没有 checkpoint 可回退。
                if cp_row is None:
                    continue
                old_cp = cp_row[0]
                if new_cp < old_cp:
                    con.execute(_CHECKPOINT_UPDATE, (new_cp, now, chat_id))
                    rollback[chat_id] = (old_cp, new_cp)

            return RetrySummary(entries, rollback)

    def delete_failure(self, source_chat_id, source_message_id: int):
        with self._connect() as con:
            con.execute(_FAILURE_DELETE, (str(source_chat_id), source_message_id))

    def clear_failures(self, source_message_ids: list[int] | None = None) -> ClearSummary:
        """
        None 清全部，入口 id 跨 chat 匹配；空列表必须不操作，避免过滤空参数后误清全表。
        仅清失败账，不触发重扫；清 skipped 会失去该入口的失败线索。
        """
        with self._connect() as con:
            if source_message_ids is None:
                cur = con.execute("DELETE FROM archive_failures")
                return ClearSummary(cur.rowcount, [])
            if not source_message_ids:
                return ClearSummary(0, [])
            placeholders = ",".join("?" * len(source_message_ids))
            matched = [r[0] for r in con.execute(
                f"SELECT DISTINCT source_message_id FROM archive_failures "
                f"WHERE source_message_id IN ({placeholders})",
                source_message_ids).fetchall()]
            cur = con.execute(
                f"DELETE FROM archive_failures WHERE source_message_id IN ({placeholders})",
                source_message_ids)
            return ClearSummary(cur.rowcount, matched)

    def pending_failures(self) -> set[tuple[str, int]]:
        """以 (入口 chat_id, 入口 message_id) 阻塞对应 checkpoint，不能用真实来源 id。"""
        with self._connect() as con:
            rows = con.execute(
                "SELECT source_chat_id, source_message_id FROM archive_failures WHERE status='retrying'"
            ).fetchall()
            return {(str(r[0]), r[1]) for r in rows}

    def find_messages_by_source_ids(self, source_message_ids: list[int]) -> list[sqlite3.Row]:
        """按入口 id 查找，供删除预览与 checkpoint 回退跨度检查。"""
        if not source_message_ids:
            return []
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return _fetch_messages_by_source(con, source_message_ids)

    def purge_messages(self, source_message_ids: list[int], *, rollback: bool = True) -> PurgeSummary:
        """
        messages、孤儿 files、失败账与 checkpoint 回退必须同一事务提交。
        拆开会漏重扫，或被残留 files 去重挡住；FTS 由触发器在同一事务清理。
        按入口 chat 退至最小入口 id - 1，只退不进；rollback=False 用于删除而不重扫。
        """
        if not source_message_ids:
            return PurgeSummary(0, [], [], {})
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            rows = _fetch_messages_by_source(con, source_message_ids)
            if not rows:
                return PurgeSummary(0, [], [], {})

            ids = [row["id"] for row in rows]
            cur = con.execute(
                f"DELETE FROM messages WHERE id IN ({','.join('?' * len(ids))})", ids)
            deleted_messages = cur.rowcount

            # 保留仍被其他 messages 引用的 files，避免破坏去重。
            deleted_files = []
            for fuid in {row["file_unique_id"] for row in rows if row["file_unique_id"]}:
                refs = con.execute(
                    "SELECT COUNT(*) FROM messages WHERE file_unique_id=?", (fuid,)
                ).fetchone()[0]
                if refs == 0:
                    cur = con.execute("DELETE FROM files WHERE file_unique_id=?", (fuid,))
                    if cur.rowcount:
                        deleted_files.append(fuid)

            cleared_failures = []
            entry_keys = sorted({(str(row["source_chat_id"]), row["source_message_id"])
                                 for row in rows if row["source_chat_id"]})
            for chat_id, msg_id in entry_keys:
                cur = con.execute(_FAILURE_DELETE, (chat_id, msg_id))
                if cur.rowcount:
                    cleared_failures.append((chat_id, msg_id))

            rolled_back: dict[str, tuple[int, int]] = {}
            if rollback:
                chat_min: dict[str, int] = {}
                for row in rows:
                    if row["source_chat_id"]:
                        chat_id = str(row["source_chat_id"])
                        chat_min[chat_id] = min(chat_min.get(chat_id, row["source_message_id"]),
                                                row["source_message_id"])
                for chat_id, min_id in chat_min.items():
                    new_cp = min_id - 1
                    cp_row = con.execute(
                        "SELECT last_message_id FROM channels WHERE chat_id=?", (chat_id,)
                    ).fetchone()
                    # 未登记的频道没有 checkpoint 可回退。
                    if cp_row is None:
                        continue
                    old_cp = cp_row[0]
                    if new_cp < old_cp:
                        con.execute(_CHECKPOINT_UPDATE, (new_cp, _now(), chat_id))
                        rolled_back[chat_id] = (old_cp, new_cp)

            return PurgeSummary(deleted_messages, deleted_files, cleared_failures, rolled_back)

    def backup_to(self, dest_path: str) -> None:
        """
        VACUUM INTO 生成含全部已提交数据的独立快照，不可包在事务内。
        本方法仅发此语句，不触发 DML 的隐式事务；异步调用方须用 to_thread 避免阻塞。
        """
        with self._connect() as con:
            con.execute("VACUUM INTO ?", (dest_path,))
