"""
共享数据层：SQLite + FTS5

四张表：
  channels          — 每个来源（接收频道 / 各个 tdl 批量频道）各自的 checkpoint
  files             — 去重表，file_unique_id 或 sha256 命中即视为重复
  messages          — 原始消息元数据（入口/来源/发送者/时间/caption），供全文搜索
  archive_failures  — 归档失败账，控制重试与跳过

schema 变更走 PRAGMA user_version + MIGRATIONS 有序迁移，
SCHEMA 常量始终保持目标形态。

stash-listener 和 tdl-sync 两个服务各自拷贝一份这个文件，
但通过挂载同一个 /data/db 卷，实际操作的是同一个 SQLite 文件。
"""

import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# 建表部分。FTS 单独拆出去，因为迁移时要 DROP 重建，定义只能有一份
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
    source_chat_id TEXT NOT NULL,
    source_message_id INTEGER NOT NULL,
    failure_stage TEXT NOT NULL,   -- 'download' | 'verify' | 'convert' | 'upload' | 'unknown'
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

# tokenize='trigram'：SQLite 默认的 unicode61 分词器几乎无法处理中文（没有空格分隔，
# 整段中文常被当成一个 token，导致搜不到子串）。trigram 按每 3 个字符切一次，
# 对中文更实用，但代价是搜索词必须 >= 3 个字符，2 字词搜不到。
# 索引 origin_title 而不是 source_channel_title：后者是接收频道标题，
# 所有行同一个值，在索引里纯属噪音。
#
# 拆成单条语句的列表而不是一整段脚本：迁移要在显式事务里重建 FTS，
# 而 executescript 会先隐式 COMMIT，把事务拆散。con.execute 逐条执行才能留在事务内。
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

# 迁移时先把旧定义整套拆掉，再按 FTS_CREATE_STATEMENTS 重建。
# 触发器体里写死了列名，必须跟着 DROP，否则触发器会往不存在的列写
FTS_DROP_STATEMENTS = [
    "DROP TRIGGER IF EXISTS messages_ai",
    "DROP TRIGGER IF EXISTS messages_ad",
    "DROP TRIGGER IF EXISTS messages_au",
    "DROP TABLE IF EXISTS messages_fts",
]

# 供 __init__ 的 executescript 使用；定义的唯一来源仍是上面那个列表
FTS_SCHEMA = ";\n".join(FTS_CREATE_STATEMENTS) + ";\n"

SCHEMA = TABLES_SCHEMA + FTS_SCHEMA

# 迁移要补的列。SCHEMA 里已经写全，这份表只服务于「已有数据的旧库」
MIGRATION_COLUMNS = {
    "files": [("file_name", "TEXT"), ("mime_type", "TEXT"), ("media_kind", "TEXT")],
    "messages": [
        ("origin_chat_id", "TEXT"), ("origin_message_id", "INTEGER"),
        ("origin_title", "TEXT"), ("origin_type", "TEXT"),
        ("file_name", "TEXT"), ("media_kind", "TEXT"),
    ],
}


# journal_mode 是数据库的持久属性，建库时设一次就够。每次连接都无条件重设会踩坑：
# 切换 journal_mode 需要独占锁，而且不吃 busy_timeout——另一个连接正持有写事务时
# 会直接抛 database is locked。所以先读后判断，已经是 WAL 就不去动它
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
            # 建库瞬间两个进程可能同时切换。只要对端切成功了，这边读到 WAL 就收工
            if str(con.execute("PRAGMA journal_mode").fetchone()[0]).upper() == "WAL":
                return
            if attempt == WAL_SWITCH_ATTEMPTS - 1:
                raise
            time.sleep(WAL_SWITCH_DELAY_SECONDS)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def build_match_query(raw: str) -> str | None:
    """
    把用户输入转成安全的 FTS5 MATCH 表达式。

    直接把原始输入喂给 MATCH 会炸：文件名里的 `.` 触发语法错误，
    `-` 会让后半段被当成列名（no such column）。逐词包成字符串字面量，
    内部双引号翻倍转义。词与词之间留空格——FTS5 里这仍是 AND，
    不会退化成 OR，也不会变成要求相邻的短语。

    全空白输入返回 None，调用方直接返回空结果（MATCH '' 会报错）。
    """
    terms = [t for t in raw.split() if t]
    if not terms:
        return None
    return " ".join('"' + t.replace('"', '""') + '"' for t in terms)


def _add_missing_columns(con) -> None:
    """按 PRAGMA table_info 检测缺列再 ALTER。新库上是 no-op，因此天然幂等。"""
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
    # external content FTS：DROP 不动 messages 表，rebuild 从 content table 重填
    con.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
    logger.info("迁移：messages_fts 索引列已切换并重建")


# (版本号, 迁移函数)，按版本号升序。加列本身幂等，
# user_version 存在的唯一理由是让 FTS 重建这类非幂等步骤只跑一次
MIGRATIONS = [(1, _migrate_v1)]


class ArchiveDB:
    def __init__(self, path: str):
        self._path = path
        with self._connect() as con:
            con.executescript(SCHEMA)
            self._migrate(con)

    def _migrate(self, con) -> None:
        if con.execute("PRAGMA user_version").fetchone()[0] >= SCHEMA_VERSION:
            return
        # 先拿写锁再读版本号。WAL 下「先读后写」的连接无法升级成写事务，
        # 两个服务同时启动会有一个直接吃到 SQLITE_BUSY——busy_timeout 对
        # 锁升级无效。BEGIN IMMEDIATE 一开始就取写锁，慢的那个改为等待。
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
        # 遇到锁时等待 30 秒而非立即报 SQLITE_BUSY，容忍两个服务同时写入的瞬间冲突
        con.execute("PRAGMA busy_timeout=30000")
        # WAL 模式：读不阻塞写，写不阻塞读——两个服务共享同一个 db 文件的基础
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
            con.execute(
                "UPDATE channels SET last_message_id=?, last_run_at=? WHERE chat_id=?",
                (message_id, _now(), str(chat_id)),
            )

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
            con.execute(
                """INSERT OR IGNORE INTO files
                   (file_unique_id, sha256, size, archived_chat_id, archived_message_id,
                    source, source_channel, first_seen_at, file_name, mime_type, media_kind)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    file_unique_id,
                    sha256,
                    size,
                    str(archived_chat_id) if archived_chat_id is not None else None,
                    archived_message_id,
                    source,
                    str(source_channel) if source_channel is not None else None,
                    _now(),
                    file_name,
                    mime_type,
                    media_kind,
                ),
            )

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
            con.execute(
                """INSERT INTO messages
                   (source_chat_id, source_message_id, source_channel_title, sender, sent_at,
                    caption, file_unique_id, media_group_id, archived_chat_id, archived_message_id,
                    created_at, origin_chat_id, origin_message_id, origin_title, origin_type,
                    file_name, media_kind)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    str(source_chat_id) if source_chat_id is not None else None,
                    source_message_id,
                    source_channel_title,
                    sender,
                    sent_at,
                    caption,
                    file_unique_id,
                    str(media_group_id) if media_group_id is not None else None,
                    str(archived_chat_id) if archived_chat_id is not None else None,
                    archived_message_id,
                    _now(),
                    str(origin_chat_id) if origin_chat_id is not None else None,
                    origin_message_id,
                    origin_title,
                    origin_type,
                    file_name,
                    media_kind,
                ),
            )

    def rows_missing_origin(self, limit: int | None = None):
        """
        待回填的行：origin_type IS NULL 才算「还没回填过」。

        'original'（原创直发）和 'unknown'（查不到原消息）都已经是终态，
        必须排除，否则每轮回填都会把它们重新拉一遍。
        """
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
        """回填单行 messages 的来源与文件身份。"""
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
        """回填 files 的文件身份。COALESCE 保住已有非空值，回填不覆盖好数据。"""
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
        """累计一次失败；不存在则插入。返回该消息当前的累计次数。"""
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

    def delete_failure(self, source_chat_id, source_message_id: int):
        with self._connect() as con:
            con.execute(
                "DELETE FROM archive_failures WHERE source_chat_id=? AND source_message_id=?",
                (str(source_chat_id), source_message_id),
            )

    def pending_failures(self) -> list[str]:
        """返回所有仍在重试中的失败消息，格式 'chat:msg'。"""
        with self._connect() as con:
            rows = con.execute(
                "SELECT source_chat_id, source_message_id FROM archive_failures WHERE status='retrying'"
            ).fetchall()
            return [f"{r[0]}:{r[1]}" for r in rows]
