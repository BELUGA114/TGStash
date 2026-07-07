"""
共享数据层：SQLite + FTS5

三张表：
  channels — 每个来源（接收频道 / 各个 tdl 批量频道）各自的 checkpoint
  files    — 去重表，file_unique_id 或 sha256 命中即视为重复
  messages — 原始消息元数据（来源频道/发送者/时间/caption），供全文搜索

stash-listener 和 tdl-sync 两个服务各自拷贝一份这个文件，
但通过挂载同一个 /data/db 卷，实际操作的是同一个 SQLite 文件。
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    chat_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,          -- 'manual_forward' 或 'tdl_bulk'
    last_message_id INTEGER NOT NULL DEFAULT 0,
    last_run_at TEXT
);

CREATE TABLE IF NOT EXISTS files (
    file_unique_id TEXT PRIMARY KEY,
    sha256 TEXT,
    size INTEGER,
    archived_chat_id TEXT,
    archived_message_id INTEGER,
    source TEXT,                        -- 'manual_forward' 或 'tdl_bulk'
    source_channel TEXT,
    first_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_chat_id TEXT,
    source_message_id INTEGER,
    source_channel_title TEXT,
    sender TEXT,
    sent_at TEXT,
    caption TEXT,
    file_unique_id TEXT,
    media_group_id TEXT,
    archived_chat_id TEXT,
    archived_message_id INTEGER,
    created_at TEXT
);

-- tokenize='trigram'：SQLite 默认的 unicode61 分词器几乎无法处理中文（没有空格分隔，
-- 整段中文常被当成一个 token，导致搜不到子串）。trigram 按每 3 个字符切一次，
-- 对中文更实用，但代价是搜索词必须 >= 3 个字符，2 字词搜不到。
-- 如果以后觉得不够用，可以按之前讨论的升级路径接入 Meilisearch。
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    caption, source_channel_title, sender,
    content='messages', content_rowid='id',
    tokenize='trigram'
);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, caption, source_channel_title, sender)
    VALUES (new.id, new.caption, new.source_channel_title, new.sender);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, caption, source_channel_title, sender)
    VALUES('delete', old.id, old.caption, old.source_channel_title, old.sender);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, caption, source_channel_title, sender)
    VALUES('delete', old.id, old.caption, old.source_channel_title, old.sender);
    INSERT INTO messages_fts(rowid, caption, source_channel_title, sender)
    VALUES (new.id, new.caption, new.source_channel_title, new.sender);
END;
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ArchiveDB:
    def __init__(self, path: str):
        self._path = path
        with self._connect() as con:
            con.executescript(SCHEMA)

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(self._path, timeout=30)
        # WAL 模式：读不阻塞写，写不阻塞读——两个服务共享同一个 db 文件的基础
        con.execute("PRAGMA journal_mode=WAL")
        # 遇到锁时等待 30 秒而非立即报 SQLITE_BUSY，容忍两个服务同时写入的瞬间冲突
        con.execute("PRAGMA busy_timeout=30000")
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
    ):
        with self._connect() as con:
            con.execute(
                """INSERT OR IGNORE INTO files
                   (file_unique_id, sha256, size, archived_chat_id, archived_message_id,
                    source, source_channel, first_seen_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    file_unique_id,
                    sha256,
                    size,
                    str(archived_chat_id) if archived_chat_id is not None else None,
                    archived_message_id,
                    source,
                    str(source_channel) if source_channel is not None else None,
                    _now(),
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
    ):
        with self._connect() as con:
            con.execute(
                """INSERT INTO messages
                   (source_chat_id, source_message_id, source_channel_title, sender, sent_at,
                    caption, file_unique_id, media_group_id, archived_chat_id, archived_message_id,
                    created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
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
                ),
            )

    def search(self, query: str, limit: int = 20):
        with self._connect() as con:
            con.row_factory = sqlite3.Row
            return con.execute(
                """SELECT m.* FROM messages_fts f
                   JOIN messages m ON m.id = f.rowid
                   WHERE messages_fts MATCH ?
                   ORDER BY rank LIMIT ?""",
                (query, limit),
            ).fetchall()
