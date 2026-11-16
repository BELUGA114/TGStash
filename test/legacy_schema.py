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
