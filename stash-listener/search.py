"""
命令行搜索归档内容：

    docker compose exec stash-listener python search.py 关键词

注意：搜索用 FTS5 trigram 分词器，每个关键词至少要 3 个字符。2 字及更短的词
不产生 token：单独搜返回空（搜「猫咪」得不到结果），和 ≥3 字的词一起用时它不起
任何约束作用（「猫咪 橘猫在」等同于只搜「橘猫在」）。多个关键词之间是 AND。
"""

import logging
import os
import sys

from db import ArchiveDB
from logging_setup import configure_logging

DB_PATH = os.path.join(os.environ.get("DATA_DIR", "/data"), "db", "archive.db")

logger = logging.getLogger(__name__)


def archive_link(chat_id, message_id) -> str:
    """把备份频道的 chat_id + message_id 拼成 t.me/c 链接。私有频道去掉 -100 前缀。"""
    text = str(chat_id)
    bare = text[4:] if text.startswith("-100") else text.lstrip("-")
    return f"https://t.me/c/{bare}/{message_id}"


def format_result(row) -> str:
    """
    把一条搜索结果排成单行文本。命令行与 bot `/search` 共用这一份格式。

    展示真实来源（origin_*）而不是入口频道 —— 入口对所有行恒定，没有信息量。
    """
    origin = row["origin_title"] or row["origin_chat_id"] or "?"
    sender = row["sender"] or ""
    kind = row["media_kind"] or "?"
    caption = (row["caption"] or "").replace("\n", " ")[:80]
    name = f" [{row['file_name']}]" if row["file_name"] else ""
    archived = ""
    if row["archived_chat_id"] and row["archived_message_id"]:
        archived = f"  -> {archive_link(row['archived_chat_id'], row['archived_message_id'])}"
    return f"[{row['sent_at'] or '?'}] ({kind}) {origin} {sender}: {caption}{name}{archived}"


def main():
    # 日志配置属于进程启动，不属于 import
    configure_logging()
    if len(sys.argv) < 2:
        logger.info("用法：python search.py 关键词")
        sys.exit(1)

    query = " ".join(sys.argv[1:])
    db = ArchiveDB(DB_PATH)
    rows = db.search(query, limit=30)

    if not rows:
        logger.info("没搜到跟「%s」相关的内容（提示：每个关键词至少要 3 个字符）", query)
        return

    for r in rows:
        logger.info("%s", format_result(r))


if __name__ == "__main__":
    main()
