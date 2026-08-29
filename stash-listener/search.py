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

DB_PATH = os.path.join(os.environ.get("DATA_DIR", "/data"), "db", "archive.db")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def archive_link(chat_id, message_id) -> str:
    """把备份频道的 chat_id + message_id 拼成 t.me/c 链接。私有频道去掉 -100 前缀。"""
    text = str(chat_id)
    bare = text[4:] if text.startswith("-100") else text.lstrip("-")
    return f"https://t.me/c/{bare}/{message_id}"


def main():
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
        # 展示真实来源而不是入口频道——入口对所有行恒定，没有信息量
        origin = r["origin_title"] or r["origin_chat_id"] or "?"
        sender = r["sender"] or ""
        kind = r["media_kind"] or "?"
        caption = (r["caption"] or "").replace("\n", " ")[:80]
        name = f" [{r['file_name']}]" if r["file_name"] else ""
        archived = ""
        if r["archived_chat_id"] and r["archived_message_id"]:
            archived = f"  -> {archive_link(r['archived_chat_id'], r['archived_message_id'])}"
        logger.info("[%s] (%s) %s %s: %s%s%s",
                    r["sent_at"] or "?", kind, origin, sender, caption, name, archived)


if __name__ == "__main__":
    main()
