"""
命令行搜索归档内容：

    docker compose exec stash-listener python search.py 关键词

注意：搜索用的是 FTS5 trigram 分词器，关键词至少要 3 个字符，
2 字词（比如"猫咪"里搜"猫咪"是 2 字没问题，但搜单字"猫"搜不到）会搜不到结果。
"""

import sys

from db import ArchiveDB

DB_PATH = "/data/db/archive.db"


def main():
    if len(sys.argv) < 2:
        print("用法：python search.py 关键词")
        sys.exit(1)

    query = " ".join(sys.argv[1:])
    db = ArchiveDB(DB_PATH)
    rows = db.search(query, limit=30)

    if not rows:
        print(f"没搜到跟「{query}」相关的内容（提示：关键词至少要 3 个字符）")
        return

    for r in rows:
        chat = r["source_channel_title"] or r["source_chat_id"] or "?"
        sender = r["sender"] or ""
        caption = (r["caption"] or "").replace("\n", " ")[:80]
        archived = ""
        if r["archived_chat_id"] and r["archived_message_id"]:
            archived = f"  -> 备份频道消息 {r['archived_message_id']}"
        print(f"[{r['sent_at'] or '?'}] {chat} {sender}: {caption}{archived}")


if __name__ == "__main__":
    main()
