"""
按 source_message_id 删除数据库中与该消息相关的所有记录，并回退 checkpoint
使项目下次扫描时重新备份该消息。

用法：
    # 单个 ID
    python scripts/delete_message.py 12345 --db data/db/archive.db
    # 多个 ID
    python scripts/delete_message.py 12345 12346 12347 --db data/db/archive.db
    # 先预览
    python scripts/delete_message.py 12345 12346 --db data/db/archive.db --dry-run

会删除 / 回退：
  - messages 表中匹配 source_message_id 的行（FTS 索引由触发器自动清理）
  - files 表中对应的行（仅当没有其他消息引用同一个 file_unique_id 时）
  - channels 表中对应 source_chat_id 的 checkpoint，回退到 min(source_message_id) - 1
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stash-listener"))
from db import ArchiveDB


def main():
    parser = argparse.ArgumentParser(description="按 source_message_id 删除数据库记录并回退 checkpoint")
    parser.add_argument("msg_ids", type=int, nargs="+", help="要删除的 source_message_id（可多个）")
    parser.add_argument("--db", default="/data/db/archive.db", help="archive.db 路径（默认 /data/db/archive.db）")
    parser.add_argument("--dry-run", action="store_true", help="只查看匹配的记录，不实际删除")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"数据库文件不存在：{args.db}")
        sys.exit(1)

    db = ArchiveDB(args.db)
    msg_ids = args.msg_ids

    # 查找所有匹配的消息记录（同时取出 source_chat_id 用于回退 checkpoint）
    placeholders = ",".join("?" * len(msg_ids))
    with db._connect() as con:
        rows = con.execute(
            f"SELECT id, source_message_id, file_unique_id, source_chat_id, "
            f"caption, sender, sent_at "
            f"FROM messages WHERE source_message_id IN ({placeholders})",
            msg_ids,
        ).fetchall()

    if not rows:
        print(f"没找到 source_message_id in {msg_ids} 的记录")
        return

    found_ids = {row[1] for row in rows}
    missing = [mid for mid in msg_ids if mid not in found_ids]
    print(f"找到 {len(rows)} 条记录（{len(found_ids)} 个 message_id）")
    if missing:
        print(f"未找到：{missing}")
    for row in rows:
        caption_preview = (row[4] or "")[:60].replace("\n", " ")
        print(f"  DB id={row[0]}  msg_id={row[1]}  chat={row[3]}  "
              f"file_unique_id={row[2]}  sender={row[5]}  sent={row[6]}  "
              f"caption={caption_preview}")

    # 按 source_chat_id 分组，找出每个频道的最小 message_id，用于回退 checkpoint
    chat_min_msg: dict[str, int] = {}
    for row in rows:
        chat_id = str(row[3]) if row[3] else None
        msg_id = row[1]
        if chat_id and chat_id not in chat_min_msg:
            chat_min_msg[chat_id] = msg_id
        elif chat_id:
            chat_min_msg[chat_id] = min(chat_min_msg[chat_id], msg_id)

    if args.dry_run:
        print("\n--dry-run，不执行删除")
        if chat_min_msg:
            print("checkpoint 将回退到：")
            for chat_id, min_id in chat_min_msg.items():
                old_cp = db.get_checkpoint(chat_id)
                new_cp = min_id - 1
                print(f"  chat={chat_id}: {old_cp} → {new_cp}")
        return

    # 收集所有 file_unique_id
    file_unique_ids = {row[2] for row in rows if row[2]}

    with db._connect() as con:
        # 删除消息（FTS 触发器自动清理 messages_fts）
        ids_to_delete = [row[0] for row in rows]
        cur = con.execute(
            f"DELETE FROM messages WHERE id IN ({','.join('?' * len(ids_to_delete))})",
            ids_to_delete,
        )
        deleted_msgs = cur.rowcount
        print(f"\n已删除 messages: {deleted_msgs} 条")

        # 对每个 file_unique_id，检查是否还有其他消息引用
        deleted_files = 0
        for fuid in file_unique_ids:
            ref_count = con.execute(
                "SELECT COUNT(*) FROM messages WHERE file_unique_id = ?", (fuid,)
            ).fetchone()[0]
            if ref_count == 0:
                cur = con.execute("DELETE FROM files WHERE file_unique_id = ?", (fuid,))
                if cur.rowcount:
                    deleted_files += 1
                    print(f"  同时删除 files: {fuid}（无其他消息引用）")

    # 回退 checkpoint：确保下次扫描会重新处理这些消息
    print()
    for chat_id, min_id in chat_min_msg.items():
        old_cp = db.get_checkpoint(chat_id)
        new_cp = min_id - 1
        if new_cp < old_cp:
            db.set_checkpoint(chat_id, new_cp)
            print(f"checkpoint 回退 chat={chat_id}: {old_cp} → {new_cp}")

    print(f"\n完成：删除 {deleted_msgs} 条消息记录，{deleted_files} 条文件记录")


if __name__ == "__main__":
    main()
