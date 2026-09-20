"""
把 archive_failures 里 status='skipped' 的行重排回重试队列，并回退接收频道
checkpoint，让下轮扫描重扫那几条媒体。

skipped 多为外部临时原因（代理断、Telegram 抽风、tdl session 过期）。修好底层后
那几条媒体永久不再归档，且 checkpoint 早已推过——本脚本把它们救回来。

source_message_id 是**入口** id（接收频道里那条消息）。archive_failures.source_*
与 checkpoint 同为入口语义，回退落在同一个 id 空间。双层去重（file_unique_id →
SHA-256）保证已归档的不会重复上传。

checkpoint 只退不进：回退目标 min(选中行 id) - 1 只在它小于当前 checkpoint 时才写。

注意：脚本与主服务共用同一份 session/DB。运行前 `docker compose stop stash-listener`，
跑完 `docker compose up -d`（与 backfill 脚本同一约束）。

用法：
    # 全部 skipped
    python scripts/retry_skipped.py --db data/db/archive.db
    # 指定入口 id（可多个）
    python scripts/retry_skipped.py 12345 12346 --db data/db/archive.db
    # 先预览
    python scripts/retry_skipped.py --db data/db/archive.db --dry-run
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stash-listener"))
from db import ArchiveDB


def main():
    parser = argparse.ArgumentParser(
        description="把 skipped 的失败行重排回重试队列并回退 checkpoint")
    parser.add_argument("msg_ids", type=int, nargs="*",
                        help="要重试的入口 source_message_id（省略 = 全部 skipped）")
    parser.add_argument("--db", default="/data/db/archive.db",
                        help="archive.db 路径（默认 /data/db/archive.db）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印将重置哪些行、checkpoint 将回退到哪，不写库")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"数据库文件不存在：{args.db}")
        sys.exit(1)

    db = ArchiveDB(args.db)
    targets = args.msg_ids if args.msg_ids else None

    if args.dry_run:
        # 预览：只读地查有哪些 skipped 会被选中，不写库。用 get_failure 逐个探测状态
        _preview(db, targets)
        return

    summary = db.retry_skipped(targets)
    if not summary.reset_entries:
        print("没有匹配的 skipped 行，checkpoint 未改动")
        return

    print(f"已重置 {len(summary.reset_entries)} 条 skipped 行为 retrying（attempts 清零）：")
    for chat_id, msg_id in summary.reset_entries:
        print(f"  chat={chat_id} msg_id={msg_id}")

    if summary.rollback:
        for chat_id, (old_cp, new_cp) in summary.rollback.items():
            print(f"checkpoint 回退 chat={chat_id}: {old_cp} → {new_cp}")
    else:
        print("checkpoint 未回退（目标不小于当前值，或频道行不存在）")

    print("\n完成。请 docker compose up -d 让下轮扫描重扫。")


def _preview(db: ArchiveDB, targets):
    """--dry-run：打印将重置哪些行、checkpoint 将回退到哪，不写库。"""
    # retry_skipped 是唯一的选择逻辑真相，但它会写库（重置行 + 回退 checkpoint）。
    # 预览这里复刻它的 SELECT 语义：
    # 直接查库里的 skipped 行，避免选择逻辑出现第二份实现。
    import sqlite3

    con = sqlite3.connect(db._path)
    con.row_factory = sqlite3.Row
    try:
        if targets is None:
            rows = con.execute(
                "SELECT source_chat_id, source_message_id FROM archive_failures "
                "WHERE status='skipped'").fetchall()
        else:
            placeholders = ",".join("?" * len(targets))
            rows = con.execute(
                f"SELECT source_chat_id, source_message_id FROM archive_failures "
                f"WHERE status='skipped' AND source_message_id IN ({placeholders})",
                targets).fetchall()
    finally:
        con.close()

    if not rows:
        print("没有匹配的 skipped 行，--dry-run 结束")
        return

    entries = sorted((str(r["source_chat_id"]), r["source_message_id"]) for r in rows)
    print(f"--dry-run：将重置 {len(entries)} 条 skipped 行为 retrying：")
    for chat_id, msg_id in entries:
        print(f"  chat={chat_id} msg_id={msg_id}")

    chat_min: dict[str, int] = {}
    for chat_id, msg_id in entries:
        chat_min[chat_id] = min(chat_min.get(chat_id, msg_id), msg_id)
    print("checkpoint 将回退到：")
    for chat_id, min_id in chat_min.items():
        new_cp = min_id - 1
        old_cp = db.get_checkpoint(chat_id)
        if new_cp < old_cp:
            print(f"  chat={chat_id}: {old_cp} → {new_cp}")
        else:
            print(f"  chat={chat_id}: 不回退（{new_cp} >= 当前 {old_cp}）")


if __name__ == "__main__":
    main()
