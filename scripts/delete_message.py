"""
按 source_message_id 删除数据库中与该消息相关的所有记录，并回退 checkpoint
使项目下次扫描时重新备份该消息。

source_message_id 是**入口** id，即接收频道里那条消息。真实来源在 origin_* 列，
不要拿它来回退 checkpoint。

注意历史数据：两条路径一致地写入口 id，是从「入口与来源分离」那次修复起才成立的。
更早的路径二（t.me 链接）行里，source_message_id 存的是**源频道**的消息 id，而
source_chat_id 仍是接收频道——拿这种行回退 checkpoint 会把它打到毫不相干的位置。
回填脚本靠 file_unique_id 自证匹配主动跳过了这些行，不会修正它们，所以它们的
source_message_id 依旧不可信。本脚本对跨度过大的回退会拦下来，要 --force 才执行。

删除本身走 db.purge_messages：messages + 孤儿 files + 失败账 + checkpoint 回退
在一个事务里，中途失败整笔回滚（两段式各有静默坏窗口，见 db.py 的文档）。

用法：
    # 单个 ID
    python scripts/delete_message.py 12345 --db data/db/archive.db
    # 多个 ID
    python scripts/delete_message.py 12345 12346 12347 --db data/db/archive.db
    # 先预览
    python scripts/delete_message.py 12345 12346 --db data/db/archive.db --dry-run

会删除 / 回退（一个事务）：
  - messages 表中匹配 source_message_id 的行（FTS 索引由触发器自动清理）
  - files 表中对应的行（仅当没有其他消息引用同一个 file_unique_id 时）
  - archive_failures 中这些入口的失败账（残留的 attempt_count 会让重来的那次一失败就跳过）
  - channels 表中对应 source_chat_id 的 checkpoint，回退到 min(source_message_id) - 1
"""

import argparse
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "stash-listener"))
from db import ArchiveDB

# 回退跨度超过这个值就拦下来要 --force。挑 100 的依据：默认 BATCH_SIZE=10、
# SCAN_INTERVAL_SECONDS=300，100 条要重扫约 50 分钟，再大就该先确认是不是选错了行
ROLLBACK_WARN_SPAN = 100


def rollback_span(old_cp: int, new_cp: int) -> int:
    """checkpoint 实际会往回走多少条。不回退（new_cp >= old_cp）记 0。"""
    return max(0, old_cp - new_cp)


def too_far_back(old_cp: int, new_cp: int, threshold: int = ROLLBACK_WARN_SPAN) -> bool:
    """回退跨度是否大到值得人工确认。"""
    return rollback_span(old_cp, new_cp) > threshold


def _chat_min_message_id(rows: list[sqlite3.Row]) -> dict[str, int]:
    """按入口 chat_id 分组，找各频道最小的 message_id（回退预览与跨度守卫用）。"""
    chat_min: dict[str, int] = {}
    for row in rows:
        if row["source_chat_id"]:
            chat_id = str(row["source_chat_id"])
            chat_min[chat_id] = min(chat_min.get(chat_id, row["source_message_id"]),
                                    row["source_message_id"])
    return chat_min


def main():
    parser = argparse.ArgumentParser(description="按 source_message_id 删除数据库记录并回退 checkpoint")
    parser.add_argument("msg_ids", type=int, nargs="+", help="要删除的 source_message_id（可多个）")
    parser.add_argument("--db", default="/data/db/archive.db", help="archive.db 路径（默认 /data/db/archive.db）")
    parser.add_argument("--dry-run", action="store_true", help="只查看匹配的记录，不实际删除")
    parser.add_argument("--force", action="store_true",
                        help=f"允许 checkpoint 回退超过 {ROLLBACK_WARN_SPAN} 条（默认拦下）")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"数据库文件不存在：{args.db}")
        sys.exit(1)

    db = ArchiveDB(args.db)
    rows = db.find_messages_by_source_ids(args.msg_ids)

    if not rows:
        print(f"没找到 source_message_id in {args.msg_ids} 的记录")
        return

    found_ids = {row["source_message_id"] for row in rows}
    missing = [mid for mid in args.msg_ids if mid not in found_ids]
    print(f"找到 {len(rows)} 条记录（{len(found_ids)} 个 message_id）")
    if missing:
        print(f"未找到：{missing}")
    for row in rows:
        caption_preview = (row["caption"] or "")[:60].replace("\n", " ")
        print(f"  DB id={row['id']}  msg_id={row['source_message_id']}  "
              f"chat={row['source_chat_id']}  file_unique_id={row['file_unique_id']}  "
              f"sender={row['sender']}  sent={row['sent_at']}  caption={caption_preview}")

    # 先算回退跨度：跨度离谱通常意味着选中了 source_message_id 不可信的历史行。
    # 预览数字可能与实删时有毫秒级出入（主服务在推进 checkpoint），purge 在事务内
    # 自己算回退值，正确性不受影响
    chat_min_msg = _chat_min_message_id(rows)
    far_back = []
    for chat_id, min_id in chat_min_msg.items():
        old_cp = db.get_checkpoint(chat_id)
        new_cp = min_id - 1
        if too_far_back(old_cp, new_cp):
            far_back.append((chat_id, old_cp, new_cp, rollback_span(old_cp, new_cp)))

    if args.dry_run:
        print("\n--dry-run，不执行删除")
        entry_keys = sorted({(str(row["source_chat_id"]), row["source_message_id"])
                             for row in rows if row["source_chat_id"]})
        pending_keys = [k for k in entry_keys if db.get_failure(*k) is not None]
        if pending_keys:
            print(f"将清除 {len(pending_keys)} 条失败账：{pending_keys}")
        if chat_min_msg:
            print("checkpoint 将回退到：")
            for chat_id, min_id in chat_min_msg.items():
                old_cp = db.get_checkpoint(chat_id)
                new_cp = min_id - 1
                print(f"  chat={chat_id}: {old_cp} → {new_cp}"
                      f"（回退 {rollback_span(old_cp, new_cp)} 条）")
        for chat_id, old_cp, new_cp, span in far_back:
            print(f"  警告 chat={chat_id} 回退跨度 {span} 条，超过 {ROLLBACK_WARN_SPAN}，"
                  f"实际执行需要 --force")
        return

    if far_back and not args.force:
        print()
        for chat_id, old_cp, new_cp, span in far_back:
            print(f"拒绝执行：chat={chat_id} 的 checkpoint 会从 {old_cp} 回退到 {new_cp}，"
                  f"跨 {span} 条（上限 {ROLLBACK_WARN_SPAN}）")
        print("跨度这么大通常是选中了 source_message_id 不可信的历史行，见模块说明。")
        print("先用 --dry-run 核对，确认无误再加 --force。")
        sys.exit(1)

    summary = db.purge_messages(args.msg_ids)

    print(f"\n已删除 messages: {summary.deleted_messages} 条")
    for fuid in summary.deleted_files:
        print(f"  同时删除 files: {fuid}（无其他消息引用）")
    for chat_id, msg_id in summary.cleared_failures:
        print(f"  同时清除失败账: chat={chat_id} msg_id={msg_id}")
    print()
    for chat_id, (old_cp, new_cp) in summary.rollback.items():
        print(f"checkpoint 回退 chat={chat_id}: {old_cp} → {new_cp}")

    print(f"\n完成：删除 {summary.deleted_messages} 条消息记录，"
          f"{len(summary.deleted_files)} 条文件记录")


if __name__ == "__main__":
    main()
