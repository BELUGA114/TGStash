"""
路径二：重点（含禁止转发）频道 -> tdl 批量导出/下载 -> 去重 -> tdl up 到私有备份频道

跟路径一是完全独立的 checkpoint 和独立的 tdl session，用时间窗口而不是消息 id
做增量：每个频道记录 last_run_at，每轮处理 [last_run_at, 现在] 这个区间，
成功后把 last_run_at 推进到这一轮的结束时间。

首次跑某个频道时（last_run_at 为空）只回溯 INITIAL_BACKFILL_DAYS 天，
不会從频道第一条消息开始全量导出。

已知的简化 / 后续可以增强的点：
  - 这条路径没有拿到 Telegram 的 file_unique_id（tdl 的导出 JSON 没有解析），
    去重完全靠下载后的 SHA-256，够用但比路径一的双层去重弱一点
  - caption / 发送者这些元数据这一版没有回填（tdl 的导出 JSON 结构没有解析），
    messages 表里这条路径的记录 caption 是空的，全文搜索搜不到这部分内容，
    只能按文件名/来源频道定位；想要更完整的元数据可以在这基础上加一个
    用 Kurigram 读 get_messages(chat, message_id) 回填的步骤
"""

import hashlib
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from db import ArchiveDB

ARCHIVE_CHAT = os.environ["ARCHIVE_CHAT_ID"]
PRIORITY_CHANNELS = [c.strip() for c in os.environ.get("PRIORITY_CHANNELS", "").split(",") if c.strip()]
SCAN_INTERVAL_SECONDS = int(os.environ.get("TDL_SCAN_INTERVAL_HOURS", "6")) * 3600
INITIAL_BACKFILL_DAYS = int(os.environ.get("INITIAL_BACKFILL_DAYS", "30"))
TDL_NAMESPACE = os.environ.get("TDL_NAMESPACE", "archiver")

DB_PATH = "/data/db/archive.db"
WORK_DIR = "/data/tmp/tdl"

os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

db = ArchiveDB(DB_PATH)


def run_tdl(*args):
    cmd = ["tdl", "-n", TDL_NAMESPACE, *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"命令 {' '.join(cmd)} 失败：{result.stderr.strip()[-2000:]}")
    return result.stdout


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def process_channel(chat: str):
    last_run_iso = db.get_last_run(chat)
    if last_run_iso is None:
        start_ts = int(time.time()) - INITIAL_BACKFILL_DAYS * 86400
    else:
        start_ts = int(datetime.fromisoformat(last_run_iso).timestamp())
    end_ts = int(time.time())

    # 时间窗口太短时跳过：tdl 的时间过滤在窗口边界附近可能漏消息，
    # 留 60 秒最小间隔保证每次导出有足够的覆盖
    if end_ts - start_ts < 60:
        return

    chat_workdir = os.path.join(WORK_DIR, chat.replace("/", "_").replace("@", ""))
    download_dir = os.path.join(chat_workdir, "downloads")
    export_path = os.path.join(chat_workdir, "export.json")
    os.makedirs(download_dir, exist_ok=True)

    print(f"[{chat}] 导出 {start_ts} -> {end_ts}（{(end_ts - start_ts) // 3600} 小时）")
    run_tdl(
        "chat", "export", "-c", chat, "-T", "time",
        "-i", f"{start_ts},{end_ts}", "-o", export_path,
    )

    print(f"[{chat}] 下载媒体")
    run_tdl(
        "dl", "-f", export_path, "-d", download_dir,
        "--skip-same", "--continue",
        "--template", "{{.MessageID}}_{{.FileName}}",
    )

    new_count, dup_count = 0, 0
    for fname in sorted(os.listdir(download_dir)):
        fpath = os.path.join(download_dir, fname)
        if not os.path.isfile(fpath):
            continue

        # 文件命名模板是 {{.MessageID}}_{{.FileName}}，
        # 用第一个下划线分割就能取出 message_id
        message_id = fname.split("_", 1)[0]
        sha256 = sha256_of_file(fpath)
        size = os.path.getsize(fpath)

        if db.find_by_sha256(sha256):
            dup_count += 1
            os.remove(fpath)
            continue

        print(f"[{chat}] 上传新文件 {fname}")
        run_tdl("up", "-p", fpath, "-c", ARCHIVE_CHAT)

        # 这条路径拿不到真正的 Telegram file_unique_id，用 chat+message_id+哈希
        # 拼一个稳定且唯一的替代 key，跟路径一的真实 file_unique_id 共用同一张去重表
        synthetic_id = f"tdl:{chat}:{message_id}:{sha256[:16]}"

        db.record_file(
            file_unique_id=synthetic_id,
            sha256=sha256,
            size=size,
            archived_chat_id=ARCHIVE_CHAT,
            archived_message_id=None,
            source="tdl_bulk",
            source_channel=chat,
        )
        db.record_message(
            source_chat_id=chat,
            source_message_id=int(message_id) if message_id.isdigit() else None,
            source_channel_title=chat,
            caption=None,
            file_unique_id=synthetic_id,
            archived_chat_id=ARCHIVE_CHAT,
        )
        new_count += 1
        os.remove(fpath)

    db.set_last_run(chat, datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat())
    print(f"[{chat}] 完成：新增 {new_count}，重复跳过 {dup_count}")


def main():
    for chat in PRIORITY_CHANNELS:
        db.ensure_channel(chat, "tdl_bulk")

    print(f"[tdl-sync] 监控频道：{PRIORITY_CHANNELS}，每 {SCAN_INTERVAL_SECONDS}s 跑一轮")
    while True:
        for chat in PRIORITY_CHANNELS:
            try:
                process_channel(chat)
            except Exception as e:
                print(f"[{chat}] 出错，本轮跳过，下轮重试：{e}", file=sys.stderr)
        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    if not PRIORITY_CHANNELS:
        print("未配置 PRIORITY_CHANNELS，退出", file=sys.stderr)
        sys.exit(1)
    main()
