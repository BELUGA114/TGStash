"""
路径二：手动备份禁止转发的消息

用法：python archiver.py <消息链接>

支持两种 t.me 链接格式：
  https://t.me/<username>/<msg_id>      公开用户名
  https://t.me/c/<channel_id>/<msg_id>  私有频道（需已加入）

流程：解析链接 → tdl 导出单条消息 → 下载媒体 → SHA-256 去重 → 上传备份频道 → 写 DB
"""

import hashlib
import os
import re
import subprocess
import sys

from db import ArchiveDB

ARCHIVE_CHAT = os.environ["ARCHIVE_CHAT_ID"]
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


def parse_message_link(link: str) -> tuple[str, int]:
    """解析 t.me 链接，返回 (chat_identifier, message_id)

    https://t.me/username/123    → ("@username", 123)
    https://t.me/c/123456/123    → ("-100123456", 123)
    """
    link = link.strip().split("?")[0].rstrip("/")

    # 私有频道：t.me/c/<channel_id>/<msg_id>
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)$", link)
    if m:
        return (f"-100{m.group(1)}", int(m.group(2)))

    # 公开用户名：t.me/<username>/<msg_id>
    m = re.match(r"https?://t\.me/([^/]+)/(\d+)$", link)
    if m:
        return (f"@{m.group(1)}", int(m.group(2)))

    raise ValueError(f"无法解析链接：{link}")


def main():
    if len(sys.argv) < 2:
        print("用法：python archiver.py <消息链接>")
        sys.exit(1)

    link = sys.argv[1]
    chat, message_id = parse_message_link(link)

    db.ensure_channel(chat, "tdl_bulk")

    chat_slug = chat.replace("/", "_").replace("@", "")
    export_path = os.path.join(WORK_DIR, f"{chat_slug}_{message_id}.json")
    download_dir = os.path.join(WORK_DIR, chat_slug, str(message_id))
    os.makedirs(download_dir, exist_ok=True)

    # 按消息 ID 导出单条消息的 JSON
    print(f"导出 {chat}/{message_id}")
    run_tdl(
        "chat", "export", "-c", chat, "-T", "id",
        "-i", str(message_id), "-o", export_path,
    )

    # 下载导出结果中的媒体文件
    print("下载媒体")
    run_tdl(
        "dl", "-f", export_path, "-d", download_dir,
        "--skip-same", "--continue",
        "--template", "{{.MessageID}}_{{.FileName}}",
    )

    # 去重 → 上传 → 记录
    new_count, dup_count = 0, 0
    for fname in sorted(os.listdir(download_dir)):
        fpath = os.path.join(download_dir, fname)
        if not os.path.isfile(fpath):
            continue

        sha256 = sha256_of_file(fpath)
        size = os.path.getsize(fpath)

        if db.find_by_sha256(sha256):
            dup_count += 1
            os.remove(fpath)
            continue

        print(f"上传 {fname}")
        run_tdl("up", "-p", fpath, "-c", ARCHIVE_CHAT)

        # tdl 拿不到真正的 Telegram file_unique_id，用 chat+message_id+哈希
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
            source_message_id=message_id,
            source_channel_title=chat,
            caption=None,
            file_unique_id=synthetic_id,
            archived_chat_id=ARCHIVE_CHAT,
        )
        new_count += 1
        os.remove(fpath)

    if new_count:
        print(f"完成：新增 {new_count} 个文件")
    elif dup_count:
        print(f"已完成：{dup_count} 个文件已存在，跳过")
    else:
        print("这条消息没有可下载的媒体文件")


if __name__ == "__main__":
    main()
