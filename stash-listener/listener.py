"""
路径一：手动转发 -> 接收频道 -> 定时批量脚本 -> 私有备份频道

每 SCAN_INTERVAL_SECONDS 跑一次：
  1. 用 min_id + reverse=True 拉取接收频道里 checkpoint 之后的新消息（旧到新）
  2. media_group 的消息整组一起处理，单条消息单独处理
  3. 先查 file_unique_id，命中直接判重；没命中就下载后算 SHA-256 再查一次
  4. 新文件：上传到私有备份频道，写 files + messages 两张表
     重复文件：跳过上传，只记录
  5. 不论新旧，原消息 caption 前面都加 "✅ 已归档" 或 "✅ 已归档（重复）" 标记
  6. 每处理完一条就把 checkpoint 推进到这条消息的 id，异常时中断，
     下次从上一个成功的 checkpoint 继续，不会漏也不会重复归档
"""

import asyncio
import hashlib
import os
import sys
import traceback

from pyrogram.client import Client
from pyrogram.types import (
    Message,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
)

from db import ArchiveDB

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
RECEIVE_CHAT = os.environ["RECEIVE_CHAT_ID"]
ARCHIVE_CHAT = os.environ["ARCHIVE_CHAT_ID"]
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))

SESSION_DIR = "/data/session"
DB_PATH = "/data/db/archive.db"
DOWNLOAD_DIR = "/data/tmp/listener"

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

db = ArchiveDB(DB_PATH)
app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR)

MEDIA_ATTRS = ("document", "video", "photo", "audio", "animation", "voice", "video_note")

# 各媒体类型对应的发送方法名 / 媒体组 InputMedia 类
SEND_METHOD = {
    "document": "send_document",
    "video": "send_video",
    "photo": "send_photo",
    "audio": "send_audio",
    "animation": "send_animation",
    "voice": "send_voice",
    "video_note": "send_video_note",
}
# send_media_group 只支持这四种类型，voice/video_note/animation 不能编组
INPUT_MEDIA_CLASS = {
    "document": InputMediaDocument,
    "video": InputMediaVideo,
    "photo": InputMediaPhoto,
    "audio": InputMediaAudio,
}


def get_media(message: Message):
    """返回 (媒体类型, 媒体对象)，都没有就返回 (None, None)"""
    for attr in MEDIA_ATTRS:
        obj = getattr(message, attr, None)
        if obj:
            return attr, obj
    return None, None


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sender_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or str(message.from_user.id)
    if message.sender_chat:
        return message.sender_chat.title or str(message.sender_chat.id)
    return ""


async def mark_processed(message: Message, duplicate: bool):
    """把原消息 caption 前面加处理标记，媒体本身不动"""
    chat = message.chat
    if chat is None:
        return
    assert chat.id is not None  # 频道消息一定有 id
    prefix = "✅ 已归档（重复，未重新上传）" if duplicate else "✅ 已归档"
    original = message.caption or ""
    new_caption = f"{prefix}\n{original}" if original else prefix
    try:
        await app.edit_message_caption(chat.id, message.id, new_caption[:1024])  # Telegram caption 上限 1024 字符
    except Exception as e:
        print(f"[warn] 编辑消息 {message.id} 的 caption 失败（可能是纯文本消息或权限问题）：{e}")


async def archive_single(message: Message):
    kind, media = get_media(message)
    if not media:
        return

    file_unique_id = media.file_unique_id

    if db.find_by_unique_id(file_unique_id):
        await mark_processed(message, duplicate=True)
        return

    # file_name 末尾的下划线是故意的：Pyrogram 会自动补扩展名，
    # 用 message.id 前缀 + 下划线可以避免它猜错扩展名
    local_path = await app.download_media(message=message, file_name=os.path.join(DOWNLOAD_DIR, f"{message.id}_"))  # type: ignore[call-overload]
    try:
        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)

        dup = db.find_by_sha256(sha256)
        if dup:
            db.record_file(
                file_unique_id=file_unique_id,
                sha256=sha256,
                size=size,
                archived_chat_id=dup["archived_chat_id"],
                archived_message_id=dup["archived_message_id"],
                source="manual_forward",
                source_channel=RECEIVE_CHAT,
            )
            await mark_processed(message, duplicate=True)
            return

        caption = message.caption or ""
        if kind is None:
            return
        send = getattr(app, SEND_METHOD[kind])
        sent = await send(ARCHIVE_CHAT, local_path, caption=caption)

        db.record_file(
            file_unique_id=file_unique_id,
            sha256=sha256,
            size=size,
            archived_chat_id=ARCHIVE_CHAT,
            archived_message_id=sent.id,
            source="manual_forward",
            source_channel=RECEIVE_CHAT,
        )
        db.record_message(
            source_chat_id=RECEIVE_CHAT,
            source_message_id=message.id,
            source_channel_title=message.chat.title if message.chat else None,
            sender=sender_name(message),
            sent_at=message.date.isoformat() if message.date else None,
            caption=caption,
            file_unique_id=file_unique_id,
            media_group_id=message.media_group_id,
            archived_chat_id=ARCHIVE_CHAT,
            archived_message_id=sent.id,
        )
        await mark_processed(message, duplicate=False)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


async def archive_group(messages: list[Message]):
    """媒体组：整组一起下载、按顺序打包成一条 send_media_group 上传，保持相册形态"""
    to_upload: list[tuple] = []
    dup_messages = []
    new_messages = []
    local_paths = []

    try:
        for message in messages:
            kind, media = get_media(message)
            if not media:
                continue
            if db.find_by_unique_id(media.file_unique_id):
                dup_messages.append(message)
                continue

            # file_name 末尾的下划线是故意的：Pyrogram 会自动补扩展名
            local_path = await app.download_media(message=message, file_name=os.path.join(DOWNLOAD_DIR, f"{message.id}_"))  # type: ignore[call-overload]
            local_paths.append(local_path)
            sha256 = sha256_of_file(local_path)
            size = os.path.getsize(local_path)

            dup = db.find_by_sha256(sha256)
            if dup:
                db.record_file(
                    file_unique_id=media.file_unique_id,
                    sha256=sha256,
                    size=size,
                    archived_chat_id=dup["archived_chat_id"],
                    archived_message_id=dup["archived_message_id"],
                    source="manual_forward",
                    source_channel=RECEIVE_CHAT,
                )
                dup_messages.append(message)
                continue

            if kind not in INPUT_MEDIA_CLASS:
                # 语音/视频留言等不支持编组的类型，退回单条处理
                new_messages.append(message)
                continue

            to_upload.append((kind, media.file_unique_id, sha256, size, local_path, message))

        if to_upload:
            input_media = [
                INPUT_MEDIA_CLASS[kind](path, caption=(m.caption or "") if i == 0 else "")
                for i, (kind, _, _, _, path, m) in enumerate(to_upload)
            ]
            sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)
            for (kind, file_unique_id, sha256, size, _, message), sent in zip(to_upload, sent_list):
                db.record_file(
                    file_unique_id=file_unique_id,
                    sha256=sha256,
                    size=size,
                    archived_chat_id=ARCHIVE_CHAT,
                    archived_message_id=sent.id,
                    source="manual_forward",
                    source_channel=RECEIVE_CHAT,
                )
                db.record_message(
                    source_chat_id=RECEIVE_CHAT,
                    source_message_id=message.id,
                    source_channel_title=message.chat.title if message.chat else None,
                    sender=sender_name(message),
                    sent_at=message.date.isoformat() if message.date else None,
                    caption=message.caption or "",
                    file_unique_id=file_unique_id,
                    media_group_id=message.media_group_id,
                    archived_chat_id=ARCHIVE_CHAT,
                    archived_message_id=sent.id,
                )

        for message in new_messages:
            await archive_single(message)
        for item in to_upload:
            await mark_processed(item[5], duplicate=False)  # item[5] 是 Message 对象
        for message in dup_messages:
            await mark_processed(message, duplicate=True)
    finally:
        for p in local_paths:
            if os.path.exists(p):
                os.remove(p)


async def scan_once():
    last_id = db.get_checkpoint(RECEIVE_CHAT)
    new_messages = []
    # reverse=True 让消息从旧到新排列——配合 min_id checkpoint 机制，
    # 每处理完一条就推进 checkpoint，中途崩溃可以从最后成功的那条继续
    async for msg in app.get_chat_history(RECEIVE_CHAT, min_id=last_id, reverse=True):
        new_messages.append(msg)

    if not new_messages:
        return

    handled_groups = set()
    for msg in new_messages:
        if msg.media_group_id:
            if msg.media_group_id in handled_groups:
                db.set_checkpoint(RECEIVE_CHAT, msg.id)
                continue
            group = await app.get_media_group(msg.chat.id, msg.id)
            group = sorted(group, key=lambda m: m.id)
            await archive_group(group)
            handled_groups.add(msg.media_group_id)
            db.set_checkpoint(RECEIVE_CHAT, max(m.id for m in group))
            continue

        kind, _ = get_media(msg)
        if kind:
            await archive_single(msg)
        # 非媒体消息（纯文本等）不归档，但依然推进 checkpoint，避免反复扫描
        db.set_checkpoint(RECEIVE_CHAT, msg.id)


async def main():
    async with app:
        db.ensure_channel(RECEIVE_CHAT, "manual_forward")
        me = await app.get_me()
        print(f"[listener] 已登录：{me.first_name} (id={me.id})，每 {SCAN_INTERVAL_SECONDS}s 扫描一次")
        while True:
            try:
                await scan_once()
            except Exception:
                print("[listener] 本轮扫描出错：", file=sys.stderr)
                traceback.print_exc()
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
