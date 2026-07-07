"""
两条路径共享同一个接收频道，每 SCAN_INTERVAL_SECONDS 扫描一次：

路径一（转发媒体）：
  1. min_id + reverse=True 拉取 checkpoint 之后的新消息（旧到新）
  2. media_group 整组处理，单条单独处理
  3. file_unique_id 判重（快速通道）→ 下载 → SHA-256 判重（精确通道）
  4. 新文件上传备份频道 + 写 DB，重复文件跳过上传只记录
  5. 原消息 caption 加 "✅ 已归档" 标记
  6. 每处理完一条推进 checkpoint，异常中断下次从成功处继续

路径二（转发 t.me 链接）：
  1. 检测文本消息中的 t.me 链接
  2. parse_message_link() 解析出 chat + message_id
  3. Pyrogram get_messages() 获取消息（你是成员，可直接下载）
  4. 媒体组 → get_media_group() 整组处理；单条 → archive_single()
  5. 复用路径一的 file_unique_id/SHA-256 双层去重 → 上传 → 写 DB
  6. 原消息编辑为 "✅ 已归档"
"""

import asyncio
import hashlib
import os
import re
import sys
import time
import traceback
from urllib.parse import urlparse

from PIL import Image

from pyrogram.client import Client
from pyrogram.errors import PhotoExtInvalid
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
RECEIVE_CHAT = int(os.environ["RECEIVE_CHAT_ID"])
ARCHIVE_CHAT = int(os.environ["ARCHIVE_CHAT_ID"])
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
# 每轮最多处理的消息数。宁可慢不可冒险——账号比速度重要
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
# 每次上传文件后等待的秒数，降低 Telegram 服务端感知频率
UPLOAD_COOLDOWN_SECONDS = int(os.environ.get("UPLOAD_COOLDOWN_SECONDS", "5"))
HTTP_PROXY = os.environ.get("HTTP_PROXY", "")

SESSION_DIR = "/data/session"
DB_PATH = "/data/db/archive.db"
DOWNLOAD_DIR = "/data/tmp/listener"

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

db = ArchiveDB(DB_PATH)

if HTTP_PROXY:
    u = urlparse(HTTP_PROXY)
    app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR,
                 proxy=dict(scheme=u.scheme, hostname=u.hostname, port=u.port))
else:
    app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR)
# 并发下载数上限，MTProto 单连接慢，2 个并行可有效提速
_dl_sem = asyncio.Semaphore(2)

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


def _fix_media_format(path: str, kind: str | None) -> str:
    """下载后的文件格式可能与 Telegram 声称的类型不匹配。

    - WebP/PNG/GIF → 转 JPEG，保证 Telegram 内联展示
    - 无后缀文件 → 检测格式后补上后缀
    - 非 photo 类型 → 原样返回"""
    if kind != "photo":
        return path
    try:
        with Image.open(path) as img:
            fmt = img.format  # 'JPEG', 'WEBP', 'PNG', 'GIF', etc.
            ext = os.path.splitext(path)[1]

            # 非 JPEG 格式统一转为 JPEG，Telegram 才能内联显示
            if fmt in ("WEBP", "PNG", "GIF"):
                new_path = path + ".jpg"
                if img.mode in ("RGBA", "P", "PA"):
                    img = img.convert("RGB")
                img.save(new_path, "JPEG", quality=95)
                os.remove(path)
                print(f"  └─ 格式转换 {fmt} → JPEG")
                return new_path

            # 本身就是 JPEG 但文件没有后缀，补上
            if fmt == "JPEG" and not ext:
                new_path = path + ".jpg"
                os.rename(path, new_path)
                print(f"  └─ 补后缀 {fmt}")
                return new_path

            # 其他图片格式但无后缀，统一加 .jpg
            if not ext:
                new_path = path + ".jpg"
                os.rename(path, new_path)
                print(f"  └─ 补后缀 {fmt or '未知'}")
                return new_path
    except Exception:
        pass
    return path


def sender_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or str(message.from_user.id)
    if message.sender_chat:
        return message.sender_chat.title or str(message.sender_chat.id)
    return ""


def parse_message_link(link: str) -> tuple[str, int]:
    """解析 t.me 链接，返回 (chat_identifier, message_id)

    https://t.me/username/123    → ("@username", 123)
    https://t.me/c/123456/123    → ("-100123456", 123)
    """
    link = link.strip().split("?")[0].rstrip("/")
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)$", link)
    if m:
        return (f"-100{m.group(1)}", int(m.group(2)))
    m = re.match(r"https?://t\.me/([^/]+)/(\d+)$", link)
    if m:
        return (f"@{m.group(1)}", int(m.group(2)))
    raise ValueError(f"无法解析链接：{link}")


async def mark_processed(message: Message, duplicate: bool):
    """回复原消息标记处理状态（转发的消息无法编辑，用回复形式）"""
    chat = message.chat
    if chat is None:
        return
    assert chat.id is not None
    text = "✅ 已归档（重复）" if duplicate else "✅ 已归档"
    try:
        await app.send_message(chat.id, text, reply_to_message_id=message.id)
    except Exception:
        pass


async def archive_single(message: Message, *, mark: bool = True) -> bool:
    """处理单条媒体消息。返回 True 表示成功（含跳过重复），False 表示需下轮重试。"""
    kind, media = get_media(message)
    if not media:
        return True

    file_unique_id = media.file_unique_id

    if db.find_by_unique_id(file_unique_id):
        if mark:
            await mark_processed(message, duplicate=True)
        print(f"  └─ 跳过重复 {message.id} ({kind})")
        return True

    # 每条消息下载到独立子目录，文件名保持原名（上传时不会带 num_ 前缀）
    msg_dir = os.path.join(DOWNLOAD_DIR, str(message.id))
    os.makedirs(msg_dir, exist_ok=True)
    orig_name = getattr(media, "file_name", None)
    dl_name = orig_name or f"{message.id}_"
    try:
        local_path = await app.download_media(message=message, file_name=os.path.join(msg_dir, dl_name))  # type: ignore[call-overload]
    except Exception:
        # 下载失败不推进 checkpoint，下轮重试
        print(f"[warn] 下载 {message.id} 失败，下轮重试", file=sys.stderr)
        return False
    try:
        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)

        # 文件格式转换（如 WebP→JPEG），让 Telegram 可以内联展示
        local_path = _fix_media_format(local_path, kind)

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
            if mark:
                await mark_processed(message, duplicate=True)
            return True

        caption = message.caption or ""
        if kind is None:
            return True
        # 视频需要显式传入时长和分辨率，否则 Telegram 可能无法生成缩略图、时长显示 00:00
        if kind == "video":
            sent = await app.send_video(
                ARCHIVE_CHAT, local_path,
                duration=media.duration,
                width=media.width,
                height=media.height,
                caption=caption,
            )
        else:
            send = getattr(app, SEND_METHOD[kind])
            try:
                sent = await send(ARCHIVE_CHAT, local_path, caption=caption)
            except PhotoExtInvalid:
                # WebP 等格式 Pyrogram 归为 photo，但 Telegram 拒绝以 photo 重传
                sent = await app.send_document(ARCHIVE_CHAT, local_path, caption=caption)
        assert sent is not None and sent.id is not None

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
        if mark:
            await mark_processed(message, duplicate=False)
        print(f"  └─ 归档 {message.id} ({kind})")
        await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            # 清理空子目录
            try:
                os.rmdir(os.path.dirname(local_path))
            except OSError:
                pass

    return True


async def archive_group(messages: list[Message], *, mark: bool = True):
    """媒体组：并行下载（最多 2 个）→ 顺序处理 → 打包成 send_media_group 上传"""
    to_upload: list[tuple] = []
    dup_messages = []
    new_messages = []
    local_paths = []

    # 阶段一：准备下载任务，跳过重复
    downloads: list[tuple] = []
    for message in messages:
        kind, media = get_media(message)
        if not media or kind is None:
            continue
        if db.find_by_unique_id(media.file_unique_id):
            dup_messages.append(message)
            continue

        msg_dir = os.path.join(DOWNLOAD_DIR, str(message.id))
        os.makedirs(msg_dir, exist_ok=True)
        orig_name = getattr(media, "file_name", None)
        dl_name = orig_name or f"{message.id}_"
        downloads.append((message, kind, msg_dir, dl_name, media))

    # 阶段二：并行下载（Semaphore 限流，最多 2 个同时）
    async def _dl_one(msg, msg_dir, dl_name):
        async with _dl_sem:
            return await app.download_media(message=msg, file_name=os.path.join(msg_dir, dl_name))  # type: ignore[call-overload]

    results = await asyncio.gather(
        *[_dl_one(msg, md, dn) for msg, _, md, dn, _ in downloads],
        return_exceptions=True,
    )

    # 阶段三：顺序处理（SHA-256 / 去重 / 格式转换）
    for (message, kind, msg_dir, dl_name, media), result in zip(downloads, results):
        if isinstance(result, BaseException):
            print(f"[warn] 下载 {message.id} 失败，跳过", file=sys.stderr)
            continue

        local_path = result
        local_paths.append(local_path)

        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)

        local_path = _fix_media_format(local_path, kind)
        local_paths[-1] = local_path

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

    # 阶段四：上传 + 清理
    try:
        if to_upload:
            try:
                input_media = [
                    INPUT_MEDIA_CLASS[kind](path, caption=(m.caption or "") if i == 0 else "")
                    for i, (kind, _, _, _, path, m) in enumerate(to_upload)
                ]
                sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)
            except PhotoExtInvalid:
                # WebP 等格式不能作为 photo 编组，回退到全部作为 document 的媒体组
                input_media = [
                    InputMediaDocument(path, caption=(m.caption or "") if i == 0 else "")
                    for i, (_, _, _, _, path, m) in enumerate(to_upload)
                ]
                sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)  # type: ignore[arg-type]

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

        if to_upload:
            await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
            print(f"  └─ 归档媒体组 {len(to_upload)} 张")
        for message in new_messages:
            await archive_single(message, mark=mark)
        if mark:
            for item in to_upload:
                await mark_processed(item[5], duplicate=False)  # item[5] 是 Message 对象
            for message in dup_messages:
                await mark_processed(message, duplicate=True)

    finally:
        for p in local_paths:
            if os.path.exists(p):
                os.remove(p)
                try:
                    os.rmdir(os.path.dirname(p))
                except OSError:
                    pass


async def process_link_message(message: Message):
    """处理包含 t.me 链接的文本消息：Pyrogram 直接获取消息 → 复用路径一的去重+上传管道

    你是频道成员，Pyrogram 可以直接下载（不能"转发"但能"读取+下载"）。
    支持媒体组——检测到 media_group_id 后拉取整组，每张照片的 caption 一并保留。
    返回实际归档的文件数。"""
    text = message.text or ""
    raw_links = re.findall(r"https?://t\.me/\S+", text)
    seen = set()
    links: list[str] = []
    for link in raw_links:
        link = link.rstrip(".,;:!?)")
        if link not in seen:
            seen.add(link)
            links.append(link)

    archived = 0
    for link in links:
        try:
            chat, msg_id = parse_message_link(link)
        except ValueError:
            continue

        try:
            msg = await app.get_messages(chat, msg_id)
            if msg is None:
                print(f"[warn] 消息 {link} 不可访问或已删除", file=sys.stderr)
                continue

            # 媒体组：拉整组，复用 archive_group()
            if msg.media_group_id:
                group = await app.get_media_group(chat, msg_id)
                group = sorted(group, key=lambda m: m.id)
                # mark=False：不编辑源频道消息，最终只标记接收频道里的链接消息
                await archive_group(group, mark=False)
                archived += len(group)
                print(f"  └─ 链接 {link} → 媒体组 {len(group)} 张")
            else:
                kind, _ = get_media(msg)
                if kind:
                    if await archive_single(msg, mark=False):
                        archived += 1
                        print(f"  └─ 链接 {link} → {kind}")
                    else:
                        print(f"  └─ 链接 {link} → 下载失败")
                else:
                    print(f"  └─ 链接 {link} → 无媒体")
        except Exception:
            print(f"[warn] 处理链接 {link} 失败", file=sys.stderr)
            traceback.print_exc()

    # 只有确实归档了文件才打标记
    if archived > 0:
        try:
            chat = message.chat
            if chat is not None and chat.id is not None:
                await app.edit_message_text(chat.id, message.id,
                    f"✅ 已归档\n{text}"[:4096])
        except Exception:
            pass

    return archived


def _has_tme_link(message: Message) -> bool:
    """检查消息文本是否包含 t.me 链接"""
    text = message.text or message.caption or ""
    return bool(re.search(r"https?://t\.me/", text))


async def scan_once():
    last_id = db.get_checkpoint(RECEIVE_CHAT)
    new_messages = []
    # reverse=True 让消息从旧到新排列——配合 min_id checkpoint 机制，
    # 每处理完一条就推进 checkpoint，中途崩溃可以从最后成功的那条继续
    async for msg in app.get_chat_history(RECEIVE_CHAT, min_id=last_id, reverse=True):
        new_messages.append(msg)

    if not new_messages:
        return 0

    total = len(new_messages)
    # 限制每轮处理量，剩余留给下轮，避免短时间大量上传触发 Telegram 风控
    if total > BATCH_SIZE:
        new_messages = new_messages[:BATCH_SIZE]
        print(f"[listener] 待处理 {total} 条，本轮处理 {BATCH_SIZE} 条，剩余 {total - BATCH_SIZE} 条下轮继续")

    handled_groups = set()
    processed = 0
    for msg in new_messages:
        if msg.media_group_id:
            if msg.media_group_id in handled_groups:
                continue
            group = await app.get_media_group(msg.chat.id, msg.id)
            group = sorted(group, key=lambda m: m.id)
            await archive_group(group)
            handled_groups.add(msg.media_group_id)
            db.set_checkpoint(RECEIVE_CHAT, max(m.id for m in group))
            processed += len(group)
            continue

        kind, _ = get_media(msg)
        if kind:
            if await archive_single(msg):
                processed += 1
            else:
                # 下载失败，不推进 checkpoint，下轮重试
                continue
        elif _has_tme_link(msg):
            n = await process_link_message(msg)
            if n > 0:
                processed += 1
        # 非媒体且无链接的消息不归档，但依然推进 checkpoint，避免反复扫描
        db.set_checkpoint(RECEIVE_CHAT, msg.id)

    if processed:
        print(f"[listener] 本轮完成：处理 {processed} 条消息")
    return processed


async def main():
    async with app:
        db.ensure_channel(RECEIVE_CHAT, "manual_forward")
        me = await app.get_me()
        print(f"[listener] 已登录：{me.first_name} (id={me.id})，冷却间隔 {SCAN_INTERVAL_SECONDS}s")
        last_processed_at = 0.0

        while True:
            try:
                elapsed = time.time() - last_processed_at
                if elapsed < SCAN_INTERVAL_SECONDS:
                    wait = SCAN_INTERVAL_SECONDS - elapsed
                    print(f"[listener] 冷却中，{wait:.0f}s 后扫描")
                    await asyncio.sleep(wait)

                n = await scan_once()
                if n > 0:
                    last_processed_at = time.time()
                else:
                    print(f"[listener] 无新消息，{SCAN_INTERVAL_SECONDS}s 后再查")
                    await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            except Exception:
                print("[listener] 本轮扫描出错：", file=sys.stderr)
                traceback.print_exc()
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
