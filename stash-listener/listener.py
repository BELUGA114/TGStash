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
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from typing import NamedTuple
from urllib.parse import urlparse

from archive_entry import ROUTE_FORWARD, ROUTE_LINK, ArchiveItem, Entry, Outcome
from compress_video import maybe_compress_video
from db import ArchiveDB
from media_ops import (
    fix_media_format,
    get_media,
    make_thumbnail,
    probe_video,
    sha256_of_file,
    verify_download_size,
)
from origin import normalize_origin, origin_from_link
from pyrogram.client import Client
from pyrogram.errors import PhotoExtInvalid
from pyrogram.types import (
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
    ReplyParameters,
)
from tdl_downloader import TDLDownloader

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
TDL_NAMESPACE = os.environ.get("TDL_NAMESPACE", "archiver")
TDL_THREADS = int(os.environ.get("TDL_THREADS", "4"))
TDL_LIMIT = int(os.environ.get("TDL_LIMIT", "2"))
TDL_DELAY_SECONDS = int(os.environ.get("TDL_DELAY_SECONDS", "1"))
TDL_TIMEOUT_SECONDS = int(os.environ.get("TDL_TIMEOUT_SECONDS", "0"))
# 同一条消息累计失败多少次后跳过/剔除（满 N 轮告警并推进，避免卡住 checkpoint）
RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "3"))
# 视频压缩：默认关闭；体积超过阈值才转码，压缩产物比原始小才用压缩版
VIDEO_COMPRESS_ENABLED = os.environ.get("VIDEO_COMPRESS_ENABLED", "false").lower() == "true"
VIDEO_COMPRESS_MIN_SIZE_MB = int(os.environ.get("VIDEO_COMPRESS_MIN_SIZE_MB", "100"))
VIDEO_COMPRESS_CRF = int(os.environ.get("VIDEO_COMPRESS_CRF", "28"))

# 容器内默认 /data；测试和本机可用 DATA_DIR 覆盖，避免模块导入期就往根目录建目录
DATA_DIR = os.environ.get("DATA_DIR", "/data")
SESSION_DIR = os.path.join(DATA_DIR, "session")
DB_PATH = os.path.join(DATA_DIR, "db", "archive.db")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "tmp", "listener")

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

db = ArchiveDB(DB_PATH)

if HTTP_PROXY:
    u = urlparse(HTTP_PROXY)
    app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR,
                 max_concurrent_transmissions=2,
                 proxy={"scheme": u.scheme, "hostname": u.hostname, "port": u.port})
else:
    app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR,
                 max_concurrent_transmissions=2)
# 并发下载数上限，MTProto 单连接慢，2 个并行可有效提速
_dl_sem = asyncio.Semaphore(2)

tdl_downloader = TDLDownloader(
    namespace=TDL_NAMESPACE,
    threads=TDL_THREADS,
    limit=TDL_LIMIT,
    delay=TDL_DELAY_SECONDS,
    timeout=TDL_TIMEOUT_SECONDS,
    proxy=HTTP_PROXY,
)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Pyrogram 内部 MTProto 传输日志每个 TCP 包一条，抑制到 WARNING
logging.getLogger("pyrogram").setLevel(logging.WARNING)

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


def sender_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or str(message.from_user.id)
    if message.sender_chat:
        return message.sender_chat.title or str(message.sender_chat.id)
    return ""


def parse_message_link(link: str) -> tuple[str, int]:
    """
    解析 t.me 链接，返回 (chat_identifier, message_id)

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
        await app.send_message(chat.id, text, reply_parameters=ReplyParameters(message_id=message.id))
    except Exception:
        logger.warning("回复归档标记失败（已归档结果不受影响）：%s", message.id, exc_info=True)


async def alert_failure(entry: Entry, text: str):
    """在接收频道回复入口消息提醒归档失败。尽力而为，失败仅记日志。"""
    if entry.chat_id is None:
        return
    try:
        await app.send_message(
            int(entry.chat_id), text,
            reply_parameters=ReplyParameters(message_id=entry.message_id),
        )
    except Exception:
        logger.debug("失败告警发送失败", exc_info=True)


def _entry_chat(entry: Entry) -> str:
    """入口所在频道。入口恒来自接收频道，chat 缺失时兜底到 RECEIVE_CHAT。"""
    return entry.chat_id or str(RECEIVE_CHAT)


async def _record_failure(entry: Entry, stage: str, error: str) -> str:
    """
    记录一次失败并决定后续动作。返回 'retry'（未满 N 轮，不推进 checkpoint）
    或 'skip'（已满 N 轮，跳过/剔除）。

    只接受 Entry：失败账、告警、checkpoint 一律以入口为准。路径二若拿源频道
    那条消息记账，行会落在另一个 id 空间，pending_failures() 永远读不到，
    等于既不重试也不阻塞 checkpoint。
    """
    chat_id = _entry_chat(entry)
    msg_id = entry.message_id
    count = db.increment_failure(chat_id, msg_id, stage, error)
    if count == 1:
        await alert_failure(entry, f"⚠️ 归档失败，将自动重试（共 {RETRY_MAX_ATTEMPTS} 次）。阶段: {stage}")
    if count >= RETRY_MAX_ATTEMPTS:
        db.mark_failure_skipped(chat_id, msg_id, f"重试 {count} 次仍失败: {stage}")
        await alert_failure(entry, f"⚠️ 归档失败 {count} 次，已跳过。原消息保留。阶段: {stage}，最近错误: {error}")
        return "skip"
    return "retry"


def _clear_failure(entry: Entry):
    """归档成功后自愈：清除该入口的失败记录（如有）。"""
    db.delete_failure(_entry_chat(entry), entry.message_id)


async def _settle(entry: Entry, outcomes: list[Outcome]) -> bool:
    """
    结算一个入口：返回它是否已结清（可以推进 checkpoint）。

    全部条目成功才清失败记录；任一条目失败就记一次失败。不能在每个条目成功时
    就地清记录 —— 路径二一个入口对多个条目，先成功的会把后失败的那条刚写的
    记录冲掉。
    """
    failed = [o for o in outcomes if not o.ok]
    if not failed:
        _clear_failure(entry)
        return True
    first = failed[0]
    outcome = await _record_failure(entry, first.stage or "unknown", first.error or "")
    # 满 N 轮已标记 skipped，视为结清，否则这条入口会永久卡住 checkpoint
    return outcome == "skip"


async def _settle_all(outcomes: list[Outcome]) -> bool:
    """
    按入口分组结算全部结果。返回是否所有入口都已结清。

    没有 Outcome 的入口不参与结算 —— 它们本轮没有确定结论（例如整组上传失败时
    后续条目根本没被尝试），清记录会丢掉往轮累计的重试次数。
    """
    grouped: dict[tuple[str, int], tuple[Entry, list[Outcome]]] = {}
    for outcome in outcomes:
        entry = outcome.item.entry
        key = (_entry_chat(entry), entry.message_id)
        grouped.setdefault(key, (entry, []))[1].append(outcome)

    settled = True
    for entry, entry_outcomes in grouped.values():
        if not await _settle(entry, entry_outcomes):
            settled = False
    return settled


async def _prepare_video(
    local_path: str,
    media: object,
    temp_files: list[str],
    thumb_dir: str,
    message_id: int,
) -> tuple[str, str | None, dict]:
    """压缩视频、探测元数据并生成缩略图，返回实际路径和上传参数。"""
    if VIDEO_COMPRESS_ENABLED:
        local_path = await asyncio.to_thread(
            maybe_compress_video,
            local_path,
            temp_files,
            VIDEO_COMPRESS_ENABLED,
            VIDEO_COMPRESS_MIN_SIZE_MB,
            VIDEO_COMPRESS_CRF,
            # 媒体组整组文件同在 batch_dir，产物名必须按消息 id 区分，否则互相覆盖
            tag=message_id,
        )

    logger.debug("ffprobe 探测 %s ...", message_id)
    meta = await asyncio.to_thread(probe_video, local_path)
    if meta is None:
        logger.debug("ffprobe %s 失败，回退源消息元数据", message_id)
        meta = {
            "duration": getattr(media, "duration", 0) or 0,
            "width": getattr(media, "width", 0) or 0,
            "height": getattr(media, "height", 0) or 0,
        }
    elif meta["duration"] == 0:
        # ffprobe 有时拿不到 duration，优先保留源消息中的有效值。
        source_dur = getattr(media, "duration", 0) or 0
        if source_dur:
            meta["duration"] = source_dur
            logger.debug("ffprobe %s duration=0，回退源数据 duration=%s", message_id, source_dur)
    logger.debug("ffprobe %s: duration=%s, %sx%s",
                 message_id, meta["duration"], meta["width"], meta["height"])

    logger.debug("生成缩略图 %s ...", message_id)
    thumb_path = os.path.join(thumb_dir, "thumb.jpg")
    thumb_path = await asyncio.to_thread(make_thumbnail, local_path, thumb_path)
    if thumb_path:
        logger.debug("缩略图 %s: %s (%s bytes)", message_id, thumb_path, os.path.getsize(thumb_path))
        temp_files.append(thumb_path)

    return local_path, thumb_path, meta


async def _send_media(
    local_path: str,
    kind: str,
    caption: str,
    *,
    thumb_path: str | None = None,
    meta: dict | None = None,
) -> Message | None:
    """上传单个媒体；照片格式不被 Telegram 接受时回退为 document。"""
    if kind == "video":
        video_meta = meta or {}
        sent = await app.send_video(
            ARCHIVE_CHAT,
            local_path,
            duration=video_meta["duration"],
            width=video_meta["width"],
            height=video_meta["height"],
            thumb=thumb_path,  # type: ignore[arg-type]
            caption=caption,
        )
    else:
        send = getattr(app, SEND_METHOD[kind])
        try:
            sent = await send(ARCHIVE_CHAT, local_path, caption=caption)
        except PhotoExtInvalid:
            logger.debug("PhotoExtInvalid，回退 send_document")
            sent = await app.send_document(ARCHIVE_CHAT, local_path, caption=caption)

    return sent


def _file_identity(kind: str, media: object) -> dict:
    """files 表的文件身份三件套。Photo 类型没有 file_name / mime_type，取不到就是 None。"""
    return {
        "file_name": getattr(media, "file_name", None),
        "mime_type": getattr(media, "mime_type", None),
        "media_kind": kind,
    }


def _record_dedup_file(item: ArchiveItem, sha256: str, size: int, dup) -> None:
    """
    SHA-256 去重命中：文件已在备份频道，只补一条指向它的 files 记录。

    这里不写 messages —— 没有新的归档消息可记。但文件身份和 source 必须照写，
    否则去重命中的行会永久缺 file_name/mime_type/media_kind，而且回填脚本救不了：
    它按 messages.origin_type IS NULL 选行，这些行没有对应的 messages 记录。
    """
    kind, media = get_media(item.media)
    assert kind is not None and media is not None
    db.record_file(
        file_unique_id=media.file_unique_id,
        sha256=sha256,
        size=size,
        archived_chat_id=dup["archived_chat_id"],
        archived_message_id=dup["archived_message_id"],
        source=item.entry.route,
        source_channel=_entry_chat(item.entry),
        **_file_identity(kind, media),
    )


def _record_archived_media(
    item: ArchiveItem, sha256: str, size: int, sent: Message, caption: str,
) -> None:
    """
    记录已上传文件及其来源消息。

    入口（item.entry）落到 messages.source_* 与 files.source，delete_message.py
    靠前者回退 checkpoint；来源（item.media）落到 origin_*。两者混用会让路径二的
    记录落在错误的 id 空间 —— 源频道的消息 id 被当成接收频道的 id 去回退 checkpoint。
    """
    assert sent.id is not None
    kind, media = get_media(item.media)
    assert kind is not None and media is not None
    entry = item.entry
    entry_chat = _entry_chat(entry)
    identity = _file_identity(kind, media)
    origin = (normalize_origin(item.media) if entry.route == ROUTE_FORWARD
              else origin_from_link(item.media))

    db.record_file(
        file_unique_id=media.file_unique_id,
        sha256=sha256,
        size=size,
        archived_chat_id=ARCHIVE_CHAT,
        archived_message_id=sent.id,
        source=entry.route,
        source_channel=entry_chat,
        **identity,
    )
    db.record_message(
        source_chat_id=entry_chat,
        source_message_id=entry.message_id,
        source_channel_title=entry.chat_title,
        sender=sender_name(item.media),
        sent_at=item.media.date.isoformat() if item.media.date else None,
        caption=caption,
        file_unique_id=media.file_unique_id,
        media_group_id=item.media.media_group_id,
        archived_chat_id=ARCHIVE_CHAT,
        archived_message_id=sent.id,
        file_name=identity["file_name"],
        media_kind=kind,
        **origin,
    )


def _cleanup_temp_files(temp_files: list[str]) -> None:
    """删除临时文件，并尽力清理其所在的空目录。"""
    for path in temp_files:
        if os.path.exists(path):
            os.remove(path)
            try:
                os.rmdir(os.path.dirname(path))
            except OSError:
                pass


async def archive_single(item: ArchiveItem) -> Outcome:
    """
    处理单条媒体消息，返回该条目的结论。

    失败不再就地记账 —— 调用方用 _settle_all 按入口统一结算。
    去重跳过算 ok：文件已在备份频道，没有待重试的事。
    """
    message = item.media
    entry = item.entry
    mark = entry.route == ROUTE_FORWARD
    kind, media = get_media(message)
    if not media or kind is None:
        return Outcome.success(item)

    if db.find_by_unique_id(media.file_unique_id):
        if mark:
            await mark_processed(message, duplicate=True)
        logger.info("跳过重复 %s (%s)", message.id, kind)
        return Outcome.success(item)

    # 每条消息下载到独立子目录，文件名保持原名（上传时不会带 num_ 前缀）
    msg_dir = os.path.join(DOWNLOAD_DIR, str(message.id))
    os.makedirs(msg_dir, exist_ok=True)
    dl_name = getattr(media, "file_name", None) or f"{message.id}_"

    logger.info("开始处理 %s (%s)", message.id, kind)

    # 下载：tdl 并行分块，失败自动回退 Pyrogram；下载失败不推进 checkpoint
    links = {message.id: item.link} if item.link else None
    try:
        paths = await tdl_downloader.download(
            [message],
            msg_dir,
            fallback=lambda m, path: app.download_media(message=m, file_name=path),  # type: ignore[call-overload]
            links=links,
            fallback_paths={message.id: os.path.join(msg_dir, dl_name)},
        )
    except Exception as e:
        logger.warning("下载 %s 失败，下轮重试", message.id)
        return Outcome.failure(item, "download", str(e))
    local_path = paths.get(message.id)
    if local_path is None:
        logger.warning("下载 %s 失败，下轮重试", message.id)
        return Outcome.failure(item, "download", "tdl 返回空路径")
    logger.debug("下载完成 %s → %s (%s bytes)", message.id, local_path, os.path.getsize(local_path))

    # 显式追踪所有临时文件，finally 统一清理
    temp_files = [local_path]

    try:
        # 校验只包住 verify_download_size 自己：把整段包在 except RuntimeError 里
        # 会把别处的 RuntimeError 也误报成校验失败
        try:
            verify_download_size(local_path, getattr(media, "file_size", None))
        except RuntimeError as e:
            logger.warning("文件校验失败 %s，下轮重试", message.id)
            return Outcome.failure(item, "verify", str(e))
        logger.debug("校验通过 %s", message.id)

        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        # 文件格式转换（如 WebP→JPEG），让 Telegram 可以内联展示
        local_path = fix_media_format(local_path, kind)
        temp_files[0] = local_path  # fix_media_format 可能改了路径（WebP→JPEG），跟踪新文件

        dup = db.find_by_sha256(sha256)
        if dup:
            _record_dedup_file(item, sha256, size, dup)
            if mark:
                await mark_processed(message, duplicate=True)
            return Outcome.success(item)

        # 转发到频道的文档类消息（.iso/.apk），文本可能在 text 而非 caption 字段
        caption = message.caption or message.text or ""

        # 视频：ffprobe 探测真实元数据（三层回退） + ffmpeg 生成缩略图
        thumb_path = None
        meta = None
        if kind == "video":
            local_path, thumb_path, meta = await _prepare_video(
                local_path, media, temp_files, msg_dir, message.id,
            )

        logger.debug("上传 %s %s ...", kind, message.id)
        try:
            sent = await _send_media(local_path, kind, caption, thumb_path=thumb_path, meta=meta)
        except Exception as e:
            logger.warning("上传 %s %s 失败，记录待重试", kind, message.id, exc_info=True)
            return Outcome.failure(item, "upload", str(e))

        assert sent is not None and sent.id is not None
        _record_archived_media(item, sha256, size, sent, caption)
        if mark:
            await mark_processed(message, duplicate=False)
        logger.info("归档 %s (%s)", message.id, kind)
        await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
    finally:
        _cleanup_temp_files(temp_files)

    return Outcome.success(item)


@dataclass
class _PendingUpload:
    """阶段三产出、阶段四消费的待上传条目。取代原先 9 元素的 tuple。"""

    item: ArchiveItem
    kind: str
    sha256: str
    size: int
    local_path: str
    thumb_path: str | None = None
    meta: dict | None = None


async def archive_group(items: list[ArchiveItem]) -> list[Outcome]:
    """
    媒体组：并行下载（最多 2 个）→ 顺序处理 → 打包成 send_media_group 上传。

    返回每个有确定结论的条目的 Outcome。整组上传失败时只有第一条待上传条目
    产出失败结论，其余条目本轮无结论（根本没被尝试），由调用方留到下轮 ——
    给每条都产结论会让路径一的媒体组变成 N 条告警。
    """
    outcomes: list[Outcome] = []
    to_upload: list[_PendingUpload] = []
    dup_items: list[ArchiveItem] = []
    single_items: list[ArchiveItem] = []  # 不支持编组的类型，退回单条处理
    temp_files: list[str] = []            # 视频 + 缩略图，finally 统一清理

    mark = bool(items) and items[0].entry.route == ROUTE_FORWARD

    # 媒体组的 caption 由 Telegram 只存在第一条消息上。提前抓取，
    # 以防第一条被去重/下载失败/校验失败过滤后 caption 丢失。
    # 转发到频道的文档类消息，文本可能在 text 而非 caption 字段。
    # 多条转发消息被 Telegram 编组后，各自可能带独立文字，全部收集。
    caps: list[str] = []
    for it in items:
        cap = (it.media.caption or it.media.text or "").strip()
        if cap and cap not in caps:
            caps.append(cap)
    group_caption = "\n".join(caps)

    # 阶段一：准备下载任务，跳过重复
    downloads: list[tuple] = []
    for it in items:
        kind, media = get_media(it.media)
        if not media or kind is None:
            continue
        if db.find_by_unique_id(media.file_unique_id):
            dup_items.append(it)
            outcomes.append(Outcome.success(it))
            continue

        msg_dir = os.path.join(DOWNLOAD_DIR, str(it.media.id))
        os.makedirs(msg_dir, exist_ok=True)
        dl_name = getattr(media, "file_name", None) or f"{it.media.id}_"
        downloads.append((it, kind, msg_dir, dl_name, media))

    # 阶段二：tdl 并行分块下载；缺失或失败的文件回退 Pyrogram（Semaphore 限流）
    paths: dict[int, str] = {}
    if downloads:
        first_msg = downloads[0][0].media
        batch_dir = os.path.join(
            DOWNLOAD_DIR,
            f"batch_{abs(first_msg.chat.id)}_{first_msg.id}_{len(downloads)}",
        )
        os.makedirs(batch_dir, exist_ok=True)
        fallback_paths = {
            it.media.id: os.path.join(md, dn)
            for it, _, md, dn, _ in downloads
        }
        # 只有链接指向的那一条带 link，推出来正好一个 URL，tdl 才会加 --group
        # 一次拉整组（tdl_downloader.py:113）。给每条都编链接会改掉下载语义
        links = {it.media.id: it.link for it, _, _, _, _ in downloads if it.link} or None

        async def _fallback(message, path):
            async with _dl_sem:
                return await app.download_media(message=message, file_name=path)  # type: ignore[call-overload]

        paths = await tdl_downloader.download(
            [it.media for it, _, _, _, _ in downloads],
            batch_dir,
            fallback=_fallback,
            links=links,
            fallback_paths=fallback_paths,
        )

    # 阶段三：顺序处理（校验 / SHA-256 / 去重 / 格式转换 / ffprobe / 缩略图）
    for it, kind, msg_dir, _dl_name, media in downloads:
        message = it.media
        local_path = paths.get(message.id)
        if local_path is None:
            logger.warning("下载 %s 失败，记录待重试", message.id)
            outcomes.append(Outcome.failure(it, "download", "tdl 返回空路径"))
            continue

        # tdl 成功时文件在 batch_dir，msg_dir 已无用途，避免残留空目录
        if os.path.dirname(local_path) != msg_dir:
            try:
                os.rmdir(msg_dir)
            except OSError:
                pass

        temp_files.append(local_path)
        logger.debug("下载完成 %s → %s (%s bytes)", message.id, local_path, os.path.getsize(local_path))

        # 文件完整性校验（失败跳过本条，不阻塞整组）
        try:
            verify_download_size(local_path, getattr(media, "file_size", None))
        except RuntimeError as e:
            logger.warning("文件校验失败 %s，记录待重试", message.id)
            outcomes.append(Outcome.failure(it, "verify", str(e)))
            continue
        logger.debug("校验通过 %s", message.id)

        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        local_path = fix_media_format(local_path, kind)
        temp_files[-1] = local_path  # fix_media_format 可能改了路径，更新追踪

        dup = db.find_by_sha256(sha256)
        if dup:
            _record_dedup_file(it, sha256, size, dup)
            dup_items.append(it)
            outcomes.append(Outcome.success(it))
            continue

        if kind not in INPUT_MEDIA_CLASS:
            # 语音/视频留言等不支持编组的类型，退回单条处理
            single_items.append(it)
            continue

        # 视频：ffprobe 探测真实元数据（三层回退） + ffmpeg 生成缩略图
        thumb_path = None
        meta: dict = {}
        if kind == "video":
            local_path, thumb_path, meta = await _prepare_video(
                local_path, media, temp_files, msg_dir, message.id,
            )

        to_upload.append(_PendingUpload(
            item=it, kind=kind, sha256=sha256, size=size,
            local_path=local_path, thumb_path=thumb_path, meta=meta,
        ))

    # 阶段四：上传 + 清理
    try:
        if to_upload:
            # 多条消息各有独立文字（转发文档被 Telegram 编组）→ 拆组单独上传，
            # 每条带自己的 caption，还原 "文件A+文字A、文件B+文字B" 的原始布局
            if len(caps) > 1:
                for pending in to_upload:
                    message = pending.item.media
                    caption = message.caption or message.text or ""
                    try:
                        sent = await _send_media(
                            pending.local_path,
                            pending.kind,
                            caption,
                            thumb_path=pending.thumb_path,
                            meta=pending.meta,
                        )
                    except Exception as e:
                        logger.warning("拆组上传 %s 失败，记录待重试", message.id, exc_info=True)
                        outcomes.append(Outcome.failure(pending.item, "upload", str(e)))
                        return outcomes

                    assert sent is not None and sent.id is not None
                    _record_archived_media(
                        pending.item, pending.sha256, pending.size, sent, caption,
                    )
                    outcomes.append(Outcome.success(pending.item))
                    await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
                logger.info("归档媒体组（拆组）%s 条", len(to_upload))
            else:
                # 普通媒体组：共用一条 caption，send_media_group 打包上传
                input_media = []
                for i, pending in enumerate(to_upload):
                    caption = group_caption if i == 0 else ""
                    if pending.kind == "video":
                        meta = pending.meta or {}
                        input_media.append(InputMediaVideo(
                            pending.local_path,
                            caption=caption,
                            duration=meta["duration"],
                            width=meta["width"],
                            height=meta["height"],
                            thumb=pending.thumb_path,
                        ))
                    else:
                        input_media.append(
                            INPUT_MEDIA_CLASS[pending.kind](pending.local_path, caption=caption)
                        )

                logger.debug("上传媒体组 %s 条 ...", len(to_upload))
                try:
                    try:
                        sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)
                    except PhotoExtInvalid:
                        # WebP 等格式不能作为 photo 编组，回退到全部作为 document 的媒体组
                        # thumb/meta 在 InputMediaDocument 中无效，丢弃即可
                        logger.debug("PhotoExtInvalid，回退整组 send_document")
                        input_media = [
                            InputMediaDocument(p.local_path, caption=group_caption if i == 0 else "")
                            for i, p in enumerate(to_upload)
                        ]
                        sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)  # type: ignore[arg-type]
                except Exception as e:
                    # 整组上传失败：只有第一条产出失败结论，其余条目本轮无结论
                    # （它们根本没被尝试）。给每条都产结论会让路径一变成 N 条告警
                    logger.warning("媒体组上传失败，记录待重试", exc_info=True)
                    outcomes.append(Outcome.failure(to_upload[0].item, "upload", str(e)))
                    return outcomes

                for pending, sent in zip(to_upload, sent_list):
                    _record_archived_media(
                        pending.item, pending.sha256, pending.size, sent, group_caption,
                    )
                    outcomes.append(Outcome.success(pending.item))
                await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
                logger.info("归档媒体组 %s 张", len(to_upload))

        for it in single_items:
            outcomes.append(await archive_single(it))

        if mark:
            for pending in to_upload:
                await mark_processed(pending.item.media, duplicate=False)
            for it in dup_items:
                await mark_processed(it.media, duplicate=True)

    finally:
        _cleanup_temp_files(temp_files)

    return outcomes


def _group_settled(group: list[Message]) -> bool:
    """
    组内所有消息都不再处于重试状态。checkpoint 的推进交给 scan_once 统一负责。

    以库里的 pending 为准而不是本轮的 Outcome：往轮残留的 'retrying' 行同样
    必须挡住推进，否则那条消息永远不会被重试。
    """
    ids = {str(m.id) for m in group}
    chat_id = str(group[0].chat.id) if group[0].chat and group[0].chat.id else str(RECEIVE_CHAT)
    pending = {p.split(":", 1)[1] for p in db.pending_failures() if p.split(":", 1)[0] == chat_id}
    if pending & ids:
        logger.warning("媒体组仍有 %s 条待重试，本轮不推进 checkpoint", len(pending & ids))
        return False
    return True


async def process_link_message(entry: Entry) -> list[Outcome]:
    """
    处理包含 t.me 链接的入口消息：Pyrogram 直接获取消息 → 复用路径一的去重+上传管道

    你是频道成员，Pyrogram 可以直接下载（不能"转发"但能"读取+下载"）。
    支持媒体组——检测到 media_group_id 后拉取整组，每张照片的 caption 一并保留。

    返回每个条目的结论。永久性失败（链接解析不了、消息取不到、无媒体）只记
    warning 不产出失败结论：重试无用，当成失败会让这条入口永久卡住 checkpoint。
    """
    message = entry.message
    text = message.text or ""
    raw_links = re.findall(r"https?://t\.me/\S+", text)
    seen = set()
    links: list[str] = []
    for link in raw_links:
        link = link.rstrip(".,;:!?)")
        if link not in seen:
            seen.add(link)
            links.append(link)

    outcomes: list[Outcome] = []
    for link in links:
        try:
            chat, msg_id = parse_message_link(link)
        except ValueError:
            logger.warning("无法解析链接，跳过：%s", link)
            continue

        try:
            msg = await app.get_messages(chat, msg_id)
        except Exception as e:
            logger.warning("获取链接消息 %s 失败，下轮重试", link, exc_info=True)
            outcomes.append(Outcome.failure(
                ArchiveItem(media=message, entry=entry, link=link), "download", str(e)))
            continue

        if msg is None:
            logger.warning("消息 %s 不可访问或已删除", link)
            continue

        # 媒体组：拉整组，复用 archive_group()
        if msg.media_group_id:
            try:
                group = sorted(await app.get_media_group(chat, msg_id), key=lambda m: m.id)
            except Exception as e:
                logger.warning("获取媒体组 %s 失败，下轮重试", link, exc_info=True)
                outcomes.append(Outcome.failure(
                    ArchiveItem(media=msg, entry=entry, link=link), "download", str(e)))
                continue
            # link 只挂在链接指向的那一条上：tdl 靠「多条消息只给一个 URL」
            # 决定加 --group 一次拉整组（tdl_downloader.py:113）
            items = [
                ArchiveItem(media=m, entry=entry, link=link if m.id == msg.id else None)
                for m in group
            ]
            outcomes.extend(await archive_group(items))
            logger.info("链接 %s → 媒体组 %s 张", link, len(group))
        else:
            kind, _ = get_media(msg)
            if kind is None:
                logger.info("链接 %s → 无媒体", link)
                continue
            outcomes.append(await archive_single(ArchiveItem(media=msg, entry=entry, link=link)))
            logger.info("链接 %s → %s", link, kind)

    # 只有确实归档了文件才打标记（去重跳过也算——文件已在备份频道）
    if any(o.ok for o in outcomes) and entry.chat_id is not None:
        try:
            await app.edit_message_text(
                int(entry.chat_id), entry.message_id, f"✅ 已归档\n{text}"[:4096])
        except Exception:
            logger.warning("编辑链接消息标记失败（已归档结果不受影响）：%s",
                           entry.message_id, exc_info=True)

    return outcomes


def _has_tme_link(message: Message) -> bool:
    """检查消息文本是否包含 t.me 链接"""
    text = message.text or message.caption or ""
    return bool(re.search(r"https?://t\.me/", text))


class EntryResult(NamedTuple):
    settled: bool     # 已结清，可以推进 checkpoint
    advance_to: int   # checkpoint 推进到哪条消息 id
    handled: int      # 本条入口实际处理的文件数，只用于冷却计时


async def _handle_entry(msg: Message, handled_groups: set) -> EntryResult:
    """
    处理接收频道的一条消息。三条路径统一成同一个返回形状，
    checkpoint 的推进由 scan_once 一处负责。
    """
    if msg.media_group_id:
        group = sorted(await app.get_media_group(msg.chat.id, msg.id), key=lambda m: m.id)
        handled_groups.add(msg.media_group_id)
        items = [
            ArchiveItem(media=m, entry=Entry(message=m, route=ROUTE_FORWARD))
            for m in group
        ]
        outcomes = await archive_group(items)
        # _settle_all 负责写失败账；组是否结清以库里的 pending 为准，
        # 这样往轮残留的 'retrying' 行也能挡住推进
        await _settle_all(outcomes)
        return EntryResult(_group_settled(group), max(m.id for m in group), len(group))

    kind, _ = get_media(msg)
    if kind:
        item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
        outcome = await archive_single(item)
        settled = await _settle_all([outcome])
        return EntryResult(settled, msg.id, 1 if outcome.ok else 0)

    if _has_tme_link(msg):
        entry = Entry(message=msg, route=ROUTE_LINK)
        outcomes = await process_link_message(entry)
        settled = await _settle_all(outcomes)
        return EntryResult(settled, msg.id, sum(1 for o in outcomes if o.ok))

    # 非媒体且无链接的消息不归档，但依然推进 checkpoint，避免反复扫描
    return EntryResult(True, msg.id, 0)


async def scan_once():
    last_id = db.get_checkpoint(RECEIVE_CHAT)
    new_messages = []
    # reverse=True 让消息从旧到新排列——配合 min_id checkpoint 机制，
    # 每处理完一条就推进 checkpoint，中途崩溃可以从最后成功的那条继续
    # min_id 在 Pyrogram 是包含边界 >=，+1 确保不重复拉取已 checkpoint 的消息
    async for msg in app.get_chat_history(RECEIVE_CHAT, min_id=last_id + 1, reverse=True):
        new_messages.append(msg)

    if not new_messages:
        return 0

    total = len(new_messages)
    # 限制每轮处理量，剩余留给下轮，避免短时间大量上传触发 Telegram 风控
    if total > BATCH_SIZE:
        new_messages = new_messages[:BATCH_SIZE]
        logger.info("待处理 %s 条，本轮处理 %s 条，剩余 %s 条下轮继续", total, BATCH_SIZE, total - BATCH_SIZE)

    handled_groups: set = set()
    processed = 0
    for msg in new_messages:
        if msg.media_group_id and msg.media_group_id in handled_groups:
            continue

        result = await _handle_entry(msg, handled_groups)
        processed += result.handled
        if not result.settled:
            # 必须停止本轮：否则下一条消息会把 checkpoint 推过这个未结清的入口，
            # 它永远不会被重试
            logger.info("入口 %s 未结清，本轮停止，不推进 checkpoint", msg.id)
            break
        db.set_checkpoint(RECEIVE_CHAT, result.advance_to)

    if processed:
        logger.info("本轮完成：处理 %s 条消息", processed)
    return processed


async def main():
    # 启动预检：ffmpeg/ffprobe 缺失时直接退出，让 Docker 重启
    if not shutil.which("ffprobe") or not shutil.which("ffmpeg"):
        logger.error("ffmpeg/ffprobe 未安装，退出")
        sys.exit(1)

    async with app:
        db.ensure_channel(RECEIVE_CHAT, "manual_forward")
        me = await app.get_me()
        logger.info("已登录：%s (id=%s)，冷却间隔 %ss", me.first_name, me.id, SCAN_INTERVAL_SECONDS)
        last_processed_at = 0.0

        while True:
            try:
                elapsed = time.time() - last_processed_at
                if elapsed < SCAN_INTERVAL_SECONDS:
                    wait = SCAN_INTERVAL_SECONDS - elapsed
                    logger.debug("冷却中，%.0fs 后扫描", wait)
                    await asyncio.sleep(wait)

                n = await scan_once()
                if n > 0:
                    last_processed_at = time.time()
                else:
                    logger.debug("无新消息，%ss 后再查", SCAN_INTERVAL_SECONDS)
                    await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            except Exception:
                logger.exception("本轮扫描出错")
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
