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
from urllib.parse import urlparse

import json, logging, shutil, subprocess

from PIL import Image

from pyrogram.client import Client
from pyrogram.errors import PhotoExtInvalid
from pyrogram.types import (
    Message,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    ReplyParameters,
)

from compress_video import maybe_compress_video
from db import ArchiveDB
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
                 max_concurrent_transmissions=2,
                 proxy=dict(scheme=u.scheme, hostname=u.hostname, port=u.port))
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

MIN_PLAUSIBLE_SIZE = 1024  # 1KB；网络中断留下的文件通常离谱地小或是 0 字节

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


def verify_download_size(local_path: str, expected_size) -> None:
    """
    基线校验：抓最离谱的截断情况，不追求精确匹配。
    expected_size 拿不到就只做最小体积检查，不当作可疑信号。
    """
    actual_size = os.path.getsize(local_path)

    if actual_size < MIN_PLAUSIBLE_SIZE:
        raise RuntimeError(f"文件小到不合理（{actual_size} 字节），大概率下载中断")

    if expected_size:
        if actual_size != expected_size:
            raise RuntimeError(
                f"文件大小对不上（期望 {expected_size}，实际 {actual_size}），疑似下载不完整"
            )


def _fix_media_format(path: str, kind: str | None, mime_type: str = "") -> str:
    """
    下载后的文件格式可能与 Telegram 声称的类型不匹配。

    - WebP/PNG/GIF → 转 JPEG，保证 Telegram 内联展示
    - 无后缀视频/图片 → 补后缀，否则 Telegram 解析不出缩略图和时长
    """
    if kind == "video":
        if not os.path.splitext(path)[1]:
            suffix = ".mp4" if "mp4" in (mime_type or "") else ".mp4"
            new_path = path + suffix
            os.rename(path, new_path)
            logger.debug("补后缀 video → %s", suffix)
            return new_path
        return path

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
                logger.debug("格式转换 %s → JPEG", fmt)
                return new_path

            # 本身就是 JPEG 但文件没有后缀，补上
            if fmt == "JPEG" and not ext:
                new_path = path + ".jpg"
                os.rename(path, new_path)
                logger.debug("补后缀 %s", fmt)
                return new_path

            # 其他图片格式但无后缀，统一加 .jpg
            if not ext:
                new_path = path + ".jpg"
                os.rename(path, new_path)
                logger.debug("补后缀 %s", fmt or "未知")
                return new_path
    except Exception:
        pass
    return path


def probe_video(path: str) -> dict | None:
    """
    用 ffprobe 量出真实的 duration/width/height。
    Telegram 对大文件可能解析不出元数据（duration=0），不能信任源消息自带的值。
    任何失败都返回 None，调用方看到 None 回退到源消息元数据 → 0，不阻塞归档。
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True, check=True, timeout=30,
        )
        info = json.loads(result.stdout)
        stream = next((s for s in info["streams"] if s.get("codec_type") == "video"), None)
        if stream is None:
            logger.warning("ffprobe 没解析出视频流：%s", path)
            return None
        duration = int(float(info.get("format", {}).get("duration", 0)))
        return {
            "duration": duration,
            "width": stream.get("width", 0),
            "height": stream.get("height", 0),
        }
    except FileNotFoundError:
        logger.error("ffprobe 可执行文件不存在，检查镜像是否装了 ffmpeg")
        return None
    except subprocess.CalledProcessError as e:
        logger.warning("ffprobe 处理失败（退出码 %s）：%s：%s",
                       e.returncode, path, (e.stderr or "")[:200])
        return None
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe 超时（30s）：%s", path)
        return None
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning("ffprobe 输出解析失败：%s：%s", path, e)
        return None


def make_thumbnail(video_path: str, thumb_path: str,
                   timestamp: str = "00:00:01") -> str | None:
    """
    用 ffmpeg 抽一帧当缩略图。
    Telegram 对大文件不保证生成缩略图，Pyrogram send_video 的 thumb 参数
    是唯一可靠途径——客户端主动提供缩略图，不指望服务端。
    画质从 5 递减到 31 重试；任何失败返回 None，不阻塞归档。
    """
    for q in (5, 10, 20, 31):
        try:
            result = subprocess.run(
                ["ffmpeg", "-y", "-ss", timestamp, "-i", video_path,
                 "-vframes", "1",
                 "-vf", "scale=320:320:force_original_aspect_ratio=decrease:force_divisible_by=2",
                 "-q:v", str(q), thumb_path],
                capture_output=True, timeout=20,
            )
        except FileNotFoundError:
            logger.error("ffmpeg 可执行文件不存在，检查镜像是否装了 ffmpeg")
            return None
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg 截图超时（20s），放弃：%s", video_path)
            return None  # 超时卡在解码上，换 -q:v 不会变快，直接跳出循环

        if result.returncode == 0 and os.path.exists(thumb_path):
            if os.path.getsize(thumb_path) <= 200 * 1024:
                return thumb_path

    # 所有画质档位都试过了仍超标（320px 下极少发生），放弃
    if os.path.exists(thumb_path):
        os.remove(thumb_path)
    return None


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
        pass


async def alert_failure(message: Message | None, text: str):
    """在接收频道回复原消息提醒归档失败。尽力而为，失败仅记日志。"""
    if message is None:
        return
    chat = message.chat
    if chat is None or chat.id is None:
        return
    try:
        await app.send_message(chat.id, text, reply_parameters=ReplyParameters(message_id=message.id))
    except Exception:
        logger.debug("失败告警发送失败", exc_info=True)


async def _record_failure(message: Message, stage: str, error: str) -> str:
    """记录一次失败并决定后续动作。返回 'retry'（未满 N 轮，不推进 checkpoint）或 'skip'（已满 N 轮，跳过/剔除）。"""
    chat_id = str(message.chat.id) if message.chat and message.chat.id else str(RECEIVE_CHAT)
    msg_id = message.id
    count = db.increment_failure(chat_id, msg_id, stage, error)
    if count == 1:
        await alert_failure(message, f"⚠️ 归档失败，将自动重试（共 {RETRY_MAX_ATTEMPTS} 次）。阶段: {stage}")
    if count >= RETRY_MAX_ATTEMPTS:
        db.mark_failure_skipped(chat_id, msg_id, f"重试 {count} 次仍失败: {stage}")
        await alert_failure(message, f"⚠️ 归档失败 {count} 次，已跳过。原消息保留。阶段: {stage}，最近错误: {error}")
        return "skip"
    return "retry"


def _clear_failure(message: Message):
    """归档成功后自愈：清除该消息的失败记录（如有）。"""
    chat_id = str(message.chat.id) if message.chat and message.chat.id else str(RECEIVE_CHAT)
    db.delete_failure(chat_id, message.id)


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


def _record_archived_media(
    message: Message,
    file_unique_id: str,
    sha256: str,
    size: int,
    sent: Message,
    caption: str,
) -> None:
    """记录已上传文件及其来源消息。"""
    assert sent.id is not None
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


def _cleanup_temp_files(temp_files: list[str]) -> None:
    """删除临时文件，并尽力清理其所在的空目录。"""
    for path in temp_files:
        if os.path.exists(path):
            os.remove(path)
            try:
                os.rmdir(os.path.dirname(path))
            except OSError:
                pass


async def archive_single(
    message: Message,
    *,
    mark: bool = True,
    tme_link: str | None = None,
) -> bool:
    """处理单条媒体消息。返回 True 表示成功（含跳过重复），False 表示需下轮重试。"""
    kind, media = get_media(message)
    if not media:
        return True

    file_unique_id = media.file_unique_id

    if db.find_by_unique_id(file_unique_id):
        if mark:
            await mark_processed(message, duplicate=True)
        logger.info("跳过重复 %s (%s)", message.id, kind)
        return True

    # 每条消息下载到独立子目录，文件名保持原名（上传时不会带 num_ 前缀）
    msg_dir = os.path.join(DOWNLOAD_DIR, str(message.id))
    os.makedirs(msg_dir, exist_ok=True)
    orig_name = getattr(media, "file_name", None)
    dl_name = orig_name or f"{message.id}_"

    logger.info("开始处理 %s (%s)", message.id, kind)

    # 下载：tdl 并行分块，失败自动回退 Pyrogram；下载失败不推进 checkpoint
    links = {message.id: tme_link} if tme_link else None
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
        outcome = await _record_failure(message, "download", str(e))
        return False if outcome == "retry" else True
    local_path = paths.get(message.id)
    if local_path is None:
        logger.warning("下载 %s 失败，下轮重试", message.id)
        outcome = await _record_failure(message, "download", "tdl 返回空路径")
        return False if outcome == "retry" else True
    logger.debug("下载完成 %s → %s (%s bytes)", message.id, local_path, os.path.getsize(local_path))

    # 显式追踪所有临时文件，finally 统一清理
    temp_files = [local_path]

    try:
        # 文件完整性校验（抛 RuntimeError → return False，不推进 checkpoint）
        verify_download_size(local_path, getattr(media, "file_size", None))
        logger.debug("校验通过 %s", message.id)

        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        # 文件格式转换（如 WebP→JPEG），让 Telegram 可以内联展示
        local_path = _fix_media_format(local_path, kind, getattr(media, "mime_type", ""))
        temp_files[0] = local_path  # _fix_media_format 可能改了路径（WebP→JPEG），跟踪新文件

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

        # 转发到频道的文档类消息（.iso/.apk），文本可能在 text 而非 caption 字段
        caption = message.caption or message.text or ""

        # 视频：ffprobe 探测真实元数据（三层回退） + ffmpeg 生成缩略图
        thumb_path = None
        meta = None
        if kind == "video":
            local_path, thumb_path, meta = await _prepare_video(
                local_path, media, temp_files, msg_dir, message.id,
            )

            logger.debug("上传视频 %s ...", message.id)
            try:
                sent = await _send_media(
                    local_path, kind, caption, thumb_path=thumb_path, meta=meta,
                )
            except Exception as e:
                logger.warning("上传视频 %s 失败，记录待重试", message.id, exc_info=True)
                outcome = await _record_failure(message, "upload", str(e))
                return False if outcome == "retry" else True
        else:
            assert kind is not None
            logger.debug("上传 %s %s ...", kind, message.id)
            try:
                sent = await _send_media(local_path, kind, caption)
            except Exception as e:
                logger.warning("上传 %s %s 失败，记录待重试", kind, message.id, exc_info=True)
                outcome = await _record_failure(message, "upload", str(e))
                return False if outcome == "retry" else True

        assert sent is not None and sent.id is not None
        _record_archived_media(message, file_unique_id, sha256, size, sent, caption)
        if mark:
            await mark_processed(message, duplicate=False)
        _clear_failure(message)
        logger.info("归档 %s (%s)", message.id, kind)
        await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)

    except RuntimeError as e:
        # verify_download_size 抛出的校验失败，不推进 checkpoint，下轮重试
        logger.warning("文件校验失败 %s，下轮重试", message.id)
        outcome = await _record_failure(message, "verify", str(e))
        return False if outcome == "retry" else True
    finally:
        _cleanup_temp_files(temp_files)

    return True


async def archive_group(
    messages: list[Message],
    *,
    mark: bool = True,
    tme_links: dict[int, str] | None = None,
):
    """媒体组：并行下载（最多 2 个）→ 顺序处理 → 打包成 send_media_group 上传"""
    to_upload: list[tuple] = []
    dup_messages = []
    new_messages = []
    temp_files = []  # 显式追踪所有临时文件（视频 + 缩略图），finally 统一清理

    # 媒体组的 caption 由 Telegram 只存在第一条消息上。提前抓取，
    # 以防第一条被去重/下载失败/校验失败过滤后 caption 丢失。
    # 转发到频道的文档类消息，文本可能在 text 而非 caption 字段。
    # 多条转发消息被 Telegram 编组后，各自可能带独立文字，全部收集。
    caps = []
    for m in messages:
        cap = (m.caption or m.text or "").strip()
        if cap and cap not in caps:
            caps.append(cap)
    group_caption = "\n".join(caps)

    # 阶段一：准备下载任务，跳过重复
    downloads: list[tuple] = []
    for message in messages:
        kind, media = get_media(message)
        if not media or kind is None:
            continue
        if db.find_by_unique_id(media.file_unique_id):
            dup_messages.append(message)
            _clear_failure(message)
            continue

        msg_dir = os.path.join(DOWNLOAD_DIR, str(message.id))
        os.makedirs(msg_dir, exist_ok=True)
        orig_name = getattr(media, "file_name", None)
        dl_name = orig_name or f"{message.id}_"
        downloads.append((message, kind, msg_dir, dl_name, media))

    # 阶段二：tdl 并行分块下载；缺失或失败的文件回退 Pyrogram（Semaphore 限流）
    paths: dict[int, str] = {}
    if downloads:
        first_msg = downloads[0][0]
        batch_dir = os.path.join(
            DOWNLOAD_DIR,
            f"batch_{abs(first_msg.chat.id)}_{first_msg.id}_{len(downloads)}",
        )
        os.makedirs(batch_dir, exist_ok=True)
        fallback_paths = {
            m.id: os.path.join(md, dn)
            for m, _, md, dn, _ in downloads
        }

        async def _fallback(message, path):
            async with _dl_sem:
                return await app.download_media(message=message, file_name=path)  # type: ignore[call-overload]

        paths = await tdl_downloader.download(
            [m for m, _, _, _, _ in downloads],
            batch_dir,
            fallback=_fallback,
            links=tme_links,
            fallback_paths=fallback_paths,
        )

    # 阶段三：顺序处理（校验 / SHA-256 / 去重 / 格式转换 / ffprobe / 缩略图）
    for message, kind, msg_dir, dl_name, media in downloads:
        local_path = paths.get(message.id)
        if local_path is None:
            logger.warning("下载 %s 失败，记录待重试", message.id)
            await _record_failure(message, "download", "tdl 返回空路径")
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
            await _record_failure(message, "verify", str(e))
            continue
        logger.debug("校验通过 %s", message.id)

        sha256 = sha256_of_file(local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        local_path = _fix_media_format(local_path, kind, getattr(media, "mime_type", ""))
        temp_files[-1] = local_path  # _fix_media_format 可能改了路径，更新追踪

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
            _clear_failure(message)
            continue

        if kind not in INPUT_MEDIA_CLASS:
            # 语音/视频留言等不支持编组的类型，退回单条处理
            new_messages.append(message)
            continue

        # 视频：ffprobe 探测真实元数据（三层回退） + ffmpeg 生成缩略图
        thumb_path = None
        meta = {}
        if kind == "video":
            local_path, thumb_path, meta = await _prepare_video(
                local_path, media, temp_files, msg_dir, message.id,
            )

        # to_upload: 从 6 元素扩到 8 元素（+thumb_path +meta）
        to_upload.append((kind, media.file_unique_id, sha256, size, local_path, message, thumb_path, meta))

    # 阶段四：上传 + 清理
    try:
        if to_upload:
            # 多条消息各有独立文字（转发文档被 Telegram 编组）→ 拆组单独上传，
            # 每条带自己的 caption，还原 "文件A+文字A、文件B+文字B" 的原始布局
            if len(caps) > 1:
                for kind, file_unique_id, sha256, size, local_path, message, thumb_path, meta in to_upload:
                    caption = message.caption or message.text or ""
                    try:
                        sent = await _send_media(
                            local_path,
                            kind,
                            caption,
                            thumb_path=thumb_path,
                            meta=meta,
                        )
                    except Exception as e:
                        logger.warning("拆组上传 %s 失败，记录待重试", message.id, exc_info=True)
                        await _record_failure(message, "upload", str(e))
                        return False

                    assert sent is not None and sent.id is not None
                    _record_archived_media(
                        message, file_unique_id, sha256, size, sent, caption,
                    )
                    _clear_failure(message)
                    await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
                logger.info("归档媒体组（拆组）%s 条", len(to_upload))
            else:
                # 普通媒体组：共用一条 caption，send_media_group 打包上传
                input_media = []
                for i, (kind, _, _, _, path, m, thumb, meta) in enumerate(to_upload):
                    caption = group_caption if i == 0 else ""
                    if kind == "video":
                        input_media.append(InputMediaVideo(
                            path,
                            caption=caption,
                            duration=meta["duration"],
                            width=meta["width"],
                            height=meta["height"],
                            thumb=thumb,
                        ))
                    else:
                        input_media.append(INPUT_MEDIA_CLASS[kind](path, caption=caption))

                logger.debug("上传媒体组 %s 条 ...", len(to_upload))
                try:
                    try:
                        sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)
                    except PhotoExtInvalid:
                        # WebP 等格式不能作为 photo 编组，回退到全部作为 document 的媒体组
                        # thumb/meta 在 InputMediaDocument 中无效，丢弃即可
                        logger.debug("PhotoExtInvalid，回退整组 send_document")
                        input_media = [
                            InputMediaDocument(path, caption=group_caption if i == 0 else "")
                            for i, (_, _, _, _, path, m, thumb, meta) in enumerate(to_upload)
                        ]
                        sent_list = await app.send_media_group(ARCHIVE_CHAT, input_media)  # type: ignore[arg-type]
                except Exception as e:
                    # 整组上传失败：记录待重试（挂在组内第一条待上传消息上），本轮不推进 checkpoint
                    logger.warning("媒体组上传失败，记录待重试", exc_info=True)
                    if to_upload:
                        await _record_failure(to_upload[0][5], "upload", str(e))
                    return False

                for (kind, file_unique_id, sha256, size, _, message, thumb, meta), sent in zip(to_upload, sent_list):
                    _record_archived_media(
                        message, file_unique_id, sha256, size, sent, group_caption,
                    )
                    _clear_failure(message)
                await asyncio.sleep(UPLOAD_COOLDOWN_SECONDS)
                logger.info("归档媒体组 %s 张", len(to_upload))
        for message in new_messages:
            await archive_single(message, mark=mark)
        if mark:
            for item in to_upload:
                await mark_processed(item[5], duplicate=False)  # item[5] 仍是 Message 对象
            for message in dup_messages:
                await mark_processed(message, duplicate=True)

    finally:
        _cleanup_temp_files(temp_files)


async def _advance_group_checkpoint(group: list[Message]) -> bool:
    """
    组内所有消息都不再处于重试状态才推进 checkpoint。
    返回 True 表示已推进，False 表示仍有未满 N 轮的失败待下轮重试。
    """
    ids = {str(m.id) for m in group}
    chat_id = str(group[0].chat.id) if group[0].chat and group[0].chat.id else str(RECEIVE_CHAT)
    pending = {p.split(":", 1)[1] for p in db.pending_failures() if p.split(":", 1)[0] == chat_id}
    if pending & ids:
        logger.warning("媒体组仍有 %s 条待重试，本轮不推进 checkpoint", len(pending & ids))
        return False
    db.set_checkpoint(chat_id, max(m.id for m in group))
    return True


async def process_link_message(message: Message):
    """
    处理包含 t.me 链接的文本消息：Pyrogram 直接获取消息 → 复用路径一的去重+上传管道

    你是频道成员，Pyrogram 可以直接下载（不能"转发"但能"读取+下载"）。
    支持媒体组——检测到 media_group_id 后拉取整组，每张照片的 caption 一并保留。
    返回实际归档的文件数。
    """
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
                logger.warning("消息 %s 不可访问或已删除", link)
                continue

            # 媒体组：拉整组，复用 archive_group()
            if msg.media_group_id:
                group = await app.get_media_group(chat, msg_id)
                group = sorted(group, key=lambda m: m.id)
                # mark=False：不编辑源频道消息，最终只标记接收频道里的链接消息
                await archive_group(group, mark=False, tme_links={msg.id: link})
                archived += len(group)
                logger.info("链接 %s → 媒体组 %s 张", link, len(group))
            else:
                kind, _ = get_media(msg)
                if kind:
                    if await archive_single(msg, mark=False, tme_link=link):
                        archived += 1
                        logger.info("链接 %s → %s", link, kind)
                    else:
                        logger.warning("链接 %s → 下载失败", link)
                else:
                    logger.info("链接 %s → 无媒体", link)
        except Exception:
            logger.warning("处理链接 %s 失败", link, exc_info=True)

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
            await _advance_group_checkpoint(group)
            processed += len(group)
            continue

        kind, _ = get_media(msg)
        if kind:
            if await archive_single(msg):
                processed += 1
            else:
                # 失败未满 N 轮：本轮不推进 checkpoint，且必须停止处理后续消息，
                # 否则下一条会把 checkpoint 推进过这条失败消息，导致它永远不被重试
                logger.debug("消息 %s 需重试，本轮停止推进 checkpoint", msg.id)
                break
        elif _has_tme_link(msg):
            n = await process_link_message(msg)
            if n > 0:
                processed += 1
        # 非媒体且无链接的消息不归档，但依然推进 checkpoint，避免反复扫描
        db.set_checkpoint(RECEIVE_CHAT, msg.id)

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
                logger.error("本轮扫描出错", exc_info=True)
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
