"""
归档管道：一批条目进来，下载→校验→去重→转换→上传→写库，产出 Outcome。

依赖全部构造时注入（client / db / downloader / 标记回调 / 配置），模块导入期
没有任何副作用。管道不知道 checkpoint、失败记账、告警的存在 —— 那些是
「扫描一轮接收频道」的职责，留在 listener.py，由它按 Outcome 结算。

依赖注入的由来见 docs/superpowers/specs/2026-08-31-pipeline-extraction-design.md：
模块级全局让主路径几乎测不动（测一条要 monkeypatch app/db/tdl_downloader 三个全局，
外加四个环境变量）。tdl_downloader.py 的 runner 参数是同一路子的先例。

media_ops 一律用模块限定调用（media_ops.probe_video(...)），不用 from-import：
测试要能 monkeypatch media_ops 上的函数而不必知道是谁在调用它。
（`MediaKind` 是类型别名，不是可替换的函数，所以按名字 import 不违反这条。）
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol

import media_ops
from archive_entry import ROUTE_FORWARD, ArchiveItem, Outcome
from compress_video import maybe_compress_video
from db import ArchiveDB
from media_ops import MediaKind
from origin import normalize_origin, origin_from_link
from pyrogram.client import Client
from pyrogram.errors import PhotoExtInvalid
from pyrogram.types import (
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)

logger = logging.getLogger(__name__)


class MediaFile(Protocol):
    """
    Pyrogram 的 Document / Video / Photo / ... 没有公共基类，但它们都有 file_unique_id。

    只标这一个必需属性：其余（file_name / mime_type / file_size / duration ...）各类型
    有无不一，一律 getattr 带默认值取。标成 object 会让 file_unique_id 也取不到，
    标成 Any 等于什么都不检查。
    """

    file_unique_id: str


@dataclass(frozen=True)
class MediaKindSpec:
    """
    一种媒体类型的上传形状。

    这四件事以前分散在 SEND_METHOD / INPUT_MEDIA_CLASS / CAPTIONLESS_KINDS 三张表
    加若干处 `kind == "video"` 里，加一种类型要同时想起四个地方，漏一个不会有任何
    静态错误 —— video_note 的 caption 缺陷就是这么来的。现在一种类型一行。
    """

    # Client 上的发送方法名，单条上传用
    send_method: str
    # send_media_group 用的 InputMedia 类；None 表示不能编组，只能单发
    input_media: type | None = None
    # send_* 是否接受 caption。send_video_note 是唯一不接受的 —— Telegram 的圆形视频
    # 在协议层就没有 caption 字段（对比 voice：messageVoiceNote 有 caption，
    # send_voice 也接受），不是 Pyrogram 的疏漏。源消息因此也不可能带文字：
    # video_note 走到 _send_media 时 caption 恒为空串。这个开关照样必需 ——
    # 不被接受的关键字传空串也是 TypeError
    accepts_caption: bool = True
    # 是否走 _prepare_video（压缩 / ffprobe / 缩略图）并按视频形状上传
    # （duration / width / height / thumb）。video_note 今天是 False，于是它拿不到
    # duration 与 length —— 见 CLAUDE.md 里记的已知限制。要修就从这一行开始，
    # 但 send_video_note 不接受 width/height，届时这个标志要拆成两个
    video_meta: bool = False


# 取值表以 media_ops.MediaKind 为准，test_pipeline.py 的
# test_media_kind_vocabulary_matches 盯着两边一致
MEDIA_KINDS: dict[MediaKind, MediaKindSpec] = {
    "document": MediaKindSpec("send_document", InputMediaDocument),
    "video": MediaKindSpec("send_video", InputMediaVideo, video_meta=True),
    "photo": MediaKindSpec("send_photo", InputMediaPhoto),
    "audio": MediaKindSpec("send_audio", InputMediaAudio),
    "animation": MediaKindSpec("send_animation"),
    "voice": MediaKindSpec("send_voice"),
    "video_note": MediaKindSpec("send_video_note", accepts_caption=False),
}

# 标记回调：(媒体消息, 是否去重命中) -> None。listener 注入，pipeline 不管它怎么发
MarkProcessed = Callable[[Message, bool], Awaitable[None]]


@dataclass(frozen=True)
class PipelineConfig:
    """
    今天散在 listener 模块级的那些常量。测试里想关掉冷却就传 0。

    构造时必填的四个值（冷却、压缩阈值、CRF、编码线程数）没有默认值：生产由 listener
    从环境变量读出后显式传入，那里才是唯一的真相。测试的默认值由 conftest 的
    make_pipeline 提供。
    （别把这几个叫「风控三件套」—— CLAUDE.md 里那个词专指 BATCH_SIZE /
    UPLOAD_COOLDOWN_SECONDS / SCAN_INTERVAL_SECONDS，后三个压缩参数只是调参。）
    """

    upload_cooldown_seconds: int
    video_compress_min_size_mb: int
    video_compress_crf: int
    video_compress_threads: int
    min_plausible_size: int = media_ops.MIN_PLAUSIBLE_SIZE
    video_compress_enabled: bool = False


@dataclass
class _DownloadTask:
    """阶段一产出、阶段二三消费。取代原先 5 元素的 tuple。"""

    item: ArchiveItem
    kind: MediaKind
    msg_dir: str      # 回退下载的落点；tdl 命中 batch_dir 时阶段三会 rmdir 掉
    dl_name: str
    media: MediaFile


@dataclass
class _PendingUpload:
    """阶段三产出、阶段四消费的待上传条目。取代原先 9 元素的 tuple。"""

    item: ArchiveItem
    kind: MediaKind
    # 媒体对象顺着条目一起传下来。写库时别再 get_media(item.media) 推一遍：
    # 重新推导除了多一次遍历，还要在结果上 assert 非空，等于把「已经知道的事」
    # 又变成一次运行期断言
    media: MediaFile
    sha256: str
    size: int
    local_path: str
    # 缩略图真的可能没有（make_thumbnail 失败返回 None），这个 None 是诚实的；
    # meta 则恒为 dict：非视频是空 dict，视频由 _prepare_video 填满
    thumb_path: str | None = None
    meta: dict = field(default_factory=dict)


class _Planned(NamedTuple):
    tasks: list[_DownloadTask]
    duplicates: list[ArchiveItem]   # file_unique_id 命中，不下载


class _ItemResult(NamedTuple):
    """一条条目走完「校验→SHA-256→格式修正→去重→视频处理」之后的结论。"""

    pending: _PendingUpload | None   # 可以上传了
    outcome: Outcome | None          # 已有确定结论（校验失败 / 去重命中），不必上传
    groupable: bool = False          # 该类型能进 send_media_group，决定上传形状
    duplicate: bool = False          # SHA-256 命中 —— 调用方要按「重复」打标记


class _Processed(NamedTuple):
    pending: list[_PendingUpload]
    duplicates: list[ArchiveItem]     # SHA-256 命中
    singles: list[_PendingUpload]     # 不支持编组的类型，逐条单发
    outcomes: list[Outcome]           # 下载/校验失败与去重命中的结论


class _Uploaded(NamedTuple):
    outcomes: list[Outcome]
    aborted: bool   # 上传失败：后续条目本轮不再尝试，等价于原先那个 early return


def sender_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or str(message.from_user.id)
    if message.sender_chat:
        return message.sender_chat.title or str(message.sender_chat.id)
    return ""


def _file_identity(kind: MediaKind, media: MediaFile) -> dict:
    """files 表的文件身份三件套。Photo 类型没有 file_name / mime_type，取不到就是 None。"""
    return {
        "file_name": getattr(media, "file_name", None),
        "mime_type": getattr(media, "mime_type", None),
        "media_kind": kind,
    }


def _safe_dl_name(raw: str | None) -> str | None:
    """
    把 Telegram 自带的 file_name 收成一个纯文件名；收不出来返回 None，调用方自己兜。

    file_name 由上传者控制，可以是 `../../../db/archive.db` 或绝对路径。它会被拼成
    Pyrogram 回退下载的落点（`download_media` 只做 `os.path.split` + `makedirs`，
    不校验穿越段），于是一个恶意命名的文档足以让 `open(..., "wb")` 落到 msg_dir 之外，
    把 archive.db 整个截断。只取最后一段并挡掉纯点名字，落点就恒在 msg_dir 内。
    tdl 主路径不受影响（DOWNLOAD_TEMPLATE 走 filenamify），这里补的是回退那一路。

    分隔符两种都切：来源平台未知，POSIX 上 `os.path.basename` 不认 `\\`。
    盘符交给 `splitdrive`（POSIX 上是空操作，Windows 上挡掉 `C:x` 这种盘符相对路径）。
    """
    if not raw:
        return None
    name = os.path.splitdrive(raw.replace("\\", "/"))[1].rsplit("/", 1)[-1]
    # NUL 会让 open 直接抛 ValueError，顺手去掉
    name = name.replace("\x00", "").strip()
    if name in ("", ".", ".."):
        return None
    return name


def _cleanup_temp_files(temp_files: list[str]) -> None:
    """
    删除临时文件，并尽力清理其所在的空目录。

    单个文件删不掉不能中断整轮清理，也不能让异常冒出去：这个函数只在 finally 里被调用，
    从那里抛出的异常会顶掉已经算好的 Outcome，并绕过 listener 的失败记账与熔断。
    """
    for path in temp_files:
        if not os.path.exists(path):
            continue
        try:
            os.remove(path)
        except OSError:
            logger.warning("临时文件删除失败，留待下次：%s", path, exc_info=True)
            continue
        try:
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass


def _collect_captions(items: list[ArchiveItem]) -> list[str]:
    """
    媒体组的全部文字，去重且保序。

    媒体组的 caption 由 Telegram 只存在第一条消息上。提前抓取，
    以防第一条被去重/下载失败/校验失败过滤后 caption 丢失。
    转发到频道的文档类消息，文本可能在 text 而非 caption 字段。
    多条转发消息被 Telegram 编组后，各自可能带独立文字，全部收集。
    """
    caps: list[str] = []
    for it in items:
        cap = (it.media.caption or it.media.text or "").strip()
        if cap and cap not in caps:
            caps.append(cap)
    return caps


def _first_unconcluded_failure(items: list[ArchiveItem], outcomes: list[Outcome],
                               error: Exception) -> Outcome | None:
    """
    非预期异常 → 给本轮尚无结论的第一条条目产出一条失败结论。

    与整组上传失败的既有约定一致：一次故障只产出一条结论。路径一的媒体组里
    每条媒体各是自己的入口，给每条都产结论会变成 N 条告警。全都有结论了就不补。
    """
    concluded = {o.item.media.id for o in outcomes}
    for it in items:
        if it.media.id not in concluded:
            return Outcome.failure(it, "process", error)
    return None


class ArchivePipeline:
    """一批条目进来，归档掉，产出 Outcome。"""

    def __init__(
        self,
        *,
        client: Client,
        db: ArchiveDB,
        downloader,                    # TDLDownloader；只用到 .download()
        mark_processed: MarkProcessed,
        config: PipelineConfig,
        archive_chat: int,
        receive_chat: int,
        download_dir: str,
    ):
        self._client = client
        self._db = db
        self._downloader = downloader
        self._mark = mark_processed
        self._config = config
        self._archive_chat = archive_chat
        self._receive_chat = receive_chat
        self._download_dir = download_dir
        # 并发下载数上限，MTProto 单连接慢，2 个并行可有效提速
        self._dl_sem = asyncio.Semaphore(2)

    async def _mark_processed(self, message: Message, *, duplicate: bool) -> None:
        await self._mark(message, duplicate)

    def _entry_chat(self, entry) -> str:
        """入口所在频道。入口恒来自接收频道，chat 缺失时兜底到 receive_chat。"""
        return entry.chat_id or str(self._receive_chat)

    def _record_dedup_file(self, item: ArchiveItem, kind: MediaKind, media: MediaFile,
                           sha256: str, size: int, dup) -> None:
        """
        SHA-256 去重命中：文件已在备份频道，只补一条指向它的 files 记录。

        这里不写 messages —— 没有新的归档消息可记。但文件身份和 source 必须照写，
        否则去重命中的行会永久缺 file_name/mime_type/media_kind，而且回填脚本救不了：
        它按 messages.origin_type IS NULL 选行，这些行没有对应的 messages 记录。
        """
        self._db.record_file(
            file_unique_id=media.file_unique_id,
            sha256=sha256,
            size=size,
            archived_chat_id=dup["archived_chat_id"],
            archived_message_id=dup["archived_message_id"],
            source=item.entry.route,
            source_channel=self._entry_chat(item.entry),
            **_file_identity(kind, media),
        )

    def _record_archived_media(
        self, upload: _PendingUpload, sent: Message, caption: str,
    ) -> None:
        """
        记录已上传文件及其来源消息 —— files 与 messages 一个事务写完。

        入口（item.entry）落到 messages.source_* 与 files.source，delete_message.py
        靠前者回退 checkpoint；来源（item.media）落到 origin_*。两者混用会让路径二的
        记录落在错误的 id 空间 —— 源频道的消息 id 被当成接收频道的 id 去回退 checkpoint。

        走 record_archived 而不是 record_file + record_message：两次调用是两个事务，
        第二次失败会留下「files 有行、messages 没行」的库，而下轮重试会因为
        file_unique_id 命中被当成去重跳过，那条 messages 永远补不上。
        """
        assert sent.id is not None
        item = upload.item
        entry = item.entry
        entry_chat = self._entry_chat(entry)
        identity = _file_identity(upload.kind, upload.media)
        origin = (normalize_origin(item.media) if entry.route == ROUTE_FORWARD
                  else origin_from_link(item.media))

        self._db.record_archived(
            file_unique_id=upload.media.file_unique_id,
            sha256=upload.sha256,
            size=upload.size,
            source=entry.route,
            source_channel=entry_chat,
            archived_chat_id=self._archive_chat,
            archived_message_id=sent.id,
            source_chat_id=entry_chat,
            source_message_id=entry.message_id,
            source_channel_title=entry.chat_title,
            sender=sender_name(item.media),
            sent_at=item.media.date.isoformat() if item.media.date else None,
            caption=caption,
            media_group_id=item.media.media_group_id,
            **identity,
            **origin,
        )

    async def _prepare_video(
        self,
        local_path: str,
        media: MediaFile,
        temp_files: list[str],
        message_id: int,
    ) -> tuple[str, str | None, dict]:
        """压缩视频、探测元数据并生成缩略图，返回实际路径和上传参数。"""
        if self._config.video_compress_enabled:
            local_path = await asyncio.to_thread(
                maybe_compress_video,
                local_path,
                temp_files,
                self._config.video_compress_enabled,
                self._config.video_compress_min_size_mb,
                self._config.video_compress_crf,
                # 媒体组整组文件同在 batch_dir，产物名必须按消息 id 区分，否则互相覆盖
                tag=message_id,
                threads=self._config.video_compress_threads,
            )

        logger.debug("ffprobe 探测 %s ...", message_id)
        meta = await asyncio.to_thread(media_ops.probe_video, local_path)
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
        # 产物写源文件所在目录并带上消息 id：媒体组 tdl 命中时文件在 batch_dir，
        # msg_dir 已被 rmdir，写进去必然失败（还要白跑四档画质重试）；整组文件同在
        # batch_dir，固定名会被 ffmpeg -y 互相覆盖。与 compressed_{tag}.mp4 同一套约定。
        # dirname 必须在压缩之后取：压缩成功时 local_path 已经换成产物路径
        thumb_path = os.path.join(os.path.dirname(local_path), f"thumb_{message_id}.jpg")
        thumb_path = await asyncio.to_thread(media_ops.make_thumbnail, local_path, thumb_path)
        if thumb_path:
            logger.debug("缩略图 %s: %s (%s bytes)",
                         message_id, thumb_path, os.path.getsize(thumb_path))
            temp_files.append(thumb_path)

        return local_path, thumb_path, meta

    async def _send_media(
        self,
        local_path: str,
        kind: MediaKind,
        caption: str,
        *,
        thumb_path: str | None = None,
        meta: dict,
    ) -> Message | None:
        """
        上传单个媒体；照片格式不被 Telegram 接受时回退为 document。

        方法名与关键字全部由 MEDIA_KINDS 那一行决定，这里不再有 `kind == "video"`：
        新增类型只改表。

        meta 必填：唯一的调用方 _upload_one 恒从 _PendingUpload.meta 取，
        那个字段恒为 dict（非视频是空 dict）。
        """
        spec = MEDIA_KINDS[kind]
        send = getattr(self._client, spec.send_method)

        kwargs: dict = {}
        if spec.accepts_caption:
            kwargs["caption"] = caption
        elif caption:
            # 不接受 caption 的类型不能传这个关键字（空串也不行，传了就是 TypeError）。
            # 今天不可达：唯一 accepts_caption=False 的 video_note 在协议层就没有
            # caption 字段，源消息带不了文字。留着只为哪天 Telegram 给圆形视频加上
            # caption 时有一行日志，而不是静默丢掉
            logger.warning("%s 不支持 caption，文字只留在 DB 里（FTS 仍可搜到）", kind)
        if spec.video_meta:
            kwargs |= {
                "duration": meta["duration"],
                "width": meta["width"],
                "height": meta["height"],
                "thumb": thumb_path,
            }

        try:
            return await send(self._archive_chat, local_path, **kwargs)
        except PhotoExtInvalid:
            logger.debug("PhotoExtInvalid，回退 send_document")
            return await self._client.send_document(
                self._archive_chat, local_path, caption=caption)

    async def _prepare_item(self, task: _DownloadTask, local_path: str,
                            temp_files: list[str]) -> _ItemResult:
        """
        下载产物 → 待上传条目。两条路径共用，唯一的一份实现。

        temp_files 由调用方持有，且下载产物已由调用方登记：本方法只往后追加自己造出来的
        新文件（格式修正后的新路径、压缩产物、缩略图），从不替换已有条目。因此这里不再有
        「最后一项必须是本条的下载产物」那个契约 —— 它曾经要求替换动作排在 _prepare_video
        之前（之后 temp_files[-1] 已经是缩略图了）。

        不产出 Outcome.failure("download")：下载归调用方 —— 单条与整组的批次形状不同。
        视频处理留在这里：产物命名依赖 local_path 所在目录（见 CLAUDE.md「临时产物命名」），
        这条约束必须只有一处实现。
        """
        it, kind, media = task.item, task.kind, task.media
        message = it.media
        logger.debug("下载完成 %s → %s (%s bytes)",
                     message.id, local_path, os.path.getsize(local_path))

        # 校验只包住 verify_download_size 自己：把整段包在 except RuntimeError 里
        # 会把别处的 RuntimeError 也误报成校验失败
        try:
            media_ops.verify_download_size(
                local_path, getattr(media, "file_size", None),
                min_size=self._config.min_plausible_size)
        except RuntimeError as e:
            logger.warning("文件校验失败 %s，记录待重试", message.id)
            return _ItemResult(pending=None, outcome=Outcome.failure(it, "verify", e))
        logger.debug("校验通过 %s", message.id)

        # 整文件哈希与图片转码都是同步的重活，必须丢线程池：几 GB 的视频在事件循环里
        # 哈希会一并卡住 Pyrogram 的 ping（session.PING_INTERVAL=5s，
        # 服务端的 disconnect_delay 是 WAIT_TIMEOUT+10=25s），慢盘上足以掉线重连。
        # 旁边的压缩 / ffprobe / 抽帧本来就在 to_thread 里，这里的形状要一致
        sha256 = await asyncio.to_thread(media_ops.sha256_of_file, local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        # 文件格式转换（如 WebP→JPEG），让 Telegram 可以内联展示
        fixed_path = await asyncio.to_thread(media_ops.fix_media_format, local_path, kind)
        if fixed_path != local_path:
            # 追加而不是替换：fix_media_format 换路径时已经把原文件删掉/改名了，
            # 旧条目留在列表里只会在 _cleanup_temp_files 的 exists 检查上落空。
            # 这样就不必维持「temp_files[-1] 必须是本条的下载产物」那个契约
            temp_files.append(fixed_path)
        local_path = fixed_path

        dup = self._db.find_by_sha256(sha256)
        if dup:
            self._record_dedup_file(it, kind, media, sha256, size, dup)
            return _ItemResult(pending=None, outcome=Outcome.success(it), duplicate=True)

        # 视频：（可选）压缩 → ffprobe 探测真实元数据（三层回退） → ffmpeg 生成缩略图
        spec = MEDIA_KINDS[kind]
        thumb_path = None
        meta: dict = {}
        if spec.video_meta:
            local_path, thumb_path, meta = await self._prepare_video(
                local_path, media, temp_files, message.id,
            )

        return _ItemResult(
            pending=_PendingUpload(item=it, kind=kind, media=media, sha256=sha256, size=size,
                                   local_path=local_path, thumb_path=thumb_path, meta=meta),
            outcome=None,
            groupable=spec.input_media is not None,
        )

    async def _upload_one(self, upload: _PendingUpload, caption: str) -> Outcome:
        """
        单条上传 + 写库，产出结论。archive_one、拆组分支、不可编组条目共用。

        不含冷却：调用方在成功后自己 sleep —— 打包上传整组只睡一次，逐条上传每条都睡。
        """
        message = upload.item.media
        logger.debug("上传 %s %s ...", upload.kind, message.id)
        try:
            sent = await self._send_media(
                upload.local_path, upload.kind, caption,
                thumb_path=upload.thumb_path, meta=upload.meta)
        except Exception as e:
            logger.warning("上传 %s %s 失败，记录待重试", upload.kind, message.id, exc_info=True)
            return Outcome.failure(upload.item, "upload", e)

        assert sent is not None and sent.id is not None
        self._record_archived_media(upload, sent, caption)
        logger.info("归档 %s (%s)", message.id, upload.kind)
        return Outcome.success(upload.item)

    async def archive_one(self, item: ArchiveItem) -> Outcome:
        """
        处理单条媒体消息，返回该条目的结论。

        失败不就地记账 —— 调用方用 _settle_all 按入口统一结算。
        去重跳过算 ok：文件已在备份频道，没有待重试的事。

        中间那段（校验→SHA-256→格式修正→去重→视频处理）走 _prepare_item，与整组路径
        共用唯一一份实现；这里只负责单条的下载形状（自己的 msg_dir、无 Semaphore）
        与单条的上传形状（_send_media，PhotoExtInvalid 只回退这一条）。
        """
        message = item.media
        mark = item.entry.route == ROUTE_FORWARD
        temp_files: list[str] = []

        try:
            planned = self._plan([item])
            if planned.duplicates:
                if mark:
                    await self._mark_processed(message, duplicate=True)
                logger.info("跳过重复 %s", message.id)
                return Outcome.success(item)
            if not planned.tasks:
                return Outcome.success(item)   # 无媒体：本轮有结论，checkpoint 可推进
            task = planned.tasks[0]
            logger.info("开始处理 %s (%s)", message.id, task.kind)

            # 下载：tdl 并行分块，失败自动回退 Pyrogram；下载失败不推进 checkpoint
            links = {message.id: item.link} if item.link else None
            try:
                paths = await self._downloader.download(
                    [message],
                    task.msg_dir,
                    fallback=lambda m, path: self._client.download_media(message=m, file_name=path),  # type: ignore[call-overload]
                    links=links,
                    fallback_paths={message.id: os.path.join(task.msg_dir, task.dl_name)},
                )
            except Exception as e:
                logger.warning("下载 %s 失败，下轮重试", message.id)
                return Outcome.failure(item, "download", e)
            local_path = paths.get(message.id)
            if local_path is None:
                logger.warning("下载 %s 失败，下轮重试", message.id)
                return Outcome.failure(item, "download", "tdl 返回空路径")

            temp_files.append(local_path)
            result = await self._prepare_item(task, local_path, temp_files)
            if result.outcome is not None:
                if result.duplicate and mark:
                    await self._mark_processed(message, duplicate=True)
                return result.outcome

            assert result.pending is not None
            # 转发到频道的文档类消息（.iso/.apk），文本可能在 text 而非 caption 字段
            caption = message.caption or message.text or ""
            outcome = await self._upload_one(result.pending, caption)
            if not outcome.ok:
                return outcome
            if mark:
                await self._mark_processed(message, duplicate=False)
            await asyncio.sleep(self._config.upload_cooldown_seconds)
        except Exception as e:
            logger.warning("处理 %s 失败，下轮重试", message.id, exc_info=True)
            return Outcome.failure(item, "process", e)
        finally:
            _cleanup_temp_files(temp_files)

        return Outcome.success(item)

    async def archive_batch(self, items: list[ArchiveItem]) -> list[Outcome]:
        """
        媒体组：并行下载（最多 2 个）→ 顺序处理 → 打包成 send_media_group 上传。

        返回每个有确定结论的条目的 Outcome。任一阶段的非预期异常都在这里翻译成
        一条失败结论（只给本轮尚无结论的第一条），交回 listener 记账/熔断 ——
        阶段方法保持「出错就抛」，不在内部就地降级。整组上传失败同样只有第一条
        产出失败结论，其余条目本轮无结论（根本没被尝试），由调用方留到下轮 ——
        给每条都产结论会让路径一的媒体组变成 N 条告警。
        """
        outcomes: list[Outcome] = []
        temp_files: list[str] = []

        try:
            mark = bool(items) and items[0].entry.route == ROUTE_FORWARD
            caps = _collect_captions(items)
            group_caption = "\n".join(caps)
            planned = self._plan(items)
            outcomes.extend(Outcome.success(it) for it in planned.duplicates)
            paths = await self._download(planned.tasks)
            # 下载产物一落盘就登记：_process_each 中途抛异常时，还没轮到的那几条
            # 也得被 finally 清掉（否则永久留在 batch_dir）
            temp_files.extend(paths.values())
            processed = await self._process_each(planned.tasks, paths, temp_files)
            outcomes.extend(processed.outcomes)
            duplicates = planned.duplicates + processed.duplicates

            if processed.pending:
                uploaded = await self._upload(processed.pending, caps, group_caption)
                outcomes.extend(uploaded.outcomes)
                if uploaded.aborted:
                    # 不再处理不可编组的条目、不打标记：那些条目本轮根本没被尝试
                    return outcomes

            for upload in processed.singles:
                single_msg = upload.item.media
                # 不可编组的条目各带自己的文字：它们本来就不在一个媒体组的 caption 里
                caption = single_msg.caption or single_msg.text or ""
                outcome = await self._upload_one(upload, caption)
                outcomes.append(outcome)
                if outcome.ok:
                    # 就地标记 + 就地冷却，与原先回调 archive_one 时的顺序一致
                    if mark:
                        await self._mark_processed(single_msg, duplicate=False)
                    await asyncio.sleep(self._config.upload_cooldown_seconds)

            if mark:
                for pending in processed.pending:
                    await self._mark_processed(pending.item.media, duplicate=False)
                for it in duplicates:
                    await self._mark_processed(it.media, duplicate=True)
        except Exception as e:
            failure = _first_unconcluded_failure(items, outcomes, e)
            if failure is not None:
                logger.warning("媒体组处理失败，记录待重试", exc_info=True)
                outcomes.append(failure)
            else:
                # 所有条目都已有结论（异常发生在结算之后，例如统一打标记那一段）：
                # 没有条目能承载这个失败，_settle_all 会按全成功结算并推进 checkpoint，
                # 这个异常就此消失 —— 只剩这行日志
                logger.exception("媒体组收尾出错，本轮已无条目可记账，checkpoint 仍会推进")
        finally:
            _cleanup_temp_files(temp_files)

        return outcomes

    def _plan(self, items: list[ArchiveItem]) -> _Planned:
        """阶段一：跳过 file_unique_id 命中的条目，其余准备下载任务。"""
        tasks: list[_DownloadTask] = []
        duplicates: list[ArchiveItem] = []
        for it in items:
            kind, media = media_ops.get_media(it.media)
            if not media or kind is None:
                continue
            if self._db.find_by_unique_id(media.file_unique_id):
                duplicates.append(it)
                continue

            msg_dir = os.path.join(self._download_dir, str(it.media.id))
            os.makedirs(msg_dir, exist_ok=True)
            # file_name 由上传者控制，必须先收成纯文件名：它是回退下载的落点
            dl_name = _safe_dl_name(getattr(media, "file_name", None)) or f"{it.media.id}_"
            tasks.append(_DownloadTask(item=it, kind=kind, msg_dir=msg_dir,
                                       dl_name=dl_name, media=media))
        return _Planned(tasks=tasks, duplicates=duplicates)

    async def _download(self, tasks: list[_DownloadTask]) -> dict[int, str]:
        """阶段二：tdl 并行分块下载；缺失或失败的文件回退 Pyrogram（Semaphore 限流）。"""
        if not tasks:
            return {}
        first_msg = tasks[0].item.media
        chat = first_msg.chat
        # Kurigram 把 chat 与 chat.id 都声明成 Optional。这里没有配置值可以顶替：
        # 路径二的媒体来自任意源频道，self._receive_chat 是入口频道而不是它的频道。
        # 缺 chat 说明消息对象本身不完整，抛出去由 archive_batch 兜成 'process' 失败账
        # （tdl_downloader 拿到这批消息后同样 assert，这里只是让它提前且带类型）
        assert chat is not None and chat.id is not None
        batch_dir = os.path.join(
            self._download_dir,
            f"batch_{abs(chat.id)}_{first_msg.id}_{len(tasks)}",
        )
        os.makedirs(batch_dir, exist_ok=True)
        fallback_paths = {
            t.item.media.id: os.path.join(t.msg_dir, t.dl_name) for t in tasks
        }
        # 只有链接指向的那一条带 link，推出来正好一个 URL，tdl 才会加 --group
        # 一次拉整组（tdl_downloader.py:113）。给每条都编链接会改掉下载语义
        links = {t.item.media.id: t.item.link for t in tasks if t.item.link} or None

        async def _fallback(message, path):
            async with self._dl_sem:
                return await self._client.download_media(message=message, file_name=path)  # type: ignore[call-overload]

        return await self._downloader.download(
            [t.item.media for t in tasks],
            batch_dir,
            fallback=_fallback,
            links=links,
            fallback_paths=fallback_paths,
        )

    async def _process_each(self, tasks: list[_DownloadTask], paths: dict[int, str],
                            temp_files: list[str]) -> _Processed:
        """阶段三：顺序处理（校验 / SHA-256 / 去重 / 格式转换 / ffprobe / 缩略图）。

        临时文件登记在调用方的 temp_files 里：本方法抛异常时 archive_batch 的
        finally 仍然要清得掉它们。下载产物由调用方在 _download 返回后一次登记完
        （本方法中途抛异常时，还没轮到的条目也已经落盘了）；本方法只让 _prepare_item
        往后追加它自己造出来的文件。
        """
        pending: list[_PendingUpload] = []
        duplicates: list[ArchiveItem] = []
        singles: list[_PendingUpload] = []
        outcomes: list[Outcome] = []

        for task in tasks:
            it = task.item
            message = it.media
            local_path = paths.get(message.id)
            if local_path is None:
                logger.warning("下载 %s 失败，记录待重试", message.id)
                outcomes.append(Outcome.failure(it, "download", "tdl 返回空路径"))
                continue

            # tdl 成功时文件在 batch_dir，msg_dir 已无用途，避免残留空目录
            if os.path.dirname(local_path) != task.msg_dir:
                try:
                    os.rmdir(task.msg_dir)
                except OSError:
                    pass

            result = await self._prepare_item(task, local_path, temp_files)
            if result.outcome is not None:
                outcomes.append(result.outcome)
                if result.duplicate:
                    duplicates.append(it)
                continue

            assert result.pending is not None
            if result.groupable:
                pending.append(result.pending)
            else:
                # 语音/视频留言等不支持编组的类型，逐条单发（不再重新下载一遍）
                singles.append(result.pending)

        return _Processed(pending=pending, duplicates=duplicates, singles=singles,
                          outcomes=outcomes)

    async def _upload(self, pending: list[_PendingUpload], caps: list[str],
                      group_caption: str) -> _Uploaded:
        """阶段四：多条各带独立文字则拆组单发，否则 send_media_group 打包。"""
        outcomes: list[Outcome] = []

        # 多条消息各有独立文字（转发文档被 Telegram 编组）→ 拆组单独上传，
        # 每条带自己的 caption，还原 "文件A+文字A、文件B+文字B" 的原始布局
        if len(caps) > 1:
            for upload in pending:
                message = upload.item.media
                caption = message.caption or message.text or ""
                outcome = await self._upload_one(upload, caption)
                outcomes.append(outcome)
                if not outcome.ok:
                    return _Uploaded(outcomes, aborted=True)
                await asyncio.sleep(self._config.upload_cooldown_seconds)
            logger.info("归档媒体组（拆组）%s 条", len(pending))
            return _Uploaded(outcomes, aborted=False)

        # 普通媒体组：共用一条 caption，send_media_group 打包上传
        input_media = []
        for i, upload in enumerate(pending):
            caption = group_caption if i == 0 else ""
            spec = MEDIA_KINDS[upload.kind]
            if spec.video_meta:
                input_media.append(InputMediaVideo(
                    upload.local_path,
                    caption=caption,
                    duration=upload.meta["duration"],
                    width=upload.meta["width"],
                    height=upload.meta["height"],
                    thumb=upload.thumb_path,
                ))
            else:
                # pending 里只有 groupable 的条目，input_media 必非 None
                assert spec.input_media is not None
                input_media.append(spec.input_media(upload.local_path, caption=caption))

        logger.debug("上传媒体组 %s 条 ...", len(pending))
        try:
            try:
                sent_list = await self._client.send_media_group(self._archive_chat, input_media)
            except PhotoExtInvalid:
                # WebP 等格式不能作为 photo 编组，回退到全部作为 document 的媒体组
                # thumb/meta 在 InputMediaDocument 中无效，丢弃即可
                logger.debug("PhotoExtInvalid，回退整组 send_document")
                input_media = [
                    InputMediaDocument(p.local_path, caption=group_caption if i == 0 else "")
                    for i, p in enumerate(pending)
                ]
                sent_list = await self._client.send_media_group(self._archive_chat, input_media)  # type: ignore[arg-type]
        except Exception as e:
            # 整组上传失败：只有第一条产出失败结论，其余条目本轮无结论
            # （它们根本没被尝试）。给每条都产结论会让路径一变成 N 条告警
            logger.warning("媒体组上传失败，记录待重试", exc_info=True)
            outcomes.append(Outcome.failure(pending[0].item, "upload", e))
            return _Uploaded(outcomes, aborted=True)

        # strict=True：sent_list 比 pending 短就抛，而不是静默漏记。少记一条等于
        # 「文件已在备份频道、DB 里没有任何行」，而那一条也拿不到 Outcome，
        # _settle_all 会跳过它、checkpoint 照常推进 —— 永久的去重盲区。
        # 抛出来则由 archive_batch 的兜底记进失败账
        for upload, sent in zip(pending, sent_list, strict=True):
            self._record_archived_media(upload, sent, group_caption)
            outcomes.append(Outcome.success(upload.item))
        await asyncio.sleep(self._config.upload_cooldown_seconds)
        logger.info("归档媒体组 %s 张", len(pending))
        return _Uploaded(outcomes, aborted=False)
