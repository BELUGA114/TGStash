"""
归档管道产出 Outcome；checkpoint、失败账与告警由 listener 按入口结算。

依赖在构造时注入，避免导入时初始化服务。media_ops 函数通过模块调用，
使测试替换函数时不依赖调用方。关键约束见 docs/pipeline-invariants.md。
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
    """Pyrogram 媒体没有公共基类；只要求共有的 file_unique_id，其余属性可缺省。"""

    file_unique_id: str


@dataclass(frozen=True)
class MediaKindSpec:
    """发送能力集中在一处，避免新增媒体类型时漏改分派规则。"""

    send_method: str
    # None 表示 Telegram 不支持编组，只能单发。
    input_media: type | None = None
    # send_video_note 不接受 caption，传空串也会抛 TypeError。
    accepts_caption: bool = True
    # video_note 不接受 width/height，不能直接套用这组视频参数。
    video_meta: bool = False


# 媒体类型以 media_ops.MediaKind 为准，所有发送能力只在此处分派。
MEDIA_KINDS: dict[MediaKind, MediaKindSpec] = {
    "document": MediaKindSpec("send_document", InputMediaDocument),
    "video": MediaKindSpec("send_video", InputMediaVideo, video_meta=True),
    "photo": MediaKindSpec("send_photo", InputMediaPhoto),
    "audio": MediaKindSpec("send_audio", InputMediaAudio),
    "animation": MediaKindSpec("send_animation"),
    "voice": MediaKindSpec("send_voice"),
    "video_note": MediaKindSpec("send_video_note", accepts_caption=False),
}

# listener 注入标记回调，管道不决定回复方式。
MarkProcessed = Callable[[Message, bool], Awaitable[None]]


@dataclass(frozen=True)
class PipelineConfig:
    """
    冷却、压缩阈值、CRF 与线程数必须显式注入，避免生产默认值多处定义。
    环境变量及默认值由 runtime_config.py 管理；测试由 make_pipeline 提供。
    """

    upload_cooldown_seconds: int
    video_compress_min_size_mb: int
    video_compress_crf: int
    video_compress_threads: int
    min_plausible_size: int = media_ops.MIN_PLAUSIBLE_SIZE
    video_compress_enabled: bool = False


@dataclass
class _DownloadTask:
    item: ArchiveItem
    kind: MediaKind
    msg_dir: str      # Pyrogram 回退落点；tdl 成功后可删除空目录
    dl_name: str
    media: MediaFile


@dataclass
class _PendingUpload:
    item: ArchiveItem
    kind: MediaKind
    # 保留已识别的媒体，写库时无需重新推导并断言。
    media: MediaFile
    sha256: str
    size: int
    local_path: str
    # 缩略图失败不阻止归档；非视频的 meta 为空 dict。
    thumb_path: str | None = None
    meta: dict = field(default_factory=dict)


class _Planned(NamedTuple):
    tasks: list[_DownloadTask]
    duplicates: list[ArchiveItem]   # file_unique_id 命中，不下载


class _ItemResult(NamedTuple):
    pending: _PendingUpload | None
    outcome: Outcome | None          # 校验失败或去重命中，无需上传
    groupable: bool = False
    duplicate: bool = False          # 调用方须按重复打标记


class _Processed(NamedTuple):
    pending: list[_PendingUpload]
    duplicates: list[ArchiveItem]     # SHA-256 命中
    singles: list[_PendingUpload]     # 不支持编组的类型，逐条单发
    outcomes: list[Outcome]           # 下载/校验失败与去重命中的结论


class _Uploaded(NamedTuple):
    outcomes: list[Outcome]
    aborted: bool   # 上传失败，后续条目留待下轮


def sender_name(message: Message) -> str:
    if message.from_user:
        return message.from_user.first_name or str(message.from_user.id)
    if message.sender_chat:
        return message.sender_chat.title or str(message.sender_chat.id)
    return ""


def _file_identity(kind: MediaKind, media: MediaFile) -> dict:
    """Photo 缺少 file_name/mime_type，保留为 None。"""
    return {
        "file_name": getattr(media, "file_name", None),
        "mime_type": getattr(media, "mime_type", None),
        "media_kind": kind,
    }


def _safe_dl_name(raw: str | None) -> str | None:
    """
    上传者可控的 file_name 必须限制在 msg_dir 内，防止 Pyrogram 回退下载覆盖库文件。
    来源平台未知，需同时处理两种分隔符；splitdrive 防止 Windows 盘符相对路径。
    无有效文件名时返回 None，由调用方兜底；tdl 主路径另有文件名清洗。
    """
    if not raw:
        return None
    name = os.path.splitdrive(raw.replace("\\", "/"))[1].rsplit("/", 1)[-1]
    # NUL 会让 open 抛 ValueError。
    name = name.replace("\x00", "").strip()
    if name in ("", ".", ".."):
        return None
    return name


def _cleanup_temp_files(temp_files: list[str]) -> None:
    """
    尽力清理文件与空目录；删除失败不能中断后续清理。
    在 finally 中抛出清理异常会覆盖已有 Outcome，破坏 listener 的结算。
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
    过滤条目前收集文字，避免带 caption 的消息被跳过后丢失组说明。
    文档文字也可能在 text；保留各条独立文字并去重、保序。
    """
    caps: list[str] = []
    for it in items:
        cap = (it.media.caption or it.media.text or "").strip()
        if cap and cap not in caps:
            caps.append(cap)
    return caps


def _first_unconcluded_failure(items: list[ArchiveItem], outcomes: list[Outcome],
                               error: Exception) -> Outcome | None:
    """只给首个尚无结论的条目记失败，避免一次组故障触发多条入口告警。"""
    concluded = {o.item.media.id for o in outcomes}
    for it in items:
        if it.media.id not in concluded:
            return Outcome.failure(it, "process", error)
    return None


class ArchivePipeline:
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
        # 只限制媒体组的 Pyrogram 回退下载；tdl 并发由下载器控制。
        self._dl_sem = asyncio.Semaphore(2)

    async def _mark_processed(self, message: Message, *, duplicate: bool) -> None:
        await self._mark(message, duplicate)

    def _entry_chat(self, entry) -> str:
        """入口来自接收频道，chat 缺失时仍用入口频道兜底。"""
        return entry.chat_id or str(self._receive_chat)

    def _record_dedup_file(self, item: ArchiveItem, kind: MediaKind, media: MediaFile,
                           sha256: str, size: int, dup) -> None:
        """
        SHA-256 命中只补 files：复用归档落点，没有新 messages 可记。
        文件身份与路径仍须写全，回填脚本无法修复没有 messages 的去重行。
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
        messages.source_* 存入口，origin_* 存真实来源；checkpoint 只能按入口回退。
        files.source 存归档路径（Entry.route），不是来源位置。
        files 与 messages 必须原子写入，否则去重会阻止补写缺失的消息记录。
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
                # 组文件共享目录，产物名必须带消息 id，避免覆盖。
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
        # 压缩后从实际路径取目录：msg_dir 可能已删除；消息 id 避免组内产物覆盖。
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
        """照片格式被拒时仅回退本条；发送方法与参数能力由 MEDIA_KINDS 决定。"""
        spec = MEDIA_KINDS[kind]
        send = getattr(self._client, spec.send_method)

        kwargs: dict = {}
        if spec.accepts_caption:
            kwargs["caption"] = caption
        elif caption:
            # 若协议以后允许源消息带文字，也不能静默丢弃不支持上传的 caption。
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
        单条与整组共用唯一的校验、哈希、格式修正、去重和视频处理实现。
        下载形状由调用方决定，下载失败也由调用方判定。
        调用方登记下载产物并在 finally 清理；这里只追加新产物，不替换追踪项。
        """
        it, kind, media = task.item, task.kind, task.media
        message = it.media
        logger.debug("下载完成 %s → %s (%s bytes)",
                     message.id, local_path, os.path.getsize(local_path))

        # 仅捕获校验自身的 RuntimeError，避免误报其他处理阶段的异常。
        try:
            media_ops.verify_download_size(
                local_path, getattr(media, "file_size", None),
                min_size=self._config.min_plausible_size)
        except RuntimeError as e:
            logger.warning("文件校验失败 %s，记录待重试", message.id)
            return _ItemResult(pending=None, outcome=Outcome.failure(it, "verify", e))
        logger.debug("校验通过 %s", message.id)

        # sha256/size 必须认转换、压缩前的下载产物；重活放线程池，避免阻塞心跳。
        sha256 = await asyncio.to_thread(media_ops.sha256_of_file, local_path)
        size = os.path.getsize(local_path)
        logger.debug("SHA-256 %s: %s", message.id, sha256[:16])

        # 修正格式以便 Telegram 内联展示；图片转码同样不能阻塞事件循环。
        fixed_path = await asyncio.to_thread(media_ops.fix_media_format, local_path, kind)
        if fixed_path != local_path:
            # 追加可保留整组追踪；已删除的旧路径会被清理函数跳过。
            temp_files.append(fixed_path)
        local_path = fixed_path

        dup = self._db.find_by_sha256(sha256)
        if dup:
            self._record_dedup_file(it, kind, media, sha256, size, dup)
            return _ItemResult(pending=None, outcome=Outcome.success(it), duplicate=True)

        # 去重命中已返回，避免为重复视频跑压缩、探测和抽帧。
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
        """单条、拆组与不可编组条目共用；冷却由调用方控制，整组一次、单发每条一次。"""
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
        单条使用独立下载目录且不加组回退限流；照片回退只影响本条。
        媒体处理共用 _prepare_item，下载与上传形状保留独立流程。
        去重算成功；失败交回 listener 按入口结算。
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
        媒体组共用下载目录，只有 Pyrogram 回退下载最多并发两条。
        阶段方法的非预期异常在此记给首个尚无结论的条目，交回 listener 结算。
        整组上传失败也只记一条，避免一次故障触发多条告警；其余留待重试。
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
            # 整批登记，保证处理中途抛异常时，未处理的下载产物也由 finally 清理。
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
                # 复用已下载产物，不能回调 archive_one；上传失败不阻止后续单发。
                caption = single_msg.caption or single_msg.text or ""
                outcome = await self._upload_one(upload, caption)
                outcomes.append(outcome)
                if outcome.ok:
                    # 单发成功即标记、冷却；可编组条目等整组上传完成后统一标记。
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
                # 所有条目已有结论，仍交给 listener 结算；收尾异常只能记日志。
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
        """组下载失败或缺失时回退 Pyrogram；Semaphore 只限制回退并发。"""
        if not tasks:
            return {}
        first_msg = tasks[0].item.media
        chat = first_msg.chat
        # 媒体可能来自任意源频道，缺 chat 时不能拿入口 receive_chat 顶替。
        assert chat is not None and chat.id is not None
        batch_dir = os.path.join(
            self._download_dir,
            f"batch_{abs(chat.id)}_{first_msg.id}_{len(tasks)}",
        )
        os.makedirs(batch_dir, exist_ok=True)
        fallback_paths = {
            t.item.media.id: os.path.join(t.msg_dir, t.dl_name) for t in tasks
        }
        # 只保留原链接，单个 URL 才触发 tdl 的 --group 整组下载。
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
        """共用 _prepare_item 准备上传形状；临时文件始终由 archive_batch 持有和清理。"""
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

        # 拆组保留每条独立文字；与不可编组的 singles 不同，首次失败即停止。
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

        # 共用组说明只挂第一条，避免重复文字。
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
                # 照片被拒时整组回退 document，保留编组和组说明。
                logger.debug("PhotoExtInvalid，回退整组 send_document")
                input_media = [
                    InputMediaDocument(p.local_path, caption=group_caption if i == 0 else "")
                    for i, p in enumerate(pending)
                ]
                sent_list = await self._client.send_media_group(self._archive_chat, input_media)  # type: ignore[arg-type]
        except Exception as e:
            # 一次组故障只记一条失败，避免多条入口告警。
            logger.warning("媒体组上传失败，记录待重试", exc_info=True)
            outcomes.append(Outcome.failure(pending[0].item, "upload", e))
            return _Uploaded(outcomes, aborted=True)

        # 数量不符必须抛给 archive_batch，避免文件已上传却漏写库、漏结算。
        for upload, sent in zip(pending, sent_list, strict=True):
            self._record_archived_media(upload, sent, group_caption)
            outcomes.append(Outcome.success(upload.item))
        await asyncio.sleep(self._config.upload_cooldown_seconds)
        logger.info("归档媒体组 %s 张", len(pending))
        return _Uploaded(outcomes, aborted=False)
