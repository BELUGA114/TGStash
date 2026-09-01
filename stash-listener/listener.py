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
import functools
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
from db import ArchiveDB
from media_ops import get_media
from pipeline import ArchivePipeline, PipelineConfig
from pyrogram.client import Client
from pyrogram.types import Message, ReplyParameters
from tdl_downloader import TDLDownloader

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

# 容器内默认 /data；测试和本机可用 DATA_DIR 覆盖
DATA_DIR = os.environ.get("DATA_DIR", "/data")
SESSION_DIR = os.path.join(DATA_DIR, "session")
DB_PATH = os.path.join(DATA_DIR, "db", "archive.db")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "tmp", "listener")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """日志配置属于进程启动，不属于 import。"""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Pyrogram 内部 MTProto 传输日志每个 TCP 包一条，抑制到 WARNING
    logging.getLogger("pyrogram").setLevel(logging.WARNING)


@dataclass(frozen=True)
class ListenerContext:
    """
    一轮扫描要用到的运行时依赖。

    只装「可替换的协作者」与「必填配置」。带默认值的环境变量常量
    （RETRY_MAX_ATTEMPTS、BATCH_SIZE、SCAN_INTERVAL_SECONDS ...）留在模块级：
    它们是纯读取，没有导入期副作用，进 ctx 只会让所有调用点变长。
    """

    client: Client
    db: ArchiveDB
    pipeline: ArchivePipeline
    receive_chat: int


def _build_client(api_id: int, api_hash: str) -> Client:
    """构造 Pyrogram Client。单独拆出来是为了测试能替掉它（它会碰 workdir）。"""
    kwargs = {"api_id": api_id, "api_hash": api_hash, "workdir": SESSION_DIR,
              "max_concurrent_transmissions": 2}
    if HTTP_PROXY:
        u = urlparse(HTTP_PROXY)
        kwargs["proxy"] = {"scheme": u.scheme, "hostname": u.hostname, "port": u.port}
    return Client("listener", **kwargs)


def _build_context() -> ListenerContext:
    """
    组装运行时依赖。必填环境变量在这里读、目录在这里建、库在这里开、
    client 在这里构造 —— import listener 不该有任何副作用，否则「测一条归档」
    先得凑齐四个环境变量和一个可写的数据目录。
    """
    # chat_id 必须 int：字符串会触发 Pyrogram 的解析路径 bug
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
    receive_chat = int(os.environ["RECEIVE_CHAT_ID"])
    archive_chat = int(os.environ["ARCHIVE_CHAT_ID"])

    os.makedirs(SESSION_DIR, exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    archive_db = ArchiveDB(DB_PATH)
    client = _build_client(api_id, api_hash)
    downloader = TDLDownloader(
        namespace=TDL_NAMESPACE,
        threads=TDL_THREADS,
        limit=TDL_LIMIT,
        delay=TDL_DELAY_SECONDS,
        timeout=TDL_TIMEOUT_SECONDS,
        proxy=HTTP_PROXY,
    )
    pipeline = ArchivePipeline(
        client=client,
        db=archive_db,
        downloader=downloader,
        mark_processed=functools.partial(mark_processed, client),
        config=PipelineConfig(
            upload_cooldown_seconds=UPLOAD_COOLDOWN_SECONDS,
            video_compress_enabled=VIDEO_COMPRESS_ENABLED,
            video_compress_min_size_mb=VIDEO_COMPRESS_MIN_SIZE_MB,
            video_compress_crf=VIDEO_COMPRESS_CRF,
        ),
        archive_chat=archive_chat,
        receive_chat=receive_chat,
        download_dir=DOWNLOAD_DIR,
    )
    return ListenerContext(client=client, db=archive_db, pipeline=pipeline,
                           receive_chat=receive_chat)


TME_LINK_RE = re.compile(r"https?://t\.me/\S+")
# 链接尾部紧跟的句读不属于链接本身
LINK_TRAILING_PUNCT = ".,;:!?)"


def extract_tme_links(text: str) -> list[str]:
    """
    从消息文本里提取 t.me 链接，去掉尾部标点，保序去重。

    只剥 LINK_TRAILING_PUNCT 里那几个字符。合法的 t.me 链接总以数字消息 id
    （或 ?single 这类查询串）结尾，剥尾部标点不会伤到它。
    """
    links: list[str] = []
    for raw in TME_LINK_RE.findall(text or ""):
        link = raw.rstrip(LINK_TRAILING_PUNCT)
        if link and link not in links:
            links.append(link)
    return links


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


async def mark_processed(client: Client, message: Message, duplicate: bool):
    """回复原消息标记处理状态（转发的消息无法编辑，用回复形式）"""
    chat = message.chat
    if chat is None:
        return
    assert chat.id is not None
    text = "✅ 已归档（重复）" if duplicate else "✅ 已归档"
    try:
        await client.send_message(
            chat.id, text, reply_parameters=ReplyParameters(message_id=message.id))
    except Exception:
        logger.warning("回复归档标记失败（已归档结果不受影响）：%s", message.id, exc_info=True)


async def alert_failure(client: Client, entry: Entry, text: str):
    """在接收频道回复入口消息提醒归档失败。尽力而为，失败仅记日志。"""
    if entry.chat_id is None:
        return
    try:
        await client.send_message(
            int(entry.chat_id), text,
            reply_parameters=ReplyParameters(message_id=entry.message_id),
        )
    except Exception:
        logger.debug("失败告警发送失败", exc_info=True)


def _entry_chat(ctx: ListenerContext, entry: Entry) -> str:
    """入口所在频道。入口恒来自接收频道，chat 缺失时兜底到接收频道 id。"""
    return entry.chat_id or str(ctx.receive_chat)


async def _record_failure(ctx: ListenerContext, entry: Entry, stage: str, error: str) -> str:
    """
    记录一次失败并决定后续动作。返回 'retry'（未满 N 轮，不推进 checkpoint）
    或 'skip'（已满 N 轮，跳过/剔除）。

    只接受 Entry：失败账、告警、checkpoint 一律以入口为准。路径二若拿源频道
    那条消息记账，行会落在另一个 id 空间，pending_failures() 永远读不到，
    等于既不重试也不阻塞 checkpoint。
    """
    chat_id = _entry_chat(ctx, entry)
    msg_id = entry.message_id
    count = ctx.db.increment_failure(chat_id, msg_id, stage, error)
    if count == 1:
        await alert_failure(
            ctx.client, entry,
            f"⚠️ 归档失败，将自动重试（共 {RETRY_MAX_ATTEMPTS} 次）。阶段: {stage}")
    if count >= RETRY_MAX_ATTEMPTS:
        ctx.db.mark_failure_skipped(chat_id, msg_id, f"重试 {count} 次仍失败: {stage}")
        await alert_failure(
            ctx.client, entry,
            f"⚠️ 归档失败 {count} 次，已跳过。原消息保留。阶段: {stage}，最近错误: {error}")
        return "skip"
    return "retry"


def _clear_failure(ctx: ListenerContext, entry: Entry):
    """归档成功后自愈：清除该入口的失败记录（如有）。"""
    ctx.db.delete_failure(_entry_chat(ctx, entry), entry.message_id)


async def _settle(ctx: ListenerContext, entry: Entry, outcomes: list[Outcome]) -> bool:
    """
    结算一个入口：返回它是否已结清（可以推进 checkpoint）。

    全部条目成功才清失败记录；任一条目失败就记一次失败。不能在每个条目成功时
    就地清记录 —— 路径二一个入口对多个条目，先成功的会把后失败的那条刚写的
    记录冲掉。
    """
    failed = [o for o in outcomes if not o.ok]
    if not failed:
        _clear_failure(ctx, entry)
        return True
    first = failed[0]
    outcome = await _record_failure(ctx, entry, first.stage or "unknown", first.error or "")
    # 满 N 轮已标记 skipped，视为结清，否则这条入口会永久卡住 checkpoint
    return outcome == "skip"


async def _settle_all(ctx: ListenerContext, outcomes: list[Outcome]) -> bool:
    """
    按入口分组结算全部结果。返回是否所有入口都已结清。

    没有 Outcome 的入口不参与结算 —— 它们本轮没有确定结论（例如整组上传失败时
    后续条目根本没被尝试），清记录会丢掉往轮累计的重试次数。
    """
    grouped: dict[tuple[str, int], tuple[Entry, list[Outcome]]] = {}
    for outcome in outcomes:
        entry = outcome.item.entry
        key = (_entry_chat(ctx, entry), entry.message_id)
        grouped.setdefault(key, (entry, []))[1].append(outcome)

    settled = True
    for entry, entry_outcomes in grouped.values():
        if not await _settle(ctx, entry, entry_outcomes):
            settled = False
    return settled


def _group_settled(ctx: ListenerContext, group: list[Message]) -> bool:
    """
    组内所有消息都不再处于重试状态。checkpoint 的推进交给 scan_once 统一负责。

    以库里的 pending 为准而不是本轮的 Outcome：往轮残留的 'retrying' 行同样
    必须挡住推进，否则那条消息永远不会被重试。
    """
    ids = {str(m.id) for m in group}
    chat_id = (str(group[0].chat.id) if group[0].chat and group[0].chat.id
               else str(ctx.receive_chat))
    pending = {p.split(":", 1)[1] for p in ctx.db.pending_failures()
               if p.split(":", 1)[0] == chat_id}
    if pending & ids:
        logger.warning("媒体组仍有 %s 条待重试，本轮不推进 checkpoint", len(pending & ids))
        return False
    return True


async def process_link_message(ctx: ListenerContext, entry: Entry) -> list[Outcome]:
    """
    处理包含 t.me 链接的入口消息：Pyrogram 直接获取消息 → 复用路径一的去重+上传管道

    你是频道成员，Pyrogram 可以直接下载（不能"转发"但能"读取+下载"）。
    支持媒体组——检测到 media_group_id 后拉取整组，每张照片的 caption 一并保留。

    返回每个条目的结论。永久性失败（链接解析不了、消息取不到、无媒体）只记
    warning 不产出失败结论：重试无用，当成失败会让这条入口永久卡住 checkpoint。
    """
    message = entry.message
    text = message.text or ""
    links = extract_tme_links(text)

    outcomes: list[Outcome] = []
    for link in links:
        try:
            chat, msg_id = parse_message_link(link)
        except ValueError:
            logger.warning("无法解析链接，跳过：%s", link)
            continue

        try:
            msg = await ctx.client.get_messages(chat, msg_id)
        except Exception as e:
            logger.warning("获取链接消息 %s 失败，下轮重试", link, exc_info=True)
            outcomes.append(Outcome.failure(
                ArchiveItem(media=message, entry=entry, link=link), "download", str(e)))
            continue

        if msg is None:
            logger.warning("消息 %s 不可访问或已删除", link)
            continue

        # 媒体组：拉整组，复用 pipeline.archive_batch()
        if msg.media_group_id:
            try:
                group = sorted(await ctx.client.get_media_group(chat, msg_id),
                               key=lambda m: m.id)
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
            outcomes.extend(await ctx.pipeline.archive_batch(items))
            logger.info("链接 %s → 媒体组 %s 张", link, len(group))
        else:
            kind, _ = get_media(msg)
            if kind is None:
                logger.info("链接 %s → 无媒体", link)
                continue
            outcomes.append(await ctx.pipeline.archive_one(
                ArchiveItem(media=msg, entry=entry, link=link)))
            logger.info("链接 %s → %s", link, kind)

    # 只有确实归档了文件才打标记（去重跳过也算——文件已在备份频道）
    if any(o.ok for o in outcomes) and entry.chat_id is not None:
        try:
            await ctx.client.edit_message_text(
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


async def _handle_entry(ctx: ListenerContext, msg: Message, handled_groups: set) -> EntryResult:
    """
    处理接收频道的一条消息。三条路径统一成同一个返回形状，
    checkpoint 的推进由 scan_once 一处负责。
    """
    if msg.media_group_id:
        group = sorted(await ctx.client.get_media_group(msg.chat.id, msg.id),
                       key=lambda m: m.id)
        handled_groups.add(msg.media_group_id)
        items = [
            ArchiveItem(media=m, entry=Entry(message=m, route=ROUTE_FORWARD))
            for m in group
        ]
        outcomes = await ctx.pipeline.archive_batch(items)
        # _settle_all 负责写失败账；组是否结清以库里的 pending 为准，
        # 这样往轮残留的 'retrying' 行也能挡住推进
        await _settle_all(ctx, outcomes)
        return EntryResult(_group_settled(ctx, group), max(m.id for m in group), len(group))

    kind, _ = get_media(msg)
    if kind:
        item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
        outcome = await ctx.pipeline.archive_one(item)
        settled = await _settle_all(ctx, [outcome])
        return EntryResult(settled, msg.id, 1 if outcome.ok else 0)

    if _has_tme_link(msg):
        entry = Entry(message=msg, route=ROUTE_LINK)
        outcomes = await process_link_message(ctx, entry)
        settled = await _settle_all(ctx, outcomes)
        return EntryResult(settled, msg.id, sum(1 for o in outcomes if o.ok))

    # 非媒体且无链接的消息不归档，但依然推进 checkpoint，避免反复扫描
    return EntryResult(True, msg.id, 0)


async def scan_once(ctx: ListenerContext):
    last_id = ctx.db.get_checkpoint(ctx.receive_chat)
    new_messages = []
    # reverse=True 让消息从旧到新排列——配合 min_id checkpoint 机制，
    # 每处理完一条就推进 checkpoint，中途崩溃可以从最后成功的那条继续
    # min_id 在 Pyrogram 是包含边界 >=，+1 确保不重复拉取已 checkpoint 的消息
    async for msg in ctx.client.get_chat_history(ctx.receive_chat, min_id=last_id + 1,
                                                 reverse=True):
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

        result = await _handle_entry(ctx, msg, handled_groups)
        processed += result.handled
        if not result.settled:
            # 必须停止本轮：否则下一条消息会把 checkpoint 推过这个未结清的入口，
            # 它永远不会被重试
            logger.info("入口 %s 未结清，本轮停止，不推进 checkpoint", msg.id)
            break
        ctx.db.set_checkpoint(ctx.receive_chat, result.advance_to)

    if processed:
        logger.info("本轮完成：处理 %s 条消息", processed)
    return processed


async def main():
    _configure_logging()
    # 启动预检：ffmpeg/ffprobe 缺失时直接退出，让 Docker 重启
    if not shutil.which("ffprobe") or not shutil.which("ffmpeg"):
        logger.error("ffmpeg/ffprobe 未安装，退出")
        sys.exit(1)

    ctx = _build_context()
    async with ctx.client:
        ctx.db.ensure_channel(ctx.receive_chat, "manual_forward")
        me = await ctx.client.get_me()
        logger.info("已登录：%s (id=%s)，冷却间隔 %ss", me.first_name, me.id, SCAN_INTERVAL_SECONDS)
        last_processed_at = 0.0

        while True:
            try:
                elapsed = time.time() - last_processed_at
                if elapsed < SCAN_INTERVAL_SECONDS:
                    wait = SCAN_INTERVAL_SECONDS - elapsed
                    logger.debug("冷却中，%.0fs 后扫描", wait)
                    await asyncio.sleep(wait)

                n = await scan_once(ctx)
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
