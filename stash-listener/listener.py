"""按入口统一结算失败账，再推进 checkpoint，避免成功条目掩盖同入口的失败。"""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
import time
from dataclasses import dataclass
from typing import NamedTuple

from admin_operations import AdminOperations
from archive_entry import ROUTE_FORWARD, ROUTE_LINK, ArchiveItem, Entry, Outcome
from backups import BackupService
from bootstrap import build_runtime
from bot_commands import (
    BotContext,
    register_command_menu,
    register_handlers,
)
from bot_pending import PendingStore
from db import ArchiveDB
from links import has_tme_link, process_link_message
from logging_setup import configure_logging
from media_ops import get_media
from pipeline import ArchivePipeline
from pyrogram.client import Client
from pyrogram.types import Message, ReplyParameters
from runtime_config import ScanConfig, load_config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ListenerContext:
    client: Client
    db: ArchiveDB
    pipeline: ArchivePipeline
    receive_chat: int
    scan: ScanConfig


async def mark_processed(client: Client, message: Message, duplicate: bool):
    """转发消息无法编辑，只能回复标记；标记失败不撤销已完成的归档。"""
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
    return entry.chat_id or str(ctx.receive_chat)


async def _record_failure(ctx: ListenerContext, entry: Entry, stage: str, error: str) -> str:
    """只按入口记账；链接源频道的 id 无法阻塞接收频道 checkpoint。"""
    chat_id = _entry_chat(ctx, entry)
    msg_id = entry.message_id
    count = ctx.db.increment_failure(chat_id, msg_id, stage, error)
    if count == 1:
        await alert_failure(
            ctx.client, entry,
            f"⚠️ 归档失败，将自动重试（共 {ctx.scan.retry_max_attempts} 次）。阶段: {stage}")
    if count >= ctx.scan.retry_max_attempts:
        ctx.db.mark_failure_skipped(chat_id, msg_id, f"重试 {count} 次仍失败: {stage}")
        await alert_failure(
            ctx.client, entry,
            f"⚠️ 归档失败 {count} 次，已跳过。原消息保留。阶段: {stage}，最近错误: {error}")
        return "skip"
    return "retry"


def _clear_failure(ctx: ListenerContext, entry: Entry):
    ctx.db.delete_failure(_entry_chat(ctx, entry), entry.message_id)


async def _settle(ctx: ListenerContext, entry: Entry, outcomes: list[Outcome]) -> bool:
    """全成功才清失败账，避免同入口的成功条目冲掉其他条目的失败记录。"""
    failed = [o for o in outcomes if not o.ok]
    if not failed:
        _clear_failure(ctx, entry)
        return True
    first = failed[0]
    outcome = await _record_failure(ctx, entry, first.stage or "unknown", first.error or "")
    # 满 N 轮已标记 skipped，视为结清，否则这条入口会永久卡住 checkpoint
    return outcome == "skip"


async def _settle_all(ctx: ListenerContext, outcomes: list[Outcome]) -> bool:
    """没有结果的入口保留往轮失败账，避免丢掉尚未尝试条目的重试次数。"""
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
    """往轮残留的 retrying 行也必须挡住推进，因此以数据库为准。"""
    ids = {m.id for m in group}
    chat_id = (str(group[0].chat.id) if group[0].chat and group[0].chat.id
               else str(ctx.receive_chat))
    pending = {msg_id for chat, msg_id in ctx.db.pending_failures() if chat == chat_id}
    if pending & ids:
        logger.warning("媒体组仍有 %s 条待重试，本轮不推进 checkpoint", len(pending & ids))
        return False
    return True


class EntryResult(NamedTuple):
    settled: bool
    advance_to: int
    handled: int


async def _handle_entry(ctx: ListenerContext, msg: Message, handled_groups: set) -> EntryResult:
    if msg.media_group_id:
        # 扫描只读接收频道，不以 Optional 的 msg.chat 决定是否处理整组。
        group = sorted(await ctx.client.get_media_group(ctx.receive_chat, msg.id),
                       key=lambda m: m.id)
        handled_groups.add(msg.media_group_id)
        items = [
            ArchiveItem(media=m, entry=Entry(message=m, route=ROUTE_FORWARD))
            for m in group
        ]
        outcomes = await ctx.pipeline.archive_batch(items)
        await _settle_all(ctx, outcomes)
        return EntryResult(_group_settled(ctx, group), max(m.id for m in group), len(group))

    kind, _ = get_media(msg)
    if kind:
        item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
        outcome = await ctx.pipeline.archive_one(item)
        settled = await _settle_all(ctx, [outcome])
        return EntryResult(settled, msg.id, 1 if outcome.ok else 0)

    if has_tme_link(msg):
        entry = Entry(message=msg, route=ROUTE_LINK)
        outcomes = await process_link_message(ctx.client, ctx.pipeline, entry)
        settled = await _settle_all(ctx, outcomes)
        return EntryResult(settled, msg.id, sum(1 for o in outcomes if o.ok))

    # 非媒体且无链接的消息不归档，但依然推进 checkpoint，避免反复扫描
    return EntryResult(True, msg.id, 0)


async def scan_once(ctx: ListenerContext):
    last_id = ctx.db.get_checkpoint(ctx.receive_chat)
    new_messages: list[Message] = []
    has_more = False
    # 从旧到新扫描；min_id 包含边界，+1 排除已推进的消息。
    async for msg in ctx.client.get_chat_history(ctx.receive_chat, min_id=last_id + 1,
                                                 reverse=True):
        new_messages.append(msg)
        # 多取一条探测积压即可，避免每轮遍历全部历史。
        if len(new_messages) > ctx.scan.batch_size:
            new_messages.pop()
            has_more = True
            break

    if not new_messages:
        return 0
    if has_more:
        logger.info("本轮处理 %s 条，仍有剩余，下轮继续", len(new_messages))

    handled_groups: set = set()
    processed = 0
    for msg in new_messages:
        if msg.media_group_id and msg.media_group_id in handled_groups:
            continue

        try:
            result = await _handle_entry(ctx, msg, handled_groups)
        except Exception as e:
            # 管道外获取或结算异常也要进入失败账，否则入口会永久卡住。
            logger.warning("入口 %s 处理出错，记入失败账", msg.id, exc_info=True)
            entry = Entry(message=msg, route=ROUTE_FORWARD)
            settled = await _settle(ctx, entry, [
                Outcome.failure(ArchiveItem(media=msg, entry=entry), "process", e)])
            result = EntryResult(settled, msg.id, 0)

        processed += result.handled
        if not result.settled:
            # 停止本轮，避免后续消息把 checkpoint 推过仍需重试的入口。
            logger.info("入口 %s 未结清，本轮停止，不推进 checkpoint", msg.id)
            break
        ctx.db.set_checkpoint(ctx.receive_chat, result.advance_to)

    if processed:
        logger.info("本轮完成：处理 %s 条消息", processed)
    return processed


def _write_heartbeat(path: str, now: float) -> None:
    """心跳写入失败只记日志，避免观测手段拖垮归档服务。"""
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(int(now)))
    except OSError:
        logger.warning("写心跳失败：%s", path, exc_info=True)


async def scan_loop(ctx: ListenerContext, lock: asyncio.Lock, backup: BackupService) -> None:
    """扫描与管理写操作共用锁，防止一轮扫描中途被改动 checkpoint。"""
    last_processed_at = 0.0
    # 重启视为刚备份过，避免频繁重启触发连续快照。
    last_backup_at = time.time()

    while True:
        try:
            elapsed = time.time() - last_processed_at
            if elapsed < ctx.scan.interval_seconds:
                wait = ctx.scan.interval_seconds - elapsed
                logger.debug("冷却中，%.0fs 后扫描", wait)
                await asyncio.sleep(wait)

            async with lock:
                n = await scan_once(ctx)
                # 自动与手动备份共用目录，锁内执行避免保留策略互相干扰。
                last_backup_at = await backup.maybe_run(time.time(), last_backup_at)

            # 空闲轮也写心跳，避免把没有新消息误判为进程失活。
            _write_heartbeat(ctx.scan.heartbeat_path, time.time())
            if n > 0:
                last_processed_at = time.time()

            if n == 0:
                logger.debug("无新消息，%ss 后再查", ctx.scan.interval_seconds)
                await asyncio.sleep(ctx.scan.interval_seconds)
        except Exception:
            logger.exception("本轮扫描出错")
            await asyncio.sleep(ctx.scan.interval_seconds)


async def _serve_bot(bot: Client, ctx: ListenerContext, lock: asyncio.Lock,
                     backup: BackupService, admin_ids: frozenset[int]) -> None:
    """Kurigram 自行驱动 bot dispatcher，扫描循环维持进程存活。"""
    async with bot:
        # handler 在 client.start() 之后再挂：dispatcher 那时才在跑
        register_handlers(bot, BotContext(
            operations=AdminOperations(ctx.db, lock),
            admin_ids=admin_ids,
            run_backup=lambda: backup.run(time.time()),
            pending=PendingStore(),
            pending_clear=PendingStore(),
        ))
        await register_command_menu(bot)
        logger.info("bot 命令已启用，admin 白名单：%s",
                    ", ".join(str(i) for i in sorted(admin_ids)) or "（空——谁都不授权）")
        await scan_loop(ctx, lock, backup)


async def main():
    configure_logging()
    # 启动预检：ffmpeg/ffprobe 缺失时直接退出，让 Docker 重启
    if not shutil.which("ffprobe") or not shutil.which("ffmpeg"):
        logger.error("ffmpeg/ffprobe 未安装，退出")
        sys.exit(1)

    config = load_config()
    runtime = build_runtime(config, mark_processed)
    ctx = ListenerContext(runtime.client, runtime.db, runtime.pipeline,
                          config.receive_chat, config.scan)
    # 同一把锁注入扫描和 bot，避免两侧各自加锁却无法互斥。
    db_lock = asyncio.Lock()
    bot_client = runtime.bot

    async with ctx.client:
        ctx.db.ensure_channel(ctx.receive_chat, "manual_forward")
        me = await ctx.client.get_me()
        logger.info("已登录：%s (id=%s)，冷却间隔 %ss", me.first_name, me.id, ctx.scan.interval_seconds)

        if bot_client is None:
            logger.info("未设 TG_BOT_TOKEN，bot 命令关闭")
            await scan_loop(ctx, db_lock, runtime.backup)
            return

        await _serve_bot(bot_client, ctx, db_lock, runtime.backup, config.admin_ids)


if __name__ == "__main__":
    asyncio.run(main())
