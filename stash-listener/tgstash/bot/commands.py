"""bot 独立于轮询扫描响应命令，才能及时处理确认按钮。"""

from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pyrogram.client import Client
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from tgstash.bot.operations import (
    AdminOperations,
    DeleteStateChanged,
    PendingClearAll,
    PendingDelete,
)
from tgstash.bot.pending import PendingStore
from tgstash.bot.views import (
    format_delete_preview,
    format_failures,
    format_purge,
    format_retry,
    format_stats,
    truncate,
)
from tgstash.storage.search import format_result

logger = logging.getLogger(__name__)

# 为搜索回复留出标题与截断余量。
SEARCH_LIMIT = 20


@dataclass(frozen=True)
class BotContext:

    operations: AdminOperations
    admin_ids: frozenset[int]
    run_backup: Callable[[], Awaitable[str]]
    pending: PendingStore[PendingDelete]
    pending_clear: PendingStore[PendingClearAll]


CommandHandler = Callable[[BotContext, Message, list[str]], Awaitable[None]]

CallbackHandler = Callable[[BotContext, CallbackQuery, str], Awaitable[None]]




def parse_command(text: str | None) -> tuple[str, list[str]] | None:
    """群聊命令带 @bot 后缀，手机输入法也可能改变大小写。"""
    if not text or not text.startswith("/"):
        return None
    parts = text.split()
    name = parts[0][1:].split("@")[0].lower()
    if not name:
        return None
    return name, parts[1:]


def parse_callback(data: str | None) -> tuple[str, str] | None:
    if not data:
        return None
    action, _, arg = data.partition(":")
    if not action:
        return None
    return action, arg


def user_allowed(user, admin_ids: frozenset[int]) -> bool:
    return user is not None and user.id in admin_ids


def is_allowed(message: Message, admin_ids: frozenset[int]) -> bool:
    return user_allowed(message.from_user, admin_ids)


def parse_delete_args(args: list[str]) -> tuple[list[int], bool]:
    usage = "用法：/delete <消息id ...> [rollback|keep]（默认不回退 checkpoint）"
    ids: list[int] = []
    rollback: bool | None = None
    for a in args:
        low = a.lower()
        if low == "rollback":
            if rollback is False:
                raise ValueError("keep 和 rollback 只能给一个")
            rollback = True
        elif low == "keep":
            if rollback is True:
                raise ValueError("keep 和 rollback 只能给一个")
            rollback = False
        else:
            try:
                ids.append(int(a))
            except ValueError:
                raise ValueError(f"{usage}（{a} 不是整数或已知标志）")
    if not ids:
        raise ValueError(usage)
    return ids, bool(rollback)


async def _cmd_stats(ctx: BotContext, message: Message, args: list[str]) -> None:
    await message.reply_text(truncate(format_stats(ctx.operations.db.stats())))


async def _cmd_failures(ctx: BotContext, message: Message, args: list[str]) -> None:
    if args and args[0].lower() == "clear":
        await _failures_clear(ctx, message, args[1:])
        return
    rows = ctx.operations.db.list_failures()
    if not rows:
        await message.reply_text("失败账为空")
        return
    await message.reply_text(truncate(format_failures(rows)))


async def _failures_clear(ctx: BotContext, message: Message, args: list[str]) -> None:
    """清空会抹掉 skipped 行的重试线索，因此无参数操作需要确认。"""
    if not args:
        count = len(ctx.operations.db.list_failures())
        if count == 0:
            await message.reply_text("失败账为空，无需清除")
            return
        user = message.from_user
        if user is None:
            return
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("确认清空", callback_data="fclr:ok"),
            InlineKeyboardButton("取消", callback_data="fclr:no"),
        ]])
        sent = await message.reply_text(
            f"将清空全部失败账 {count} 条，确认后不可恢复。\n"
            "清除只是忘掉失败记录：不重试、不动 checkpoint。skipped 行清掉后无法再 /retry。\n"
            "点「确认清空」执行，「取消」放弃。",
            reply_markup=keyboard)
        ctx.pending_clear.put(sent.id, PendingClearAll(count), user.id)
        return
    ids: list[int] = []
    for a in args:
        try:
            ids.append(int(a))
        except ValueError:
            await message.reply_text(f"用法：/failures clear [消息 id ...]（{a} 不是整数）")
            return
    summary = await ctx.operations.clear_failures(ids)
    lines = [f"已清除 {summary.deleted} 条失败账"]
    matched = set(summary.matched_ids)
    missing = [i for i in dict.fromkeys(ids) if i not in matched]
    if missing:
        lines.append(f"未找到：{missing}")
    await message.reply_text(truncate("\n".join(lines)))


async def _cmd_retry(ctx: BotContext, message: Message, args: list[str]) -> None:
    ids: list[int] = []
    for arg in args:
        try:
            ids.append(int(arg))
        except ValueError:
            await message.reply_text(f"用法：/retry [消息 id ...]（{arg} 不是整数）")
            return
    summary = await ctx.operations.retry_skipped(ids or None)
    await message.reply_text(truncate(format_retry(summary)))


async def _cmd_backup(ctx: BotContext, message: Message, args: list[str]) -> None:
    dest = await ctx.operations.backup(ctx.run_backup)
    await message.reply_text(f"备份完成：{os.path.basename(dest)}")


async def _cmd_search(ctx: BotContext, message: Message, args: list[str]) -> None:
    query = " ".join(args)
    if not query:
        await message.reply_text("用法：/search 关键词（每个关键词至少 3 个字符）")
        return
    rows = ctx.operations.db.search(query, limit=SEARCH_LIMIT)
    if not rows:
        await message.reply_text(
            f"没搜到跟「{query}」相关的内容（提示：每个关键词至少要 3 个字符）")
        return
    body = "\n".join(format_result(r) for r in rows)
    await message.reply_text(truncate(f"搜索「{query}」：\n{body}"))


async def _cmd_delete(ctx: BotContext, message: Message, args: list[str]) -> None:
    try:
        ids, rollback = parse_delete_args(args)
    except ValueError as e:
        await message.reply_text(str(e))
        return
    user = message.from_user
    if user is None:
        return
    preview = ctx.operations.preview_delete(ids, rollback)
    if not preview.rows:
        await message.reply_text(f"没找到 source_message_id in {ids} 的记录")
        return
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("确认删除", callback_data="del:ok"),
        InlineKeyboardButton("取消", callback_data="del:no"),
    ]])
    sent = await message.reply_text(
        truncate(format_delete_preview(preview)), reply_markup=keyboard)
    ctx.pending.put(sent.id, preview.confirmation(), user.id)


async def _cb_delete(ctx: BotContext, cq: CallbackQuery, arg: str) -> None:
    if arg not in ("ok", "no"):
        await cq.answer("未知操作")
        return
    msg = cq.message
    user = cq.from_user
    if msg is None or user is None:
        await cq.answer("该确认已失效，请重新 /delete", show_alert=True)
        return
    payload = ctx.pending.take(msg.id, user.id)
    if payload is None:
        await cq.answer("该确认已失效或不属于你，请重新 /delete", show_alert=True)
        return
    if arg == "no":
        await msg.edit_text("已取消删除")
        await cq.answer("已取消")
        return
    try:
        summary = await ctx.operations.confirm_delete(payload)
    except DeleteStateChanged as e:
        await msg.edit_text(
            f"频道在你确认前又收了新消息，回退跨度越过警戒线（{e}），"
            "已取消以防误伤。请重新 /delete 查看最新预览。")
        await cq.answer("已取消（状态已变化）", show_alert=True)
        return
    await msg.edit_text(format_purge(summary, payload))
    await cq.answer("已删除")


async def _cb_clear_failures(ctx: BotContext, cq: CallbackQuery, arg: str) -> None:
    if arg not in ("ok", "no"):
        await cq.answer("未知操作")
        return
    msg = cq.message
    user = cq.from_user
    if msg is None or user is None:
        await cq.answer("该确认已失效，请重新 /failures clear", show_alert=True)
        return
    payload = ctx.pending_clear.take(msg.id, user.id)
    if payload is None:
        await cq.answer("该确认已失效或不属于你，请重新 /failures clear", show_alert=True)
        return
    if arg == "no":
        await msg.edit_text("已取消清空")
        await cq.answer("已取消")
        return
    summary = await ctx.operations.clear_failures(None)
    await msg.edit_text(f"已清空全部失败账，删除 {summary.deleted} 条")
    await cq.answer("已清空")


CALLBACKS: dict[str, CallbackHandler] = {
    "del": _cb_delete,
    "fclr": _cb_clear_failures,
}


# 菜单和分发共用命令表，避免展示与实际支持的命令漂移。
COMMANDS: dict[str, tuple[CommandHandler, str]] = {
    "stats": (_cmd_stats, "归档统计"),
    "search": (_cmd_search, "搜索归档：/search 关键词"),
    "failures": (_cmd_failures, "列出/清除失败账：/failures [clear [id...]]"),
    "retry": (_cmd_retry, "重试失败的条目：/retry [消息 id ...]"),
    "backup": (_cmd_backup, "立即备份数据库"),
    "delete": (_cmd_delete, "删除条目并可选回退：/delete <id...> [rollback]"),
}


def usage_text() -> str:
    return "可用命令：\n" + "\n".join(
        f"/{name} - {desc}" for name, (_, desc) in COMMANDS.items())


async def handle_message(ctx: BotContext, message: Message) -> None:
    """对非白名单用户保持静默，避免暴露可用的管理命令。"""
    if not is_allowed(message, ctx.admin_ids):
        return

    parsed = parse_command(message.text)
    if parsed is None:
        return
    name, args = parsed

    entry = COMMANDS.get(name)
    if entry is None:
        await message.reply_text(usage_text())
        return

    try:
        await entry[0](ctx, message, args)
    except Exception as e:
        # dispatcher 不会向用户解释异常，这里保留堆栈并回复失败原因。
        logger.exception("bot 命令 /%s 执行失败", name)
        await message.reply_text(f"命令执行失败：{e}")


async def handle_callback(ctx: BotContext, cq: CallbackQuery) -> None:
    """旧按钮也可能被非白名单用户点击；每条回调都要结束客户端等待。"""
    if not user_allowed(cq.from_user, ctx.admin_ids):
        await cq.answer()
        return
    data = cq.data.decode() if isinstance(cq.data, bytes) else cq.data
    parsed = parse_callback(data)
    if parsed is None:
        await cq.answer()
        return
    action, arg = parsed
    handler = CALLBACKS.get(action)
    if handler is None:
        await cq.answer("按钮已失效")
        return
    try:
        await handler(ctx, cq, arg)
    except Exception as e:
        logger.exception("bot callback %s 执行失败", action)
        await cq.answer(f"执行失败：{e}", show_alert=True)


def register_handlers(bot: Client, ctx: BotContext) -> None:
    """在 Client 启动后注册，确保 dispatcher 已经能够接收 handler。"""

    @bot.on_message()
    async def _on_message(_client: Client, message: Message) -> None:
        await handle_message(ctx, message)

    @bot.on_callback_query()
    async def _on_callback(_client: Client, cq: CallbackQuery) -> None:
        await handle_callback(ctx, cq)


async def register_command_menu(bot: Client) -> None:
    """菜单注册失败不应阻止归档服务启动。"""
    try:
        await bot.set_bot_commands([
            BotCommand(name, desc) for name, (_, desc) in COMMANDS.items()
        ])
    except Exception:
        logger.warning("注册 bot 命令菜单失败（命令仍可用）", exc_info=True)
