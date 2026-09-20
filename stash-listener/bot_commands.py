"""
Telegram bot 命令交互：在手机上查状态、做运维，不必登服务器。

bot Client 与扫描用的 userbot 同进程共存（`listener.main` 构造两侧），handler 由
Kurigram dispatcher 驱动；本模块只负责「权限 → 解析 → 执行 → 回复」。

为什么用 bot 而不是 userbot 拦截命令：userbot（用户账号）发不了 inline keyboard、
注册不了原生命令菜单、收不到 callback query；而且它走轮询扫描循环，命令最多等一轮
（默认 5 分钟）才回。bot 模式 Client 能实时响应。

依赖（db、写锁、admin 白名单、备份回调）全部构造时注入，模块级不留任何全局：
import 本模块不读环境变量、不开库、不构造 Client。环境变量的唯一真相在 listener.py。
"""

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from db import ArchiveDB, RetrySummary, Stats
from pyrogram.client import Client
from pyrogram.types import BotCommand, Message
from search import format_result

logger = logging.getLogger(__name__)

# Telegram 单条消息文本上限（字符），超了整条 send 会被拒。与
# listener.TELEGRAM_TEXT_LIMIT 是同一协议上限的两份常量：这里不能 import listener
# （listener 要 import 本模块装 handler，会成环），4096 也不值得为它造模块
TELEGRAM_TEXT_LIMIT = 4096

# 一次回复最多列几条明细。头部先给总数，超出的折成一行计数 —— 即使被 4096 截断，
# 最要紧的计数与 checkpoint 变化也已经发出去了
MAX_LISTED_ITEMS = 20

# 搜索返回条数。30 条 × 每行最长 ~150 字符会顶到 4096，取 20 留余量
SEARCH_LIMIT = 20


@dataclass(frozen=True)
class BotContext:
    """bot 命令的运行时依赖。全部构造时注入，模块级不留全局。"""

    db: ArchiveDB
    lock: asyncio.Lock                             # 与扫描循环共用：写命令与扫描轮互斥
    admin_ids: frozenset[int]                      # 空白名单 = 谁都不授权
    run_backup: Callable[[], Awaitable[str]]       # 打快照 + 保留 + 可选上传，返回快照路径


# 命令实现签名：依赖全在 ctx 里，回复走 message.reply_text，不需要 Client 参数
CommandHandler = Callable[[BotContext, Message, list[str]], Awaitable[None]]


def parse_admin_ids(raw: str) -> frozenset[int]:
    """
    解析 BOT_ADMIN_IDS：逗号分隔的 user id，空白项跳过。

    未设/空 → 空集合 = 谁都不授权（bot 登录着但任何命令都不响应）。这是刻意的默认：
    漏配白名单的后果应该是「没人能用」，不是「谁都能用」。非法项只记 warning 不抛 ——
    一个打错的 id 不该让整个服务起不来。
    """
    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            logger.warning("BOT_ADMIN_IDS 里的 %r 不是整数，已忽略", part)
    return frozenset(ids)


def parse_command(text: str | None) -> tuple[str, list[str]] | None:
    """
    '/retry 100 105' → ('retry', ['100', '105'])；非命令返回 None。

    群聊里客户端发的是 /stats@my_bot，@ 后缀要剥掉。首 token 以 / 开头就算命令，
    名字认不认识留给调用方判定 ——「未知命令回一句用法」与「不是命令就当没看见」
    是两种不同的处理。命令名统一小写，手机输入法容易把首字母大写。
    """
    if not text or not text.startswith("/"):
        return None
    parts = text.split()
    name = parts[0][1:].split("@")[0].lower()
    if not name:
        return None
    return name, parts[1:]


def is_allowed(message: Message, admin_ids: frozenset[int]) -> bool:
    """白名单判定。from_user 为空（频道匿名、sender_chat 发帖）一律拒。"""
    user = message.from_user
    return user is not None and user.id in admin_ids


def truncate(text: str) -> str:
    """按 Telegram 单条文本上限截断，末尾留个省略号表明还有内容。"""
    if len(text) <= TELEGRAM_TEXT_LIMIT:
        return text
    return text[:TELEGRAM_TEXT_LIMIT - 1] + "…"


def listed(items: list[str]) -> list[str]:
    """明细行最多列 MAX_LISTED_ITEMS 条，其余折成一行计数。"""
    if len(items) <= MAX_LISTED_ITEMS:
        return items
    return items[:MAX_LISTED_ITEMS] + [f"…还有 {len(items) - MAX_LISTED_ITEMS} 条"]


def one_line(text: str | None, limit: int = 120) -> str:
    """把可能多行的字段压成一行，过长截断（回复里一行一条才读得下去）。"""
    return (text or "").replace("\n", " ")[:limit]


def format_stats(stats: Stats) -> str:
    kinds = " / ".join(
        f"{kind} {count}"
        for kind, count in sorted(stats.by_kind.items(), key=lambda kv: (-kv[1], kv[0]))
    ) or "（无）"
    failures = " / ".join(
        f"{status} {count}" for status, count in sorted(stats.failures.items())
    ) or "0"
    return "\n".join([
        "归档统计",
        f"文件总数：{stats.total_files}",
        f"消息条目：{stats.message_count}（其中去重命中 {stats.dedup_hits}）",
        f"类型分布：{kinds}",
        f"失败账：{failures}",
    ])


async def _cmd_stats(ctx: BotContext, message: Message, args: list[str]) -> None:
    await message.reply_text(truncate(format_stats(ctx.db.stats())))


def format_failures(rows) -> str:
    lines = [
        f"[{r['status']}] msg={r['source_message_id']} 阶段={r['failure_stage']} "
        f"次数={r['attempt_count']} {one_line(r['last_error'])}"
        for r in rows
    ]
    return "\n".join([f"失败账共 {len(rows)} 条", *listed(lines)])


async def _cmd_failures(ctx: BotContext, message: Message, args: list[str]) -> None:
    rows = ctx.db.list_failures()
    if not rows:
        await message.reply_text("失败账为空")
        return
    await message.reply_text(truncate(format_failures(rows)))


def format_retry(summary: RetrySummary) -> str:
    if not summary.reset_entries:
        return "没有匹配的 skipped 行，checkpoint 未改动"
    lines = [f"已重置 {len(summary.reset_entries)} 条 skipped 行为 retrying（attempts 清零）"]
    if summary.rollback:
        for chat_id, (old_cp, new_cp) in summary.rollback.items():
            lines.append(f"checkpoint 回退 chat={chat_id}: {old_cp} → {new_cp}")
    else:
        lines.append("checkpoint 未回退（目标不小于当前值）")
    lines += listed([f"msg={msg_id}" for _, msg_id in summary.reset_entries])
    lines.append("下轮扫描重扫这些消息")
    return "\n".join(lines)


async def _cmd_retry(ctx: BotContext, message: Message, args: list[str]) -> None:
    ids: list[int] = []
    for arg in args:
        try:
            ids.append(int(arg))
        except ValueError:
            await message.reply_text(f"用法：/retry [消息 id ...]（{arg} 不是整数）")
            return
    # 写命令与扫描轮共用一把锁：等当前扫描轮跑完再改失败账与 checkpoint，
    # 扫描也不会中途撞上被改掉的起点。空参数 = 全部 skipped
    async with ctx.lock:
        summary = ctx.db.retry_skipped(ids or None)
    await message.reply_text(truncate(format_retry(summary)))


async def _cmd_backup(ctx: BotContext, message: Message, args: list[str]) -> None:
    # 同样拿锁：备份编排要写 BACKUP_DIR，且与扫描轮抢同一个 db 连接池
    async with ctx.lock:
        dest = await ctx.run_backup()
    await message.reply_text(f"备份完成：{os.path.basename(dest)}")


async def _cmd_search(ctx: BotContext, message: Message, args: list[str]) -> None:
    query = " ".join(args)
    if not query:
        await message.reply_text("用法：/search 关键词（每个关键词至少 3 个字符）")
        return
    rows = ctx.db.search(query, limit=SEARCH_LIMIT)
    if not rows:
        await message.reply_text(
            f"没搜到跟「{query}」相关的内容（提示：每个关键词至少要 3 个字符）")
        return
    # 格式与命令行 search.py 共用 format_result，两处不会各排各的
    body = "\n".join(format_result(r) for r in rows)
    await message.reply_text(truncate(f"搜索「{query}」：\n{body}"))


# 命令表：名字 → (实现, 菜单说明)。这一张表同时驱动分发、BotFather 菜单与未知命令的
# 用法提示，加命令只改这里一处
COMMANDS: dict[str, tuple[CommandHandler, str]] = {
    "stats": (_cmd_stats, "归档统计"),
    "search": (_cmd_search, "搜索归档：/search 关键词"),
    "failures": (_cmd_failures, "列出失败账"),
    "retry": (_cmd_retry, "重试失败的条目：/retry [消息 id ...]"),
    "backup": (_cmd_backup, "立即备份数据库"),
}


def usage_text() -> str:
    return "可用命令：\n" + "\n".join(
        f"/{name} - {desc}" for name, (_, desc) in COMMANDS.items())


async def handle_message(ctx: BotContext, message: Message) -> None:
    """
    命令入口。非白名单**直接 return 不回复**：回一句「无权限」等于向陌生人确认 bot
    存在、可以枚举命令。未知命令只对白名单用户回用法提示。
    """
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
        # 命令失败不能让它抛进 dispatcher（那里没人回告，admin 只看到命令没反应）：
        # 记全栈，并把错误原文回给 admin
        logger.exception("bot 命令 /%s 执行失败", name)
        await message.reply_text(f"命令执行失败：{e}")


def register_handlers(bot: Client, ctx: BotContext) -> None:
    """
    把命令 handler 挂到 bot Client 上。依赖全在 ctx 里，由闭包捕获。

    必须在 client.start() 之后调用（dispatcher 那时才在跑，add_handler 会把新
    handler 排进处理队列）。
    """

    @bot.on_message()
    async def _on_message(_client: Client, message: Message) -> None:
        await handle_message(ctx, message)


async def register_command_menu(bot: Client) -> None:
    """
    注册 BotFather 侧的原生命令菜单（输入框里的补全）。菜单项来自 COMMANDS 表，
    与 handler 共用一处真相。

    失败只记 warning：菜单是锦上添花，命令本身照常可用，不该因为一次 flood 就起不来。
    """
    try:
        await bot.set_bot_commands([
            BotCommand(name, desc) for name, (_, desc) in COMMANDS.items()
        ])
    except Exception:
        logger.warning("注册 bot 命令菜单失败（命令仍可用）", exc_info=True)
