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
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import NamedTuple

from db import ArchiveDB, PurgeSummary, RetrySummary, Stats
from pyrogram.client import Client
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
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

# 待确认删除的暂存过期时间（秒）。按钮点下去若超过它就当失效，防止没点的确认无限堆积
PENDING_TTL_SECONDS = 600

# checkpoint 回退跨度超过它就在预览里高亮警告，提示可能选中了 source_message_id 不可信的
# 历史行。镜像 scripts/delete_message.py 的 ROLLBACK_WARN_SPAN；bot 侧靠人点按钮确认，
# 不设 --force 等价物
ROLLBACK_WARN_SPAN = 100


@dataclass(frozen=True)
class PendingDelete:
    """一次 /delete 待确认的载荷：只带预览时真正命中的入口 id 与当时算出的 checkpoint
    回退计划。确认时据此复核——删的只认预览列过的 id（TTL 内新归档的不会被顺带删），
    回退跨度越过警戒线而预览没警告过就拒（预览→确认之间可能又扫描一轮把跨度撑大）。"""

    found_ids: tuple[int, ...]                          # 预览命中的 source_message_id，确认只删这些
    rollback: bool
    cp_changes: tuple[tuple[str, int, int], ...] = ()   # 预览时算出的 (chat, old_cp, new_cp)，供确认复核


class PendingStore[T]:
    """
    confirm-then-act 的通用待确认暂存：以预览消息的 message_id 为 key，存待执行载荷。

    横切关注点集中在这里，业务数据（载荷）不进来：pop-once 防连点重放、TTL 过期防泄漏、
    发起人绑定（只有发起者本人能确认）。进程重启丢暂存 —— 点旧按钮会 take 落空当失效，可接受。
    只被 bot dispatcher 上的协程读写，不与扫描轮共享。
    """

    def __init__(self, ttl_seconds: int = PENDING_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        self._items: dict[int, tuple[T, int, float]] = {}   # message_id -> (载荷, 发起人, 存入时刻)

    def _prune(self, now: float) -> None:
        expired = [k for k, (_, _, ts) in self._items.items() if now - ts > self._ttl]
        for k in expired:
            del self._items[k]

    def put(self, message_id: int, payload: T, requester_id: int) -> None:
        now = time.monotonic()
        self._prune(now)
        self._items[message_id] = (payload, requester_id, now)

    def take(self, message_id: int, by_user: int) -> T | None:
        now = time.monotonic()
        entry = self._items.get(message_id)
        if entry is None:
            return None
        payload, requester_id, ts = entry
        if by_user != requester_id or now - ts > self._ttl:
            return None
        del self._items[message_id]     # pop-once：命中即移除
        return payload


@dataclass(frozen=True)
class BotContext:
    """bot 命令的运行时依赖。全部构造时注入，模块级不留全局。"""

    db: ArchiveDB
    lock: asyncio.Lock                             # 与扫描循环共用：写命令与扫描轮互斥
    admin_ids: frozenset[int]                      # 空白名单 = 谁都不授权
    run_backup: Callable[[], Awaitable[str]]       # 打快照 + 保留 + 可选上传，返回快照路径
    pending: PendingStore[PendingDelete]           # /delete 待确认暂存，构造时注入


# 命令实现签名：依赖全在 ctx 里，回复走 message.reply_text，不需要 Client 参数
CommandHandler = Callable[[BotContext, Message, list[str]], Awaitable[None]]

# callback 实现签名：arg 是 callback_data 冒号后那段
CallbackHandler = Callable[[BotContext, CallbackQuery, str], Awaitable[None]]


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


def parse_callback(data: str | None) -> tuple[str, str] | None:
    """'del:ok' → ('del', 'ok')；'del' → ('del', '')；无 data / 无 action 返回 None。"""
    if not data:
        return None
    action, _, arg = data.partition(":")
    if not action:
        return None
    return action, arg


def user_allowed(user, admin_ids: frozenset[int]) -> bool:
    """白名单判定。user 为空（频道匿名、sender_chat 发帖）一律拒。命令与 callback 两处共用。"""
    return user is not None and user.id in admin_ids


def is_allowed(message: Message, admin_ids: frozenset[int]) -> bool:
    """白名单判定（消息入口）。委托 user_allowed，与 callback 入口共用同一判定。"""
    return user_allowed(message.from_user, admin_ids)


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


class DeletePreview(NamedTuple):
    """/delete 预览所需的、已从库里取好的数据。格式化纯函数只吃它，不再碰 IO。"""

    rows: list                                   # messages 行（sqlite3.Row 或 dict）
    missing: list[int]                           # 请求了但没找到的入口 id
    rollback: bool
    cp_changes: list[tuple[str, int, int]]       # (chat, old_cp, new_cp)，仅 rollback 时非空
    cleared_failures: list[tuple[str, int]]      # (chat, msg_id)，将被清的失败账


def parse_delete_args(args: list[str]) -> tuple[list[int], bool]:
    """
    解析 /delete 参数：整数是入口 id，标志词 rollback/keep 决定是否回退 checkpoint。
    默认不回退。非法输入 / 冲突标志 / 无 id 时抛 ValueError（消息即用法文本）。
    """
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


def format_delete_preview(preview: DeletePreview) -> str:
    head = f"将删除 {len(preview.rows)} 条记录，确认后不可恢复："
    lines = [
        f"  msg={r['source_message_id']} chat={r['source_chat_id']} "
        f"fuid={r['file_unique_id']} sender={one_line(r['sender'], 30)} "
        f"caption={one_line(r['caption'], 40)}"
        for r in preview.rows
    ]
    body = [head, *listed(lines)]
    if preview.missing:
        body.append(f"未找到：{preview.missing}")
    if preview.cleared_failures:
        body.append(f"将清除失败账 {len(preview.cleared_failures)} 条")
    if not preview.rollback:
        body.append("checkpoint 不变（keep，不重扫）")
    elif not preview.cp_changes:
        body.append("checkpoint 不变（目标不小于当前值）")
    else:
        body.append("checkpoint 将回退（下轮扫描重抓）：")
        for chat_id, old_cp, new_cp in preview.cp_changes:
            span = old_cp - new_cp
            warn = f"  ⚠ 回退 {span} 条 > {ROLLBACK_WARN_SPAN}，请确认没选错行" \
                if span > ROLLBACK_WARN_SPAN else ""
            body.append(f"  chat={chat_id}: {old_cp} → {new_cp}（回退 {span} 条）{warn}")
    body.append("点「确认删除」执行，「取消」放弃。")
    return "\n".join(body)


def format_purge(summary: PurgeSummary, payload: PendingDelete) -> str:
    lines = [(f"已删除 {summary.deleted_messages} 条消息记录，"
              f"{len(summary.deleted_files)} 条文件记录")]
    if summary.cleared_failures:
        lines.append(f"清除失败账 {len(summary.cleared_failures)} 条")
    if not payload.rollback:
        lines.append("checkpoint 未回退（按 keep 保留）")
    elif summary.rollback:
        for chat_id, (old_cp, new_cp) in summary.rollback.items():
            lines.append(f"checkpoint 回退 chat={chat_id}: {old_cp} → {new_cp}")
    else:
        lines.append("checkpoint 未回退（目标不小于当前值）")
    return "\n".join(lines)


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


def _gather_delete_preview(db: ArchiveDB, ids: list[int], rollback: bool) -> DeletePreview:
    """预览所需数据的 IO 汇集：查行、算缺失、算将清失败账，rollback 时算 checkpoint 回退。"""
    rows = db.find_messages_by_source_ids(ids)
    found = {r["source_message_id"] for r in rows}
    missing = [i for i in ids if i not in found]
    entry_keys = sorted({(str(r["source_chat_id"]), r["source_message_id"])
                         for r in rows if r["source_chat_id"]})
    cleared = [(c, m) for c, m in entry_keys if db.get_failure(c, m) is not None]
    cp_changes: list[tuple[str, int, int]] = []
    if rollback:
        chat_min: dict[str, int] = {}
        for r in rows:
            if r["source_chat_id"]:
                c = str(r["source_chat_id"])
                chat_min[c] = min(chat_min.get(c, r["source_message_id"]),
                                  r["source_message_id"])
        for c, min_id in chat_min.items():
            old_cp = db.get_checkpoint(c)
            new_cp = min_id - 1
            if new_cp < old_cp:
                cp_changes.append((c, old_cp, new_cp))
    return DeletePreview(rows, missing, rollback, cp_changes, cleared)


async def _cmd_delete(ctx: BotContext, message: Message, args: list[str]) -> None:
    try:
        ids, rollback = parse_delete_args(args)
    except ValueError as e:
        await message.reply_text(str(e))
        return
    user = message.from_user
    if user is None:            # is_allowed 已保证非空；就地收窄给 pyright
        return
    preview = _gather_delete_preview(ctx.db, ids, rollback)
    if not preview.rows:
        await message.reply_text(f"没找到 source_message_id in {ids} 的记录")
        return
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("确认删除", callback_data="del:ok"),
        InlineKeyboardButton("取消", callback_data="del:no"),
    ]])
    sent = await message.reply_text(
        truncate(format_delete_preview(preview)), reply_markup=keyboard)
    # 只暂存预览真正命中的 id：确认时 purge 就不会碰到 TTL 内新归档、预览里没出现过的条目
    found_ids = sorted({r["source_message_id"] for r in preview.rows})
    ctx.pending.put(
        sent.id, PendingDelete(tuple(found_ids), rollback, tuple(preview.cp_changes)), user.id)


def _rollback_drift(shown: tuple[tuple[str, int, int], ...],
                    actual: list[tuple[str, int, int]]) -> str | None:
    """以确认时「真正要发生的回退」actual 为准，对比预览警告过的 chat。预览→确认之间可能
    又扫描一轮（TTL 600s > 扫描间隔 300s）把 checkpoint 推高——甚至预览时判「目标不小于当前
    值」不回退、确认时却要回退，所以不能只看预览列出的 shown。某 chat 现在越过
    ROLLBACK_WARN_SPAN、而预览没就它警告过（span 未越线，含预览压根没打算回退它），返回
    漂移描述让上层拒绝并要求重看；否则 None。"""
    warned = {c for c, old, new in shown if old - new > ROLLBACK_WARN_SPAN}
    for c, old, new in actual:
        span = old - new
        if span > ROLLBACK_WARN_SPAN and c not in warned:
            return f"chat={c} 回退 {span} 条越过警戒线（预览未警告）"
    return None


async def _cb_delete(ctx: BotContext, cq: CallbackQuery, arg: str) -> None:
    if arg not in ("ok", "no"):
        await cq.answer("未知操作")
        return
    msg = cq.message
    user = cq.from_user
    if msg is None or user is None:          # 老消息/inline 场景 message 可能为空
        await cq.answer("该确认已失效，请重新 /delete", show_alert=True)
        return
    payload = ctx.pending.take(msg.id, user.id)
    if payload is None:                      # 过期 / 已处理 / 非本人
        await cq.answer("该确认已失效或不属于你，请重新 /delete", show_alert=True)
        return
    if arg == "no":
        await msg.edit_text("已取消删除")
        await cq.answer("已取消")
        return
    # 真删走共享锁：等当前扫描轮跑完再改库，扫描也不撞上被改的 checkpoint
    async with ctx.lock:
        # 预览是快照；确认前可能又扫了一轮把 checkpoint 推高。锁内按「确认时真正要发生的回退」
        # 复核（含预览时不打算回退、现在却要回退的 chat）：越过警戒线而预览没警告过就拒，逼重看
        fresh = _gather_delete_preview(ctx.db, list(payload.found_ids), payload.rollback)
        drift = _rollback_drift(payload.cp_changes, fresh.cp_changes)
        if drift is not None:
            await msg.edit_text(
                f"频道在你确认前又收了新消息，回退跨度越过警戒线（{drift}），"
                "已取消以防误伤。请重新 /delete 查看最新预览。")
            await cq.answer("已取消（状态已变化）", show_alert=True)
            return
        summary = ctx.db.purge_messages(list(payload.found_ids), rollback=payload.rollback)
    await msg.edit_text(format_purge(summary, payload))   # edit 不带 keyboard，顺带抹掉按钮
    await cq.answer("已删除")


# callback 路由表：action → 实现。加按钮功能只在这里加一项（对称 COMMANDS）
CALLBACKS: dict[str, CallbackHandler] = {
    "del": _cb_delete,
}


# 命令表：名字 → (实现, 菜单说明)。这一张表同时驱动分发、BotFather 菜单与未知命令的
# 用法提示，加命令只改这里一处
COMMANDS: dict[str, tuple[CommandHandler, str]] = {
    "stats": (_cmd_stats, "归档统计"),
    "search": (_cmd_search, "搜索归档：/search 关键词"),
    "failures": (_cmd_failures, "列出失败账"),
    "retry": (_cmd_retry, "重试失败的条目：/retry [消息 id ...]"),
    "backup": (_cmd_backup, "立即备份数据库"),
    "delete": (_cmd_delete, "删除条目并可选回退：/delete <id...> [rollback]"),
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


async def handle_callback(ctx: BotContext, cq: CallbackQuery) -> None:
    """
    callback 入口。按钮在聊天记录里长期存在，非白名单也可能点到 —— 一律先鉴权。
    每条 callback 都要 answer 一次以清掉客户端转圈；异常兜底记全栈并 answer。
    """
    if not user_allowed(cq.from_user, ctx.admin_ids):
        await cq.answer()                    # 静默清转圈，不泄露 bot 存在
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
    """
    把命令 handler 挂到 bot Client 上。依赖全在 ctx 里，由闭包捕获。

    必须在 client.start() 之后调用（dispatcher 那时才在跑，add_handler 会把新
    handler 排进处理队列）。
    """

    @bot.on_message()
    async def _on_message(_client: Client, message: Message) -> None:
        await handle_message(ctx, message)

    @bot.on_callback_query()
    async def _on_callback(_client: Client, cq: CallbackQuery) -> None:
        await handle_callback(ctx, cq)


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
