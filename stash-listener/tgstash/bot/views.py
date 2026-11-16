from __future__ import annotations

from tgstash.bot.operations import ROLLBACK_WARN_SPAN, DeletePreview, PendingDelete
from tgstash.storage.db import PurgeSummary, RetrySummary, Stats

TELEGRAM_TEXT_LIMIT = 4096
# 先显示总数，再限制明细，避免截断后连操作范围都看不见。
MAX_LISTED_ITEMS = 20


def truncate(text: str) -> str:
    if len(text) <= TELEGRAM_TEXT_LIMIT:
        return text
    return text[:TELEGRAM_TEXT_LIMIT - 1] + "…"


def listed(items: list[str]) -> list[str]:
    if len(items) <= MAX_LISTED_ITEMS:
        return items
    return items[:MAX_LISTED_ITEMS] + [f"…还有 {len(items) - MAX_LISTED_ITEMS} 条"]


def one_line(text: str | None, limit: int = 120) -> str:
    return (text or "").replace("\n", " ")[:limit]


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


def format_failures(rows) -> str:
    lines = [
        f"[{r['status']}] msg={r['source_message_id']} 阶段={r['failure_stage']} "
        f"次数={r['attempt_count']} {one_line(r['last_error'])}"
        for r in rows
    ]
    return "\n".join([f"失败账共 {len(rows)} 条", *listed(lines)])


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
