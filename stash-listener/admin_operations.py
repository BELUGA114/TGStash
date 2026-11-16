"""管理写操作与扫描共用锁，避免失败账和 checkpoint 在一轮扫描中途变化。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import NamedTuple

from db import ArchiveDB, ClearSummary, PurgeSummary, RetrySummary

# 与离线删除工具使用同一警戒跨度；bot 通过预览确认表达知情选择。
ROLLBACK_WARN_SPAN = 100


@dataclass(frozen=True)
class PendingDelete:
    """确认只认预览命中的入口 id；当时的回退计划用于检查未告知的跨度变化。"""

    found_ids: tuple[int, ...]
    rollback: bool
    cp_changes: tuple[tuple[str, int, int], ...] = ()


@dataclass(frozen=True)
class PendingClearAll:
    """预览条数只供展示；清空全部的确认覆盖执行时存在的失败账。"""

    count: int


class DeletePreview(NamedTuple):

    rows: list
    missing: list[int]
    rollback: bool
    cp_changes: list[tuple[str, int, int]]
    cleared_failures: list[tuple[str, int]]

    def confirmation(self) -> PendingDelete:
        # 请求中未命中的 id 不能进入确认载荷，否则会误删预览后才归档的条目。
        found_ids = tuple(sorted({r["source_message_id"] for r in self.rows}))
        return PendingDelete(found_ids, self.rollback, tuple(self.cp_changes))


def _rollback_drift(shown: tuple[tuple[str, int, int], ...],
                    actual: list[tuple[str, int, int]]) -> str | None:
    """预览可能没有回退，确认时却有；必须遍历实际计划，才能检查新增的大跨度回退。"""
    warned = {c for c, old, new in shown if old - new > ROLLBACK_WARN_SPAN}
    for c, old, new in actual:
        span = old - new
        if span > ROLLBACK_WARN_SPAN and c not in warned:
            return f"chat={c} 回退 {span} 条越过警戒线（预览未警告）"
    return None


class DeleteStateChanged(RuntimeError):
    pass


class AdminOperations:
    def __init__(self, db: ArchiveDB, lock: asyncio.Lock):
        self.db = db
        self.lock = lock

    def preview_delete(self, ids: list[int], rollback: bool) -> DeletePreview:
        rows = self.db.find_messages_by_source_ids(ids)
        found = {r["source_message_id"] for r in rows}
        missing = [i for i in ids if i not in found]
        entry_keys = sorted({(str(r["source_chat_id"]), r["source_message_id"])
                             for r in rows if r["source_chat_id"]})
        cleared = [(c, m) for c, m in entry_keys if self.db.get_failure(c, m) is not None]
        cp_changes: list[tuple[str, int, int]] = []
        if rollback:
            chat_min: dict[str, int] = {}
            for r in rows:
                if r["source_chat_id"]:
                    c = str(r["source_chat_id"])
                    chat_min[c] = min(chat_min.get(c, r["source_message_id"]),
                                      r["source_message_id"])
            for c, min_id in chat_min.items():
                old_cp = self.db.get_checkpoint(c)
                new_cp = min_id - 1
                if new_cp < old_cp:
                    cp_changes.append((c, old_cp, new_cp))
        return DeletePreview(rows, missing, rollback, cp_changes, cleared)


    async def confirm_delete(self, payload: PendingDelete) -> PurgeSummary:
        async with self.lock:
            # 等待扫描结束后再复核；等待锁之前的 checkpoint 可能已经过时。
            fresh = self.preview_delete(list(payload.found_ids), payload.rollback)
            drift = _rollback_drift(payload.cp_changes, fresh.cp_changes)
            if drift is not None:
                raise DeleteStateChanged(drift)
            return self.db.purge_messages(list(payload.found_ids), rollback=payload.rollback)

    async def clear_failures(self, ids: list[int] | None) -> ClearSummary:
        async with self.lock:
            return self.db.clear_failures(ids)

    async def retry_skipped(self, ids: list[int] | None) -> RetrySummary:
        async with self.lock:
            return self.db.retry_skipped(ids)

    async def backup(self, run_backup: Callable[[], Awaitable[str]]) -> str:
        # 自动备份也在扫描锁内，避免两个快照任务同时操作保留目录。
        async with self.lock:
            return await run_backup()
