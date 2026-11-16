"""确认载荷只在内存保存，重启后旧按钮自然失效。"""

from __future__ import annotations

import time

PENDING_TTL_SECONDS = 600


class PendingStore[T]:
    """发起人绑定防代点，单次消费防重放，过期清理避免未确认操作持续堆积。"""

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
        del self._items[message_id]
        return payload
