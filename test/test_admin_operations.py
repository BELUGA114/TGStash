from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from admin_operations import AdminOperations, DeleteStateChanged, PendingDelete


def test_delete_rechecks_checkpoint_after_waiting_for_scan():
    async def scenario():
        lock = asyncio.Lock()
        checkpoint = 350
        reads = []

        def get_checkpoint(chat):
            reads.append((checkpoint, lock.locked()))
            return checkpoint

        db = SimpleNamespace(
            find_messages_by_source_ids=lambda ids: [
                {"source_chat_id": "c", "source_message_id": 300}],
            get_failure=lambda c, m: None,
            get_checkpoint=get_checkpoint,
            purge_messages=lambda *a, **k: pytest.fail("未预告的大跨度回退不应执行"),
        )
        operations = AdminOperations(db, lock)
        payload = PendingDelete((300,), True, (("c", 350, 299),))
        async with lock:
            task = asyncio.create_task(operations.confirm_delete(payload))
            await asyncio.sleep(0)
            assert reads == []
            checkpoint = 500
        with pytest.raises(DeleteStateChanged):
            await task
        assert reads == [(500, True)]
        assert not lock.locked()

    asyncio.run(scenario())
