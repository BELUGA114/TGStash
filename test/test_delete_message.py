"""checkpoint 回退跨度守卫测试。"""

import pytest
from delete_message import ROLLBACK_WARN_SPAN, rollback_span, too_far_back


def test_span_is_distance_from_current_checkpoint():
    assert rollback_span(old_cp=15000, new_cp=41) == 14959


def test_no_span_when_checkpoint_would_not_move():
    """new_cp >= old_cp 时不会回退，跨度记 0"""
    assert rollback_span(old_cp=100, new_cp=200) == 0
    assert rollback_span(old_cp=100, new_cp=100) == 0


def test_normal_rollback_is_allowed():
    assert too_far_back(old_cp=15000, new_cp=14990) is False


def test_huge_rollback_is_flagged():
    """
    历史路径二的行 source_message_id 存的是源频道 id。拿它回退接收频道
    checkpoint 会把 15000 打回 41，BATCH_SIZE=10 / 每 300 秒一轮的话要重扫好几天。
    """
    assert too_far_back(old_cp=15000, new_cp=41) is True


@pytest.mark.parametrize("span", [ROLLBACK_WARN_SPAN - 1, ROLLBACK_WARN_SPAN])
def test_threshold_boundary(span):
    assert too_far_back(old_cp=span + 10, new_cp=10) is (span > ROLLBACK_WARN_SPAN)


def _seed(tmp_path, *, entry_id=300, chat="-1001234567890"):
    """建一个只有「入口 entry_id 归档过一次、且留了失败账」的库。"""
    from db import ArchiveDB

    path = str(tmp_path / "archive.db")
    db = ArchiveDB(path)
    db.ensure_channel(chat, "manual_forward")
    db.set_checkpoint(chat, entry_id + 50)
    db.record_archived(
        file_unique_id="FUID_DEL", sha256="a" * 64, size=2048, source="link",
        source_channel=chat, archived_chat_id="-1009876543210",
        archived_message_id=777, source_chat_id=chat, source_message_id=entry_id,
        caption="要被删掉的内容", media_kind="document", origin_type="link",
    )
    # 路径二一个入口对多个条目：先成功的留下 messages 行，后失败的留下失败账
    db.increment_failure(chat, entry_id, "upload", "同一入口的另一条失败了")
    return path, db


def test_main_clears_failure_ledger_for_deleted_entries(tmp_path, monkeypatch):
    """
    回退 checkpoint 必须一并清失败账。

    残留的 attempt_count（或 status='skipped'）会让重来的那次一失败就直接跳过、
    不再真的重试满 N 轮 —— 「删掉记录让它重新归档」于是只兑现了一半。
    """
    import delete_message

    path, db = _seed(tmp_path)
    monkeypatch.setattr("sys.argv", ["delete_message.py", "300", "--db", path])

    delete_message.main()

    assert db.get_failure("-1001234567890", 300) is None
    assert db.get_checkpoint("-1001234567890") == 299
    assert db.find_by_unique_id("FUID_DEL") is None


def test_dry_run_keeps_failure_ledger(tmp_path, monkeypatch):
    import delete_message

    path, db = _seed(tmp_path)
    monkeypatch.setattr(
        "sys.argv", ["delete_message.py", "300", "--db", path, "--dry-run"])

    delete_message.main()

    assert db.get_failure("-1001234567890", 300) is not None
    assert db.get_checkpoint("-1001234567890") == 350
