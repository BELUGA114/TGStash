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
