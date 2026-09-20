"""search.py 的链接拼装测试。私有频道 -100 前缀的处理容易写错，锁一下。"""

from search import archive_link, format_result


def test_private_channel_strips_100_prefix():
    assert archive_link("-1009876543210", 42) == "https://t.me/c/9876543210/42"


def test_plain_negative_id_strips_only_sign():
    assert archive_link("-1234", 7) == "https://t.me/c/1234/7"


def test_positive_id_kept_as_is():
    assert archive_link("1234", 7) == "https://t.me/c/1234/7"


def _row(**over):
    row = {
        "sent_at": "2026-09-01 10:00:00", "media_kind": "document",
        "origin_title": "某频道", "origin_chat_id": "-1001234",
        "sender": "张三", "caption": "报告\n正文", "file_name": "a.pdf",
        "archived_chat_id": "-1009876543210", "archived_message_id": 7,
    }
    row.update(over)
    return row


def test_format_result_line_carries_origin_caption_and_link():
    line = format_result(_row())

    assert "某频道" in line
    assert "(document)" in line
    assert "张三" in line
    assert "报告 正文" in line          # caption 里的换行压成空格
    assert "[a.pdf]" in line
    assert "https://t.me/c/9876543210/7" in line


def test_format_result_without_archive_link():
    """没归档落点的行不拼链接，也不留一个孤零零的箭头。"""
    line = format_result(_row(archived_chat_id=None, archived_message_id=None))

    assert "->" not in line


def test_format_result_falls_back_to_origin_chat_id():
    line = format_result(_row(origin_title=None))

    assert "-1001234" in line
