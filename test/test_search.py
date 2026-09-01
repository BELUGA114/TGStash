"""search.py 的链接拼装测试。私有频道 -100 前缀的处理容易写错，锁一下。"""

from search import archive_link


def test_private_channel_strips_100_prefix():
    assert archive_link("-1009876543210", 42) == "https://t.me/c/9876543210/42"


def test_plain_negative_id_strips_only_sign():
    assert archive_link("-1234", 7) == "https://t.me/c/1234/7"


def test_positive_id_kept_as_is():
    assert archive_link("1234", 7) == "https://t.me/c/1234/7"
