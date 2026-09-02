"""入口/条目/结果值对象测试。纯数据，用 SimpleNamespace 桩，不连 Telegram。"""

import dataclasses
from types import SimpleNamespace

import pytest
from archive_entry import ROUTE_FORWARD, ROUTE_LINK, ArchiveItem, Entry, Outcome


def _msg(message_id, chat_id=-1001234567890, title="接收频道"):
    return SimpleNamespace(id=message_id, chat=SimpleNamespace(id=chat_id, title=title))


def _item(media=None, entry_msg=None, route=ROUTE_FORWARD, link=None):
    entry_msg = entry_msg or _msg(1)
    return ArchiveItem(
        media=media or entry_msg,
        entry=Entry(message=entry_msg, route=route),
        link=link,
    )


class TestEntry:
    def test_exposes_entry_identity(self):
        entry = Entry(message=_msg(100), route=ROUTE_FORWARD)
        assert entry.message_id == 100
        assert entry.chat_id == "-1001234567890"
        assert entry.chat_title == "接收频道"

    def test_chat_id_is_string_like_db_columns(self):
        """chat_id 统一存字符串，与 db.py 其他 chat_id 列一致"""
        assert isinstance(Entry(message=_msg(1), route=ROUTE_LINK).chat_id, str)

    def test_missing_chat_returns_none(self):
        """chat 缺失不抛异常，调用方自己兜底"""
        entry = Entry(message=SimpleNamespace(id=7, chat=None), route=ROUTE_FORWARD)
        assert entry.chat_id is None
        assert entry.chat_title is None

    def test_frozen(self):
        entry = Entry(message=_msg(1), route=ROUTE_FORWARD)
        with pytest.raises(dataclasses.FrozenInstanceError):
            entry.route = ROUTE_LINK  # type: ignore[misc]


class TestArchiveItem:
    def test_link_defaults_to_none(self):
        assert _item().link is None

    def test_link_path_carries_source_message_and_link(self):
        source = _msg(42, chat_id=-1009999999999, title="私有频道")
        item = _item(
            media=source, entry_msg=_msg(300), route=ROUTE_LINK,
            link="https://t.me/c/9999999999/42",
        )
        assert item.media.id == 42
        assert item.entry.message_id == 300
        assert item.entry.route == ROUTE_LINK
        assert item.link.endswith("/42")


class TestOutcome:
    def test_success(self):
        outcome = Outcome.success(_item())
        assert outcome.ok is True
        assert outcome.stage is None
        assert outcome.error is None

    def test_failure_carries_stage_and_error(self):
        outcome = Outcome.failure(_item(), "download", "tdl 返回空路径")
        assert outcome.ok is False
        assert outcome.stage == "download"
        assert outcome.error == "tdl 返回空路径"

    def test_failure_accepts_exception_and_normalizes_text(self):
        """
        异常直接传进来即可，归一化在 Outcome.failure 里。

        无参异常（尤其 assert 失败）的 str() 是空串，落库会让
        archive_failures.last_error 为空、告警文案里「最近错误」一片空白。
        以前靠每个调用点自己写 `str(e) or repr(e)`，8 处里 4 处忘了后半截。
        """
        assert Outcome.failure(_item(), "upload", RuntimeError("传不上去")).error == "传不上去"
        assert Outcome.failure(_item(), "process", AssertionError()).error == "AssertionError()"
