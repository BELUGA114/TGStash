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
