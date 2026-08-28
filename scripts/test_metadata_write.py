"""入口/来源分离的写入路径测试。不连 Telegram，monkeypatch 掉 listener 的 db。"""

from types import SimpleNamespace

import listener
import pytest
from db import ArchiveDB
from origin import ORIGIN_LINK


@pytest.fixture
def db(tmp_path, monkeypatch):
    real = ArchiveDB(str(tmp_path / "t.db"))
    monkeypatch.setattr(listener, "db", real)
    return real


def _chat(chat_id, title):
    return SimpleNamespace(id=chat_id, title=title)


def _sent(message_id=555):
    return SimpleNamespace(id=message_id)


def _media(file_unique_id="FUID_W", file_name="cat.mp4", mime_type="video/mp4"):
    return SimpleNamespace(file_unique_id=file_unique_id, file_name=file_name,
                           mime_type=mime_type)


def _msg(message_id, chat, forward_origin=None):
    return SimpleNamespace(
        id=message_id, chat=chat, forward_origin=forward_origin,
        date=None, media_group_id=None, from_user=None, sender_chat=None,
    )


def test_forward_path_entry_is_the_message_itself(db: ArchiveDB):
    """路径一：入口就是接收频道那条消息，未转发则来源为 original"""
    entry = _msg(100, _chat(-1001234567890, "接收频道"))
    listener._record_archived_media(
        entry, entry, "FUID_W", "a" * 64, 2048, _sent(), "正文内容",
        "video", _media(),
    )
    row = db.search("正文内容")[0]
    assert row["source_message_id"] == 100
    assert row["source_chat_id"] == "-1001234567890"
    assert row["source_channel_title"] == "接收频道"
    assert row["origin_type"] == "original"
    assert row["file_name"] == "cat.mp4"
    assert row["media_kind"] == "video"
    file_row = db.find_by_unique_id("FUID_W")
    assert file_row["source"] == "manual_forward"
    assert file_row["mime_type"] == "video/mp4"
    assert file_row["media_kind"] == "video"


def test_forwarded_message_records_channel_origin(db: ArchiveDB):
    """路径一的转发消息：来源从 forward_origin 取"""
    origin = SimpleNamespace(
        type=SimpleNamespace(value="channel"),
        chat=_chat(-1001111111111, "某个公开频道"),
        message_id=42,
    )
    entry = _msg(101, _chat(-1001234567890, "接收频道"), forward_origin=origin)
    listener._record_archived_media(
        entry, entry, "FUID_F", "c" * 64, 512, _sent(), "转发的内容",
        "photo", _media("FUID_F", file_name=None, mime_type="image/jpeg"),
    )
    row = db.search("某个公开频道")[0]
    assert row["source_message_id"] == 101
    assert row["origin_chat_id"] == "-1001111111111"
    assert row["origin_message_id"] == 42
    assert row["origin_type"] == "channel"
    assert row["file_name"] is None


def test_link_path_entry_is_the_link_message_not_the_source(db: ArchiveDB):
    """
    路径二回归测试：入口必须是接收频道里那条链接消息，来源才是源频道那条。

    修复前 source_message_id 填的是源频道的 id（这里是 42），
    delete_message.py 会拿 42 去回退接收频道的 checkpoint。
    """
    entry = _msg(300, _chat(-1001234567890, "接收频道"))
    source = _msg(42, _chat(-1009999999999, "私有频道"))
    listener._record_archived_media(
        source, entry, "FUID_L", "b" * 64, 1024, _sent(), "链接来的内容",
        "video", _media("FUID_L"),
    )
    row = db.search("链接来的内容")[0]
    # 入口 = 接收频道的链接消息
    assert row["source_message_id"] == 300
    assert row["source_chat_id"] == "-1001234567890"
    assert row["source_channel_title"] == "接收频道"
    # 来源 = 源频道那条
    assert row["origin_chat_id"] == "-1009999999999"
    assert row["origin_message_id"] == 42
    assert row["origin_title"] == "私有频道"
    assert row["origin_type"] == ORIGIN_LINK
    assert db.find_by_unique_id("FUID_L")["source"] == "link"
