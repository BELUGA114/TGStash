"""forward_origin 五个变体的归一化测试。纯函数，不连 Telegram。"""

from types import SimpleNamespace

from origin import (
    ORIGIN_LINK,
    ORIGIN_ORIGINAL,
    normalize_origin,
    origin_from_link,
)


def _origin(type_value, **kwargs):
    """模拟 MessageOrigin*：type 是枚举，取值靠 .value"""
    return SimpleNamespace(type=SimpleNamespace(value=type_value), **kwargs)


def _msg(forward_origin=None, message_id=7, chat=None):
    return SimpleNamespace(id=message_id, forward_origin=forward_origin, chat=chat)


def test_channel_origin():
    chat = SimpleNamespace(id=-1001111111111, title="某个公开频道")
    result = normalize_origin(_msg(_origin("channel", chat=chat, message_id=42)))
    assert result == {
        "origin_chat_id": "-1001111111111",
        "origin_message_id": 42,
        "origin_title": "某个公开频道",
        "origin_type": "channel",
    }


def test_chat_origin():
    sender_chat = SimpleNamespace(id=-1002222222222, title="某个群组")
    result = normalize_origin(_msg(_origin("chat", sender_chat=sender_chat)))
    assert result["origin_chat_id"] == "-1002222222222"
    assert result["origin_message_id"] is None
    assert result["origin_title"] == "某个群组"
    assert result["origin_type"] == "chat"


def test_user_origin_prefers_first_name():
    user = SimpleNamespace(id=12345, first_name="阿猫", username="cat")
    result = normalize_origin(_msg(_origin("user", sender_user=user)))
    assert result["origin_chat_id"] == "12345"
    assert result["origin_title"] == "阿猫"
    assert result["origin_type"] == "user"


def test_user_origin_falls_back_to_username():
    user = SimpleNamespace(id=12345, first_name=None, username="cat")
    assert normalize_origin(_msg(_origin("user", sender_user=user)))["origin_title"] == "cat"


def test_hidden_user_origin():
    result = normalize_origin(_msg(_origin("hidden_user", sender_user_name="匿名用户")))
    assert result["origin_chat_id"] is None
    assert result["origin_title"] == "匿名用户"
    assert result["origin_type"] == "hidden_user"


def test_import_origin():
    result = normalize_origin(_msg(_origin("import", sender_user_name="导入来源")))
    assert result["origin_title"] == "导入来源"
    assert result["origin_type"] == "import"


def test_no_forward_origin_is_original_not_null():
    """原创直发必须落 'original'。若也落 NULL，回填脚本会永久反复选中它"""
    result = normalize_origin(_msg(None))
    assert result["origin_type"] == ORIGIN_ORIGINAL
    assert result["origin_chat_id"] is None
    assert result["origin_message_id"] is None
    assert result["origin_title"] is None


def test_unknown_origin_type_degrades_gracefully():
    """Telegram 将来加新变体时不能炸，落 unknown 让回填不再重试"""
    result = normalize_origin(_msg(_origin("some_future_type")))
    assert result["origin_type"] == "unknown"


def test_origin_from_link_uses_source_chat():
    """路径二：消息直接从源频道读取，来源就是它自己所在的聊天"""
    chat = SimpleNamespace(id=-1003333333333, title="私有频道")
    result = origin_from_link(_msg(None, message_id=99, chat=chat))
    assert result == {
        "origin_chat_id": "-1003333333333",
        "origin_message_id": 99,
        "origin_title": "私有频道",
        "origin_type": ORIGIN_LINK,
    }


def test_real_kurigram_enum_values_match_origin_type():
    """
    origin_type 的取值直接来自 MessageOriginType.value。
    如果 Kurigram 改了枚举字符串，这里会先报警而不是静默写错数据。
    """
    from pyrogram import enums

    assert {m.value for m in enums.MessageOriginType} == {
        "channel", "chat", "user", "hidden_user", "import",
    }
