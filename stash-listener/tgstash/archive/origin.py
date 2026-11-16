"""
转发来源归一化：把 Kurigram 的 forward_origin 五个变体压平成四个字段。

Kurigram 2.2.23 用的是 Bot API 7.0 的 message.forward_origin，不是旧 Pyrogram
的 forward_from_* 系列字段。变体的 type 是 MessageOriginType 枚举，取值要用
.value —— str(枚举) 得到的是 'MessageOriginType.CHANNEL'，不是 'channel'。

纯函数，不碰 IO，也不 import pyrogram：只按属性取值，测试里用轻量 stub 即可。
"""

from __future__ import annotations

# 转发五变体之外的三个取值
ORIGIN_LINK = "link"          # 路径二：消息由 t.me 链接直读，来源即所在频道
ORIGIN_ORIGINAL = "original"  # 原创直发到接收频道，没有转发来源
ORIGIN_UNKNOWN = "unknown"    # 认不出的变体，或回填时查不到原消息

# 与 pyrogram.enums.MessageOriginType 的 value 一一对应
_KNOWN_TYPES = ("channel", "chat", "user", "hidden_user", "import")


def _chat_id(obj) -> str | None:
    """chat_id 统一存字符串，与 db.py 其他列的存法保持一致。"""
    value = getattr(obj, "id", None) if obj is not None else None
    return str(value) if value is not None else None


def empty_origin(origin_type: str | None = None) -> dict:
    """四个字段全空的 origin，只带一个 origin_type。"""
    return {
        "origin_chat_id": None,
        "origin_message_id": None,
        "origin_title": None,
        "origin_type": origin_type,
    }


def normalize_origin(message) -> dict:
    """
    把 message.forward_origin 归一到 origin_* 四个字段。

    原创消息（无 forward_origin）返回 origin_* 全 None 但
    origin_type='original'，以此与「未回填的历史行」（origin_type IS NULL）
    区分开——两者都空，但只有 NULL 表示还没回填过。
    """
    origin = getattr(message, "forward_origin", None)
    if origin is None:
        return empty_origin(ORIGIN_ORIGINAL)

    origin_type = getattr(getattr(origin, "type", None), "value", None)
    if origin_type not in _KNOWN_TYPES:
        # Telegram 将来加变体时不能炸，也不能留 NULL 让回填反复重试
        return empty_origin(ORIGIN_UNKNOWN)

    if origin_type == "channel":
        chat = getattr(origin, "chat", None)
        return {
            "origin_chat_id": _chat_id(chat),
            "origin_message_id": getattr(origin, "message_id", None),
            "origin_title": getattr(chat, "title", None),
            "origin_type": origin_type,
        }

    if origin_type == "chat":
        sender_chat = getattr(origin, "sender_chat", None)
        return {
            "origin_chat_id": _chat_id(sender_chat),
            "origin_message_id": None,
            "origin_title": getattr(sender_chat, "title", None),
            "origin_type": origin_type,
        }

    if origin_type == "user":
        user = getattr(origin, "sender_user", None)
        return {
            "origin_chat_id": _chat_id(user),
            "origin_message_id": None,
            "origin_title": getattr(user, "first_name", None) or getattr(user, "username", None),
            "origin_type": origin_type,
        }

    # hidden_user / import：只有一个名字，连 id 都拿不到
    return {
        "origin_chat_id": None,
        "origin_message_id": None,
        "origin_title": getattr(origin, "sender_user_name", None),
        "origin_type": origin_type,
    }


def origin_from_link(message) -> dict:
    """
    路径二的来源：消息是 get_messages(chat, msg_id) 直接从源频道读的，
    不经过转发，所以来源就是它自己所在的聊天。
    """
    chat = getattr(message, "chat", None)
    return {
        "origin_chat_id": _chat_id(chat),
        "origin_message_id": getattr(message, "id", None),
        "origin_title": getattr(chat, "title", None),
        "origin_type": ORIGIN_LINK,
    }
