"""
归档管道的值对象：入口、待归档条目、处理结果。

「入口」是接收频道里那条消息 —— checkpoint、失败记账、失败告警、
messages.source_* 一律以它为准。「来源」是被归档的那条媒体消息，
origin_* 从它算。两者曾经以裸参数在同一条调用链里流动，靠每个函数
自己记住该用哪个，记漏了就错位。这里把它们分成两个字段，写错在类型上
就不可能。

纯数据，不 import pyrogram（沿用 origin.py 的惯例：只按属性取值，
测试用 SimpleNamespace 桩即可），也不碰 IO。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyrogram.types import Message

# files.source 的取值。第三个 'tdl_bulk' 属于尚未实现的批量同步路径
ROUTE_FORWARD = "manual_forward"  # 路径一：媒体直接转发进接收频道
ROUTE_LINK = "link"               # 路径二：接收频道里发 t.me 链接


def _error_text(error: object) -> str:
    """
    异常或字符串统一成可落库的文本。

    无参异常（尤其 assert 失败）的 str() 是空串，直接落库会让
    archive_failures.last_error 为空、告警文案里「最近错误」一片空白，只剩 stage 可查。
    归一化收在这里而不是每个调用点各写一遍 `str(e) or repr(e)`：写在调用点就会有一半
    地方忘掉后半截（历史上 8 处里 4 处是裸 `str(e)`）。调用方直接把异常对象传进
    Outcome.failure 即可。
    """
    text = str(error)
    return text if text else repr(error)


@dataclass(frozen=True)
class Entry:
    """
    接收频道里的那条消息 —— 入口。

    message 落到 messages.source_*，route 落到 files.source。
    路径一的入口就是媒体消息本身；路径二的入口是那条链接消息，
    整批媒体共享它。
    """

    message: Message
    route: str

    @property
    def message_id(self) -> int:
        return self.message.id

    @property
    def chat_id(self) -> str | None:
        """入口所在频道。统一存字符串，与 db.py 其他 chat_id 列一致。"""
        chat = getattr(self.message, "chat", None)
        chat_id = getattr(chat, "id", None) if chat is not None else None
        return str(chat_id) if chat_id is not None else None

    @property
    def chat_title(self) -> str | None:
        chat = getattr(self.message, "chat", None)
        return getattr(chat, "title", None) if chat is not None else None


@dataclass(frozen=True)
class ArchiveItem:
    """一条待归档的媒体消息，连同它的入口。"""

    media: Message           # 被归档的媒体消息 —— 来源
    entry: Entry
    # 取到这条媒体的 t.me 链接（路径二）。放在条目而不是入口上：
    # 一条链接消息的文本里可以有多个链接，入口共享、链接每批各一个
    link: str | None = None


@dataclass(frozen=True)
class Outcome:
    """一个条目的处理结果。已归档或去重跳过都算 ok。"""

    item: ArchiveItem
    ok: bool
    stage: str | None = None   # 'download' | 'verify' | 'convert' | 'upload' | 'process'
    error: str | None = None

    @classmethod
    def success(cls, item: ArchiveItem) -> Outcome:
        return cls(item=item, ok=True)

    @classmethod
    def failure(cls, item: ArchiveItem, stage: str, error: object) -> Outcome:
        """error 收异常对象或字符串，一律经 _error_text 归一化后落库。"""
        return cls(item=item, ok=False, stage=stage, error=_error_text(error))
