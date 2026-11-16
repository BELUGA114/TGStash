"""链接路径保留接收频道入口身份，复用归档管道而不自行结算失败账。"""

from __future__ import annotations

import logging
import re

from archive_entry import ArchiveItem, Entry, Outcome
from media_ops import get_media
from pipeline import ArchivePipeline
from pyrogram.client import Client
from pyrogram.types import Message

logger = logging.getLogger(__name__)

TME_LINK_RE = re.compile(r"https?://t\.me/\S+")
# 中文句末标点也必须剥除，否则合法链接被当成解析失败而跳过。
LINK_TRAILING_PUNCT = ".,;:!?)]>" + "。，、；：！？）】》」』"
TELEGRAM_TEXT_LIMIT = 4096
LINK_ARCHIVED_MARK = "✅ 已归档"


def extract_tme_links(text: str) -> list[str]:
    links: list[str] = []
    for raw in TME_LINK_RE.findall(text or ""):
        link = raw.rstrip(LINK_TRAILING_PUNCT)
        if link and link not in links:
            links.append(link)
    return links


def entry_text(message: Message) -> str:
    """检测与提取共用文本字段，避免检测有链接却提取为空后静默推进 checkpoint。"""
    return message.text or message.caption or ""


def marked_link_text(text: str) -> str:
    """截断只砍原文，确保长文本也能保留归档标记。"""
    suffix = f"\n{LINK_ARCHIVED_MARK}"
    return text[:TELEGRAM_TEXT_LIMIT - len(suffix)] + suffix


def parse_message_link(link: str) -> tuple[int | str, int]:
    """私有频道必须返回整数，否则 Kurigram 会把数字字符串当电话号码解析。"""
    link = link.strip().split("?")[0].rstrip("/")
    m = re.match(r"https?://t\.me/c/(\d+)/(\d+)$", link)
    if m:
        return (int(f"-100{m.group(1)}"), int(m.group(2)))
    m = re.match(r"https?://t\.me/([^/]+)/(\d+)$", link)
    if m:
        return (f"@{m.group(1)}", int(m.group(2)))
    raise ValueError(f"无法解析链接：{link}")


async def process_link_message(client: Client, pipeline: ArchivePipeline, entry: Entry) -> list[Outcome]:
    """无效链接或不可访问消息跳过；获取异常留待重试，失败账交给入口结算。"""
    message = entry.message
    text = entry_text(message)
    links = extract_tme_links(text)

    outcomes: list[Outcome] = []
    for link in links:
        try:
            chat, msg_id = parse_message_link(link)
        except ValueError:
            logger.warning("无法解析链接，跳过：%s", link)
            continue

        try:
            msg = await client.get_messages(chat, msg_id)
        except Exception as e:
            logger.warning("获取链接消息 %s 失败，下轮重试", link, exc_info=True)
            outcomes.append(Outcome.failure(
                ArchiveItem(media=message, entry=entry, link=link), "download", e))
            continue

        if msg is None:
            logger.warning("消息 %s 不可访问或已删除", link)
            continue

        if msg.media_group_id:
            try:
                group = sorted(await client.get_media_group(chat, msg_id),
                               key=lambda m: m.id)
            except Exception as e:
                logger.warning("获取媒体组 %s 失败，下轮重试", link, exc_info=True)
                outcomes.append(Outcome.failure(
                    ArchiveItem(media=msg, entry=entry, link=link), "download", e))
                continue
            # 只挂一个 URL，tdl 才会用 --group 整组下载。
            items = [
                ArchiveItem(media=m, entry=entry, link=link if m.id == msg.id else None)
                for m in group
            ]
            outcomes.extend(await pipeline.archive_batch(items))
            logger.info("链接 %s → 媒体组 %s 张", link, len(group))
        else:
            kind, _ = get_media(msg)
            if kind is None:
                logger.info("链接 %s → 无媒体", link)
                continue
            outcomes.append(await pipeline.archive_one(
                ArchiveItem(media=msg, entry=entry, link=link)))
            logger.info("链接 %s → %s", link, kind)

    # 去重也算成功；没有归档结果时不应给入口打成功标记。
    if any(o.ok for o in outcomes) and entry.chat_id is not None:
        try:
            await client.edit_message_text(
                int(entry.chat_id), entry.message_id, marked_link_text(text))
        except Exception:
            logger.warning("编辑链接消息标记失败（已归档结果不受影响）：%s",
                           entry.message_id, exc_info=True)

    return outcomes


def has_tme_link(message: Message) -> bool:
    return bool(TME_LINK_RE.search(entry_text(message)))
