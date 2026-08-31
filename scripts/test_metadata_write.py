"""入口/来源分离的写入路径测试。不连 Telegram，monkeypatch 掉 listener 的 db。"""

from types import SimpleNamespace

import listener
import pytest
from archive_entry import ROUTE_FORWARD, ROUTE_LINK, ArchiveItem, Entry
from db import ArchiveDB
from origin import ORIGIN_LINK


@pytest.fixture
def db(tmp_path, monkeypatch):
    real = ArchiveDB(str(tmp_path / "t.db"))
    monkeypatch.setattr(listener, "db", real)
    return real


@pytest.fixture
def pipeline(db, make_pipeline):
    """写库路径只需要真 db，client / downloader 不会被调用。"""
    return make_pipeline(db=db)


def _chat(chat_id, title):
    return SimpleNamespace(id=chat_id, title=title)


def _sent(message_id=555):
    return SimpleNamespace(id=message_id)


def _media(file_unique_id="FUID_W", file_name="cat.mp4", mime_type="video/mp4"):
    return SimpleNamespace(file_unique_id=file_unique_id, file_name=file_name,
                           mime_type=mime_type)


def _msg(message_id, chat, forward_origin=None, kind=None, media=None):
    """消息桩。kind/media 把媒体对象挂到对应属性上——kind 由 get_media 推导。"""
    msg = SimpleNamespace(
        id=message_id, chat=chat, forward_origin=forward_origin,
        date=None, media_group_id=None, from_user=None, sender_chat=None,
        caption=None, text=None,
    )
    if kind:
        setattr(msg, kind, media)
    return msg


def _item(media_msg, entry_msg, route=ROUTE_FORWARD, link=None):
    return ArchiveItem(
        media=media_msg, entry=Entry(message=entry_msg, route=route), link=link,
    )


def test_forward_path_entry_is_the_message_itself(db: ArchiveDB, pipeline):
    """路径一：入口就是接收频道那条消息，未转发则来源为 original"""
    entry = _msg(100, _chat(-1001234567890, "接收频道"), kind="video", media=_media())
    pipeline._record_archived_media(_item(entry, entry), "a" * 64, 2048, _sent(), "正文内容")
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


def test_forwarded_message_records_channel_origin(db: ArchiveDB, pipeline):
    """路径一的转发消息：来源从 forward_origin 取"""
    origin = SimpleNamespace(
        type=SimpleNamespace(value="channel"),
        chat=_chat(-1001111111111, "某个公开频道"),
        message_id=42,
    )
    entry = _msg(101, _chat(-1001234567890, "接收频道"), forward_origin=origin,
                 kind="photo", media=_media("FUID_F", file_name=None, mime_type="image/jpeg"))
    pipeline._record_archived_media(_item(entry, entry), "c" * 64, 512, _sent(), "转发的内容")
    row = db.search("某个公开频道")[0]
    assert row["source_message_id"] == 101
    assert row["origin_chat_id"] == "-1001111111111"
    assert row["origin_message_id"] == 42
    assert row["origin_type"] == "channel"
    assert row["file_name"] is None


def test_link_path_entry_is_the_link_message_not_the_source(db: ArchiveDB, pipeline):
    """
    路径二回归测试：入口必须是接收频道里那条链接消息，来源才是源频道那条。

    修复前 source_message_id 填的是源频道的 id（这里是 42），
    delete_message.py 会拿 42 去回退接收频道的 checkpoint。
    """
    entry = _msg(300, _chat(-1001234567890, "接收频道"))
    source = _msg(42, _chat(-1009999999999, "私有频道"),
                  kind="video", media=_media("FUID_L"))
    pipeline._record_archived_media(
        _item(source, entry, ROUTE_LINK), "b" * 64, 1024, _sent(), "链接来的内容",
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


# ═══════════════════════════════════════════
# SHA-256 去重命中分支
# ═══════════════════════════════════════════


def _doc_message(message_id, chat, file_unique_id, file_name="report.pdf"):
    """带 document 的消息桩。document 不走 ffprobe/缩略图，最适合测去重分支。"""
    return SimpleNamespace(
        id=message_id, chat=chat, forward_origin=None,
        date=None, media_group_id=None, caption="", text="",
        from_user=SimpleNamespace(first_name="tester", id=1),
        document=SimpleNamespace(file_unique_id=file_unique_id, file_name=file_name,
                                 mime_type="application/pdf"),
    )


def _run_archive_single(monkeypatch, msg, local_file, *, entry=None, route=ROUTE_FORWARD):
    """跑一遍 archive_single，下载替换成本地已有文件。上传不该发生（去重命中会提前返回）。"""
    import asyncio

    async def fake_download(messages, *a, **k):
        return {messages[0].id: str(local_file)}

    async def boom(*a, **k):
        raise AssertionError("去重命中不应该上传")

    monkeypatch.setattr(listener.tdl_downloader, "download", fake_download)
    monkeypatch.setattr(listener.app, "send_document", boom)
    monkeypatch.setattr(listener, "mark_processed", _noop)
    item = ArchiveItem(media=msg, entry=Entry(message=entry or msg, route=route))
    return asyncio.run(listener.archive_single(item))


async def _noop(*a, **k):
    return None


def _seed_dup(db, local_file):
    """把 local_file 的 sha256 先落库，模拟这个内容已经归档过。"""
    import hashlib

    sha = hashlib.sha256(local_file.read_bytes()).hexdigest()
    db.record_file(
        file_unique_id="FUID_ALREADY", sha256=sha, size=local_file.stat().st_size,
        archived_chat_id="-1009876543210", archived_message_id=888,
        source="manual_forward", source_channel="-1001234567890",
        file_name="旧文件.pdf", mime_type="application/pdf", media_kind="document",
    )
    return sha


def test_sha256_dup_records_file_identity(db: ArchiveDB, tmp_path, monkeypatch):
    """去重命中也要写文件身份——media 就在手边，漏了这三列就永久缺失"""
    local = tmp_path / "same.pdf"
    local.write_bytes(b"\x00" * 2048)
    _seed_dup(db, local)

    msg = _doc_message(700, _chat(-1001234567890, "接收频道"), "FUID_NEW", "新名字.pdf")
    assert _run_archive_single(monkeypatch, msg, local).ok is True

    row = db.find_by_unique_id("FUID_NEW")
    assert row is not None, "去重命中必须留下指向已归档消息的 files 行"
    assert row["archived_message_id"] == 888
    assert row["file_name"] == "新名字.pdf"
    assert row["mime_type"] == "application/pdf"
    assert row["media_kind"] == "document"
    assert row["source"] == "manual_forward"


def test_sha256_dup_on_link_path_records_link_source(db: ArchiveDB, tmp_path, monkeypatch):
    """路径二命中去重时 source 必须是 link，不能硬编码成 manual_forward"""
    local = tmp_path / "same.pdf"
    local.write_bytes(b"\x01" * 2048)
    _seed_dup(db, local)

    entry = _msg(800, _chat(-1001234567890, "接收频道"))
    source_msg = _doc_message(55, _chat(-1009999999999, "私有频道"), "FUID_LINK")
    outcome = _run_archive_single(monkeypatch, source_msg, local,
                                 entry=entry, route=ROUTE_LINK)
    assert outcome.ok is True

    row = db.find_by_unique_id("FUID_LINK")
    assert row is not None
    assert row["source"] == "link"
    assert row["media_kind"] == "document"
