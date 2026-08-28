"""回填脚本的匹配与写入测试。不连 Telegram，喂假消息。"""

from types import SimpleNamespace

import pytest
from backfill_metadata import plan_updates
from db import ArchiveDB


@pytest.fixture
def db(tmp_path):
    return ArchiveDB(str(tmp_path / "t.db"))


def _media(file_unique_id, file_name="cat.mp4", mime_type="video/mp4"):
    return SimpleNamespace(file_unique_id=file_unique_id, file_name=file_name,
                           mime_type=mime_type)


def _msg(message_id, file_unique_id, forward_origin=None):
    """带 video 媒体的接收频道消息 stub。get_media 按 MEDIA_ATTRS 顺序取属性。"""
    return SimpleNamespace(
        id=message_id, forward_origin=forward_origin,
        document=None, video=_media(file_unique_id), photo=None,
        audio=None, animation=None, voice=None, video_note=None,
    )


def _channel_origin(chat_id, title, message_id):
    return SimpleNamespace(
        type=SimpleNamespace(value="channel"),
        chat=SimpleNamespace(id=chat_id, title=title),
        message_id=message_id,
    )


def test_matching_row_gets_origin_and_identity(db: ArchiveDB):
    db.record_message(source_message_id=100, file_unique_id="FUID_A")
    row = db.rows_missing_origin()[0]
    fetched = {100: _msg(100, "FUID_A", _channel_origin(-1001111111111, "某个公开频道", 42))}
    updates, unknown = plan_updates([row], fetched)
    assert unknown == []
    assert len(updates) == 1
    row_id, file_unique_id, origin, identity = updates[0]
    assert row_id == row["id"]
    assert file_unique_id == "FUID_A"
    assert origin["origin_chat_id"] == "-1001111111111"
    assert origin["origin_message_id"] == 42
    assert origin["origin_type"] == "channel"
    assert identity == {"file_name": "cat.mp4", "mime_type": "video/mp4",
                        "media_kind": "video"}


def test_file_unique_id_mismatch_is_skipped(db: ArchiveDB):
    """
    自证匹配：拉回的消息 file_unique_id 与 DB 行不符，说明这个
    source_message_id 不指向接收频道的那条消息（历史路径二行就是这样）。
    宁可留 unknown，也不能把别人的来源写进来。
    """
    db.record_message(source_message_id=42, file_unique_id="FUID_FROM_SOURCE_CHANNEL")
    row = db.rows_missing_origin()[0]
    fetched = {42: _msg(42, "FUID_SOMETHING_ELSE",
                        _channel_origin(-1002222222222, "不相干的频道", 7))}
    updates, unknown = plan_updates([row], fetched)
    assert updates == []
    assert unknown == [row["id"]]


def test_missing_message_is_marked_unknown(db: ArchiveDB):
    """接收频道里原消息已被删除"""
    db.record_message(source_message_id=200, file_unique_id="FUID_B")
    row = db.rows_missing_origin()[0]
    updates, unknown = plan_updates([row], {})
    assert updates == []
    assert unknown == [row["id"]]


def test_message_without_media_is_marked_unknown(db: ArchiveDB):
    db.record_message(source_message_id=300, file_unique_id="FUID_C")
    row = db.rows_missing_origin()[0]
    bare = SimpleNamespace(id=300, forward_origin=None, document=None, video=None,
                           photo=None, audio=None, animation=None, voice=None,
                           video_note=None)
    updates, unknown = plan_updates([row], {300: bare})
    assert updates == []
    assert unknown == [row["id"]]


def test_original_post_gets_original_not_null(db: ArchiveDB):
    """接收频道里原创直发的媒体：来源为 original，不能留 NULL"""
    db.record_message(source_message_id=400, file_unique_id="FUID_D")
    row = db.rows_missing_origin()[0]
    updates, unknown = plan_updates([row], {400: _msg(400, "FUID_D")})
    assert unknown == []
    assert updates[0][2]["origin_type"] == "original"


def test_apply_updates_writes_both_tables(db: ArchiveDB):
    """messages 与 files 都要落地——文件身份的规范位置在 files"""
    from backfill_metadata import apply_updates

    db.record_message(source_message_id=500, file_unique_id="FUID_E", caption="内容正文")
    db.record_file(file_unique_id="FUID_E", sha256="f" * 64, size=16,
                   archived_chat_id=None, archived_message_id=None,
                   source="manual_forward", source_channel=None)
    row = db.rows_missing_origin()[0]
    fetched = {500: _msg(500, "FUID_E", _channel_origin(-1001111111111, "某个公开频道", 9))}
    updates, unknown = plan_updates([row], fetched)
    apply_updates(db, updates, unknown, dry_run=False)

    msg_row = db.search("某个公开频道")[0]
    assert msg_row["origin_message_id"] == 9
    assert msg_row["media_kind"] == "video"
    file_row = db.find_by_unique_id("FUID_E")
    assert file_row["file_name"] == "cat.mp4"
    assert file_row["mime_type"] == "video/mp4"
    assert file_row["media_kind"] == "video"


def test_apply_updates_dry_run_writes_nothing(db: ArchiveDB):
    from backfill_metadata import apply_updates

    db.record_message(source_message_id=600, file_unique_id="FUID_G")
    row = db.rows_missing_origin()[0]
    updates, unknown = plan_updates([row], {600: _msg(600, "FUID_G")})
    apply_updates(db, updates, unknown, dry_run=True)
    assert len(db.rows_missing_origin()) == 1
