from __future__ import annotations

import asyncio
import hashlib
import random
import subprocess
from io import BytesIO

import pytest
from PIL import Image
from support import RECEIVE, SOURCE, Harness, item, message
from tgstash.archive.entry import ROUTE_LINK, Entry
from tgstash.archive.links import process_link_message
from tgstash.media import ops
from tgstash.runtime.listener import _settle_all, scan_once


def png_payload(seed):
    pixels = random.Random(seed).randbytes(64 * 64 * 3)
    image = BytesIO()
    Image.frombytes("RGB", (64, 64), pixels).save(image, "PNG")
    return image.getvalue()


def test_single_archive_preserves_original_identity_and_deduplicates(tmp_path):
    h = Harness(tmp_path)
    payload = png_payload(0)
    msg = message(41, "photo", size=len(payload), file_name="../../../archive.db")
    h.downloader.payloads[41] = payload
    assert asyncio.run(h.pipeline.archive_one(item(msg))).ok
    stored = h.db.find_by_unique_id("U41")
    assert stored["sha256"] == hashlib.sha256(payload).hexdigest()
    assert stored["size"] == len(payload)
    assert h.client.uploads[0][1] != payload
    with Image.open(BytesIO(h.client.uploads[0][1])) as uploaded:
        assert uploaded.format == "JPEG"
    assert h.db.search("归档核心内容")[0]["source_message_id"] == 41

    assert asyncio.run(h.pipeline.archive_one(item(msg))).ok
    assert len(h.downloader.calls) == 1
    alias = message(42, "photo", size=len(payload))
    h.downloader.payloads[42] = payload
    assert asyncio.run(h.pipeline.archive_one(item(alias))).ok
    assert len(h.client.uploads) == 1
    assert h.db.find_by_unique_id("U42")["archived_message_id"] == stored["archived_message_id"]
    assert h.marked == [(41, False), (41, True), (42, True)]

    video_path = tmp_path / "source.mp4"
    subprocess.run([
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
        "testsrc=size=64x64:rate=10", "-t", "2", "-c:v", "libx264", "-pix_fmt", "yuv420p",
        str(video_path),
    ], check=True, capture_output=True)
    video = video_path.read_bytes()
    h.downloader.payloads[43] = video
    assert asyncio.run(h.pipeline.archive_one(item(message(43, "video", size=len(video))))).ok
    kind, data, kwargs = h.client.uploads[-1]
    assert kind == "send_video" and data == video
    assert kwargs["width"] == 64 and kwargs["height"] == 64 and kwargs["duration"] > 0
    assert kwargs["thumb"] is not None
    assert h.db.find_by_unique_id("U43")["sha256"] == hashlib.sha256(video).hexdigest()
    h.empty_downloads()


@pytest.mark.parametrize("shape", ["group", "split", "mixed", "photo_fallback"])
def test_album_preserves_all_items_captions_and_upload_shape(tmp_path, shape):
    h = Harness(tmp_path)
    first = message(41, caption="第一份内容", group="album")
    second = message(42, "video_note" if shape == "mixed" else "document",
                     caption="第二份内容" if shape == "split" else "", group="album")
    h.downloader.payloads = {41: b"a" * 2048, 42: b"b" * 2048}
    if shape == "photo_fallback":
        h.downloader.payloads = {mid: png_payload(mid) for mid in [41, 42]}
        first = message(41, "photo", caption="第一份内容", group="album",
                        size=len(h.downloader.payloads[41]))
        second = message(42, "photo", caption="", group="album", size=len(h.downloader.payloads[42]))
    h.client.photo_rejected = shape == "photo_fallback"
    outcomes = asyncio.run(h.pipeline.archive_batch([item(first), item(second)]))
    assert len(outcomes) == 2 and all(o.ok for o in outcomes)
    rows = h.db.find_messages_by_source_ids([41, 42])
    assert {r["source_message_id"] for r in rows} == {41, 42}
    assert all(h.db.find_by_unique_id(f"U{mid}")["archived_message_id"] is not None for mid in [41, 42])
    assert len(h.downloader.calls) == 1
    if shape == "split":
        assert [upload[2]["caption"] for upload in h.client.uploads] == ["第一份内容", "第二份内容"]
    elif shape == "mixed":
        assert [upload[0] for upload in h.client.uploads] == ["group", "send_video_note"]
        assert "caption" not in h.client.uploads[-1][2]
    else:
        assert h.client.uploads[-1][2]["captions"] == ["第一份内容", ""]
        if shape == "photo_fallback":
            assert len(h.client.uploads) == 2
            assert h.client.uploads[-1][2]["types"] == ["InputMediaDocument"] * 2
            assert h.client.uploads[-1][1] == h.client.uploads[0][1]
            for mid in [41, 42]:
                assert h.db.find_by_unique_id(f"U{mid}")["sha256"] == hashlib.sha256(
                    h.downloader.payloads[mid]).hexdigest()
        else:
            assert h.client.uploads[-1][1] == [b"a" * 2048, b"b" * 2048]
    assert set(h.marked) == {(41, False), (42, False)}
    h.empty_downloads()


@pytest.mark.parametrize("fault", ["verify", "process", "upload", "short_response"])
def test_album_failures_are_settled_and_downloads_are_cleaned(tmp_path, monkeypatch, fault):
    h = Harness(tmp_path)
    h.downloader.payloads = {41: b"x" if fault == "verify" else b"a" * 2048, 42: b"b" * 2048}
    if fault == "process":
        def fail(path):
            raise OSError("hash unavailable")
        monkeypatch.setattr(ops, "sha256_of_file", fail)
    h.client.upload_error = fault == "upload"
    h.client.short_group = fault == "short_response"
    group = [message(41, group="album"), message(42, group="album")]
    h.client.groups[41] = group
    h.client.history = [*group, message(43, None, caption="")]
    asyncio.run(scan_once(h.ctx))
    failures = h.db.list_failures()
    assert len(failures) == 1 and failures[0]["attempt_count"] == 1
    assert failures[0]["failure_stage"] == ("process" if fault == "short_response" else fault)
    assert h.db.get_checkpoint(RECEIVE) == 0
    h.empty_downloads()


def test_scan_blocks_failed_entries_skips_at_limit_and_can_retry(tmp_path):
    h = Harness(tmp_path)
    h.client.history = [message(41), message(42)]
    h.downloader.payloads = {41: b"broken", 42: b"b" * 2048}
    for attempt in [1, 2]:
        assert asyncio.run(scan_once(h.ctx)) == 0
        assert h.db.get_checkpoint(RECEIVE) == 0
        assert h.db.get_failure(RECEIVE, 41)["attempt_count"] == attempt
        assert h.db.find_by_unique_id("U42") is None
    assert asyncio.run(scan_once(h.ctx)) == 1
    assert h.db.get_checkpoint(RECEIVE) == 42
    assert h.db.get_failure(RECEIVE, 41)["status"] == "skipped"
    assert h.db.find_by_unique_id("U42") is not None

    h.db.retry_skipped([41])
    assert h.db.get_checkpoint(RECEIVE) == 40
    h.downloader.payloads[41] = b"a" * 2048
    asyncio.run(scan_once(h.ctx))
    assert h.db.get_checkpoint(RECEIVE) == 42
    assert h.db.list_failures() == []
    assert len(h.db.search("归档核心内容")) == 2
    assert len(h.client.uploads) == 2
    h.empty_downloads()


def test_link_album_uses_entry_identity_and_keeps_sibling_failure(tmp_path):
    h = Harness(tmp_path)
    entry_message = message(300, None, caption="", text="链接：https://t.me/c/1111111111/41。")
    first = message(41, chat=SOURCE, group="source_album")
    second = message(42, chat=SOURCE, group="source_album")
    h.client.sources[41] = first
    h.client.groups[41] = [second, first]
    h.downloader.payloads = {41: b"a" * 2048, 42: b"broken"}
    entry = Entry(entry_message, ROUTE_LINK)

    outcomes = asyncio.run(process_link_message(h.client, h.pipeline, entry))
    assert not asyncio.run(_settle_all(h.ctx, outcomes))
    assert h.db.get_failure(RECEIVE, 300)["attempt_count"] == 1
    assert h.db.get_failure(SOURCE, 42) is None
    row = h.db.search("归档核心内容")[0]
    assert row["source_message_id"] == 300 and row["source_chat_id"] == str(RECEIVE)
    assert row["origin_message_id"] == 41 and row["origin_chat_id"] == str(SOURCE)
    assert h.downloader.calls[0][1] == {41: "https://t.me/c/1111111111/41"}
    assert h.client.edits[0][:2] == (RECEIVE, 300)

    h.downloader.payloads[42] = b"b" * 2048
    outcomes = asyncio.run(process_link_message(h.client, h.pipeline, entry))
    assert asyncio.run(_settle_all(h.ctx, outcomes))
    assert h.db.list_failures() == []
    assert len(h.db.find_messages_by_source_ids([300])) == 2
    h.empty_downloads()
