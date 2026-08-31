"""
ArchivePipeline 测试：假 client / 假 db / 假 downloader，
不连 Telegram、不读环境变量、不写真实数据目录。
"""

import asyncio
import os
from types import SimpleNamespace

from archive_entry import ROUTE_FORWARD, ArchiveItem, Entry


def msg_stub(msg_id, kind="document", *, chat_id=-1001234567890, caption="",
             group=None, title="接收频道", **media):
    """带一种媒体的消息桩。kind 决定 media_ops.get_media 推导出的类型。"""
    defaults = {"file_unique_id": f"U{msg_id}", "file_size": 2048}
    if kind == "document":
        defaults |= {"file_name": f"{msg_id}.pdf", "mime_type": "application/pdf"}
    elif kind == "video":
        defaults |= {"file_name": f"{msg_id}.mp4", "mime_type": "video/mp4",
                     "duration": 5, "width": 640, "height": 360}
    msg = SimpleNamespace(
        id=msg_id, chat=SimpleNamespace(id=chat_id, title=title),
        date=None, media_group_id=group, caption=caption, text="",
        from_user=SimpleNamespace(first_name="tester", id=1), sender_chat=None,
        forward_origin=None,
    )
    setattr(msg, kind, SimpleNamespace(**(defaults | media)))
    return msg


def item_of(msg, *, route=ROUTE_FORWARD, entry_msg=None, link=None):
    return ArchiveItem(media=msg, entry=Entry(message=entry_msg or msg, route=route), link=link)


def fake_db(**overrides):
    """默认「什么都没归档过」；record_* 的入参记进 .writes 供断言。"""
    writes = []
    db = SimpleNamespace(
        find_by_unique_id=lambda fuid: None,
        find_by_sha256=lambda sha: None,
        record_file=lambda **kw: writes.append(("file", kw)),
        record_message=lambda **kw: writes.append(("message", kw)),
        writes=writes,
    )
    for name, value in overrides.items():
        setattr(db, name, value)
    return db


def fake_downloader(paths):
    """paths: {message_id: 本地路径}。签名与 TDLDownloader.download 对齐。"""
    seen = {}

    async def download(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
        seen["links"] = links
        seen["dest_dir"] = dest_dir
        seen["calls"] = seen.get("calls", 0) + 1
        return {m.id: paths[m.id] for m in messages if m.id in paths}

    return SimpleNamespace(download=download, seen=seen)


class FakeClient:
    """记录所有 send_* 调用。calls 里是 (方法名, 路径或路径列表, kwargs)。"""

    def __init__(self, *, media_group_error=None, send_photo_error=None):
        self.calls = []
        self._n = 0
        self._media_group_error = media_group_error
        self._send_photo_error = send_photo_error

    def _sent(self):
        self._n += 1
        return SimpleNamespace(id=900 + self._n)

    async def send_document(self, chat, path, caption=""):
        self.calls.append(("send_document", path, {"caption": caption}))
        return self._sent()

    async def send_photo(self, chat, path, caption=""):
        self.calls.append(("send_photo", path, {"caption": caption}))
        if self._send_photo_error is not None:
            error, self._send_photo_error = self._send_photo_error, None
            raise error
        return self._sent()

    async def send_video(self, chat, path, **kwargs):
        self.calls.append(("send_video", path, kwargs))
        return self._sent()

    async def send_voice(self, chat, path, caption=""):
        self.calls.append(("send_voice", path, {"caption": caption}))
        return self._sent()

    async def send_media_group(self, chat, input_media):
        self.calls.append(("send_media_group", [m.media for m in input_media], {
            "captions": [m.caption for m in input_media],
            "classes": [type(m).__name__ for m in input_media],
        }))
        if self._media_group_error is not None:
            # 只炸第一次：验证 PhotoExtInvalid 回退整组 document 那条路径
            error, self._media_group_error = self._media_group_error, None
            raise error
        return [self._sent() for _ in input_media]


def local_file(tmp_path, name, size=2048, content=None):
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if content is not None else b"\x00" * size)
    return str(path)


def test_archive_one_uploads_and_records(tmp_path, make_pipeline):
    """
    冒烟：整条单条管道跑通，全靠注入的桩 —— 不 import listener、不碰环境变量。

    这个用例本身就是本次重构的验收：今天写不出来。
    """
    msg = msg_stub(41)
    client, db = FakeClient(), fake_db()
    downloader = fake_downloader({41: local_file(tmp_path, "src/41.pdf")})
    pipeline = make_pipeline(client=client, db=db, downloader=downloader)

    outcome = asyncio.run(pipeline.archive_one(item_of(msg)))

    assert outcome.ok is True
    assert [c[0] for c in client.calls] == ["send_document"]
    assert [w[0] for w in db.writes] == ["file", "message"]
    assert db.writes[0][1]["source"] == "manual_forward"
    assert db.writes[1][1]["source_message_id"] == 41


class TestThumbnailPath:
    def test_media_group_thumbs_land_beside_each_source(self, tmp_path, monkeypatch,
                                                       make_pipeline):
        """
        回归：缩略图必须写在源文件所在目录，且按消息 id 区分。

        修复前 thumb_dir 传的是 msg_dir，而 tdl 命中时 msg_dir 已被 rmdir，
        ffmpeg 写不进去 —— 四档画质白跑一轮，媒体组视频永远没有缩略图。
        """
        import media_ops

        thumb_calls = []

        def fake_make_thumbnail(video_path, thumb_path, timestamp="00:00:01"):
            # 目录是否存在只能在调用当时判断：archive_batch 收尾时 _cleanup_temp_files
            # 会把临时文件连所在空目录一起清掉，事后再看什么都不剩
            dir_ok = os.path.isdir(os.path.dirname(thumb_path))
            thumb_calls.append((video_path, thumb_path, dir_ok))
            if not dir_ok:
                return None  # 真 ffmpeg 在这种情况下是四档画质全跑一遍再返回 None
            # 真写一个文件：_prepare_video 成功后会 os.path.getsize 记日志，
            # 日志参数是即时求值的，返回不存在的路径会抛 FileNotFoundError
            with open(thumb_path, "wb") as f:
                f.write(b"jpg")
            return thumb_path

        monkeypatch.setattr(media_ops, "make_thumbnail", fake_make_thumbnail)
        monkeypatch.setattr(media_ops, "probe_video",
                            lambda p: {"duration": 5, "width": 640, "height": 360})

        # tdl 成功时整组文件都落在同一个 batch_dir，msg_dir 会被 rmdir
        sources = {mid: local_file(tmp_path, f"batch/{mid}.mp4") for mid in (41, 42)}
        items = [item_of(msg_stub(mid, "video", group="g1")) for mid in (41, 42)]
        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                downloader=fake_downloader(sources))

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert all(o.ok for o in outcomes)
        assert len(thumb_calls) == 2
        for src, thumb, dir_ok in thumb_calls:
            assert dir_ok, f"缩略图目录不存在，ffmpeg 必然写不进去：{thumb}"
            assert os.path.dirname(thumb) == os.path.dirname(src)
        thumbs = [t for _, t, _ in thumb_calls]
        assert len(set(thumbs)) == 2, f"两条缩略图指向同一个文件：{thumbs}"
        assert sorted(os.path.basename(t) for t in thumbs) == ["thumb_41.jpg", "thumb_42.jpg"]


class TestGroupUploadShapes:
    def test_distinct_captions_split_into_single_uploads(self, tmp_path, make_pipeline):
        """
        多条转发消息各带独立文字被 Telegram 编组 → 拆组单发，每条带自己的 caption，
        还原「文件A+文字A、文件B+文字B」的原始布局，不走 send_media_group。
        """
        client = FakeClient()
        items = [item_of(msg_stub(mid, group="g1", caption=f"文字{mid}")) for mid in (41, 42)]
        pipeline = make_pipeline(
            client=client, db=fake_db(),
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42)}),
        )

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert all(o.ok for o in outcomes)
        assert [c[0] for c in client.calls] == ["send_document", "send_document"]
        assert [c[2]["caption"] for c in client.calls] == ["文字41", "文字42"]

    def test_shared_caption_uses_media_group_with_caption_on_first_only(
            self, tmp_path, make_pipeline):
        client = FakeClient()
        items = [item_of(msg_stub(mid, group="g1", caption="同一段说明")) for mid in (41, 42)]
        pipeline = make_pipeline(
            client=client, db=fake_db(),
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42)}),
        )

        asyncio.run(pipeline.archive_batch(items))

        assert [c[0] for c in client.calls] == ["send_media_group"]
        assert client.calls[0][2]["captions"] == ["同一段说明", ""]

    def test_photo_ext_invalid_falls_back_to_document_group(self, tmp_path, make_pipeline):
        """WebP 等格式不能作为 photo 编组 → 整组回退 document，caption 仍只挂第一条"""
        from pyrogram.errors import PhotoExtInvalid

        client = FakeClient(media_group_error=PhotoExtInvalid())
        items = [item_of(msg_stub(mid, "photo", group="g1", caption="猫")) for mid in (41, 42)]
        pipeline = make_pipeline(
            client=client, db=fake_db(),
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.bin")
                                        for mid in (41, 42)}),
        )

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert all(o.ok for o in outcomes)
        assert [c[0] for c in client.calls] == ["send_media_group", "send_media_group"]
        assert client.calls[0][2]["classes"] == ["InputMediaPhoto", "InputMediaPhoto"]
        assert client.calls[1][2]["classes"] == ["InputMediaDocument", "InputMediaDocument"]
        assert client.calls[1][2]["captions"] == ["猫", ""]

    def test_non_groupable_kind_goes_through_archive_one(self, tmp_path, make_pipeline):
        """语音不支持编组 → 退回单条 send_voice（这条路径今天没测过）"""
        client = FakeClient()
        src = local_file(tmp_path, "b/41.ogg")
        pipeline = make_pipeline(client=client, db=fake_db(),
                                downloader=fake_downloader({41: src}))

        outcomes = asyncio.run(
            pipeline.archive_batch([item_of(msg_stub(41, "voice", group="g1"))]))

        assert [o.ok for o in outcomes] == [True]
        assert [c[0] for c in client.calls] == ["send_voice"]


class TestVideoMetadataFallback:
    def _run(self, tmp_path, make_pipeline, monkeypatch, probe_result):
        import media_ops

        client = FakeClient()
        monkeypatch.setattr(media_ops, "probe_video", lambda p: probe_result)
        monkeypatch.setattr(media_ops, "make_thumbnail", lambda *a, **k: None)
        pipeline = make_pipeline(
            client=client, db=fake_db(),
            downloader=fake_downloader({41: local_file(tmp_path, "b/41.mp4")}),
        )
        assert asyncio.run(pipeline.archive_one(item_of(msg_stub(41, "video")))).ok
        return client.calls[0]

    def test_probe_wins_when_it_works(self, tmp_path, make_pipeline, monkeypatch):
        call = self._run(tmp_path, make_pipeline, monkeypatch,
                         {"duration": 99, "width": 1920, "height": 1080})
        assert call[0] == "send_video"
        assert (call[2]["duration"], call[2]["width"], call[2]["height"]) == (99, 1920, 1080)

    def test_probe_failure_falls_back_to_source_message(self, tmp_path, make_pipeline,
                                                       monkeypatch):
        """第二层：ffprobe 返回 None（大文件常见）→ 用源消息自带的元数据"""
        call = self._run(tmp_path, make_pipeline, monkeypatch, None)
        assert (call[2]["duration"], call[2]["width"], call[2]["height"]) == (5, 640, 360)

    def test_probe_zero_duration_prefers_source_duration(self, tmp_path, make_pipeline,
                                                        monkeypatch):
        """ffprobe 探到了尺寸但 duration=0 → 只有 duration 回退源数据"""
        call = self._run(tmp_path, make_pipeline, monkeypatch,
                         {"duration": 0, "width": 1920, "height": 1080})
        assert (call[2]["duration"], call[2]["width"], call[2]["height"]) == (5, 1920, 1080)


def test_upload_cooldown_comes_from_config(tmp_path, make_pipeline, monkeypatch):
    """
    冷却值走 PipelineConfig：测试不再 monkeypatch 模块常量，也不再真睡 5 秒。

    风控相关，值本身不能改；这里只验证配置真的被用上。
    """
    import pipeline as pipeline_mod

    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(pipeline_mod.asyncio, "sleep", fake_sleep)
    p = make_pipeline(client=FakeClient(), db=fake_db(),
                      downloader=fake_downloader({41: local_file(tmp_path, "b/41.pdf")}),
                      upload_cooldown_seconds=7)

    asyncio.run(p.archive_one(item_of(msg_stub(41))))

    assert slept == [7]
