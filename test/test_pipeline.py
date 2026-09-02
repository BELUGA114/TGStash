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
    """带一种媒体的消息桩。kind 决定 media_ops.get_media 推导出的类型；kind=None 表示无媒体。"""
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
    if kind is not None:
        setattr(msg, kind, SimpleNamespace(**(defaults | media)))
    return msg


def item_of(msg, *, route=ROUTE_FORWARD, entry_msg=None, link=None):
    return ArchiveItem(media=msg, entry=Entry(message=entry_msg or msg, route=route), link=link)


def fake_db(**overrides):
    """
    默认「什么都没归档过」；record_* 的入参记进 .writes 供断言。

    record_archived 是 files + messages 的原子写入口，两张表各记一条 —— 生产走的是
    一次调用一个事务，断言侧仍按 ("file", ...) / ("message", ...) 两条看。
    """
    writes = []

    def record_archived(**kw):
        writes.append(("file", kw))
        writes.append(("message", kw))

    db = SimpleNamespace(
        find_by_unique_id=lambda fuid: None,
        find_by_sha256=lambda sha: None,
        record_file=lambda **kw: writes.append(("file", kw)),
        record_archived=record_archived,
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
        seen["fallback_paths"] = fallback_paths
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

    async def send_video_note(self, chat, path, duration=None, length=None, thumb=None):
        # 签名照抄真实的 Client.send_video_note：它是唯一不接受 caption 的
        # send_* 方法（Telegram 的圆形视频没有 caption 字段）。桩必须跟着少这个
        # 参数，否则「多传一个不存在的关键字」这类缺陷永远测不出来
        self.calls.append(("send_video_note", path,
                           {"duration": duration, "length": length, "thumb": thumb}))
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


class TestDownloadNameSanitizing:
    """回归：file_name 由上传者控制，回退下载的落点必须留在 msg_dir 内。"""

    def test_safe_dl_name_keeps_only_the_last_segment(self):
        from pipeline import _safe_dl_name

        assert _safe_dl_name("photo.jpg") == "photo.jpg"
        assert _safe_dl_name("../../../db/archive.db") == "archive.db"
        assert _safe_dl_name("/data/db/archive.db") == "archive.db"
        assert _safe_dl_name("..\\..\\db\\archive.db") == "archive.db"
        assert _safe_dl_name("a\x00b.pdf") == "ab.pdf"
        # 收不出文件名的一律 None，由调用方回退成消息 id
        assert _safe_dl_name("..") is None
        assert _safe_dl_name("dir/") is None
        assert _safe_dl_name("   ") is None
        assert _safe_dl_name(None) is None

    def test_traversal_name_lands_inside_msg_dir(self, tmp_path, make_pipeline):
        """
        修复前 dl_name 原样拼进 fallback_paths：Pyrogram 回退下载只做
        os.path.split + makedirs，不校验穿越段，于是 open(..., "wb") 落到
        msg_dir 之外 —— 一条名叫 ../../../db/archive.db 的转发文档
        就能把归档库整个截断。
        """
        msg = msg_stub(51, file_name="../../../db/archive.db")
        download_dir = str(tmp_path / "dl")
        downloader = fake_downloader({51: local_file(tmp_path, "src/51.pdf")})
        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=downloader, download_dir=download_dir)

        outcome = asyncio.run(pipeline.archive_one(item_of(msg)))

        assert outcome.ok is True
        fallback = downloader.seen["fallback_paths"][51]
        assert (os.path.realpath(os.path.dirname(fallback))
                == os.path.realpath(os.path.join(download_dir, "51")))


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

    def test_non_groupable_kind_uploads_singly(self, tmp_path, make_pipeline):
        """语音不支持编组 → 逐条 send_voice（不进 send_media_group）"""
        client = FakeClient()
        src = local_file(tmp_path, "b/41.ogg")
        pipeline = make_pipeline(client=client, db=fake_db(),
                                downloader=fake_downloader({41: src}))

        outcomes = asyncio.run(
            pipeline.archive_batch([item_of(msg_stub(41, "voice", group="g1"))]))

        assert [o.ok for o in outcomes] == [True]
        assert [c[0] for c in client.calls] == ["send_voice"]

    def test_non_groupable_kind_is_downloaded_once(self, tmp_path, make_pipeline):
        """
        不可编组条目不再回调 archive_one，因此只下载一次。

        修复前：_download 拉过一次，archive_one 又从零来一遍（重新下载、重新算
        SHA-256、重新查两遍去重）。
        """
        client = FakeClient()
        downloader = fake_downloader({41: local_file(tmp_path, "b/41.ogg")})
        pipeline = make_pipeline(client=client, db=fake_db(), downloader=downloader)

        outcomes = asyncio.run(
            pipeline.archive_batch([item_of(msg_stub(41, "voice", group="g1"))]))

        assert [o.ok for o in outcomes] == [True]
        assert [c[0] for c in client.calls] == ["send_voice"]
        assert downloader.seen["calls"] == 1, "不可编组条目被重新下载了"

    def test_group_with_voice_uploads_group_then_single_and_marks_both(self, tmp_path,
                                                                      make_pipeline):
        """可编组的走 send_media_group，voice 紧跟其后单发；两条都要打上标记"""
        marks = []

        async def mark(message, duplicate):
            marks.append((message.id, duplicate))

        client = FakeClient()
        pipeline = make_pipeline(
            client=client, db=fake_db(), mark=mark,
            downloader=fake_downloader({41: local_file(tmp_path, "b/41.pdf"),
                                        42: local_file(tmp_path, "b/42.ogg")}))
        items = [item_of(msg_stub(41, group="g1")),
                 item_of(msg_stub(42, "voice", group="g1"))]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert [(o.item.media.id, o.ok) for o in outcomes] == [(41, True), (42, True)]
        assert [c[0] for c in client.calls] == ["send_media_group", "send_voice"]
        # voice 上传成功后就地标记，可编组那条随全组在最后统一标记（与今天的顺序一致）
        assert marks == [(42, False), (41, False)]


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


class TestSingleVsGroupDifferences:
    """
    设计文档 2026-09-01-pipeline-debt-design.md「两条路径的真实差异」那张表的断言网。

    合流（Task 6）之前先把要保留的行为钉死。断言与表不符时先改表 —— 表是读代码
    读出来的，可能有误。
    """

    def test_no_media_single_is_success(self, make_pipeline):
        """表行 1：单条无媒体 → success（该入口本轮有结论，checkpoint 可以推进）"""
        downloader = fake_downloader({})
        pipeline = make_pipeline(db=fake_db(), downloader=downloader)

        outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41, None))))

        assert outcome.ok is True
        assert downloader.seen == {}, "无媒体不该走下载"

    def test_no_media_in_group_yields_no_outcome(self, make_pipeline):
        """表行 1：整组里的无媒体条目不产出任何 Outcome —— 该入口本轮无结论"""
        pipeline = make_pipeline(db=fake_db(), downloader=fake_downloader({}))

        assert asyncio.run(pipeline.archive_batch([item_of(msg_stub(41, None))])) == []

    def test_unique_id_hit_marks_in_place_in_single(self, make_pipeline):
        """表行 2：单条 file_unique_id 命中 → 就地标记 duplicate=True，且不下载"""
        marks = []

        async def mark(message, duplicate):
            marks.append((message.id, duplicate))

        downloader = fake_downloader({})
        pipeline = make_pipeline(
            db=fake_db(find_by_unique_id=lambda fuid: {"archived_chat_id": "-100",
                                                       "archived_message_id": 7}),
            downloader=downloader, mark=mark)

        outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

        assert outcome.ok is True
        assert marks == [(41, True)]
        assert downloader.seen == {}, "unique_id 命中不该走下载"

    def test_group_marks_after_upload_pending_before_duplicates(self, tmp_path, make_pipeline):
        """
        表行 2 / 13：整组的标记全部延到上传之后，顺序是先 pending(False) 再 duplicates(True)。

        41 是 file_unique_id 命中（不下载），42 是新文件。
        """
        events = []

        async def mark(message, duplicate):
            events.append(("mark", message.id, duplicate))

        class RecordingClient(FakeClient):
            async def send_media_group(self, chat, input_media):
                events.append(("upload", [os.path.basename(m.media) for m in input_media]))
                return await super().send_media_group(chat, input_media)

        pipeline = make_pipeline(
            client=RecordingClient(),
            db=fake_db(find_by_unique_id=lambda fuid: {"archived_chat_id": "-100",
                                                       "archived_message_id": 7}
                       if fuid == "U41" else None),
            downloader=fake_downloader({42: local_file(tmp_path, "b/42.pdf")}),
            mark=mark,
        )
        items = [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert [(o.item.media.id, o.ok) for o in outcomes] == [(41, True), (42, True)]
        assert events == [("upload", ["42.pdf"]), ("mark", 42, False), ("mark", 41, True)]

    def test_download_dirs_differ_between_paths(self, tmp_path, make_pipeline):
        """表行 3：单条下载到 download_dir/<msg_id>，整组下载到 batch_ 目录"""
        dl_root = str(tmp_path / "dl")

        single_dl = fake_downloader({41: local_file(tmp_path, "s/41.pdf")})
        p1 = make_pipeline(client=FakeClient(), db=fake_db(), downloader=single_dl,
                           download_dir=dl_root)
        assert asyncio.run(p1.archive_one(item_of(msg_stub(41)))).ok
        assert single_dl.seen["dest_dir"] == os.path.join(dl_root, "41")

        group_dl = fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                    for mid in (41, 42)})
        p2 = make_pipeline(client=FakeClient(), db=fake_db(), downloader=group_dl,
                           download_dir=dl_root)
        outcomes = asyncio.run(p2.archive_batch(
            [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)]))
        assert all(o.ok for o in outcomes)
        assert os.path.basename(group_dl.seen["dest_dir"]).startswith("batch_")

    def test_group_removes_empty_msg_dir_when_file_lands_elsewhere(self, tmp_path,
                                                                  make_pipeline):
        """
        表行 3：tdl 命中 batch_dir 之后，msg_dir 必须被 rmdir 掉，不留空目录。

        不只是「不留垃圾」：msg_dir 被 rmdir 是既有事实，缩略图与压缩产物因此必须写
        源文件所在目录。历史上缩略图写的是 msg_dir，命中 batch_dir 时那个目录已经不在，
        ffmpeg 恒失败，还白跑四档画质重试 —— 媒体组视频永远没有缩略图。
        """
        dl_root = str(tmp_path / "dl")
        pipeline = make_pipeline(
            client=FakeClient(), db=fake_db(),
            downloader=fake_downloader({41: local_file(tmp_path, "b/41.pdf")}),
            download_dir=dl_root)

        outcomes = asyncio.run(
            pipeline.archive_batch([item_of(msg_stub(41, group="g1"))]))

        # 少了这条，「一条都没规划」时下面两个断言会一起真空通过
        assert len(outcomes) == 1
        assert all(o.ok for o in outcomes)
        assert not os.path.isdir(os.path.join(dl_root, "41"))

    def test_group_missing_path_fails_only_that_item(self, tmp_path, make_pipeline):
        """表行 6：整组里某条没拿到路径 → 只有那条 failure('download')，其余照常归档"""
        pipeline = make_pipeline(
            client=FakeClient(), db=fake_db(),
            downloader=fake_downloader({42: local_file(tmp_path, "b/42.pdf")}))
        items = [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        by_id = {o.item.media.id: o for o in outcomes}
        assert by_id[41].ok is False and by_id[41].stage == "download"
        assert by_id[42].ok is True

    def test_format_fix_result_is_what_gets_uploaded_and_cleaned(self, tmp_path,
                                                                make_pipeline, monkeypatch):
        """
        表行 7：格式修正换掉路径后，被上传、被清理的都必须是新文件（两条路径同样）。

        真 fix_media_format 要靠 Pillow 认出 WebP；这里用假的模拟「改名换路径」，
        断言的是管道对返回值的追踪，不是 Pillow。
        """
        import media_ops

        def fake_fix(path, kind):
            new_path = path + ".jpg"
            os.rename(path, new_path)
            return new_path

        monkeypatch.setattr(media_ops, "fix_media_format", fake_fix)

        single_src = local_file(tmp_path, "s/41.bin")
        client = FakeClient()
        p1 = make_pipeline(client=client, db=fake_db(),
                           downloader=fake_downloader({41: single_src}))
        assert asyncio.run(p1.archive_one(item_of(msg_stub(41, "photo")))).ok
        assert client.calls[0][1] == single_src + ".jpg", "上传的应该是转换后的文件"
        assert not os.path.exists(single_src + ".jpg"), "转换后的文件没被清理"

        group_src = local_file(tmp_path, "b/42.bin")
        group_client = FakeClient()
        p2 = make_pipeline(client=group_client, db=fake_db(),
                           downloader=fake_downloader({42: group_src}))
        assert all(o.ok for o in asyncio.run(
            p2.archive_batch([item_of(msg_stub(42, "photo", group="g1"))])))
        assert group_client.calls[0][1] == [group_src + ".jpg"]
        assert not os.path.exists(group_src + ".jpg")

    def test_format_fix_tracks_each_item_in_a_multi_item_group(self, tmp_path, make_pipeline,
                                                              monkeypatch):
        """
        整组里每一条的格式修正产物都要各自被追踪。

        新契约是「往 temp_files 追加新路径」而不是「替换最后一项」：替换在 ≥2 条的组里
        会把前一条的追踪抹掉（只有一条时 [-1] 恰好是它自己，看不出问题）。
        """
        import media_ops

        def fake_fix(path, kind):
            new_path = path + ".jpg"
            os.rename(path, new_path)
            return new_path

        monkeypatch.setattr(media_ops, "fix_media_format", fake_fix)

        srcs = {mid: local_file(tmp_path, f"b/{mid}.bin") for mid in (41, 42)}
        client = FakeClient()
        pipeline = make_pipeline(client=client, db=fake_db(),
                                 downloader=fake_downloader(srcs))

        outcomes = asyncio.run(pipeline.archive_batch(
            [item_of(msg_stub(mid, "photo", group="g1")) for mid in (41, 42)]))

        assert all(o.ok for o in outcomes)
        assert client.calls[0][1] == [srcs[41] + ".jpg", srcs[42] + ".jpg"]
        leftovers = [p for p in (srcs[41] + ".jpg", srcs[42] + ".jpg") if os.path.exists(p)]
        assert leftovers == [], f"转换产物没被清掉：{leftovers}"

    def test_group_caption_is_shared_single_keeps_its_own(self, tmp_path, make_pipeline):
        """表行 8：整组写库用组级 caption（每条都写同一份），单条写库用自己那条的文字"""
        db = fake_db()
        pipeline = make_pipeline(
            client=FakeClient(), db=db,
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42)}))
        items = [item_of(msg_stub(41, group="g1", caption="组说明")),
                 item_of(msg_stub(42, group="g1"))]

        assert all(o.ok for o in asyncio.run(pipeline.archive_batch(items)))
        assert [kw["caption"] for tag, kw in db.writes if tag == "message"] == ["组说明", "组说明"]

        db2 = fake_db()
        p2 = make_pipeline(client=FakeClient(), db=db2,
                           downloader=fake_downloader({43: local_file(tmp_path, "s/43.pdf")}))
        assert asyncio.run(p2.archive_one(item_of(msg_stub(43, caption="自己的说明")))).ok
        assert [kw["caption"] for tag, kw in db2.writes if tag == "message"] == ["自己的说明"]

    def test_single_photo_ext_invalid_falls_back_to_document(self, tmp_path, make_pipeline):
        """表行 10：单条 PhotoExtInvalid → 只有那一条回退 send_document（整组是整组回退）"""
        from pyrogram.errors import PhotoExtInvalid

        client = FakeClient(send_photo_error=PhotoExtInvalid())
        pipeline = make_pipeline(client=client, db=fake_db(),
                                 downloader=fake_downloader(
                                     {41: local_file(tmp_path, "s/41.bin")}))

        assert asyncio.run(pipeline.archive_one(item_of(msg_stub(41, "photo")))).ok
        assert [c[0] for c in client.calls] == ["send_photo", "send_document"]

    def test_group_upload_failure_marks_nothing(self, tmp_path, make_pipeline):
        """表行 11 / 13：整组上传失败 → 只第一条产出失败结论，且不打任何标记"""
        marks = []

        async def mark(message, duplicate):
            marks.append((message.id, duplicate))

        class BoomClient(FakeClient):
            async def send_media_group(self, chat, input_media):
                raise RuntimeError("组上传炸了")

        pipeline = make_pipeline(
            client=BoomClient(), db=fake_db(), mark=mark,
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42)}))

        outcomes = asyncio.run(pipeline.archive_batch(
            [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)]))

        assert [(o.item.media.id, o.ok, o.stage) for o in outcomes] == [(41, False, "upload")]
        assert marks == []

    def test_split_group_stops_at_first_upload_failure(self, tmp_path, make_pipeline):
        """表行 11：拆组上传失败 → 已成功的保留 success，失败那条产出 failure，其后不再尝试"""
        class OneThenBoom(FakeClient):
            async def send_document(self, chat, path, caption=""):
                if self.calls:
                    raise RuntimeError("第二条炸了")
                return await super().send_document(chat, path, caption=caption)

        pipeline = make_pipeline(
            client=OneThenBoom(), db=fake_db(),
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42, 43)}))
        items = [item_of(msg_stub(mid, group="g1", caption=f"文字{mid}"))
                 for mid in (41, 42, 43)]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert [(o.item.media.id, o.ok) for o in outcomes] == [(41, True), (42, False)]

    def test_cooldown_once_per_group_but_per_item_when_split(self, tmp_path, make_pipeline,
                                                             monkeypatch):
        """表行 12：打包上传整组只睡一次冷却，拆组每条各睡一次"""
        import pipeline as pipeline_mod

        slept = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        # 打的是全局 asyncio 模块的 sleep（pipeline 里的 asyncio 就是它），沿用既有写法。
        # 代价：本进程内所有 await asyncio.sleep 都不再真的等 —— 因此这一手不能和
        # TestFallbackConcurrency 混在同一个用例里，那边靠真 sleep 撑出并发窗口，
        # 被替掉之后三个协程会一个接一个跑完，量到的峰值静默塌成 1。
        monkeypatch.setattr(pipeline_mod.asyncio, "sleep", fake_sleep)

        shared = make_pipeline(
            client=FakeClient(), db=fake_db(), upload_cooldown_seconds=3,
            downloader=fake_downloader({mid: local_file(tmp_path, f"g/{mid}.pdf")
                                        for mid in (41, 42)}))
        assert all(o.ok for o in asyncio.run(shared.archive_batch(
            [item_of(msg_stub(mid, group="g1", caption="同一段")) for mid in (41, 42)])))
        assert slept == [3]

        slept.clear()
        split = make_pipeline(
            client=FakeClient(), db=fake_db(), upload_cooldown_seconds=3,
            downloader=fake_downloader({mid: local_file(tmp_path, f"s/{mid}.pdf")
                                        for mid in (41, 42)}))
        assert all(o.ok for o in asyncio.run(split.archive_batch(
            [item_of(msg_stub(mid, group="g1", caption=f"文字{mid}")) for mid in (41, 42)])))
        assert slept == [3, 3]

    def test_group_cleans_every_downloaded_file(self, tmp_path, make_pipeline):
        """表行 14：整组的临时文件由 archive_batch 统一清理，一个都不留"""
        paths = {mid: local_file(tmp_path, f"b/{mid}.pdf") for mid in (41, 42)}
        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=fake_downloader(paths))

        assert all(o.ok for o in asyncio.run(pipeline.archive_batch(
            [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)])))
        assert [p for p in paths.values() if os.path.exists(p)] == []

    def test_non_groupable_writes_records_once(self, tmp_path, make_pipeline):
        """
        表行 9 的不变量：不可编组条目只写一份 files + messages 记录。

        Task 7 改的是「下载几次」，这条不变量在改动前后都必须成立。
        """
        db = fake_db()
        pipeline = make_pipeline(client=FakeClient(), db=db,
                                 downloader=fake_downloader(
                                     {41: local_file(tmp_path, "b/41.ogg")}))

        outcomes = asyncio.run(pipeline.archive_batch(
            [item_of(msg_stub(41, "voice", group="g1"))]))

        assert [o.ok for o in outcomes] == [True]
        assert [tag for tag, _ in db.writes] == ["file", "message"]


class TestFallbackConcurrency:
    """
    表行 4：整组的 Pyrogram 回退走 Semaphore(2) 限流，单条那条没有限流。

    合流时若把两条路径的 fallback 也统一了，这两个断言会立刻分出来。
    """

    class Probe(FakeClient):
        """记录 download_media 的峰值并发。"""

        def __init__(self):
            super().__init__()
            self.active = 0
            self.peak = 0

        async def download_media(self, *, message, file_name):
            self.active += 1
            self.peak = max(self.peak, self.active)
            await asyncio.sleep(0.02)
            self.active -= 1
            return file_name

    def _capturing_downloader(self, paths, captured):
        async def download(messages, dest_dir, fallback=None, *, links=None,
                           fallback_paths=None):
            captured["fallback"] = fallback
            return {m.id: paths[m.id] for m in messages if m.id in paths}

        return SimpleNamespace(download=download)

    async def _peak(self, captured, probe):
        fallback = captured["fallback"]
        await asyncio.gather(*[fallback(msg_stub(i), f"p{i}") for i in (1, 2, 3)])
        return probe.peak

    def test_group_fallback_caps_at_two(self, tmp_path, make_pipeline):
        captured, probe = {}, self.Probe()
        pipeline = make_pipeline(
            client=probe, db=fake_db(),
            downloader=self._capturing_downloader(
                {41: local_file(tmp_path, "b/41.pdf")}, captured))

        async def run():
            outcomes = await pipeline.archive_batch([item_of(msg_stub(41, group="g1"))])
            assert all(o.ok for o in outcomes), outcomes
            return await self._peak(captured, probe)

        assert asyncio.run(run()) == 2

    def test_single_fallback_is_unlimited(self, tmp_path, make_pipeline):
        captured, probe = {}, self.Probe()
        pipeline = make_pipeline(
            client=probe, db=fake_db(),
            downloader=self._capturing_downloader(
                {41: local_file(tmp_path, "s/41.pdf")}, captured))

        async def run():
            assert (await pipeline.archive_one(item_of(msg_stub(41)))).ok
            return await self._peak(captured, probe)

        assert asyncio.run(run()) == 3


class TestSingleFailureTranslation:
    """
    差异表行 5 / 11 的单条半格：archive_one 把下载/校验/上传的异常翻译成 Outcome。

    这三个 except 是单条路径唯一的熔断入口 —— listener._handle_entry 与 scan_once
    都没有 try 包住 archive_one，丢掉任何一个都等于让异常冒到扫描循环：不记账、
    不告警、checkpoint 也不推进，就是债二那种永久卡死。变异测试证明这三条在补网
    之前一个用例都没钉住（改成 raise，209 个用例全绿），所以要在动结构之前先织网。
    """

    def test_download_exception_becomes_download_failure(self, make_pipeline):
        """downloader 抛异常 → failure('download')，原始错误信息带进 Outcome"""
        async def boom(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
            raise RuntimeError("tdl 炸了")

        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=SimpleNamespace(download=boom))

        outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

        assert outcome.ok is False and outcome.stage == "download"
        assert "tdl 炸了" in (outcome.error or "")

    def test_upload_exception_becomes_upload_failure(self, tmp_path, make_pipeline):
        """上传抛异常 → failure('upload')，且 finally 仍然清掉已下载的临时文件"""
        class BoomClient(FakeClient):
            async def send_document(self, chat, path, caption=""):
                raise RuntimeError("上传炸了")

        src = local_file(tmp_path, "s/41.pdf")
        pipeline = make_pipeline(client=BoomClient(), db=fake_db(),
                                 downloader=fake_downloader({41: src}))

        outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

        assert outcome.ok is False and outcome.stage == "upload"
        assert not os.path.exists(src), "上传失败也要清临时文件"

    def test_size_mismatch_becomes_verify_failure(self, tmp_path, make_pipeline):
        """校验只比对体积：源消息说 9999 字节，实际下来 2048 → failure('verify')"""
        pipeline = make_pipeline(
            client=FakeClient(), db=fake_db(),
            downloader=fake_downloader({41: local_file(tmp_path, "s/41.pdf")}))

        outcome = asyncio.run(
            pipeline.archive_one(item_of(msg_stub(41, file_size=9999))))

        assert outcome.ok is False and outcome.stage == "verify"

    def test_group_size_mismatch_fails_only_that_item(self, tmp_path, make_pipeline):
        """整组的校验失败只跳过那一条，不阻塞整组（行 6 的近邻，同样没网）"""
        pipeline = make_pipeline(
            client=FakeClient(), db=fake_db(),
            downloader=fake_downloader({mid: local_file(tmp_path, f"b/{mid}.pdf")
                                        for mid in (41, 42)}))
        items = [item_of(msg_stub(41, group="g1", file_size=9999)),
                 item_of(msg_stub(42, group="g1"))]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        by_id = {o.item.media.id: o for o in outcomes}
        assert by_id[41].ok is False and by_id[41].stage == "verify"
        assert by_id[42].ok is True


def test_video_thumb_is_tracked_and_cleaned(tmp_path, make_pipeline, monkeypatch):
    """
    缩略图也要进 temp_files 并被清掉。

    不变量：_prepare_video 生成缩略图后必须把它追加进 temp_files，由调用方的 finally
    统一清掉 —— 漏登记的缩略图会永久留在 /data/tmp（下载产物由调用方登记，
    这里钉的是「本条自己造出来的文件也算」那一半）。
    """
    import media_ops

    def fake_make_thumbnail(video_path, thumb_path, timestamp="00:00:01"):
        with open(thumb_path, "wb") as f:
            f.write(b"jpg")
        return thumb_path

    monkeypatch.setattr(media_ops, "make_thumbnail", fake_make_thumbnail)
    monkeypatch.setattr(media_ops, "probe_video",
                        lambda p: {"duration": 5, "width": 640, "height": 360})

    src = local_file(tmp_path, "s/41.mp4")
    client = FakeClient()
    pipeline = make_pipeline(client=client, db=fake_db(),
                             downloader=fake_downloader({41: src}))

    assert asyncio.run(pipeline.archive_one(item_of(msg_stub(41, "video")))).ok
    thumb = client.calls[0][2]["thumb"]
    assert thumb is not None and os.path.basename(thumb) == "thumb_41.jpg"
    assert not os.path.exists(thumb), "缩略图没被清掉"
    assert not os.path.exists(src)


def test_sha256_hit_marks_duplicate_and_records_file_only(tmp_path, make_pipeline):
    """
    单条 SHA-256 命中：标记 duplicate=True、只补一条 files 记录、不写 messages、不上传。

    没有新的归档消息可记，但文件身份必须照写（否则回填脚本救不了这些行）。
    """
    marks = []

    async def mark(message, duplicate):
        marks.append((message.id, duplicate))

    client = FakeClient()
    db = fake_db(find_by_sha256=lambda sha: {"archived_chat_id": "-1009876543210",
                                             "archived_message_id": 55})
    pipeline = make_pipeline(client=client, db=db, mark=mark,
                             downloader=fake_downloader({41: local_file(tmp_path, "s/41.pdf")}))

    outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

    assert outcome.ok is True
    assert marks == [(41, True)]
    assert [tag for tag, _ in db.writes] == ["file"]
    assert db.writes[0][1]["archived_message_id"] == 55
    assert client.calls == [], "去重命中不该上传"


def test_sha256_hit_in_group_marks_duplicate(tmp_path, make_pipeline):
    """整组里 SHA-256 命中的条目进 duplicates 名单，标记 duplicate=True，不上传"""
    marks = []

    async def mark(message, duplicate):
        marks.append((message.id, duplicate))

    client = FakeClient()
    db = fake_db(find_by_sha256=lambda sha: {"archived_chat_id": "-1009876543210",
                                             "archived_message_id": 55})
    pipeline = make_pipeline(client=client, db=db, mark=mark,
                             downloader=fake_downloader({41: local_file(tmp_path, "b/41.pdf")}))

    outcomes = asyncio.run(pipeline.archive_batch([item_of(msg_stub(41, group="g1"))]))

    assert [o.ok for o in outcomes] == [True]
    assert marks == [(41, True)]
    assert client.calls == []


def test_single_upload_failure_marks_nothing(tmp_path, make_pipeline):
    """单条上传失败不打归档标记（合流后 mark 留在调用方手上，这是那个决定的不变量）"""
    marks = []

    async def mark(message, duplicate):
        marks.append((message.id, duplicate))

    class BoomClient(FakeClient):
        async def send_document(self, chat, path, caption=""):
            raise RuntimeError("上传炸了")

    pipeline = make_pipeline(client=BoomClient(), db=fake_db(), mark=mark,
                             downloader=fake_downloader({41: local_file(tmp_path, "s/41.pdf")}))

    outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

    assert outcome.ok is False and outcome.stage == "upload"
    assert marks == []


class TestGroupExceptionBoundary:
    """
    债二：整组的准备阶段（_plan / _download / _process_each）异常必须变成 Outcome。

    修复前这些异常冒到 main() 的兜底 except，既不记账也不告警，下一轮重扫同样炸 ——
    那条消息永久卡住 checkpoint。
    """

    def test_download_exception_yields_one_process_failure(self, tmp_path, make_pipeline):
        async def boom(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
            raise RuntimeError("tdl 炸了")

        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=SimpleNamespace(download=boom))
        items = [item_of(msg_stub(mid, group="g1")) for mid in (41, 42)]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert [(o.item.media.id, o.ok, o.stage) for o in outcomes] == [(41, False, "process")]

    def test_process_exception_cleans_temp_files(self, tmp_path, make_pipeline, monkeypatch):
        """
        阶段三抛异常时也要清临时文件。

        修复前 temp_files 随异常一起丢掉（它是 _process_each 的返回值），
        下载好的文件永久留在 /data/tmp。
        """
        import media_ops

        def boom(path):
            raise OSError("磁盘读挂了")

        monkeypatch.setattr(media_ops, "sha256_of_file", boom)
        src = local_file(tmp_path, "b/41.pdf")
        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=fake_downloader({41: src}))

        outcomes = asyncio.run(pipeline.archive_batch([item_of(msg_stub(41, group="g1"))]))

        assert [(o.item.media.id, o.ok, o.stage) for o in outcomes] == [(41, False, "process")]
        assert not os.path.exists(src), "异常路径没清临时文件"

    def test_process_exception_cleans_files_not_yet_processed(self, tmp_path, make_pipeline,
                                                             monkeypatch):
        """
        第一条就抛异常时，后面几条已经落盘的下载产物同样要被清掉。

        修复前 temp_files 是在 _process_each 的循环体内逐条登记的，还没轮到的
        条目永久留在 batch_dir。
        """
        import media_ops

        def boom(path):
            raise OSError("磁盘读挂了")

        monkeypatch.setattr(media_ops, "sha256_of_file", boom)
        paths = {mid: local_file(tmp_path, f"b/{mid}.pdf") for mid in (41, 42, 43)}
        pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                                 downloader=fake_downloader(paths))

        outcomes = asyncio.run(pipeline.archive_batch(
            [item_of(msg_stub(mid, group="g1")) for mid in (41, 42, 43)]))

        assert [(o.item.media.id, o.ok, o.stage) for o in outcomes] == [(41, False, "process")]
        assert [p for p in paths.values() if os.path.exists(p)] == [], "有下载产物没被清掉"


def test_single_process_exception_becomes_failure_and_cleans_up(tmp_path, make_pipeline,
                                                               monkeypatch):
    """
    单条路径的非预期异常同样进失败账（与整组对称），临时文件照清。

    修复前它冒到 main() 的兜底 except：不计次、不告警，下一轮同样炸。
    """
    import media_ops

    def boom(path):
        raise OSError("磁盘读挂了")

    monkeypatch.setattr(media_ops, "sha256_of_file", boom)
    src = local_file(tmp_path, "s/41.pdf")
    pipeline = make_pipeline(client=FakeClient(), db=fake_db(),
                             downloader=fake_downloader({41: src}))

    outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

    assert outcome.ok is False and outcome.stage == "process"
    assert "磁盘读挂了" in (outcome.error or "")
    assert not os.path.exists(src)


def test_single_plan_exception_becomes_failure(make_pipeline):
    """
    去重查询/建目录炸了也要进失败账 —— 与整组路径对称。

    archive_batch 的 try 覆盖 _plan（Task 4），archive_one 也必须覆盖。
    """
    def boom(file_unique_id):
        raise RuntimeError("db 锁住了")

    pipeline = make_pipeline(client=FakeClient(), db=fake_db(find_by_unique_id=boom),
                             downloader=fake_downloader({}))

    outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41))))

    assert outcome.ok is False and outcome.stage == "process"


def test_pipeline_config_requires_explicit_values():
    """
    构造时必填的四个值（冷却、压缩阈值、CRF、编码线程数）没有默认值：真相只有
    listener 从环境变量读的那一份，测试默认值在 conftest.make_pipeline。

    别管它们叫「风控三件套」—— CLAUDE.md 里那个词专指 BATCH_SIZE /
    UPLOAD_COOLDOWN_SECONDS / SCAN_INTERVAL_SECONDS。冷却确实是限流值，
    写两遍就会有人改错一边；另三个压缩参数只是调参。
    """
    import pytest
    from pipeline import PipelineConfig

    with pytest.raises(TypeError):
        PipelineConfig()


def test_media_kind_vocabulary_matches():
    """
    pipeline.MEDIA_KINDS 与 media_ops.MediaKind 必须是同一份取值。

    检测顺序（MEDIA_ATTRS）在 media_ops，上传形状（MEDIA_KINDS）在 pipeline ——
    两个模块各存一份取值表，加一种媒体类型时漏掉一边不会有任何静态错误：
    要么下载得到但上传时 KeyError，要么表里有一行永远不会被命中。
    """
    from typing import get_args

    from media_ops import MEDIA_ATTRS, MediaKind
    from pipeline import MEDIA_KINDS

    assert set(get_args(MediaKind)) == set(MEDIA_KINDS)
    # MEDIA_ATTRS 直接从 MediaKind 推导，顺序即声明顺序（document 在 video 之前）
    assert MEDIA_ATTRS == get_args(MediaKind)


class TestVideoNoteUpload:
    """
    video_note（圆形视频）的上传形状：Telegram 的圆形视频在协议层就没有 caption 字段。

    缺陷现场：_send_media 对所有非 video 的 kind 一律传 caption=，而
    send_video_note 是唯一不接受它的 → 任何 video_note 归档抛 TypeError，
    被吞成 upload 失败、重试 3 轮后跳过并告警。假 client 接受任意关键字，
    所以 250 个行为用例全绿也照样漏 —— 于是有了下面这条签名对账。
    """

    def test_send_method_kwargs_match_pyrogram_signatures(self):
        """
        MEDIA_KINDS 每一行声明的关键字，对应的 send_* 方法都必须真的接受。

        不连网、不发消息，纯拿真实签名对账，顺带盯住升级 Pyrogram 时的签名漂移。
        表里一行错（比如给 video_note 打开 video_meta，而它不接受 width/height）
        在这里就红，而不是等到线上归档时被吞成 upload 失败。
        """
        import inspect

        from pipeline import MEDIA_KINDS
        from pyrogram.client import Client

        for kind, spec in MEDIA_KINDS.items():
            params = inspect.signature(getattr(Client, spec.send_method)).parameters
            assert ("caption" in params) is spec.accepts_caption, (
                f"{spec.send_method} 的 caption 支持与 MEDIA_KINDS[{kind!r}] 不符")
            if spec.video_meta:
                missing = [n for n in ("duration", "width", "height", "thumb")
                           if n not in params]
                assert not missing, (
                    f"{spec.send_method} 不接受 {missing}，但 MEDIA_KINDS[{kind!r}] "
                    f"声明了 video_meta")

    def test_video_note_uploads_without_caption(self, tmp_path, make_pipeline):
        """归档 video_note 不该炸，也不该给 send_video_note 传 caption"""
        client = FakeClient()
        pipeline = make_pipeline(client=client, db=fake_db(),
                                 downloader=fake_downloader(
                                     {41: local_file(tmp_path, "s/41.mp4")}))

        outcome = asyncio.run(pipeline.archive_one(item_of(msg_stub(41, "video_note"))))

        assert outcome.ok is True, outcome
        assert [c[0] for c in client.calls] == ["send_video_note"]
        assert "caption" not in client.calls[0][2]

    def test_video_note_caption_survives_in_db(self, tmp_path, make_pipeline):
        """
        文字发不出去，但必须留在库里。

        messages.caption 进 FTS，搜索照样找得到；丢的只是备份频道那条消息上的可见文字。
        """
        db = fake_db()
        pipeline = make_pipeline(client=FakeClient(), db=db,
                                 downloader=fake_downloader(
                                     {41: local_file(tmp_path, "s/41.mp4")}))
        item = item_of(msg_stub(41, "video_note", caption="转发时带的文字"))

        assert asyncio.run(pipeline.archive_one(item)).ok
        assert [kw["caption"] for tag, kw in db.writes
                if tag == "message"] == ["转发时带的文字"]


class TestMediaGroupSendCountMismatch:
    def test_short_sent_list_is_not_silently_dropped(self, tmp_path, make_pipeline):
        """
        send_media_group 回来的条数比上传的少 → 抛出，而不是静默漏记。

        zip 无 strict 时少的那条既不写 DB 也拿不到 Outcome：文件已经在备份频道，
        files/messages 里却没有任何行，_settle_all 跳过它、_group_settled 也看不到它，
        checkpoint 照常推进 —— 永久的去重盲区，且再也不会被扫到。抛出来则由
        archive_batch 的兜底翻译成一条 process 失败，进失败账、有熔断。
        """
        class ShortClient(FakeClient):
            async def send_media_group(self, chat, input_media):
                await super().send_media_group(chat, input_media)
                return [SimpleNamespace(id=901)]  # 两条只回一条

        pipeline = make_pipeline(
            client=ShortClient(), db=fake_db(),
            downloader=fake_downloader({41: local_file(tmp_path, "b/41.pdf"),
                                        42: local_file(tmp_path, "b/42.pdf")}))
        items = [item_of(msg_stub(41, group="g1")), item_of(msg_stub(42, group="g1"))]

        outcomes = asyncio.run(pipeline.archive_batch(items))

        assert sum(o.ok for o in outcomes) < 2, "少回一条不能算两条都成功"
        failures = [o for o in outcomes if not o.ok]
        assert failures and failures[0].stage == "process"


def test_cleanup_survives_unremovable_entry(tmp_path):
    """
    删不掉的临时文件不能让 _cleanup_temp_files 抛异常，也不能中断后面的清理。

    它只在 finally 里被调用，从那里抛出会顶掉已经算好的 Outcome 并绕过 listener 的
    失败记账与熔断：归档明明成功，结果既没有结论也没记账。
    """
    from pipeline import _cleanup_temp_files

    stuck = tmp_path / "not-a-file"
    stuck.mkdir()  # os.remove 对目录必然抛 OSError
    good = local_file(tmp_path, "t/b.bin")

    _cleanup_temp_files([str(stuck), good])

    assert not os.path.exists(good), "第一项失败不能中断后面的清理"
    assert stuck.exists()
