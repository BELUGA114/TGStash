"""视频压缩模块测试：注入假 ffmpeg，不真正转码。"""
import asyncio
import os
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import compress_video as cv
import media_ops


def test_build_command_has_key_params():
    cmd = cv.build_compress_command("src.mp4", "dst.mp4", crf=28, threads=4)
    s = " ".join(cmd)
    assert "ffmpeg" in cmd[0]
    assert "-crf" in s and "28" in s
    assert "libx264" in s
    assert "-threads" in s
    assert "-c:a" in s and "copy" in s
    assert "-movflags" in s and "+faststart" in s
    # 只降采样不升采样：1920 上限 + force_original_aspect_ratio=decrease
    assert "min(1920" in s and "force_original_aspect_ratio=decrease" in s


def test_build_command_respects_crf_override():
    cmd = cv.build_compress_command("s.mp4", "d.mp4", crf=30, threads=4)
    assert "-crf" in cmd and cmd[cmd.index("-crf") + 1] == "30"


def test_build_command_threads_zero_omits_flag():
    """threads=0 表示不限制线程数，不传 -threads。参数直传，不再改模块常量"""
    cmd = cv.build_compress_command("s.mp4", "d.mp4", crf=28, threads=0)
    assert "-threads" not in cmd


def test_compress_video_success(tmp_path):
    src = tmp_path / "src.mp4"
    dst = tmp_path / "dst.mp4"
    src.write_bytes(b"fake")
    with patch("compress_video.subprocess.run") as m_run:
        m_run.return_value = subprocess.CompletedProcess([], 0)
        assert cv.compress_video(str(src), str(dst), crf=28, threads=4) is True
        m_run.assert_called_once()
        assert str(src) in m_run.call_args.args[0]  # src 在 dst 之前
        assert str(dst) in m_run.call_args.args[0]


def test_compress_video_failure_removes_half_product(tmp_path):
    src = tmp_path / "src.mp4"
    dst = tmp_path / "dst.mp4"
    src.write_bytes(b"fake")
    dst.write_bytes(b"partial")  # 模拟 ffmpeg 已写出半成品
    with patch("compress_video.subprocess.run") as m_run:
        m_run.return_value = subprocess.CompletedProcess([], 1)
        assert cv.compress_video(str(src), str(dst), crf=28, threads=4) is False
    assert not dst.exists()  # 半成品被删除


def test_compress_video_exception_is_caught(tmp_path):
    src = tmp_path / "src.mp4"
    dst = tmp_path / "dst.mp4"
    src.write_bytes(b"fake")
    with patch("compress_video.subprocess.run", side_effect=FileNotFoundError("ffmpeg 缺失")):
        assert cv.compress_video(str(src), str(dst), crf=28, threads=4) is False
    assert not dst.exists()


def _stub_message(msg_id: int) -> object:
    """构造极简 Message 桩：带 video 属性即可触发 archive_single 的视频路径。

    get_media 按 MEDIA_ATTRS 顺序取第一个非空属性（media_ops.MEDIA_ATTRS[1] == "video"），
    只有 video 属性才会让 kind == "video" 走压缩段。
    """
    return SimpleNamespace(
        id=msg_id,
        chat=SimpleNamespace(id=-100123, title="stub"),
        date=None,
        media_group_id=None,
        caption="",
        text="",
        from_user=SimpleNamespace(first_name="tester", id=1),  # sender_name 需要
        video=SimpleNamespace(file_unique_id=f"uniq-{msg_id}", duration=5, width=640, height=360),
    )


def _run_single_archive(msg, src_video, fake_compress, monkeypatch, make_pipeline,
                        send_assert=None, *, compress_enabled=True, min_size_mb=0):
    """跑一遍 archive_one 视频路径，fake 下载返回 src_video，mock 压缩与上传。"""
    from archive_entry import ROUTE_FORWARD, ArchiveItem, Entry

    sent = SimpleNamespace(id=7777)  # archive_one 需要 sent.id 落库

    async def fake_download(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
        return {messages[0].id: str(src_video)}

    async def fake_send_video(chat, path, **k):
        if send_assert:
            send_assert(path)
        return sent

    monkeypatch.setattr(cv, "compress_video", fake_compress)

    pipeline = make_pipeline(
        client=SimpleNamespace(send_video=fake_send_video),
        db=SimpleNamespace(
            find_by_sha256=lambda *a, **k: None,
            find_by_unique_id=lambda *a, **k: None,
            record_file=lambda *a, **k: None,
            record_archived=lambda *a, **k: None,
        ),
        downloader=SimpleNamespace(download=fake_download),
        video_compress_enabled=compress_enabled,
        video_compress_min_size_mb=min_size_mb,
    )

    item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
    assert asyncio.run(pipeline.archive_one(item)).ok is True


def test_single_video_compress_failure_falls_back_to_original(monkeypatch, tmp_path,
                                                             make_pipeline):
    # 开启压缩 + 阈值 0（任何视频都触发）
    msg = _stub_message(9001)
    calls = {"compress": 0}

    # verify_download_size 要求文件 ≥1KB 且存在，fake 下载返回真实文件
    src_video = tmp_path / "src.mp4"
    src_video.write_bytes(b"\x00" * 2048)

    def fake_compress(src, dst, crf, threads):
        calls["compress"] += 1
        return False  # 压缩失败 → 回退原始

    _run_single_archive(
        msg, src_video, fake_compress, monkeypatch, make_pipeline,
        send_assert=lambda path: path == str(src_video),  # 回退后上传原始
    )
    assert calls["compress"] == 1


def test_single_video_compress_smaller_uses_compressed(monkeypatch, tmp_path, make_pipeline):
    # 压缩成功且更小 → 用压缩版上传
    msg = _stub_message(9001)
    src_video = tmp_path / "src.mp4"
    src_video.write_bytes(b"\x00" * 4096)

    def fake_compress(src, dst, crf, threads):
        # 模拟 ffmpeg 写出更小的压缩产物
        with open(dst, "wb") as f:
            f.write(b"\x00" * 1024)
        return True

    def send_assert(path):
        assert path != str(src_video)  # 上传的是压缩版
        assert path.endswith("compressed_9001.mp4")  # 产物名带消息 id

    _run_single_archive(msg, src_video, fake_compress, monkeypatch, make_pipeline, send_assert)


def test_single_video_compress_larger_falls_back(monkeypatch, tmp_path, make_pipeline):
    # 压缩成功但产物≥原始 → 回退原始
    msg = _stub_message(9001)
    src_video = tmp_path / "src.mp4"
    src_video.write_bytes(b"\x00" * 2048)

    def fake_compress(src, dst, crf, threads):
        with open(dst, "wb") as f:
            f.write(b"\x00" * 4096)  # 比原始大 → 回退
        return True

    _run_single_archive(
        msg, src_video, fake_compress, monkeypatch, make_pipeline,
        send_assert=lambda path: path == str(src_video),
    )


def test_compress_video_not_called_below_threshold(monkeypatch, tmp_path, make_pipeline):
    # 关闭压缩 → 不调用 compress_video，直接走原管道
    msg = _stub_message(9001)
    src_video = tmp_path / "src.mp4"
    src_video.write_bytes(b"\x00" * 2048)
    calls = {"compress": 0}

    def fake_compress(src, dst, crf, threads):
        calls["compress"] += 1
        return False

    _run_single_archive(
        msg, src_video, fake_compress, monkeypatch, make_pipeline,
        send_assert=lambda path: path == str(src_video),
        compress_enabled=False,
    )
    assert calls["compress"] == 0


def test_compress_output_lands_in_source_dir(tmp_path):
    # 回归：媒体组 tdl 文件在 batch_dir（msg_dir 已被清理），压缩产物必须落在
    # local_path 所在目录，否则 ffmpeg 因目标目录不存在而恒失败
    src = tmp_path / "batch" / "src.mp4"
    src.parent.mkdir()
    src.write_bytes(b"\x00" * 2048)
    temp_files = []

    def fake_compress(src_path, dst_path, crf, threads):
        assert os.path.dirname(dst_path) == os.path.dirname(src_path)  # 同目录
        with open(dst_path, "wb") as f:
            f.write(b"\x00" * 1024)
        return True

    with patch("compress_video.compress_video", fake_compress):
        out = cv.maybe_compress_video(str(src), temp_files, True, 0, 28, tag=9001, threads=4)
    assert out == str(src.parent / "compressed_9001.mp4")
    assert temp_files == [str(src.parent / "compressed_9001.mp4")]


def _write_smaller(src_path, dst_path, crf, threads):
    """假 ffmpeg：产出比源文件小的文件，内容写源文件名以便追溯产物属于谁。"""
    with open(dst_path, "wb") as f:
        f.write(os.path.basename(src_path).encode())
    return True


def test_compress_output_name_is_unique_per_message(tmp_path):
    """
    回归：媒体组两个视频都在 batch_dir，产物名固定成 compressed.mp4 会互相覆盖。

    后压的那个覆盖先压的，两条待上传条目指向同一个文件——A 的位置发出 B 的内容，
    DB 却记着 A 的 sha256 与文件身份，归档内容与去重身份不符。
    """
    batch = tmp_path / "batch"
    batch.mkdir()
    outs = []
    for mid in (41, 42):
        src = batch / f"{mid}.mp4"
        src.write_bytes(bytes([mid]) * 2048)
        temp_files = []
        with patch("compress_video.compress_video", _write_smaller):
            outs.append(cv.maybe_compress_video(
                str(src), temp_files, True, 0, 28, tag=mid, threads=4))

    assert outs[0] != outs[1], "两个视频的压缩产物不能是同一个文件"
    assert os.path.exists(outs[0]) and os.path.exists(outs[1])
    # 产物内容对得上各自的源
    with open(outs[0], "rb") as f:
        assert f.read() == b"41.mp4"
    with open(outs[1], "rb") as f:
        assert f.read() == b"42.mp4"


def test_media_group_two_videos_upload_distinct_files(monkeypatch, tmp_path, make_pipeline):
    """
    整条路径回归：媒体组两个视频压缩后，send_media_group 必须收到两个不同的文件。

    产物名固定时两条 InputMediaVideo 都指向后压的那一个，视频 A 的位置发出 B 的内容。
    """
    from archive_entry import ROUTE_FORWARD, ArchiveItem, Entry

    # tdl 成功时整组文件都落在同一个 batch_dir —— 冲突的前提
    batch = tmp_path / "batch"
    batch.mkdir()
    sources = {}
    for mid in (41, 42):
        src = batch / f"{mid}.mp4"
        src.write_bytes(bytes([mid]) * 2048)
        sources[mid] = str(src)

    async def fake_download(messages, dest, **k):
        return {m.id: sources[m.id] for m in messages}

    captured = {}
    produced = {}

    def recording_compress(src_path, dst_path, crf, threads):
        """记录「哪个产物由哪个源转码而来」，断言时用它对账。"""
        produced[dst_path] = os.path.basename(src_path)
        return _write_smaller(src_path, dst_path, crf, threads)

    async def fake_send_media_group(chat, input_media):
        captured["paths"] = [m.media for m in input_media]
        return [SimpleNamespace(id=900 + i) for i in range(len(input_media))]

    async def noop(*a, **k):
        return None

    monkeypatch.setattr(cv, "compress_video", recording_compress)
    monkeypatch.setattr(media_ops, "probe_video", lambda p: {"duration": 5, "width": 640, "height": 360})
    monkeypatch.setattr(media_ops, "make_thumbnail", lambda *a, **k: None)

    pipeline = make_pipeline(
        client=SimpleNamespace(send_media_group=fake_send_media_group),
        db=SimpleNamespace(
            find_by_unique_id=lambda x: None,
            find_by_sha256=lambda x: None,
            record_file=lambda *a, **k: None,
            record_archived=lambda *a, **k: None,
        ),
        downloader=SimpleNamespace(download=fake_download),
        video_compress_enabled=True,
        video_compress_min_size_mb=0,
    )

    items = []
    for mid in (41, 42):
        msg = _stub_message(mid)
        items.append(ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD)))

    outcomes = asyncio.run(pipeline.archive_batch(items))

    assert all(o.ok for o in outcomes), "两条都该归档成功"
    paths = captured["paths"]
    assert paths[0] != paths[1], f"两条上传指向同一个文件：{paths}"
    # 每条上传的产物必须由它自己的源视频转码而来。产物名冲突时两条会指向同一个
    # 文件、内容都是后压的那个，这行会抓到
    assert [produced[p] for p in paths] == ["41.mp4", "42.mp4"]
