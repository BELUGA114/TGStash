"""视频压缩模块测试：注入假 ffmpeg，不真正转码。"""
import asyncio
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import compress_video as cv
import listener


def test_build_command_has_key_params():
    cmd = cv.build_compress_command("src.mp4", "dst.mp4", crf=28)
    s = " ".join(cmd)
    assert "ffmpeg" in cmd[0]
    assert "-crf" in s and "28" in s
    assert "libx264" in s
    assert "-c:a" in s and "copy" in s
    assert "-movflags" in s and "+faststart" in s
    # 只降采样不升采样：1920 上限 + force_original_aspect_ratio=decrease
    assert "min(1920" in s and "force_original_aspect_ratio=decrease" in s


def test_build_command_respects_crf_override():
    cmd = cv.build_compress_command("s.mp4", "d.mp4", crf=30)
    assert "-crf" in cmd and cmd[cmd.index("-crf") + 1] == "30"


def test_compress_video_success(tmp_path):
    src = tmp_path / "src.mp4"
    dst = tmp_path / "dst.mp4"
    src.write_bytes(b"fake")
    with patch("compress_video.subprocess.run") as m_run:
        m_run.return_value = subprocess.CompletedProcess([], 0)
        assert cv.compress_video(str(src), str(dst)) is True
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
        assert cv.compress_video(str(src), str(dst)) is False
    assert not dst.exists()  # 半成品被删除


def test_compress_video_exception_is_caught(tmp_path):
    src = tmp_path / "src.mp4"
    dst = tmp_path / "dst.mp4"
    src.write_bytes(b"fake")
    with patch("compress_video.subprocess.run", side_effect=FileNotFoundError("ffmpeg 缺失")):
        assert cv.compress_video(str(src), str(dst)) is False
    assert not dst.exists()


def _stub_message(msg_id: int) -> object:
    """构造极简 Message 桩：带 video 属性即可触发 archive_single 的视频路径。

    get_media 按 MEDIA_ATTRS 顺序取第一个非空属性（listener.MEDIA_ATTRS[1] == "video"），
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


def test_single_video_compress_failure_falls_back_to_original(monkeypatch, tmp_path):
    # 开启压缩 + 阈值 0（任何视频都触发）
    monkeypatch.setattr(listener, "VIDEO_COMPRESS_ENABLED", True)
    monkeypatch.setattr(listener, "VIDEO_COMPRESS_MIN_SIZE_MB", 0)

    msg = _stub_message(9001)
    sent = SimpleNamespace(id=7777)  # archive_single 需要 sent.id 落库
    calls = {"compress": 0}

    # verify_download_size 要求文件 ≥1KB 且存在，fake 下载返回真实文件
    src_video = tmp_path / "src.mp4"
    src_video.write_bytes(b"\x00" * 2048)

    async def fake_download(messages, *a, **k):
        return {messages[0].id: str(src_video)}

    def fake_compress(src, dst, crf):
        # 实现里用 asyncio.to_thread 调 compress_video（同步函数丢线程池），
        # 所以这里也必须是同步函数，否则 to_thread 拿到的是协程对象
        calls["compress"] += 1
        return False  # 压缩失败 → 回退原始

    async def fake_send_video(chat, path, **k):
        assert path == str(src_video)  # 回退后上传原始
        return sent

    monkeypatch.setattr(listener.tdl_downloader, "download", fake_download)
    monkeypatch.setattr(listener, "compress_video", fake_compress)
    monkeypatch.setattr(listener.app, "send_video", fake_send_video)
    monkeypatch.setattr(listener.db, "find_by_sha256", lambda *a, **k: None)
    monkeypatch.setattr(listener.db, "find_by_unique_id", lambda *a, **k: None)
    monkeypatch.setattr(listener.db, "record_file", lambda *a, **k: None)
    monkeypatch.setattr(listener.db, "record_message", lambda *a, **k: None)

    assert asyncio.run(listener.archive_single(msg, mark=False)) is True
    assert calls["compress"] == 1
