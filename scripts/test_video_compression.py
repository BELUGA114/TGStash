"""视频压缩模块测试：注入假 ffmpeg，不真正转码。"""
import os
import subprocess
from unittest.mock import patch

import compress_video as cv


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
