"""
无状态媒体操作单测。不 import listener（因此不需要环境变量、不建目录），
ffprobe / ffmpeg 一律用假 subprocess.run，只有 Pillow 走真实现。
"""

import hashlib
import json
import os
import subprocess
from types import SimpleNamespace

import media_ops
import pytest
from PIL import Image


class TestGetMedia:
    def test_returns_first_non_empty_attr_in_order(self):
        msg = SimpleNamespace(document=None, video="V", photo="P")
        assert media_ops.get_media(msg) == ("video", "V")

    def test_no_media_returns_none_pair(self):
        assert media_ops.get_media(SimpleNamespace()) == (None, None)


class TestSha256AndVerify:
    def test_sha256_matches_hashlib(self, tmp_path):
        f = tmp_path / "a.bin"
        f.write_bytes(b"\x01\x02" * 5000)
        assert media_ops.sha256_of_file(str(f)) == hashlib.sha256(f.read_bytes()).hexdigest()

    def test_too_small_raises(self, tmp_path):
        f = tmp_path / "trunc.bin"
        f.write_bytes(b"\x00" * 10)
        with pytest.raises(RuntimeError, match="小到不合理"):
            media_ops.verify_download_size(str(f), None)

    def test_size_mismatch_raises(self, tmp_path):
        f = tmp_path / "a.bin"
        f.write_bytes(b"\x00" * 2048)
        with pytest.raises(RuntimeError, match="大小对不上"):
            media_ops.verify_download_size(str(f), 4096)

    def test_missing_expected_size_only_checks_minimum(self, tmp_path):
        """expected_size 拿不到就只做最小体积检查，不当作可疑信号"""
        f = tmp_path / "a.bin"
        f.write_bytes(b"\x00" * 2048)
        media_ops.verify_download_size(str(f), None)
        media_ops.verify_download_size(str(f), 0)

    def test_min_size_is_injectable(self, tmp_path):
        """阈值可传：PipelineConfig.min_plausible_size 会喂进来"""
        f = tmp_path / "tiny.bin"
        f.write_bytes(b"\x00" * 10)
        media_ops.verify_download_size(str(f), 10, min_size=1)


def _write_image(path, fmt, mode="RGB", size=(8, 8)):
    Image.new(mode, size, "red").save(path, fmt)
    return str(path)


class TestFixMediaFormat:
    def test_webp_becomes_jpeg_and_original_removed(self, tmp_path):
        src = _write_image(tmp_path / "p.webp", "WEBP")
        out = media_ops.fix_media_format(src, "photo")
        assert out == src + ".jpg"
        assert not os.path.exists(src)
        with Image.open(out) as img:
            assert img.format == "JPEG"

    def test_rgba_png_is_converted_before_save(self, tmp_path):
        """RGBA/P/PA 不先转 RGB，JPEG 保存会抛异常"""
        src = _write_image(tmp_path / "p.png", "PNG", mode="RGBA")
        out = media_ops.fix_media_format(src, "photo")
        with Image.open(out) as img:
            assert img.format == "JPEG"

    def test_jpeg_without_suffix_gets_renamed(self, tmp_path):
        src = _write_image(tmp_path / "noext", "JPEG")
        out = media_ops.fix_media_format(src, "photo")
        assert out == src + ".jpg"
        assert os.path.exists(out) and not os.path.exists(src)

    def test_jpeg_with_suffix_untouched(self, tmp_path):
        src = _write_image(tmp_path / "p.jpg", "JPEG")
        assert media_ops.fix_media_format(src, "photo") == src

    def test_other_format_without_suffix_gets_jpg(self, tmp_path):
        """既不是 WebP/PNG/GIF 也不是 JPEG，但没后缀 —— 只补后缀不转码"""
        src = _write_image(tmp_path / "noext_bmp", "BMP")
        out = media_ops.fix_media_format(src, "photo")
        assert out == src + ".jpg"
        with Image.open(out) as img:
            assert img.format == "BMP"

    def test_video_without_suffix_gets_mp4(self, tmp_path):
        src = tmp_path / "clip"
        src.write_bytes(b"\x00" * 16)
        out = media_ops.fix_media_format(str(src), "video")
        assert out == str(src) + ".mp4"

    def test_video_with_suffix_untouched(self, tmp_path):
        src = tmp_path / "clip.mp4"
        src.write_bytes(b"\x00" * 16)
        assert media_ops.fix_media_format(str(src), "video") == str(src)

    def test_document_untouched(self, tmp_path):
        src = tmp_path / "a.pdf"
        src.write_bytes(b"\x00" * 16)
        assert media_ops.fix_media_format(str(src), "document") == str(src)

    def test_unreadable_image_falls_back_to_original(self, tmp_path):
        """Pillow 打不开就按原文件继续（记 warning），不能抛出去中断归档"""
        src = tmp_path / "broken.jpg"
        src.write_bytes(b"not an image")
        assert media_ops.fix_media_format(str(src), "photo") == str(src)
        assert os.path.exists(src)


def _fake_run(monkeypatch, side_effect):
    """替换 media_ops.subprocess.run，返回记录调用的列表。"""
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        return side_effect(cmd, len(calls))

    monkeypatch.setattr(media_ops.subprocess, "run", run)
    return calls


class TestProbeVideo:
    def test_parses_duration_and_size(self, monkeypatch):
        payload = {
            "streams": [{"codec_type": "audio"},
                        {"codec_type": "video", "width": 1920, "height": 1080}],
            "format": {"duration": "12.7"},
        }
        _fake_run(monkeypatch, lambda cmd, n: SimpleNamespace(stdout=json.dumps(payload)))
        assert media_ops.probe_video("a.mp4") == {"duration": 12, "width": 1920, "height": 1080}

    def test_no_video_stream_returns_none(self, monkeypatch):
        payload = {"streams": [{"codec_type": "audio"}], "format": {"duration": "3"}}
        _fake_run(monkeypatch, lambda cmd, n: SimpleNamespace(stdout=json.dumps(payload)))
        assert media_ops.probe_video("a.mp4") is None

    def test_missing_keys_returns_none(self, monkeypatch):
        _fake_run(monkeypatch, lambda cmd, n: SimpleNamespace(stdout="{}"))
        assert media_ops.probe_video("a.mp4") is None

    def test_bad_json_returns_none(self, monkeypatch):
        _fake_run(monkeypatch, lambda cmd, n: SimpleNamespace(stdout="not json"))
        assert media_ops.probe_video("a.mp4") is None

    @pytest.mark.parametrize("error", [
        FileNotFoundError("ffprobe 缺失"),
        subprocess.CalledProcessError(1, "ffprobe", stderr="boom"),
        subprocess.TimeoutExpired("ffprobe", 30),
    ])
    def test_failures_return_none(self, monkeypatch, error):
        """任何失败都返回 None，调用方回退源消息元数据，不阻塞归档"""
        def boom(cmd, n):
            raise error
        _fake_run(monkeypatch, boom)
        assert media_ops.probe_video("a.mp4") is None


class TestMakeThumbnail:
    def test_first_quality_success(self, monkeypatch, tmp_path):
        thumb = tmp_path / "t.jpg"

        def run(cmd, n):
            with open(cmd[-1], "wb") as f:
                f.write(b"\x00" * 1024)
            return SimpleNamespace(returncode=0)

        calls = _fake_run(monkeypatch, run)
        assert media_ops.make_thumbnail("a.mp4", str(thumb)) == str(thumb)
        assert len(calls) == 1
        # -ss 必须在 -i 之前，否则 ffmpeg 解码整段视频才 seek
        assert calls[0].index("-ss") < calls[0].index("-i")

    def test_retries_all_qualities_then_gives_up(self, monkeypatch, tmp_path):
        """四档画质都超 200KB：删掉半成品返回 None，且真的试了四档"""
        thumb = tmp_path / "t.jpg"

        def run(cmd, n):
            with open(cmd[-1], "wb") as f:
                f.write(b"\x00" * (300 * 1024))
            return SimpleNamespace(returncode=0)

        calls = _fake_run(monkeypatch, run)
        assert media_ops.make_thumbnail("a.mp4", str(thumb)) is None
        assert [c[c.index("-q:v") + 1] for c in calls] == ["5", "10", "20", "31"]
        assert not thumb.exists()

    def test_ffmpeg_missing_returns_none(self, monkeypatch, tmp_path):
        def boom(cmd, n):
            raise FileNotFoundError("ffmpeg 缺失")
        calls = _fake_run(monkeypatch, boom)
        assert media_ops.make_thumbnail("a.mp4", str(tmp_path / "t.jpg")) is None
        assert len(calls) == 1

    def test_timeout_does_not_retry(self, monkeypatch, tmp_path):
        """超时卡在解码上，换 -q:v 不会变快，直接跳出循环"""
        def boom(cmd, n):
            raise subprocess.TimeoutExpired("ffmpeg", 20)
        calls = _fake_run(monkeypatch, boom)
        assert media_ops.make_thumbnail("a.mp4", str(tmp_path / "t.jpg")) is None
        assert len(calls) == 1
