"""视频压缩：ffmpeg H.264 CRF 恒定质量转码。失败不抛异常，调用方回退原始文件。"""
from __future__ import annotations

import os
import shutil
import subprocess

DEFAULT_CRF = 28
MAX_HEIGHT = 1920  # 超过此分辨率才降采样，1080p 及以下不动


def build_compress_command(src_path: str, dst_path: str, crf: int = DEFAULT_CRF) -> list[str]:
    """构造 ffmpeg 命令。只降采样不升采样；音频流拷贝不重编码。"""
    return [
        "ffmpeg", "-y", "-i", src_path,
        "-c:v", "libx264", "-crf", str(crf), "-preset", "medium",
        "-vf", (
            f"scale='min({MAX_HEIGHT},iw)':'min({MAX_HEIGHT},ih)':"
            "force_original_aspect_ratio=decrease"
        ),
        "-c:a", "copy", "-movflags", "+faststart",
        dst_path,
    ]


def compress_video(src_path: str, dst_path: str, crf: int = DEFAULT_CRF) -> bool:
    """ffmpeg 转码到 dst_path。返回成功与否；失败删除半成品，由调用方回退原始。"""
    if shutil.which("ffmpeg") is None:
        return False
    try:
        proc = subprocess.run(
            build_compress_command(src_path, dst_path, crf),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        if os.path.exists(dst_path):
            os.remove(dst_path)
        return False
    if proc.returncode != 0:
        if os.path.exists(dst_path):
            os.remove(dst_path)
        return False
    return True
