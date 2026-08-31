"""视频压缩：ffmpeg H.264 CRF 恒定质量转码。失败不抛异常，调用方回退原始文件。"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess

DEFAULT_CRF = 28
MAX_HEIGHT = 1920  # 超过此分辨率才降采样，1080p 及以下不动
# 编码线程数上限：x264 默认按核心数×1.5 开线程，高核数机器上初始化时内存暴增可能被 OOM 杀死
DEFAULT_THREADS = int(os.environ.get("VIDEO_COMPRESS_THREADS", "4"))

logger = logging.getLogger(__name__)


def _remove_if_exists(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def build_compress_command(src_path: str, dst_path: str, crf: int = DEFAULT_CRF) -> list[str]:
    """构造 ffmpeg 命令。只降采样不升采样；音频流拷贝不重编码。"""
    cmd = [
        "ffmpeg", "-y", "-i", src_path,
        "-c:v", "libx264", "-crf", str(crf), "-preset", "medium",
    ]
    # 0 表示不限制线程数
    if DEFAULT_THREADS > 0:
        cmd += ["-threads", str(DEFAULT_THREADS)]
    cmd += [
        "-vf", (
            f"scale='min({MAX_HEIGHT},iw)':'min({MAX_HEIGHT},ih)':"
            "force_original_aspect_ratio=decrease"
        ),
        "-c:a", "copy", "-movflags", "+faststart",
        dst_path,
    ]
    return cmd


def compress_video(src_path: str, dst_path: str, crf: int = DEFAULT_CRF) -> bool:
    """ffmpeg 转码到 dst_path。返回成功与否；失败删除半成品，由调用方回退原始。"""
    if shutil.which("ffmpeg") is None:
        return False
    try:
        proc = subprocess.run(
            build_compress_command(src_path, dst_path, crf),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except Exception as e:
        logger.warning("ffmpeg 压缩启动失败 %s → %s: %s", src_path, dst_path, e)
        _remove_if_exists(dst_path)
        return False
    if proc.returncode != 0:
        logger.warning("ffmpeg 压缩失败（退出码 %s）%s → %s: %s",
                       proc.returncode, src_path, dst_path,
                       (proc.stderr or "")[-500:])
        _remove_if_exists(dst_path)
        return False
    return True


def maybe_compress_video(
    local_path: str,
    temp_files: list[str],
    enabled: bool,
    min_size_mb: int,
    crf: int,
    *,
    tag: str | int,
) -> str:
    """
    体积≥阈值且启用时转码，产物比原始小才用压缩版，否则回退原始。

    返回实际用于后续管道的文件路径（压缩版或原始）。压缩版会追加进 temp_files
    统一清理；原始文件由调用方已有追踪，不在这里重复添加。失败/未变小删除半成品。

    tag 必填（用消息 id），用来给产物取唯一名字。不给默认值是故意的：
    一旦有默认值就会有调用方不传，媒体组互相覆盖的缺陷原样复活。
    """
    if not (enabled and os.path.getsize(local_path) >= min_size_mb * 1024 * 1024):
        return local_path
    # 产物放源文件所在目录（媒体组 tdl 文件在 batch_dir，msg_dir 可能已被清理），
    # 同目录才能保证 ffmpeg 有可写位置，且 finally 清理时不留下空洞。
    # 名字必须带 tag：媒体组整组文件都在同一个 batch_dir，固定名会被 ffmpeg -y
    # 覆盖，两条待上传条目指向同一个文件——A 的位置发出 B 的内容，而 DB 记的是
    # A 的 sha256 与文件身份，归档内容与去重身份从此不符
    dst_path = os.path.join(os.path.dirname(local_path), f"compressed_{tag}.mp4")
    logger.info("压缩视频 %s (%s bytes) → CRF %s ...",
                os.path.basename(local_path), os.path.getsize(local_path), crf)
    ok = compress_video(local_path, dst_path, crf)
    if ok and os.path.getsize(dst_path) < os.path.getsize(local_path):
        temp_files.append(dst_path)
        logger.info("压缩完成 %s → %s bytes（原始 %s bytes）",
                    os.path.basename(local_path), os.path.getsize(dst_path),
                    os.path.getsize(local_path))
        return dst_path
    _remove_if_exists(dst_path)
    logger.info("压缩失败或未变小，回退原始 %s", os.path.basename(local_path))
    return local_path
