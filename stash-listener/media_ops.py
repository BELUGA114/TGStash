"""
无状态媒体操作：哈希、体积校验、格式修正、ffprobe 探测、ffmpeg 抽帧。

只碰文件系统与 ffmpeg —— 不 import pyrogram、不碰 DB、不读环境变量。
沿用 origin.py / archive_entry.py 的惯例：消息与媒体对象只按属性取值，
测试用 SimpleNamespace 桩即可。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from typing import Literal, get_args

from PIL import Image

logger = logging.getLogger(__name__)

MIN_PLAUSIBLE_SIZE = 1024  # 1KB；网络中断留下的文件通常离谱地小或是 0 字节

# 媒体类型的取值表。pipeline.MEDIA_KINDS 按这份取值给出每种类型的上传形状，
# test_pipeline.py 的 test_media_kind_vocabulary_matches 盯着两边一致
MediaKind = Literal["document", "video", "photo", "audio", "animation", "voice", "video_note"]

# get_media 的检测顺序 = Literal 的声明顺序。从 MediaKind 推导而不是再抄一遍：
# 同一份取值写两处，加类型时必然漏一边
MEDIA_ATTRS: tuple[MediaKind, ...] = get_args(MediaKind)


def get_media(message):
    """
    返回 (媒体类型, 媒体对象)，都没有就返回 (None, None)。

    返回值不标注类型：Pyrogram 的 Document/Video/Photo/... 没有公共基类，
    标成 object 会让调用方每个 .file_unique_id 访问都报错，标成联合类型
    又要 import pyrogram（本模块刻意不依赖它）。
    """
    for attr in MEDIA_ATTRS:
        obj = getattr(message, attr, None)
        if obj:
            return attr, obj
    return None, None


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_download_size(local_path: str, expected_size,
                         min_size: int = MIN_PLAUSIBLE_SIZE) -> None:
    """
    基线校验：抓最离谱的截断情况，不追求精确匹配。
    expected_size 拿不到就只做最小体积检查，不当作可疑信号。
    """
    actual_size = os.path.getsize(local_path)

    if actual_size < min_size:
        raise RuntimeError(f"文件小到不合理（{actual_size} 字节），大概率下载中断")

    if expected_size and actual_size != expected_size:
        raise RuntimeError(
            f"文件大小对不上（期望 {expected_size}，实际 {actual_size}），疑似下载不完整"
        )


def fix_media_format(path: str, kind: str | None) -> str:
    """
    下载后的文件格式可能与 Telegram 声称的类型不匹配。

    - WebP/PNG/GIF → 转 JPEG，保证 Telegram 内联展示
    - 无后缀视频/图片 → 补后缀，否则 Telegram 解析不出缩略图和时长
    """
    if kind == "video":
        if not os.path.splitext(path)[1]:
            # Telegram 的视频实际都是 mp4 容器，补后缀它才解析得出时长和缩略图
            new_path = path + ".mp4"
            os.rename(path, new_path)
            logger.debug("补后缀 video → .mp4")
            return new_path
        return path

    if kind != "photo":
        return path
    try:
        with Image.open(path) as img:
            fmt = img.format  # 'JPEG', 'WEBP', 'PNG', 'GIF', etc.

            # 非 JPEG 格式统一转为 JPEG，Telegram 才能内联显示
            if fmt in ("WEBP", "PNG", "GIF"):
                converted = path + ".jpg"
                if img.mode in ("RGBA", "P", "PA"):
                    img = img.convert("RGB")
                img.save(converted, "JPEG", quality=95)
            else:
                converted = None

        # 删除/改名必须在 with 之外：Pillow 只读文件头时不会关掉句柄，
        # Windows 上 os.rename/os.remove 会抛 PermissionError，被下面的 except
        # 吞成「按原文件继续」——后缀永远补不上，且只在 Windows 上复现
        if converted is not None:
            os.remove(path)
            logger.debug("格式转换 %s → JPEG", fmt)
            return converted

        # 无后缀的图片（含本身就是 JPEG 的）统一补 .jpg，否则 Telegram 解析不出预览
        if not os.path.splitext(path)[1]:
            renamed = path + ".jpg"
            os.rename(path, renamed)
            logger.debug("补后缀 %s", fmt or "未知")
            return renamed
    except Exception:
        logger.warning("图片格式修正失败，按原文件继续：%s", path, exc_info=True)
    return path


def probe_video(path: str) -> dict | None:
    """
    用 ffprobe 量出真实的 duration/width/height。
    Telegram 对大文件可能解析不出元数据（duration=0），不能信任源消息自带的值。
    任何失败都返回 None，调用方看到 None 回退到源消息元数据 → 0，不阻塞归档。
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", path],
            capture_output=True, text=True, check=True, timeout=30,
        )
        info = json.loads(result.stdout)
        stream = next((s for s in info["streams"] if s.get("codec_type") == "video"), None)
        if stream is None:
            logger.warning("ffprobe 没解析出视频流：%s", path)
            return None
        duration = int(float(info.get("format", {}).get("duration", 0)))
        return {
            "duration": duration,
            "width": stream.get("width", 0),
            "height": stream.get("height", 0),
        }
    except FileNotFoundError:
        logger.error("ffprobe 可执行文件不存在，检查镜像是否装了 ffmpeg")
        return None
    except subprocess.CalledProcessError as e:
        logger.warning("ffprobe 处理失败（退出码 %s）：%s：%s",
                       e.returncode, path, (e.stderr or "")[:200])
        return None
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe 超时（30s）：%s", path)
        return None
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning("ffprobe 输出解析失败：%s：%s", path, e)
        return None


def make_thumbnail(video_path: str, thumb_path: str,
                   timestamp: str = "00:00:01") -> str | None:
    """
    用 ffmpeg 抽一帧当缩略图。
    Telegram 对大文件不保证生成缩略图，Pyrogram send_video 的 thumb 参数
    是唯一可靠途径——客户端主动提供缩略图，不指望服务端。
    画质从 5 递减到 31 重试；任何失败返回 None，不阻塞归档。
    """
    for q in (5, 10, 20, 31):
        try:
            result = subprocess.run(
                ["ffmpeg", "-y", "-ss", timestamp, "-i", video_path,
                 "-vframes", "1",
                 "-vf", "scale=320:320:force_original_aspect_ratio=decrease:force_divisible_by=2",
                 "-q:v", str(q), thumb_path],
                capture_output=True, timeout=20, check=False,
            )
        except FileNotFoundError:
            logger.error("ffmpeg 可执行文件不存在，检查镜像是否装了 ffmpeg")
            return None
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg 截图超时（20s），放弃：%s", video_path)
            return None  # 超时卡在解码上，换 -q:v 不会变快，直接跳出循环

        if (result.returncode == 0 and os.path.exists(thumb_path)
                and os.path.getsize(thumb_path) <= 200 * 1024):
            return thumb_path

    # 所有画质档位都试过了仍超标（320px 下极少发生），放弃
    if os.path.exists(thumb_path):
        os.remove(thumb_path)
    return None
