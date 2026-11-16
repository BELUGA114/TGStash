"""
进程入口共用的日志配置：按 LOG_LEVEL 配根日志，抑制 Pyrogram 内部日志。

四个入口（listener / search / login / backfill_metadata）原来各抄一份，
连同那段「getLevelNamesMapping 而不是 getattr」的注释——抄到第四份时
注释已经开始走样。收进唯一一份。

调用时机属于进程启动，不属于 import：listener.py 的既有约定，
这里不强制，但新代码别在模块级调它。
"""

from __future__ import annotations

import logging
import os


def configure_logging() -> None:
    """
    配置根日志。LOG_LEVEL 从环境变量读，默认 INFO。

    非法取值静默回退 INFO，与四个调用点原先的行为一致。
    """
    # 走 getLevelNamesMapping 而不是 getattr(logging, LOG_LEVEL)：后者对小写的
    # LOG_LEVEL=debug 会取到 logging.debug 这个函数，basicConfig 直接抛
    # TypeError（Level not an integer），服务启动即崩
    level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=logging.getLevelNamesMapping().get(level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Pyrogram 内部 MTProto 传输日志每个 TCP 包一条，抑制到 WARNING
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
