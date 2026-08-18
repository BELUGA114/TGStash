"""
tdl 下载后端：把 Pyrogram Message 交给 tdl 并行分块下载。

tdl 不负责去重、转换、上传或 DB，只替换下载这一段。下载结果按
DialogID_MessageID 目录映射回消息；缺失或失败的文件回退到 Pyrogram。
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable

from pyrogram.types import Message

logger = logging.getLogger(__name__)

# 文件名以 DialogID_MessageID 开头，避免同一批次不同频道/同名文件互相覆盖；
# 前缀保留消息 ID，方便下载后映射回 Pyrogram Message。
DOWNLOAD_TEMPLATE = "{{.DialogID}}_{{.MessageID}}_{{filenamify .FileName}}"

DownloadRunner = Callable[[list[str]], Awaitable[None]]


def make_tme_link(chat_id: int, message_id: int) -> str:
    """把 Pyrogram chat.id 转成 tdl 可解析的 t.me/c 链接。

    Telegram 频道 ID 形如 -1001234567890，t.me/c 链接使用去掉 -100
    的裸频道 ID。
    """
    text = str(chat_id)
    if text.startswith("-100"):
        bare = text[4:]
    elif text.startswith("-"):
        bare = text[1:]
    else:
        bare = text
    return f"https://t.me/c/{bare}/{message_id}"


def _bare_chat_id(chat_id: int) -> str:
    text = str(chat_id)
    if text.startswith("-100"):
        return text[4:]
    return text.lstrip("-")


async def _run_tdl_process(cmd: list[str], timeout: float | None) -> None:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        if timeout is None:
            stdout, stderr = await proc.communicate()
        else:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"tdl 下载超时（{timeout} 秒）")
    if proc.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip()[-2000:]
        raise RuntimeError(f"tdl 命令失败：{detail or '无错误输出'}")
    logger.debug("tdl 下载完成：%s", cmd)


class TDLDownloader:
    def __init__(
        self,
        *,
        namespace: str = "archiver",
        threads: int = 4,
        limit: int = 2,
        delay: float = 1.0,
        timeout: float = 0,
        proxy: str = "",
        tdl_binary: str = "tdl",
        runner: DownloadRunner | None = None,
    ):
        self._namespace = namespace
        self._threads = threads
        self._limit = limit
        self._delay = delay
        self._timeout = timeout if timeout > 0 else None
        self._proxy = proxy
        self._tdl_binary = tdl_binary
        self._runner = runner

    async def download(
        self,
        messages: list[Message],
        dest_dir: str,
        fallback: Callable[[Message, str], Awaitable[str | None]],
        *,
        links: dict[int, str] | None = None,
        fallback_paths: dict[int, str] | None = None,
    ) -> dict[int, str]:
        """下载一批消息，返回 {message_id: 本地路径}。

        links 供路径二使用：默认按 message.chat.id 构造 t.me/c 链接；
        传入 links 时优先使用原始 t.me 链接。媒体组只传一个链接时自动加
        --group，让 tdl 下载整组。
        """
        os.makedirs(dest_dir, exist_ok=True)

        if links is not None:
            urls = [links[m.id] for m in messages if m.id in links]
            groups = [(messages, urls, len(messages) > 1 and len(urls) == 1)]
        else:
            by_chat: dict[int, list[Message]] = {}
            for message in messages:
                by_chat.setdefault(message.chat.id, []).append(message)
            groups = [
                (
                    msgs,
                    [make_tme_link(msgs[0].chat.id, m.id) for m in msgs],
                    False,
                )
                for msgs in by_chat.values()
            ]

        for group, urls, use_group in groups:
            if not urls:
                continue
            cmd = self._build_cmd(urls, dest_dir, use_group)
            try:
                await self._run_tdl(cmd)
            except Exception:
                logger.warning(
                    "tdl 下载失败，回退 Pyrogram：%s",
                    " ".join(urls),
                    exc_info=True,
                )

        result: dict[int, str] = {}
        fallback_paths = fallback_paths or {}
        for message in messages:
            local_path = self._find_download(message, dest_dir)
            if local_path is not None:
                result[message.id] = local_path
                continue

            fallback_path = fallback_paths.get(message.id) or os.path.join(
                dest_dir, f"{message.id}_fallback"
            )
            try:
                downloaded = await fallback(message, fallback_path)
            except Exception:
                logger.warning("Pyrogram 回退下载失败：%s", message.id, exc_info=True)
                continue
            if downloaded:
                result[message.id] = str(downloaded)

        return result

    async def _run_tdl(self, cmd: list[str]) -> None:
        if self._runner is not None:
            await self._runner(cmd)
        else:
            await _run_tdl_process(cmd, self._timeout)

    def _build_cmd(
        self,
        urls: list[str],
        dest_dir: str,
        use_group: bool,
    ) -> list[str]:
        cmd = [
            self._tdl_binary,
            "-n",
            self._namespace,
            "--threads",
            str(self._threads),
            "--limit",
            str(self._limit),
            "--delay",
            f"{self._delay:g}s",
        ]
        if self._proxy:
            cmd += ["--proxy", self._proxy]
        cmd += [
            "dl",
            "-u",
            *urls,
            "-d",
            dest_dir,
            "--template",
            DOWNLOAD_TEMPLATE,
            "--restart",
        ]
        if use_group:
            cmd += ["--group"]
        return cmd

    def _find_download(self, message: Message, dest_dir: str) -> str | None:
        bare = _bare_chat_id(message.chat.id)
        prefixes = (f"{bare}_{message.id}_", f"-100{bare}_{message.id}_")
        for name in sorted(os.listdir(dest_dir)):
            path = os.path.join(dest_dir, name)
            if os.path.isfile(path) and name.startswith(prefixes):
                return path
        return None
