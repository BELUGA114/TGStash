"""把有副作用的依赖装配留在启动路径，供扫描和 bot 共用。"""

from __future__ import annotations

import functools
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlparse

from pyrogram.client import Client
from pyrogram.types import Message

from tgstash.archive.pipeline import ArchivePipeline
from tgstash.media.downloader import TDLDownloader
from tgstash.runtime.backups import BackupService
from tgstash.runtime.config import RuntimeConfig
from tgstash.storage.db import ArchiveDB


@dataclass(frozen=True)
class Runtime:
    client: Client
    bot: Client | None
    db: ArchiveDB
    pipeline: ArchivePipeline
    backup: BackupService


def build_client(config: RuntimeConfig, *, bot: bool = False) -> Client | None:
    if bot and not config.bot_token:
        return None
    proxy = None
    if config.http_proxy:
        url = urlparse(config.http_proxy)
        proxy = {"scheme": url.scheme, "hostname": url.hostname, "port": url.port}
    # bot_token 登录新 session 仍需要 API 凭证；两种登录使用独立 session 文件。
    if bot:
        return Client("bot", api_id=config.api_id, api_hash=config.api_hash,
                      bot_token=config.bot_token, workdir=config.session_dir, proxy=proxy)
    return Client("listener", api_id=config.api_id, api_hash=config.api_hash,
                  workdir=config.session_dir, max_concurrent_transmissions=2, proxy=proxy)


def build_runtime(config: RuntimeConfig,
                  mark_processed: Callable[[Client, Message, bool], Awaitable[None]]) -> Runtime:
    for directory in (config.session_dir, config.download_dir,
                      os.path.dirname(config.db_path), config.backup.directory):
        os.makedirs(directory, exist_ok=True)
    db = ArchiveDB(config.db_path)
    client = build_client(config)
    assert client is not None
    downloader = TDLDownloader(
        namespace=config.download.namespace,
        threads=config.download.threads,
        limit=config.download.limit,
        delay=config.download.delay_seconds,
        timeout=config.download.timeout_seconds,
        proxy=config.http_proxy,
    )
    pipeline = ArchivePipeline(
        client=client, db=db, downloader=downloader,
        mark_processed=functools.partial(mark_processed, client), config=config.pipeline,
        archive_chat=config.archive_chat, receive_chat=config.receive_chat,
        download_dir=config.download_dir,
    )
    backup = BackupService(db, client, config.archive_chat, config.backup,
                           config.pipeline.upload_cooldown_seconds)
    return Runtime(client, build_client(config, bot=True), db, pipeline, backup)
