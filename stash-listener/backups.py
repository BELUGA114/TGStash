from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime

from db import ArchiveDB
from pyrogram.client import Client

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackupConfig:
    directory: str
    interval_seconds: int
    keep: int
    upload: bool


def list_backups(directory: str) -> list[str]:
    if not os.path.isdir(directory):
        return []
    return [os.path.join(directory, name) for name in os.listdir(directory)
            if name.startswith("archive-") and name.endswith(".db")]


def backup_filename(now: float) -> str:
    return datetime.fromtimestamp(now, UTC).strftime("archive-%Y%m%d-%H%M%S.db")


def backups_to_delete(paths: list[str], keep: int) -> list[str]:
    # 零填充的 UTC 时间戳让文件名字典序等同于创建顺序。
    ordered = sorted(paths, key=os.path.basename)
    if keep <= 0:
        return ordered
    return ordered[:-keep] if len(ordered) > keep else []


class BackupService:
    def __init__(self, db: ArchiveDB, client: Client, archive_chat: int,
                 config: BackupConfig, upload_cooldown_seconds: int):
        self._db = db
        self._client = client
        self._archive_chat = archive_chat
        self._config = config
        self._upload_cooldown_seconds = upload_cooldown_seconds

    async def run(self, now: float) -> str:
        os.makedirs(self._config.directory, exist_ok=True)
        dest = os.path.join(self._config.directory, backup_filename(now))
        await asyncio.to_thread(self._db.backup_to, dest)
        logger.info("DB 备份完成：%s", dest)

        for path in backups_to_delete(list_backups(self._config.directory), self._config.keep):
            try:
                os.remove(path)
                logger.info("删除旧备份：%s", path)
            except OSError:
                logger.warning("删除旧备份失败：%s", path, exc_info=True)

        if self._config.upload:
            await self._client.send_document(self._archive_chat, dest)
            await asyncio.sleep(self._upload_cooldown_seconds)
            logger.info("备份快照已上传备份频道：%s", os.path.basename(dest))
        return dest

    async def maybe_run(self, now: float, last_backup_at: float) -> float:
        interval = self._config.interval_seconds
        if interval <= 0 or now - last_backup_at < interval:
            return last_backup_at
        try:
            await self.run(now)
        except Exception:
            logger.warning("DB 备份失败，跳过本轮", exc_info=True)
        # 失败也等待完整间隔，避免每轮扫描都重试备份。
        return now
