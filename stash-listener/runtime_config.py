"""在启动时读取配置，避免导入模块就要求凭证或固定测试环境。"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from backups import BackupConfig
from pipeline import PipelineConfig

logger = logging.getLogger(__name__)


def parse_admin_ids(raw: str) -> frozenset[int]:
    """漏配白名单时拒绝所有用户；单个非法 id 不应阻止归档服务启动。"""
    ids: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            logger.warning("BOT_ADMIN_IDS 里的 %r 不是整数，已忽略", part)
    return frozenset(ids)


@dataclass(frozen=True)
class ScanConfig:
    interval_seconds: int
    batch_size: int
    retry_max_attempts: int
    heartbeat_path: str


@dataclass(frozen=True)
class DownloadConfig:
    namespace: str
    threads: int
    limit: int
    delay_seconds: int
    timeout_seconds: int


@dataclass(frozen=True)
class RuntimeConfig:
    api_id: int
    api_hash: str
    receive_chat: int
    archive_chat: int
    session_dir: str
    db_path: str
    download_dir: str
    http_proxy: str
    bot_token: str
    admin_ids: frozenset[int]
    scan: ScanConfig
    download: DownloadConfig
    pipeline: PipelineConfig
    backup: BackupConfig


def load_config() -> RuntimeConfig:
    data_dir = os.environ.get("DATA_DIR", "/data")
    return RuntimeConfig(
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        # 数字字符串会被 Kurigram 当电话号码解析。
        receive_chat=int(os.environ["RECEIVE_CHAT_ID"]),
        archive_chat=int(os.environ["ARCHIVE_CHAT_ID"]),
        session_dir=os.path.join(data_dir, "session"),
        db_path=os.path.join(data_dir, "db", "archive.db"),
        download_dir=os.path.join(data_dir, "tmp", "listener"),
        http_proxy=os.environ.get("HTTP_PROXY", ""),
        bot_token=os.environ.get("TG_BOT_TOKEN", ""),
        admin_ids=parse_admin_ids(os.environ.get("BOT_ADMIN_IDS", "")),
        # 这三个限流默认值是账号安全边界。
        scan=ScanConfig(
            interval_seconds=int(os.environ.get("SCAN_INTERVAL_SECONDS", "300")),
            batch_size=int(os.environ.get("BATCH_SIZE", "10")),
            retry_max_attempts=int(os.environ.get("RETRY_MAX_ATTEMPTS", "3")),
            heartbeat_path=os.path.join(data_dir, "tmp", "heartbeat"),
        ),
        download=DownloadConfig(
            namespace=os.environ.get("TDL_NAMESPACE", "archiver"),
            threads=int(os.environ.get("TDL_THREADS", "4")),
            limit=int(os.environ.get("TDL_LIMIT", "2")),
            delay_seconds=int(os.environ.get("TDL_DELAY_SECONDS", "1")),
            timeout_seconds=int(os.environ.get("TDL_TIMEOUT_SECONDS", "0")),
        ),
        pipeline=PipelineConfig(
            upload_cooldown_seconds=int(os.environ.get("UPLOAD_COOLDOWN_SECONDS", "5")),
            video_compress_enabled=os.environ.get("VIDEO_COMPRESS_ENABLED", "false").lower() == "true",
            video_compress_min_size_mb=int(os.environ.get("VIDEO_COMPRESS_MIN_SIZE_MB", "100")),
            video_compress_crf=int(os.environ.get("VIDEO_COMPRESS_CRF", "28")),
            # x264 自动按核心数开线程，高核数机器可能因初始化内存暴增而 OOM。
            video_compress_threads=int(os.environ.get("VIDEO_COMPRESS_THREADS", "4")),
        ),
        backup=BackupConfig(
            directory=os.path.join(data_dir, "db", "backups"),
            interval_seconds=int(os.environ.get("DB_BACKUP_INTERVAL_SECONDS", "86400")),
            keep=int(os.environ.get("DB_BACKUP_KEEP", "7")),
            upload=os.environ.get("DB_BACKUP_UPLOAD", "false").lower() == "true",
        ),
    )
