from __future__ import annotations

import pytest
from tgstash.runtime.config import load_config


def test_runtime_defaults_and_channel_types(runtime_config):
    assert runtime_config.receive_chat == -1001234567890
    assert runtime_config.archive_chat == -1009876543210
    assert runtime_config.scan.interval_seconds == 300
    assert runtime_config.scan.batch_size == 10
    assert runtime_config.scan.retry_max_attempts == 3
    assert runtime_config.pipeline.upload_cooldown_seconds == 5
    assert not runtime_config.pipeline.video_compress_enabled
    assert runtime_config.pipeline.video_compress_threads == 4
    assert runtime_config.backup.interval_seconds == 86400
    assert runtime_config.backup.keep == 7
    assert not runtime_config.backup.upload
    assert runtime_config.bot_token == ""
    assert runtime_config.admin_ids == frozenset()


def test_configuration_is_read_at_startup(runtime_config, monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "4")
    monkeypatch.setenv("VIDEO_COMPRESS_ENABLED", "TRUE")
    monkeypatch.setenv("VIDEO_COMPRESS_CRF", "24")
    monkeypatch.setenv("DB_BACKUP_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("TG_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("BOT_ADMIN_IDS", "111,222")
    config = load_config()
    assert config.scan.batch_size == 4
    assert config.pipeline.video_compress_enabled
    assert config.pipeline.video_compress_crf == 24
    assert config.backup.interval_seconds == 0
    assert config.bot_token == "123:abc"
    assert config.admin_ids == frozenset({111, 222})


def test_required_credentials_are_checked_at_startup(runtime_config, monkeypatch):
    monkeypatch.delenv("TG_API_ID")
    with pytest.raises(KeyError, match="TG_API_ID"):
        load_config()
