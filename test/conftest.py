"""pytest 全局配置：管道测试用的依赖注入工厂。"""
from types import SimpleNamespace

import pytest


@pytest.fixture
def runtime_config(tmp_path, monkeypatch):
    from runtime_config import load_config

    for name, value in {
        "TG_API_ID": "12345", "TG_API_HASH": "hash",
        "RECEIVE_CHAT_ID": "-1001234567890", "ARCHIVE_CHAT_ID": "-1009876543210",
        "DATA_DIR": str(tmp_path), "TG_BOT_TOKEN": "", "BOT_ADMIN_IDS": "",
        "HTTP_PROXY": "",
    }.items():
        monkeypatch.setenv(name, value)
    return load_config()


@pytest.fixture
def make_pipeline(tmp_path):
    """测试冷却默认 0，避免真的等待；生产默认值只在 runtime_config.load_config 定义。"""
    from pipeline import ArchivePipeline, PipelineConfig

    async def _noop_mark(message, duplicate):
        return None

    def _make(*, client=None, db=None, downloader=None, mark=None,
              download_dir=None, archive_chat=-1009876543210,
              receive_chat=-1001234567890, **config_kwargs):
        config_kwargs.setdefault("upload_cooldown_seconds", 0)
        config_kwargs.setdefault("video_compress_min_size_mb", 100)
        config_kwargs.setdefault("video_compress_crf", 28)
        config_kwargs.setdefault("video_compress_threads", 4)
        return ArchivePipeline(
            client=client if client is not None else SimpleNamespace(),
            db=db if db is not None else SimpleNamespace(),
            downloader=downloader if downloader is not None else SimpleNamespace(),
            mark_processed=mark or _noop_mark,
            config=PipelineConfig(**config_kwargs),
            archive_chat=archive_chat,
            receive_chat=receive_chat,
            download_dir=download_dir or str(tmp_path / "dl"),
        )

    return _make
