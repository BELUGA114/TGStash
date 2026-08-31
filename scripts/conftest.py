"""pytest 全局配置：管道测试用的依赖注入工厂。"""
from types import SimpleNamespace

import pytest


@pytest.fixture
def make_pipeline(tmp_path):
    """
    按需组装 ArchivePipeline：只传要断言的依赖，其余给不会被调用的空桩。

    冷却默认 0：测试不该真的睡 5 秒。要验证冷却本身就显式传值。
    """
    from pipeline import ArchivePipeline, PipelineConfig

    async def _noop_mark(message, duplicate):
        return None

    def _make(*, client=None, db=None, downloader=None, mark=None,
              download_dir=None, archive_chat=-1009876543210,
              receive_chat=-1001234567890, **config_kwargs):
        config_kwargs.setdefault("upload_cooldown_seconds", 0)
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
