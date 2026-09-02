"""pytest 全局配置：管道测试用的依赖注入工厂。"""
from types import SimpleNamespace

import pytest


@pytest.fixture
def make_pipeline(tmp_path):
    """
    按需组装 ArchivePipeline：只传要断言的依赖，其余给不会被调用的空桩。

    PipelineConfig 构造时必填的四个值（冷却、压缩阈值、CRF、编码线程数）在这里给测试
    默认值；生产的真相只有一处：listener.py 读环境变量那一段。冷却默认 0：测试不该真的
    睡 5 秒，要验证冷却本身就显式传值。
    """
    from pipeline import ArchivePipeline, PipelineConfig

    async def _noop_mark(message, duplicate):
        return None

    def _make(*, client=None, db=None, downloader=None, mark=None,
              download_dir=None, archive_chat=-1009876543210,
              receive_chat=-1001234567890, **config_kwargs):
        config_kwargs.setdefault("upload_cooldown_seconds", 0)
        # 生产默认值在 listener 那边（环境变量）；这里只给测试一个够用的值
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
