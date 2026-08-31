"""
pytest 全局配置：test_reliability.py 会 import listener，
而 listener 模块级读取 TG 环境变量（TG_API_ID 等）。未提供时注入
dummy 值保证测试可独立运行；真实值仅对测试无意义（测试会 monkeypatch
掉 listener 的 app/db）。setdefault 不覆盖调用方显式设置的环境变量。
"""
import os
import tempfile
from types import SimpleNamespace

import pytest

os.environ.setdefault("TG_API_ID", "12345")
os.environ.setdefault("TG_API_HASH", "testhash")
os.environ.setdefault("RECEIVE_CHAT_ID", "-1001234567890")
os.environ.setdefault("ARCHIVE_CHAT_ID", "-1009876543210")

# listener 模块级会建目录并打开 SQLite。指到临时目录，测试不再污染真实数据目录，
# 也不再依赖 /data 可写（Linux runner 上非 root 无权在 / 下建目录）
if "DATA_DIR" not in os.environ:
    os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="tgstash-test-")


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
