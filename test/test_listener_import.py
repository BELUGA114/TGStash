"""
导入期零副作用的回归。

单独起子进程：这个断言的前提是「环境变量不存在」，而 pytest 进程里
别的用例可能需要它们，改当前进程的 os.environ 会互相污染。
"""

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_import_needs_no_env_and_touches_no_disk(tmp_path):
    """
    import listener 不该读必填环境变量、不该建目录、不该开库、不该连 Telegram。

    回归：模块级 os.environ["TG_API_ID"] 与 os.makedirs/ArchiveDB(DB_PATH)
    让「测一条归档」必须先凑齐四个环境变量和一个可写的数据目录。
    """
    data_dir = tmp_path / "never-created"
    env = {k: v for k, v in os.environ.items()
           if k not in ("TG_API_ID", "TG_API_HASH", "RECEIVE_CHAT_ID", "ARCHIVE_CHAT_ID")}
    env["DATA_DIR"] = str(data_dir)
    env["BATCH_SIZE"] = "invalid-until-startup"
    env["PYTHONPATH"] = str(REPO_ROOT / "stash-listener")

    proc = subprocess.run([sys.executable, "-c", "import listener"],
                          capture_output=True, text=True, env=env, timeout=120,
                          check=False)

    assert proc.returncode == 0, proc.stderr[-2000:]
    assert not data_dir.exists(), "导入期建了目录"


def test_build_runtime_creates_dirs_and_wires_pipeline(tmp_path, monkeypatch, runtime_config):
    import bootstrap

    client = SimpleNamespace()
    monkeypatch.setattr(bootstrap, "build_client", lambda config, bot=False: None if bot else client)

    async def mark(client, message, duplicate):
        pass

    runtime = bootstrap.build_runtime(runtime_config, mark)

    assert runtime_config.receive_chat == -1001234567890
    assert (tmp_path / "session").is_dir() and (tmp_path / "tmp" / "listener").is_dir()
    assert (tmp_path / "db" / "archive.db").exists()
    assert (tmp_path / "db" / "backups").is_dir()
    assert runtime.pipeline is not None and runtime.db is not None
    assert runtime.client is client and runtime.bot is None
