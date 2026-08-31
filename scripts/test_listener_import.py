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
    env["PYTHONPATH"] = str(REPO_ROOT / "stash-listener")

    proc = subprocess.run([sys.executable, "-c", "import listener"],
                          capture_output=True, text=True, env=env, timeout=120,
                          check=False)

    assert proc.returncode == 0, proc.stderr[-2000:]
    assert not data_dir.exists(), "导入期建了目录"


def test_build_context_creates_dirs_and_wires_pipeline(tmp_path, monkeypatch):
    """装配路径本身也要有人看着：目录、库、pipeline 三者必须真的接上。"""
    import listener

    monkeypatch.setenv("TG_API_ID", "12345")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("RECEIVE_CHAT_ID", "-1001234567890")
    monkeypatch.setenv("ARCHIVE_CHAT_ID", "-1009876543210")
    monkeypatch.setattr(listener, "SESSION_DIR", str(tmp_path / "session"))
    monkeypatch.setattr(listener, "DOWNLOAD_DIR", str(tmp_path / "tmp"))
    monkeypatch.setattr(listener, "DB_PATH", str(tmp_path / "db" / "archive.db"))
    monkeypatch.setattr(listener, "_build_client", lambda api_id, api_hash: SimpleNamespace())

    ctx = listener._build_context()

    assert ctx.receive_chat == -1001234567890
    assert (tmp_path / "session").is_dir() and (tmp_path / "tmp").is_dir()
    assert (tmp_path / "db" / "archive.db").exists()
    assert ctx.pipeline is not None and ctx.db is not None
