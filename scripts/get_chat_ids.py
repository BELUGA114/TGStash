"""
列出当前账号加入的所有频道 ID 和标题，用于填写 .env 中的 RECEIVE_CHAT_ID / ARCHIVE_CHAT_ID。

用法（容器）：
    docker compose run --rm stash-listener python scripts/get_chat_ids.py

用法（本地运行，需先 pip install Kurigram）：
    python scripts/get_chat_ids.py

输出格式：
    -1001234567890  频道名称
    -1009876543210  备份频道
"""

import asyncio
import os
from pathlib import Path

from pyrogram.client import Client

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_dotenv() -> None:
    """读取项目根目录 .env 中缺失的环境变量（本机运行时用；容器内由 env_file 提供）。"""
    env_path = REPO_ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def session_dir() -> str:
    """容器内 session 在 /data/session；本机在项目根目录 data/session。"""
    if os.path.isdir("/data/session"):
        return "/data/session"
    return str(REPO_ROOT / "data" / "session")


async def main():
    load_dotenv()
    app = Client(
        "listener",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        workdir=session_dir(),
    )
    async with app:
        async for d in app.get_dialogs():
            c = d.chat
            if c.id is not None and c.id < 0:
                print(f"{c.id}  {c.title}")


if __name__ == "__main__":
    asyncio.run(main())
