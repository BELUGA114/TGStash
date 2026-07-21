"""
列出当前账号加入的所有频道 ID 和标题，用于填写 .env 中的 RECEIVE_CHAT_ID / ARCHIVE_CHAT_ID。

用法：
    docker compose run --rm stash-listener python scripts/get_chat_ids.py

输出格式：
    -1001234567890  频道名称
    -1009876543210  备份频道

拿到 ID 后填入 .env，再 docker compose restart stash-listener。
"""

import asyncio
import os

from pyrogram import Client


async def main():
    app = Client(
        "listener",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        workdir="/data/session",
    )
    async with app:
        async for d in app.get_dialogs():
            if d.chat.id < 0:
                print(f"{d.chat.id}  {d.chat.title}")


if __name__ == "__main__":
    asyncio.run(main())
