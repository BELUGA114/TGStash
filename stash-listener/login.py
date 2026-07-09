"""
首次部署时手动运行一次，用来生成 session 文件：

    docker compose run --rm stash-listener python login.py

会交互式地要求输入手机号 + 收到的验证码（如果开了两步验证还要输入密码）。
成功后 session 文件会写到 /data/session（映射到宿主机的 ./data/session），
之后 listener.py 正常启动就会直接复用这个 session，不会再要求验证码。
"""

import logging
import os

from pyrogram.client import Client

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_DIR = "/data/session"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

os.makedirs(SESSION_DIR, exist_ok=True)

app = Client("listener", api_id=API_ID, api_hash=API_HASH, workdir=SESSION_DIR)

with app:
    me = app.get_me()  # type: ignore[attr-defined]
    logger.info("登录成功：%s (id=%s)", me.first_name, me.id)  # type: ignore[attr-defined]
    logger.info("session 已保存，之后可以正常启动 stash-listener 服务了。")
