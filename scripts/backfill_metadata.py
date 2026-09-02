"""
回填历史行的元数据：从接收频道重新读取原消息，补齐 origin_* 与文件身份。

用法（容器）：
    docker compose run --rm stash-listener python scripts/backfill_metadata.py --dry-run
    docker compose run --rm stash-listener python scripts/backfill_metadata.py --limit 200

只处理 origin_type IS NULL 的行。'original'（原创直发）与 'unknown'（查不到原
消息）都是终态，不会被重复拉取。

正确性核心是自证匹配：拉回消息的 file_unique_id 必须等于 DB 行记录的值。
历史路径二的行，source_message_id 存的是源频道的消息 id，拿它去接收频道查会
命中一条毫不相干的消息——校验把这些行连同其他错位行一并排除，代价是它们永久
留在 unknown。写错来源比留空更糟。
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "stash-listener"))

from db import ArchiveDB
from media_ops import get_media
from origin import normalize_origin

# 每批拉取的消息数与批间等待秒数。账号安全优先，与主服务的保守取向一致
BATCH_SIZE = 100
BATCH_DELAY_SECONDS = 2

logger = logging.getLogger(__name__)


def plan_updates(rows, fetched):
    """
    纯函数：给定待回填的 DB 行与已拉回的消息，算出该写什么。

    返回 (updates, unknown_ids)：
      updates      — [(row_id, file_unique_id, origin 字段 dict, 文件身份 dict), ...]
                     file_unique_id 用于同时回填 files 表的文件身份
      unknown_ids  — 查不到 / 无媒体 / 校验不通过的行 id，标 unknown 不再重试
    """
    updates = []
    unknown_ids = []

    for row in rows:
        message = fetched.get(row["source_message_id"])
        if message is None:
            logger.debug("行 %s：接收频道里查不到消息 %s", row["id"], row["source_message_id"])
            unknown_ids.append(row["id"])
            continue

        kind, media = get_media(message)
        if media is None:
            logger.debug("行 %s：消息 %s 无媒体", row["id"], row["source_message_id"])
            unknown_ids.append(row["id"])
            continue

        # 自证匹配：不是同一个文件，就说明这个 source_message_id 不指向接收频道那条消息
        if getattr(media, "file_unique_id", None) != row["file_unique_id"]:
            logger.debug(
                "行 %s：file_unique_id 不符（DB %s ≠ 消息 %s），跳过",
                row["id"], row["file_unique_id"], getattr(media, "file_unique_id", None),
            )
            unknown_ids.append(row["id"])
            continue

        origin = normalize_origin(message)
        identity = {
            "file_name": getattr(media, "file_name", None),
            "mime_type": getattr(media, "mime_type", None),
            "media_kind": kind,
        }
        updates.append((row["id"], row["file_unique_id"], origin, identity))

    return updates, unknown_ids


def apply_updates(db, updates, unknown_ids, dry_run):
    """把 plan_updates 的结果落库。dry_run 时只打印。"""
    if dry_run:
        for row_id, _fuid, origin, identity in updates:
            logger.info("[dry-run] 行 %s ← 来源 %s/%s，文件 %s (%s)",
                        row_id, origin["origin_type"], origin["origin_title"],
                        identity["file_name"], identity["media_kind"])
        logger.info("[dry-run] %s 行将标为 unknown", len(unknown_ids))
        return

    # 不做整表 rebuild：这里全是 UPDATE messages，messages_au 触发器已经逐行
    # 同步了 FTS。而 'rebuild' 会对整表上独占写锁，主服务此时正在跑，锁等待
    # 一旦超过它的 busy_timeout=30000，record_file/record_message 抛的
    # sqlite3.OperationalError 不是 RuntimeError，archive_single 捕不住——
    # 文件已经传上去却没落库，下轮扫描会重新下载重传，正是要避免的重复归档
    for row_id, file_unique_id, origin, identity in updates:
        db.update_message_metadata(row_id, file_name=identity["file_name"],
                                   media_kind=identity["media_kind"], **origin)
        # files 表也要补：文件身份的规范位置在那里，messages 上只是 FTS 用的冗余
        db.update_file_metadata(file_unique_id, **identity)
    for row_id in unknown_ids:
        db.mark_origin_unknown(row_id)


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


def build_client():
    """按 .env 建 Pyrogram 客户端，与 listener 用同一份 session。"""
    from pyrogram.client import Client

    kwargs = {}
    proxy = os.environ.get("HTTP_PROXY", "")
    if proxy:
        from urllib.parse import urlparse

        u = urlparse(proxy)
        kwargs["proxy"] = {"scheme": u.scheme, "hostname": u.hostname, "port": u.port}
    return Client(
        "listener",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        workdir=session_dir(),
        **kwargs,
    )


async def fetch_messages(app, chat_id, message_ids):
    """
    分批拉取接收频道消息。批间等待，别把账号跑挂。

    返回 (fetched, failed_ids)：
      fetched     — {message_id: Message}
      failed_ids  — 整批拉取失败（FloodWait / 连接中断）的 id 集合

    失败的 id 必须单独报出来，不能只是从 fetched 里缺席。传输失败不等于
    「原消息查不到」：前者下次还能成功，后者才该落终态。混在一起会让一次
    FloodWait 把最多一整批可回填的行永久标成 unknown。
    """
    fetched = {}
    failed_ids: set[int] = set()
    for start in range(0, len(message_ids), BATCH_SIZE):
        batch = message_ids[start:start + BATCH_SIZE]
        logger.info("拉取 %s-%s / %s 条", start + 1, start + len(batch), len(message_ids))
        try:
            messages = await app.get_messages(chat_id, batch)
        except Exception:
            logger.warning("批次拉取失败，这 %s 条留到下次重试", len(batch), exc_info=True)
            failed_ids.update(batch)
            continue
        if not isinstance(messages, list):
            messages = [messages]
        for message in messages:
            if message is not None and getattr(message, "id", None) is not None:
                fetched[message.id] = message
        if start + BATCH_SIZE < len(message_ids):
            await asyncio.sleep(BATCH_DELAY_SECONDS)
    return fetched, failed_ids


async def main():
    parser = argparse.ArgumentParser(description="回填历史消息的来源与文件身份元数据")
    parser.add_argument("--db", default="/data/db/archive.db", help="archive.db 路径")
    parser.add_argument("--limit", type=int, default=None, help="本次最多处理多少行")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写库")
    args = parser.parse_args()

    logging.basicConfig(
        # getLevelNamesMapping 而不是 getattr(logging, LOG_LEVEL)：后者对小写的
        # LOG_LEVEL=debug 会取到 logging.debug 函数，basicConfig 直接抛 TypeError
        level=logging.getLevelNamesMapping().get(
            os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("pyrogram").setLevel(logging.WARNING)

    if not os.path.exists(args.db):
        logger.error("数据库文件不存在：%s", args.db)
        sys.exit(1)

    load_dotenv()
    db = ArchiveDB(args.db)
    rows = db.rows_missing_origin(limit=args.limit)
    if not rows:
        logger.info("没有待回填的行（origin_type IS NULL 为空）")
        return

    logger.info("待回填 %s 行", len(rows))
    receive_chat = int(os.environ["RECEIVE_CHAT_ID"])
    message_ids = sorted({r["source_message_id"] for r in rows if r["source_message_id"]})

    app = build_client()
    async with app:
        fetched, failed_ids = await fetch_messages(app, receive_chat, message_ids)

    # 失败批次的行完全不参与规划，保持 origin_type IS NULL 等下轮
    plannable = [r for r in rows if r["source_message_id"] not in failed_ids]
    skipped = len(rows) - len(plannable)
    if skipped:
        logger.warning("%s 行因批次拉取失败本轮跳过，仍为待回填状态", skipped)

    updates, unknown_ids = plan_updates(plannable, fetched)
    logger.info("可回填 %s 行，标为 unknown %s 行", len(updates), len(unknown_ids))
    apply_updates(db, updates, unknown_ids, args.dry_run)
    if not args.dry_run:
        logger.info("完成：回填 %s 行", len(updates))


if __name__ == "__main__":
    asyncio.run(main())
