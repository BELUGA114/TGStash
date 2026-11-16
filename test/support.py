from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

from pyrogram.client import Client
from pyrogram.errors import PhotoExtInvalid
from tgstash.archive.entry import ROUTE_FORWARD, ArchiveItem, Entry
from tgstash.archive.pipeline import ArchivePipeline, PipelineConfig
from tgstash.runtime.config import ScanConfig
from tgstash.runtime.listener import ListenerContext
from tgstash.storage.db import ArchiveDB

RECEIVE = -1001234567890
ARCHIVE = -1009876543210
SOURCE = -1001111111111


def message(mid, kind="document", *, chat=RECEIVE, caption="归档核心内容", group=None,
            size=2048, uid=None, text="", file_name=None):
    msg = SimpleNamespace(
        id=mid, chat=SimpleNamespace(id=chat, title="频道"), date=None,
        media_group_id=group, caption=caption, text=text,
        from_user=SimpleNamespace(first_name="tester", id=111), sender_chat=None,
        forward_origin=None,
    )
    if kind is not None:
        setattr(msg, kind, SimpleNamespace(
            file_unique_id=uid or f"U{mid}", file_size=size,
            file_name=file_name or f"{mid}.dat", mime_type="application/octet-stream",
            duration=0, width=0, height=0))
    return msg


def item(msg):
    return ArchiveItem(msg, Entry(msg, ROUTE_FORWARD))


class Downloader:
    def __init__(self, root: Path):
        self.root = root
        self.payloads = {}
        self.calls = []

    async def download(self, messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
        self.calls.append(([m.id for m in messages], links))
        result = {}
        assert fallback_paths is not None
        for msg in messages:
            path = Path(fallback_paths[msg.id])
            # 实际下载落点必须留在临时目录，消息文件名不能覆盖数据库。
            assert path.resolve().is_relative_to(self.root.resolve())
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(self.payloads[msg.id])
            result[msg.id] = str(path)
        return result


class Telegram:
    def __init__(self):
        self.uploads = []
        self.history = []
        self.sources = {}
        self.groups = {}
        self.replies = []
        self.edits = []
        self.photo_rejected = False
        self.upload_error = False
        self.short_group = False
        self.next_id = 900
        self.sent_ids = []

    def _sent(self):
        self.next_id += 1
        self.sent_ids.append(self.next_id)
        return SimpleNamespace(id=self.next_id)

    async def _send(self, method, chat, path, **kwargs):
        assert chat == ARCHIVE
        # 用真实 RPC 签名约束桩，避免桩接受 Telegram 会拒绝的参数。
        inspect.signature(getattr(Client, method)).bind(None, chat, path, **kwargs)
        self.uploads.append((method, Path(path).read_bytes(), kwargs))
        if self.upload_error:
            raise OSError("upload unavailable")
        return self._sent()

    async def send_document(self, chat, path, **kwargs):
        return await self._send("send_document", chat, path, **kwargs)

    async def send_photo(self, chat, path, **kwargs):
        return await self._send("send_photo", chat, path, **kwargs)

    async def send_video(self, chat, path, **kwargs):
        if kwargs.get("thumb"):
            assert Path(kwargs["thumb"]).is_file()
        return await self._send("send_video", chat, path, **kwargs)

    async def send_video_note(self, chat, path, **kwargs):
        return await self._send("send_video_note", chat, path, **kwargs)

    async def send_media_group(self, chat, media):
        assert chat == ARCHIVE
        self.uploads.append(("group", [Path(m.media).read_bytes() for m in media],
                             {"captions": [m.caption for m in media],
                              "types": [type(m).__name__ for m in media]}))
        if self.upload_error:
            raise OSError("upload unavailable")
        if self.photo_rejected:
            self.photo_rejected = False
            raise PhotoExtInvalid()
        return [self._sent() for _ in media[:1 if self.short_group else len(media)]]

    async def get_chat_history(self, chat, min_id, reverse):
        assert reverse
        for msg in self.history:
            if msg.id >= min_id:
                yield msg

    async def get_messages(self, chat, mid):
        assert chat == self.sources[mid].chat.id
        return self.sources[mid]

    async def get_media_group(self, chat, mid):
        return self.groups[mid]

    async def send_message(self, chat, text, **kwargs):
        self.replies.append((chat, text))

    async def edit_message_text(self, chat, mid, text):
        self.edits.append((chat, mid, text))


class Harness:
    def __init__(self, tmp_path):
        self.root = tmp_path / "downloads"
        self.db = ArchiveDB(str(tmp_path / "archive.db"))
        self.db.ensure_channel(RECEIVE, ROUTE_FORWARD)
        self.client = Telegram()
        self.downloader = Downloader(self.root)
        self.marked = []

        async def mark(msg, duplicate):
            self.marked.append((msg.id, duplicate))

        self.pipeline = ArchivePipeline(
            client=self.client, db=self.db, downloader=self.downloader,
            mark_processed=mark, config=PipelineConfig(0, 100, 28, 4),
            receive_chat=RECEIVE, archive_chat=ARCHIVE, download_dir=str(self.root))
        self.ctx = ListenerContext(self.client, self.db, self.pipeline, RECEIVE,
                                   ScanConfig(0, 10, 3, str(tmp_path / "heartbeat")))

    def empty_downloads(self):
        assert not [path for path in self.root.rglob("*") if path.is_file()]


def record(db, mid, *, uid=None, caption="归档核心内容"):
    db.record_archived(
        file_unique_id=uid or f"U{mid}", sha256=str(mid), size=2048,
        source=ROUTE_FORWARD, source_channel=RECEIVE,
        archived_chat_id=ARCHIVE, archived_message_id=900 + mid,
        source_chat_id=RECEIVE, source_message_id=mid, caption=caption)
