"""
ArchivePipeline 测试：假 client / 假 db / 假 downloader，
不连 Telegram、不读环境变量、不写真实数据目录。
"""

import asyncio
from types import SimpleNamespace

from archive_entry import ROUTE_FORWARD, ArchiveItem, Entry


def msg_stub(msg_id, kind="document", *, chat_id=-1001234567890, caption="",
             group=None, title="接收频道", **media):
    """带一种媒体的消息桩。kind 决定 media_ops.get_media 推导出的类型。"""
    defaults = {"file_unique_id": f"U{msg_id}", "file_size": 2048}
    if kind == "document":
        defaults |= {"file_name": f"{msg_id}.pdf", "mime_type": "application/pdf"}
    elif kind == "video":
        defaults |= {"file_name": f"{msg_id}.mp4", "mime_type": "video/mp4",
                     "duration": 5, "width": 640, "height": 360}
    msg = SimpleNamespace(
        id=msg_id, chat=SimpleNamespace(id=chat_id, title=title),
        date=None, media_group_id=group, caption=caption, text="",
        from_user=SimpleNamespace(first_name="tester", id=1), sender_chat=None,
        forward_origin=None,
    )
    setattr(msg, kind, SimpleNamespace(**(defaults | media)))
    return msg


def item_of(msg, *, route=ROUTE_FORWARD, entry_msg=None, link=None):
    return ArchiveItem(media=msg, entry=Entry(message=entry_msg or msg, route=route), link=link)


def fake_db(**overrides):
    """默认「什么都没归档过」；record_* 的入参记进 .writes 供断言。"""
    writes = []
    db = SimpleNamespace(
        find_by_unique_id=lambda fuid: None,
        find_by_sha256=lambda sha: None,
        record_file=lambda **kw: writes.append(("file", kw)),
        record_message=lambda **kw: writes.append(("message", kw)),
        writes=writes,
    )
    for name, value in overrides.items():
        setattr(db, name, value)
    return db


def fake_downloader(paths):
    """paths: {message_id: 本地路径}。签名与 TDLDownloader.download 对齐。"""
    seen = {}

    async def download(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
        seen["links"] = links
        seen["dest_dir"] = dest_dir
        seen["calls"] = seen.get("calls", 0) + 1
        return {m.id: paths[m.id] for m in messages if m.id in paths}

    return SimpleNamespace(download=download, seen=seen)


class FakeClient:
    """记录所有 send_* 调用。calls 里是 (方法名, 路径或路径列表, kwargs)。"""

    def __init__(self, *, media_group_error=None, send_photo_error=None):
        self.calls = []
        self._n = 0
        self._media_group_error = media_group_error
        self._send_photo_error = send_photo_error

    def _sent(self):
        self._n += 1
        return SimpleNamespace(id=900 + self._n)

    async def send_document(self, chat, path, caption=""):
        self.calls.append(("send_document", path, {"caption": caption}))
        return self._sent()

    async def send_photo(self, chat, path, caption=""):
        self.calls.append(("send_photo", path, {"caption": caption}))
        if self._send_photo_error is not None:
            error, self._send_photo_error = self._send_photo_error, None
            raise error
        return self._sent()

    async def send_video(self, chat, path, **kwargs):
        self.calls.append(("send_video", path, kwargs))
        return self._sent()

    async def send_voice(self, chat, path, caption=""):
        self.calls.append(("send_voice", path, {"caption": caption}))
        return self._sent()

    async def send_media_group(self, chat, input_media):
        self.calls.append(("send_media_group", [m.media for m in input_media], {
            "captions": [m.caption for m in input_media],
            "classes": [type(m).__name__ for m in input_media],
        }))
        if self._media_group_error is not None:
            # 只炸第一次：验证 PhotoExtInvalid 回退整组 document 那条路径
            error, self._media_group_error = self._media_group_error, None
            raise error
        return [self._sent() for _ in input_media]


def local_file(tmp_path, name, size=2048, content=None):
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content if content is not None else b"\x00" * size)
    return str(path)


def test_archive_one_uploads_and_records(tmp_path, make_pipeline):
    """
    冒烟：整条单条管道跑通，全靠注入的桩 —— 不 import listener、不碰环境变量。

    这个用例本身就是本次重构的验收：今天写不出来。
    """
    msg = msg_stub(41)
    client, db = FakeClient(), fake_db()
    downloader = fake_downloader({41: local_file(tmp_path, "src/41.pdf")})
    pipeline = make_pipeline(client=client, db=db, downloader=downloader)

    outcome = asyncio.run(pipeline.archive_one(item_of(msg)))

    assert outcome.ok is True
    assert [c[0] for c in client.calls] == ["send_document"]
    assert [w[0] for w in db.writes] == ["file", "message"]
    assert db.writes[0][1]["source"] == "manual_forward"
    assert db.writes[1][1]["source_message_id"] == 41
