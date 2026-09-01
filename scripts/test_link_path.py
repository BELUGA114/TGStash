"""
路径二（t.me 链接）的测试：链接提取纯函数 + process_link_message 的编排分支。

假 client（get_messages / get_media_group / edit_message_text）+ 假 pipeline
就能整条跑 —— 不连 Telegram、不碰 DB、不读环境变量。
"""

import asyncio
from types import SimpleNamespace

import listener
from archive_entry import ROUTE_LINK, Entry, Outcome


class TestExtractTmeLinks:
    def test_no_link_returns_empty(self):
        assert listener.extract_tme_links("今天天气不错，没有链接") == []

    def test_empty_text_returns_empty(self):
        assert listener.extract_tme_links("") == []

    def test_plain_link(self):
        assert listener.extract_tme_links(
            "https://t.me/c/123456/78") == ["https://t.me/c/123456/78"]

    def test_strips_trailing_ascii_punctuation(self):
        """尾随的半角句读不是链接的一部分"""
        for punct in (".", ",", ";", ":", "!", "?", ")"):
            assert listener.extract_tme_links(
                f"看这个 https://t.me/c/123456/78{punct}") == ["https://t.me/c/123456/78"]

    def test_markdown_wrapped_link(self):
        assert listener.extract_tme_links(
            "见 [这条](https://t.me/somechannel/12)") == ["https://t.me/somechannel/12"]

    def test_duplicate_links_collapse_and_keep_order(self):
        text = ("https://t.me/a/1 中间 https://t.me/b/2 又是 https://t.me/a/1")
        assert listener.extract_tme_links(text) == ["https://t.me/a/1", "https://t.me/b/2"]

    def test_http_and_https_both_match(self):
        assert listener.extract_tme_links("http://t.me/a/1 https://t.me/b/2") == [
            "http://t.me/a/1", "https://t.me/b/2"]

    def test_query_string_is_kept_for_parse_to_strip(self):
        """?single 留给 parse_message_link 去掉 —— 提取阶段不该猜它的语义"""
        assert listener.extract_tme_links(
            "https://t.me/c/1/2?single") == ["https://t.me/c/1/2?single"]

    def test_strips_trailing_fullwidth_punctuation(self):
        """
        中文句读同样要剥掉。

        修复前：链接连着 `。` 一起进 parse_message_link → ValueError → 只记 warning，
        那条媒体永久不被归档且无告警（checkpoint 照常推进）。
        """
        for punct in ("。", "，", "、", "；", "：", "！", "？", "）", "】", "》", "」", "』"):
            assert listener.extract_tme_links(
                f"看这个 https://t.me/c/123456/78{punct}") == ["https://t.me/c/123456/78"]

    def test_strips_bracket_and_angle_wrappers(self):
        assert listener.extract_tme_links("<https://t.me/a/1>") == ["https://t.me/a/1"]
        assert listener.extract_tme_links("[https://t.me/a/1]") == ["https://t.me/a/1"]


def _link_msg(text, *, msg_id=300, chat_id=-1001234567890):
    """接收频道里那条链接消息 —— 入口。"""
    return SimpleNamespace(id=msg_id, chat=SimpleNamespace(id=chat_id, title="接收频道"),
                          text=text, caption=None, media_group_id=None)


def _source_msg(msg_id, *, kind="document", group=None):
    """源频道里被链接指到的消息。kind=None 表示那条消息没有媒体。"""
    msg = SimpleNamespace(id=msg_id, chat=SimpleNamespace(id=-1009999999999, title="源频道"),
                         text="", caption="", media_group_id=group, date=None)
    if kind is not None:
        setattr(msg, kind, SimpleNamespace(file_unique_id=f"U{msg_id}", file_size=2048))
    return msg


class FakeLinkClient:
    """只实现路径二用到的三个方法，并记录调用。"""

    def __init__(self, *, messages=None, group=(), get_error=None, group_error=None,
                 edit_error=None):
        self.messages = messages or {}
        self.group = list(group)
        self.get_error = get_error
        self.group_error = group_error
        self.edit_error = edit_error
        self.get_calls = []
        self.group_calls = []
        self.edits = []

    async def get_messages(self, chat, message_id):
        self.get_calls.append((chat, message_id))
        if self.get_error is not None:
            raise self.get_error
        return self.messages.get(message_id)

    async def get_media_group(self, chat, message_id):
        self.group_calls.append((chat, message_id))
        if self.group_error is not None:
            raise self.group_error
        return list(self.group)

    async def edit_message_text(self, chat_id, message_id, text):
        self.edits.append((chat_id, message_id, text))
        if self.edit_error is not None:
            raise self.edit_error
        return SimpleNamespace(id=message_id)


class FakePipeline:
    """记录 archive_one / archive_batch 的入参；结论由 ok 参数决定。"""

    def __init__(self, *, ok=True):
        self.ok = ok
        self.one_calls = []
        self.batch_calls = []

    def _outcome(self, item):
        return Outcome.success(item) if self.ok else Outcome.failure(item, "upload", "炸了")

    async def archive_one(self, item):
        self.one_calls.append(item)
        return self._outcome(item)

    async def archive_batch(self, items):
        self.batch_calls.append(list(items))
        return [self._outcome(it) for it in items]


def _ctx(client, pipeline):
    """db 给空桩：路径二不碰 DB，碰了就是 AttributeError（正是想要的）。"""
    return listener.ListenerContext(client=client, db=SimpleNamespace(),
                                   pipeline=pipeline, receive_chat=-1001234567890)


def _run(client, text, pipeline=None):
    """跑一遍 process_link_message，返回 (入口, 结论列表)。"""
    pipeline = pipeline if pipeline is not None else FakePipeline()
    entry = Entry(message=_link_msg(text), route=ROUTE_LINK)
    outcomes = asyncio.run(listener.process_link_message(_ctx(client, pipeline), entry))
    return entry, outcomes


class TestProcessLinkMessage:
    """
    设计文档 2026-09-01-pipeline-debt-design.md「要覆盖的分支」那张表。

    最关键的两条：永久性失败不产出失败结论（否则入口永久卡住 checkpoint），
    媒体组里只有链接指向的那一条带 link（否则 tdl 的 --group 语义被静默改掉）。
    """

    def test_no_link_returns_empty_and_touches_no_client(self):
        client = FakeLinkClient()

        _, outcomes = _run(client, "随手发的一句话")

        assert outcomes == []
        assert client.get_calls == [] and client.edits == []

    def test_unparseable_link_yields_no_outcome(self):
        """链接解析不了是永久性失败：只记 warning，重试无用，不能产出失败结论"""
        client = FakeLinkClient()

        _, outcomes = _run(client, "https://t.me/joinchat/AbCdEf")

        assert outcomes == []
        assert client.get_calls == []

    def test_get_messages_failure_is_download_failure_on_entry_message(self):
        """取消息失败是暂时性失败：产出 failure('download')，条目的 media 是入口消息本身"""
        client = FakeLinkClient(get_error=RuntimeError("网络炸了"))

        entry, outcomes = _run(client, "https://t.me/c/123456/78")

        assert len(outcomes) == 1
        assert outcomes[0].ok is False and outcomes[0].stage == "download"
        assert outcomes[0].item.media is entry.message
        assert outcomes[0].item.link == "https://t.me/c/123456/78"
        assert client.edits == []

    def test_missing_message_yields_no_outcome(self):
        """消息不可访问或已删除 → 永久性失败，只记 warning"""
        client = FakeLinkClient(messages={})

        _, outcomes = _run(client, "https://t.me/c/123456/78")

        assert outcomes == []

    def test_single_media_goes_through_archive_one(self):
        client = FakeLinkClient(messages={78: _source_msg(78)})
        pipeline = FakePipeline()

        entry, outcomes = _run(client, "https://t.me/c/123456/78", pipeline)

        assert [o.ok for o in outcomes] == [True]
        assert pipeline.batch_calls == []
        item = pipeline.one_calls[0]
        assert item.media.id == 78
        assert item.entry is entry, "入口必须是那条链接消息，不是源频道的消息"
        assert item.link == "https://t.me/c/123456/78"

    def test_single_without_media_is_skipped(self):
        client = FakeLinkClient(messages={78: _source_msg(78, kind=None)})
        pipeline = FakePipeline()

        _, outcomes = _run(client, "https://t.me/c/123456/78", pipeline)

        assert outcomes == []
        assert pipeline.one_calls == [] and pipeline.batch_calls == []

    def test_media_group_is_sorted_and_only_target_carries_link(self):
        """
        整组按 id 排序，link 只挂链接指向的那一条。

        tdl 靠「多条消息只给一个 URL」决定加 --group 一次拉整组
        （tdl_downloader.py:113）。给每条都编链接会静默改掉下载语义。
        """
        target = _source_msg(79, group="g9")
        client = FakeLinkClient(
            messages={79: target},
            group=[_source_msg(80, group="g9"), target, _source_msg(78, group="g9")])
        pipeline = FakePipeline()

        _, outcomes = _run(client, "https://t.me/c/123456/79", pipeline)

        assert [o.ok for o in outcomes] == [True, True, True]
        assert pipeline.one_calls == []
        items = pipeline.batch_calls[0]
        assert [it.media.id for it in items] == [78, 79, 80]
        assert [it.link for it in items] == [None, "https://t.me/c/123456/79", None]

    def test_get_media_group_failure_is_download_failure_on_source_message(self):
        client = FakeLinkClient(messages={79: _source_msg(79, group="g9")},
                                group_error=RuntimeError("拉组炸了"))

        _, outcomes = _run(client, "https://t.me/c/123456/79")

        assert len(outcomes) == 1
        assert outcomes[0].ok is False and outcomes[0].stage == "download"
        assert outcomes[0].item.media.id == 79, "这一条的 media 是源消息，不是入口消息"

    def test_two_links_are_both_processed(self):
        client = FakeLinkClient(messages={78: _source_msg(78), 79: _source_msg(79)})
        pipeline = FakePipeline()

        _, outcomes = _run(client, "https://t.me/c/123456/78 和 https://t.me/c/123456/79",
                          pipeline)

        assert [o.ok for o in outcomes] == [True, True]
        assert [it.media.id for it in pipeline.one_calls] == [78, 79]

    def test_entry_is_edited_when_anything_ok(self):
        client = FakeLinkClient(messages={78: _source_msg(78)})

        _, outcomes = _run(client, "https://t.me/c/123456/78")

        assert [o.ok for o in outcomes] == [True]
        assert client.edits == [
            (-1001234567890, 300, "✅ 已归档\nhttps://t.me/c/123456/78")]

    def test_no_ok_outcome_means_no_edit(self):
        client = FakeLinkClient(messages={78: _source_msg(78)})

        _, outcomes = _run(client, "https://t.me/c/123456/78", FakePipeline(ok=False))

        assert [o.ok for o in outcomes] == [False]
        assert client.edits == []

    def test_edit_failure_does_not_change_outcomes(self):
        """标记尽力而为：编辑失败只记 warning，不影响归档结论"""
        client = FakeLinkClient(messages={78: _source_msg(78)},
                                edit_error=RuntimeError("消息太旧不能编辑"))

        _, outcomes = _run(client, "https://t.me/c/123456/78")

        assert [o.ok for o in outcomes] == [True]
        assert len(client.edits) == 1

    def test_edit_text_is_truncated_to_4096(self):
        """Telegram 的文本上限 4096，超了整条编辑会被拒"""
        client = FakeLinkClient(messages={78: _source_msg(78)})

        _, outcomes = _run(client, "https://t.me/c/123456/78 " + "补" * 5000)

        assert [o.ok for o in outcomes] == [True]
        assert len(client.edits[0][2]) == 4096
