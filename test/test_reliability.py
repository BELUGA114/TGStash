"""
可靠性补课单元测试
覆盖：archive_failures 表的记录/累计/跳过/自愈/待处理查询、按入口结算、
      scan_once 的 checkpoint 单一推进规则
"""
import asyncio
import os
import tempfile
from types import SimpleNamespace

import listener
import pytest
from archive_entry import ROUTE_FORWARD, ROUTE_LINK, ArchiveItem, Entry, Outcome
from db import SCHEMA, ArchiveDB


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = ArchiveDB(path)
    yield db
    try:
        os.remove(path)
        os.remove(path + "-wal")
    except OSError:
        pass
    try:
        os.remove(path + "-shm")
    except OSError:
        pass


class TestFailureSchema:
    def test_archive_failures_table_exists(self, db: ArchiveDB):
        with db._connect() as con:
            tables = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "archive_failures" in tables

    def test_failures_table_idempotent(self, db: ArchiveDB):
        with db._connect() as con:
            con.executescript(SCHEMA)  # 重复执行不报错

    def test_failure_index_exists(self, db: ArchiveDB):
        with db._connect() as con:
            indexes = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                ).fetchall()
            }
        assert "idx_archive_failures_status" in indexes


class TestFailures:
    def test_get_failure_unknown_returns_none(self, db: ArchiveDB):
        assert db.get_failure("-100123", 42) is None

    def test_increment_failure_first_creates(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "tdl 失败")
        row = db.get_failure("-100123", 42)
        assert row is not None
        assert row["attempt_count"] == 1
        assert row["status"] == "retrying"
        assert row["failure_stage"] == "download"
        assert row["first_failed_at"] is not None

    def test_increment_failure_counts_up(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err1")
        db.increment_failure("-100123", 42, "download", "err2")
        row = db.get_failure("-100123", 42)
        assert row["attempt_count"] == 2
        assert row["last_error"] == "err2"

    def test_mark_failure_skipped(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err")
        db.mark_failure_skipped("-100123", 42, "重试 3 次仍失败")
        row = db.get_failure("-100123", 42)
        assert row["status"] == "skipped"
        assert row["skipped_at"] is not None
        assert row["skipped_reason"] == "重试 3 次仍失败"

    def test_delete_failure(self, db: ArchiveDB):
        db.increment_failure("-100123", 42, "download", "err")
        db.delete_failure("-100123", 42)
        assert db.get_failure("-100123", 42) is None

    def test_pending_failures_only_retrying(self, db: ArchiveDB):
        db.increment_failure("-100123", 41, "download", "e")
        db.increment_failure("-100123", 42, "download", "e")
        db.mark_failure_skipped("-100123", 42, "skip")
        # 返回复合键本身（入口 chat_id, 入口 message_id），调用方不必再 split
        assert db.pending_failures() == {("-100123", 41)}


def _stub_message(message_id, chat_id=-1001234567890, caption=None, text=None):
    return SimpleNamespace(
        id=message_id,
        chat=SimpleNamespace(id=chat_id, title="接收频道"),
        caption=caption,
        text=text,
        media_group_id=None,
    )


def _entry(message_id, route=ROUTE_FORWARD, chat_id=-1001234567890):
    return Entry(message=_stub_message(message_id, chat_id=chat_id), route=route)


def _item(media_id, entry, link=None):
    return ArchiveItem(media=_stub_message(media_id), entry=entry, link=link)


def _doc_stub(message_id, file_unique_id, chat_id=-1001234567890):
    """带 document 的消息桩。document 不走 ffprobe/缩略图，最适合测管道分支。"""
    msg = _stub_message(message_id, chat_id=chat_id, caption="", text="")
    msg.document = SimpleNamespace(
        file_unique_id=file_unique_id, file_name=f"{file_unique_id}.pdf",
        mime_type="application/pdf", file_size=2048,
    )
    msg.date = None
    msg.from_user = SimpleNamespace(first_name="tester", id=1)
    msg.sender_chat = None
    msg.forward_origin = None
    return msg


async def _noop_mark(*a, **k):
    return None


def _ctx(db=None, *, client=None, pipeline=None, receive_chat=-1001234567890):
    """按需组装 ListenerContext：没传的给空桩，被意外调用会 AttributeError。"""
    return listener.ListenerContext(
        client=client if client is not None else SimpleNamespace(),
        db=db if db is not None else SimpleNamespace(),
        pipeline=pipeline if pipeline is not None else SimpleNamespace(),
        receive_chat=receive_chat,
    )


def _sender(sent):
    """假 client：只有 send_message，把发出的 (chat_id, text) 记进 sent。"""
    async def send_message(chat_id, text, reply_parameters=None):
        sent.append((chat_id, text))
        return SimpleNamespace(id=1)

    return SimpleNamespace(send_message=send_message)


class TestRecordFailure:
    def test_record_failure_first_alert_and_retry(self, db: ArchiveDB):
        """首次失败：告警一次，返回 retry（不推进 checkpoint）"""
        sent = []

        result = asyncio.run(listener._record_failure(
            _ctx(db, client=_sender(sent)), _entry(42), "download", "tdl 失败"))

        assert result == "retry"
        assert len(sent) == 1
        assert "归档失败" in sent[0][1]
        row = db.get_failure("-1001234567890", 42)
        assert row["attempt_count"] == 1
        assert row["status"] == "retrying"

    def test_record_failure_at_max_skips(self, db: ArchiveDB, monkeypatch):
        """满 RETRY_MAX_ATTEMPTS：标记 skipped + 告警，返回 skip"""
        sent = []
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 3)
        ctx = _ctx(db, client=_sender(sent))

        entry = _entry(42)
        for _ in range(2):
            asyncio.run(listener._record_failure(ctx, entry, "download", "err"))
        sent.clear()
        result = asyncio.run(listener._record_failure(ctx, entry, "download", "err"))

        assert result == "skip"
        row = db.get_failure("-1001234567890", 42)
        assert row["status"] == "skipped"
        assert len(sent) == 1
        assert "已跳过" in sent[0][1]

    def test_record_failure_midway_no_extra_alert(self, db: ArchiveDB, monkeypatch):
        """中间轮次不再告警（只首次和满 N 轮各一条）"""
        sent = []
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 5)
        ctx = _ctx(db, client=_sender(sent))

        entry = _entry(42)
        for _ in range(3):
            asyncio.run(listener._record_failure(ctx, entry, "download", "err"))

        assert len(sent) == 1  # 只有首次那一条

    def test_alert_failure_send_fails_does_not_raise(self):
        """告警发送失败不影响归档流程（尽力而为）"""
        async def fake_send_message(chat_id, text, reply_parameters=None):
            raise RuntimeError("network down")
        client = SimpleNamespace(send_message=fake_send_message)

        # 不应抛异常
        asyncio.run(listener.alert_failure(client, _entry(42), "测试告警"))
        no_chat = Entry(message=SimpleNamespace(id=43, chat=None), route=ROUTE_FORWARD)
        asyncio.run(listener.alert_failure(client, no_chat, "无 chat 的告警"))


class TestFailureKeyedByEntry:
    def test_link_path_failure_keyed_on_receive_channel(self, db: ArchiveDB):
        """
        缺陷二回归：路径二的失败必须记在接收频道那条链接消息上。

        修复前 _record_failure 拿 message.chat.id，路径二的 message 来自源频道，
        失败行落在源频道 id 空间，pending_failures / delete_message.py 永远读不到。
        """
        result = asyncio.run(listener._record_failure(
            _ctx(db, client=_sender([])), _entry(300, ROUTE_LINK), "download", "err"))

        assert result == "retry"
        assert db.get_failure("-1001234567890", 300) is not None
        assert db.pending_failures() == {("-1001234567890", 300)}

    def test_link_path_alert_goes_to_entry_channel(self, db: ArchiveDB):
        """缺陷三回归：告警发到入口所在频道，不是源频道"""
        sent = []

        asyncio.run(listener._record_failure(
            _ctx(db, client=_sender(sent)), _entry(300, ROUTE_LINK), "download", "err"))

        assert len(sent) == 1
        assert sent[0][0] == -1001234567890, "告警发错频道了"

    def test_clear_failure_keyed_on_entry(self, db: ArchiveDB):
        db.increment_failure("-1001234567890", 300, "download", "err")

        listener._clear_failure(_ctx(db), _entry(300, ROUTE_LINK))

        assert db.get_failure("-1001234567890", 300) is None


class TestArchiveOneFailure:
    def test_single_download_failure_returns_failure_outcome(self, make_pipeline):
        """下载失败：产出 failure Outcome，不再就地记账"""
        captured = {}

        async def download(messages, dest_dir, fallback=None, *, links=None, fallback_paths=None):
            captured["called"] = True
            return {}

        pipeline = make_pipeline(
            db=SimpleNamespace(find_by_unique_id=lambda x: None),
            downloader=SimpleNamespace(download=download),
        )

        msg = _stub_message(42)
        msg.photo = SimpleNamespace(file_unique_id="uniq-42")  # 无媒体会提前返回，到不了下载分支
        item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
        outcome = asyncio.run(pipeline.archive_one(item))

        assert outcome.ok is False
        assert outcome.stage == "download"
        assert captured["called"] is True


class TestGroupSettled:
    def test_pending_failure_blocks(self):
        """组内有未满 N 轮的失败：未结清"""
        ctx = _ctx(SimpleNamespace(pending_failures=lambda: {("-1001234567890", 41)}))
        assert listener._group_settled(ctx, [_stub_message(41), _stub_message(42)]) is False

    def test_no_pending_failure_settles(self):
        """组内无未满 N 轮的失败：已结清"""
        ctx = _ctx(SimpleNamespace(pending_failures=set))
        assert listener._group_settled(ctx, [_stub_message(41), _stub_message(42)]) is True


class TestSettle:
    def test_all_ok_clears_failure_record(self, db: ArchiveDB):
        db.increment_failure("-1001234567890", 100, "download", "上一轮失败")

        settled = asyncio.run(listener._settle_all(
            _ctx(db), [Outcome.success(_item(100, _entry(100)))]))

        assert settled is True
        assert db.get_failure("-1001234567890", 100) is None

    def test_any_failure_records_once(self, db: ArchiveDB, monkeypatch):
        """一个入口多个条目：只记一次失败，不是每个失败条目记一次"""
        sent = []
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 3)

        entry = _entry(300, ROUTE_LINK)
        outcomes = [
            Outcome.failure(_item(41, entry), "download", "err1"),
            Outcome.failure(_item(42, entry), "upload", "err2"),
        ]
        settled = asyncio.run(listener._settle_all(_ctx(db, client=_sender(sent)), outcomes))

        assert settled is False
        assert db.get_failure("-1001234567890", 300)["attempt_count"] == 1
        assert len(sent) == 1

    def test_success_does_not_clear_sibling_failure(self, db: ArchiveDB):
        """同一入口下先成功的条目不能清掉后失败条目刚写的记录"""
        entry = _entry(300, ROUTE_LINK)
        outcomes = [
            Outcome.success(_item(41, entry)),
            Outcome.failure(_item(42, entry), "download", "err"),
        ]
        settled = asyncio.run(listener._settle_all(_ctx(db, client=_sender([])), outcomes))

        assert settled is False
        assert db.get_failure("-1001234567890", 300) is not None

    def test_skip_at_max_counts_as_settled(self, db: ArchiveDB, monkeypatch):
        """满 RETRY_MAX_ATTEMPTS 后算已结清，checkpoint 可以推进"""
        monkeypatch.setattr(listener, "RETRY_MAX_ATTEMPTS", 2)

        db.increment_failure("-1001234567890", 100, "download", "第一轮")
        outcome = Outcome.failure(_item(100, _entry(100)), "download", "第二轮")

        assert asyncio.run(listener._settle_all(
            _ctx(db, client=_sender([])), [outcome])) is True
        assert db.get_failure("-1001234567890", 100)["status"] == "skipped"

    def test_separate_entries_settle_separately(self, db: ArchiveDB):
        """路径一媒体组：每条媒体各是自己的入口，各记各的"""
        outcomes = [
            Outcome.success(_item(41, _entry(41))),
            Outcome.failure(_item(42, _entry(42)), "verify", "err"),
        ]
        assert asyncio.run(listener._settle_all(
            _ctx(db, client=_sender([])), outcomes)) is False
        assert db.get_failure("-1001234567890", 41) is None
        assert db.get_failure("-1001234567890", 42) is not None


def _group_message(message_id, group_id="g1", chat_id=-1001234567890):
    msg = _stub_message(message_id, chat_id=chat_id)
    msg.media_group_id = group_id
    return msg


def _fake_app(messages, group=None):
    """假 app：get_chat_history 按序吐消息，get_media_group 返回整组。"""
    async def get_chat_history(chat_id, min_id=0, reverse=False):
        for m in messages:
            yield m

    async def get_media_group(chat_id, message_id):
        return list(group or [])

    return SimpleNamespace(
        get_chat_history=get_chat_history,
        get_media_group=get_media_group,
    )


class TestScanOnceCheckpoint:
    def test_group_not_settled_stops_round(self):
        """媒体组未结清：不推进 checkpoint，且不再处理后续消息（缺陷一回归）"""
        set_calls = []
        group = [_group_message(41)]
        tail = _stub_message(42)  # 非媒体无链接，修复前会被无条件推进

        async def fake_archive_batch(items):
            return []

        ctx = _ctx(
            SimpleNamespace(
                get_checkpoint=lambda c: 0,
                set_checkpoint=lambda c, m: set_calls.append(m),
                pending_failures=lambda: {("-1001234567890", 41)},
            ),
            client=_fake_app([group[0], tail], group),
            pipeline=SimpleNamespace(archive_batch=fake_archive_batch),
        )

        asyncio.run(listener.scan_once(ctx))

        assert set_calls == [], f"未结清却推进了 checkpoint：{set_calls}"

    def test_group_settled_advances_to_group_max(self):
        """媒体组已结清：推进到组内最大 id，后续消息继续处理"""
        set_calls = []
        group = [_group_message(41), _group_message(42)]
        tail = _stub_message(43)

        async def fake_archive_batch(items):
            return []

        ctx = _ctx(
            SimpleNamespace(
                get_checkpoint=lambda c: 0,
                set_checkpoint=lambda c, m: set_calls.append(m),
                pending_failures=set,
            ),
            client=_fake_app([group[0], group[1], tail], group),
            pipeline=SimpleNamespace(archive_batch=fake_archive_batch),
        )

        asyncio.run(listener.scan_once(ctx))

        assert set_calls == [42, 43]

    def test_link_entry_not_settled_stops_round(self, monkeypatch):
        """缺陷二回归：链接消息未结清就不能推进 checkpoint"""
        set_calls = []
        link_msg = _stub_message(300, text="https://t.me/c/999/42")
        tail = _stub_message(301)

        ctx = _ctx(
            SimpleNamespace(
                get_checkpoint=lambda c: 0,
                set_checkpoint=lambda c, m: set_calls.append(m),
                increment_failure=lambda *a: 1,
                mark_failure_skipped=lambda *a: None,
                delete_failure=lambda *a: None,
            ),
            client=_fake_app([link_msg, tail]),
        )
        monkeypatch.setattr(listener, "alert_failure", _noop_mark)

        async def fake_process_link(ctx, entry):
            return [Outcome.failure(_item(42, entry), "download", "err")]

        monkeypatch.setattr(listener, "process_link_message", fake_process_link)

        asyncio.run(listener.scan_once(ctx))

        assert set_calls == [], f"链接未结清却推进了 checkpoint：{set_calls}"

    def test_plain_message_advances(self):
        """非媒体且无链接的消息照常推进，不回退现有行为"""
        set_calls = []
        ctx = _ctx(
            SimpleNamespace(
                get_checkpoint=lambda c: 0,
                set_checkpoint=lambda c, m: set_calls.append(m),
            ),
            client=_fake_app([_stub_message(500)]),
        )

        asyncio.run(listener.scan_once(ctx))

        assert set_calls == [500]


class TestArchiveGroupOutcomes:
    def test_group_upload_failure_reports_only_first_pending(self, make_pipeline, tmp_path):
        """
        整组上传失败：只有第一条待上传条目产出失败结论。

        保住今天「一次组上传失败只发一条告警」的行为 —— 若给每条都产出失败结论，
        路径一媒体组每条各是自己的入口，会变成 N 条告警。
        """
        local = tmp_path / "a.pdf"
        local.write_bytes(b"\x02" * 2048)

        async def fake_download(messages, dest, **k):
            return {m.id: str(local) for m in messages}

        async def boom_group(*a, **k):
            raise RuntimeError("组上传炸了")

        pipeline = make_pipeline(
            client=SimpleNamespace(send_media_group=boom_group),
            db=SimpleNamespace(
                find_by_unique_id=lambda x: None,
                find_by_sha256=lambda x: None,
            ),
            downloader=SimpleNamespace(download=fake_download),
        )

        items = []
        for mid, fuid in ((41, "F41"), (42, "F42")):
            msg = _doc_stub(mid, fuid)
            items.append(ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD)))

        outcomes = asyncio.run(pipeline.archive_batch(items))

        failed = [o for o in outcomes if not o.ok]
        assert len(failed) == 1, f"应只有一条失败结论，实际 {len(failed)}"
        assert failed[0].item.media.id == 41
        assert failed[0].stage == "upload"

    def test_dedup_hit_is_ok_outcome(self, make_pipeline):
        """file_unique_id 命中：算 ok，不产出失败"""
        pipeline = make_pipeline(db=SimpleNamespace(
            find_by_unique_id=lambda x: {"archived_message_id": 1},
        ))

        msg = _doc_stub(41, "F41")
        item = ArchiveItem(media=msg, entry=Entry(message=msg, route=ROUTE_FORWARD))
        outcomes = asyncio.run(pipeline.archive_batch([item]))

        assert len(outcomes) == 1
        assert outcomes[0].ok is True

    def test_link_path_group_derives_single_url(self, make_pipeline):
        """
        路径二媒体组：只有链接指向的那条带 link，推导出的 links 必须正好一个键。

        tdl 靠「多条消息只给一个 URL」决定加 --group 一次拉整组
        （tdl_downloader.py:113）。给每条都编链接会静默改掉下载语义。
        """
        captured = {}

        async def fake_download(messages, dest, *, links=None, **k):
            captured["links"] = links
            return {}  # 空路径，走 download 失败分支，不进上传

        pipeline = make_pipeline(
            db=SimpleNamespace(find_by_unique_id=lambda x: None),
            downloader=SimpleNamespace(download=fake_download),
        )

        link = "https://t.me/c/999/42"
        entry = _entry(300, ROUTE_LINK)
        group = [_doc_stub(41, "F41", chat_id=-1009999999999),
                 _doc_stub(42, "F42", chat_id=-1009999999999)]
        items = [ArchiveItem(media=m, entry=entry, link=link if m.id == 42 else None)
                 for m in group]

        asyncio.run(pipeline.archive_batch(items))

        assert captured["links"] == {42: link}

    def test_group_process_exception_lands_in_failures(self, db: ArchiveDB, make_pipeline):
        """
        债二闭环：整组准备阶段的非预期异常经 _settle_all 进 archive_failures。

        修复前异常冒到 main()，_settle_all 从没被调用 —— 不计次、不告警，
        下一轮重扫同样炸，那条消息永久卡住 checkpoint。
        """
        async def boom(messages, dest, fallback=None, *, links=None, fallback_paths=None):
            raise RuntimeError("tdl 炸了")

        pipeline = make_pipeline(
            db=SimpleNamespace(find_by_unique_id=lambda x: None),
            downloader=SimpleNamespace(download=boom),
        )
        msg = _doc_stub(41, "F41")
        entry = Entry(message=msg, route=ROUTE_FORWARD)

        outcomes = asyncio.run(pipeline.archive_batch([ArchiveItem(media=msg, entry=entry)]))
        settled = asyncio.run(listener._settle_all(_ctx(db, client=_sender([])), outcomes))

        assert settled is False, "未结清才对：下一轮要重试"
        row = db.get_failure("-1001234567890", 41)
        assert row is not None and row["attempt_count"] == 1
        assert row["failure_stage"] == "process"


def _history(messages, yielded=None):
    """假 get_chat_history：按序吐消息，顺带记下实际吐了哪些 id。"""
    async def get_chat_history(chat_id, min_id=0, reverse=False):
        for m in messages:
            if yielded is not None:
                yielded.append(m.id)
            yield m

    return get_chat_history


class TestScanOnceEntryBoundary:
    """
    入口级异常边界。

    _handle_entry 里 get_media_group / _settle_all 这些调用在管道之外，管道自己的
    兜底 except 管不到它们。修复前这些异常一路冒到 main() 的兜底：不记
    archive_failures、不告警、也没有满 N 轮跳过 —— 一个持续失败的入口就是每轮炸一次、
    checkpoint 永远停在那里，而且本轮后面的消息全都不处理。
    """

    def _ctx_with_raising_group(self, db, error, alerts, messages):
        client = SimpleNamespace(
            get_chat_history=_history(messages),
            get_media_group=self._raiser(error),
            send_message=_sender(alerts).send_message,
        )
        return _ctx(db, client=client)

    @staticmethod
    def _raiser(error):
        async def get_media_group(chat_id, message_id):
            raise error

        return get_media_group

    def test_entry_exception_lands_in_failure_ledger(self, db: ArchiveDB):
        db.ensure_channel(-1001234567890, "manual_forward")
        alerts = []
        ctx = self._ctx_with_raising_group(
            db, ValueError("媒体组取不到"), alerts,
            [_group_message(41), _stub_message(42)])

        asyncio.run(listener.scan_once(ctx))

        row = db.get_failure("-1001234567890", 41)
        assert row is not None, "入口级异常必须进 archive_failures"
        assert row["failure_stage"] == "process"
        assert "媒体组取不到" in row["last_error"]
        assert row["attempt_count"] == 1
        assert alerts, "首次失败要在接收频道回复告警"
        # 未结清 → 不推进，且本轮就此停下（42 不该被处理）
        assert db.get_checkpoint(-1001234567890) == 0

    def test_entry_exception_skips_after_max_attempts(self, db: ArchiveDB):
        """满 N 轮后标 skipped 并放行 checkpoint —— 否则这个入口永久卡住整个扫描"""
        db.ensure_channel(-1001234567890, "manual_forward")
        for _ in range(listener.RETRY_MAX_ATTEMPTS - 1):
            db.increment_failure("-1001234567890", 41, "process", "上几轮也失败")
        ctx = self._ctx_with_raising_group(
            db, ValueError("还是取不到"), [], [_group_message(41)])

        asyncio.run(listener.scan_once(ctx))

        row = db.get_failure("-1001234567890", 41)
        assert row["status"] == "skipped"
        assert db.get_checkpoint(-1001234567890) == 41

    def test_empty_message_exception_still_gets_an_error_text(self, db: ArchiveDB):
        """无参异常的 str() 是空串，last_error 不能因此变空（只剩 stage 可查）"""
        db.ensure_channel(-1001234567890, "manual_forward")
        ctx = self._ctx_with_raising_group(
            db, AssertionError(), [], [_group_message(41)])

        asyncio.run(listener.scan_once(ctx))

        assert db.get_failure("-1001234567890", 41)["last_error"] == "AssertionError()"


class TestScanOnceHistoryBound:
    def test_stops_pulling_after_batch_size(self):
        """
        攒够 BATCH_SIZE 就停止拉取历史，多收一条只为知道后面还有。

        修复前是「先把 checkpoint 之后的全部消息拉进 list 再切片」：积压几千条时，
        每 SCAN_INTERVAL_SECONDS 一轮都要把整段历史重扫一遍（get_chat_history
        每 100 条一次请求），只为推进 BATCH_SIZE 条 —— 与项目自身的风控取向相反。
        """
        yielded = []
        messages = [_stub_message(i) for i in range(100, 160)]
        ctx = _ctx(
            SimpleNamespace(get_checkpoint=lambda c: 0,
                            set_checkpoint=lambda c, m: None),
            client=SimpleNamespace(get_chat_history=_history(messages, yielded)),
        )

        assert asyncio.run(listener.scan_once(ctx)) == 0  # 全是非媒体消息
        assert len(yielded) <= listener.BATCH_SIZE + 1, f"多拉了 {len(yielded)} 条"
