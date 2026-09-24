"""bot_commands：权限、命令解析、白名单与 /stats。"""
import asyncio
from types import SimpleNamespace

import bot_commands
import pytest
from bot_commands import (
    BotContext,
    DeletePreview,
    PendingDelete,
    PendingStore,
    format_delete_preview,
    format_purge,
    handle_callback,
    handle_message,
    parse_admin_ids,
    parse_callback,
    parse_command,
    parse_delete_args,
    user_allowed,
)
from db import PurgeSummary, RetrySummary, Stats

ADMIN = 111


class _FakeMessage:
    """假消息：reply_text 记回复与按钮，返回带自增 .id 的对象。user_id=None 模拟频道匿名。"""

    def __init__(self, text, user_id=ADMIN):
        self.text = text
        self.from_user = None if user_id is None else SimpleNamespace(id=user_id)
        self.sent = []
        self.markups = []
        self.reply_ids = []
        self._next_id = 1000

    async def reply_text(self, text, reply_markup=None, **kwargs):
        self.sent.append(text)
        self.markups.append(reply_markup)
        self._next_id += 1
        self.reply_ids.append(self._next_id)
        return SimpleNamespace(id=self._next_id)


class _FakeCbMessage:
    def __init__(self, mid):
        self.id = mid
        self.edited = []

    async def edit_text(self, text, **kwargs):
        self.edited.append(text)


class _FakeCallbackQuery:
    """假 callback：记录 answer 文本与被编辑的消息文本。message=None 模拟消息不可用。"""

    def __init__(self, data, message_id, user_id=ADMIN):
        self.data = data
        self.from_user = None if user_id is None else SimpleNamespace(id=user_id)
        self.message = _FakeCbMessage(message_id) if message_id is not None else None
        self.answers = []

    async def answer(self, text="", **kwargs):
        self.answers.append(text)


class _RecordingLock:
    """假锁：只记录进出。用来断言写命令确实走了锁，asyncio 本身不必再测。"""

    def __init__(self):
        self.entries = 0
        self.exits = 0

    async def __aenter__(self):
        self.entries += 1
        return self

    async def __aexit__(self, *exc):
        self.exits += 1
        return False


def _ctx(*, db=None, lock=None, admin_ids=frozenset({ADMIN}), run_backup=None, pending=None):
    async def default_backup():
        return "/data/db/backups/archive-20260920-030405.db"

    return BotContext(
        db=db if db is not None else SimpleNamespace(),
        lock=lock if lock is not None else _RecordingLock(),
        admin_ids=admin_ids,
        run_backup=run_backup or default_backup,
        pending=pending if pending is not None else PendingStore(),
    )


def _stats(**over):
    fields = {"total_files": 3, "message_count": 2, "dedup_hits": 1,
              "by_kind": {"document": 2, "video": 1}, "failures": {"retrying": 1}}
    fields.update(over)
    return Stats(**fields)


class TestParseCommand:
    def test_plain_command(self):
        assert parse_command("/stats") == ("stats", [])

    def test_args_split_on_whitespace(self):
        assert parse_command("/retry 100  105") == ("retry", ["100", "105"])

    def test_group_suffix_stripped(self):
        """群聊里客户端发的是 /stats@my_bot。"""
        assert parse_command("/stats@my_bot 关键词") == ("stats", ["关键词"])

    def test_case_insensitive(self):
        assert parse_command("/STATS") == ("stats", [])

    def test_not_a_command(self):
        assert parse_command("stats 一下") is None
        assert parse_command("") is None
        assert parse_command(None) is None

    def test_bare_slash(self):
        assert parse_command("/") is None


class TestParseAdminIds:
    def test_multiple_with_spaces(self):
        assert parse_admin_ids("111, 222 ,333") == frozenset({111, 222, 333})

    def test_empty_is_empty_allowlist(self):
        assert parse_admin_ids("") == frozenset()
        assert parse_admin_ids("  ,  ") == frozenset()

    def test_garbage_ignored_not_fatal(self):
        """一个打错的 id 不该让整个 bot 起不来。"""
        assert parse_admin_ids("111,abc,222") == frozenset({111, 222})


class TestAuthorization:
    def test_non_admin_is_ignored_silently(self):
        msg = _FakeMessage("/stats", user_id=999)

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            stats=lambda: pytest.fail("不该查库"))), msg))

        assert msg.sent == []

    def test_anonymous_from_user_is_rejected(self):
        msg = _FakeMessage("/stats", user_id=None)

        asyncio.run(handle_message(_ctx(), msg))

        assert msg.sent == []

    def test_empty_allowlist_denies_everyone(self):
        msg = _FakeMessage("/stats")

        asyncio.run(handle_message(_ctx(admin_ids=frozenset()), msg))

        assert msg.sent == []

    def test_plain_text_is_ignored(self):
        """白名单用户发的非命令文本也不回 —— bot 不做闲聊。"""
        msg = _FakeMessage("在吗")

        asyncio.run(handle_message(_ctx(), msg))

        assert msg.sent == []


class TestStatsCommand:
    def test_reply_carries_all_counts(self):
        msg = _FakeMessage("/stats")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(stats=_stats)), msg))

        assert len(msg.sent) == 1
        body = msg.sent[0]
        assert "文件总数：3" in body
        assert "消息条目：2（其中去重命中 1）" in body
        assert "document 2 / video 1" in body
        assert "retrying 1" in body

    def test_empty_db_says_zero(self):
        msg = _FakeMessage("/stats")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            stats=lambda: _stats(total_files=0, message_count=0, dedup_hits=0,
                                 by_kind={}, failures={}))), msg))

        assert "文件总数：0" in msg.sent[0]
        assert "类型分布：（无）" in msg.sent[0]
        assert "失败账：0" in msg.sent[0]


class TestUnknownCommand:
    def test_admin_gets_usage(self):
        msg = _FakeMessage("/nope")

        asyncio.run(handle_message(_ctx(), msg))

        assert "可用命令" in msg.sent[0]
        assert "/stats" in msg.sent[0]

    def test_non_admin_stays_silent(self):
        msg = _FakeMessage("/nope", user_id=999)

        asyncio.run(handle_message(_ctx(), msg))

        assert msg.sent == []


def _failure_row(mid=100, status="skipped", stage="download", attempts=3, err="代理断"):
    return {"source_chat_id": "-1001234567890", "source_message_id": mid,
            "failure_stage": stage, "last_error": err, "attempt_count": attempts,
            "status": status, "last_failed_at": "2026-09-19 10:00:00"}


class TestFailuresCommand:
    def test_lists_rows(self):
        rows = [_failure_row(100), _failure_row(105, status="retrying", stage="upload",
                                                attempts=1, err="flood")]
        msg = _FakeMessage("/failures")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(list_failures=lambda: rows)), msg))

        body = msg.sent[0]
        assert "失败账共 2 条" in body
        assert "msg=100" in body and "skipped" in body and "代理断" in body
        assert "msg=105" in body and "retrying" in body

    def test_empty_replies_plainly(self):
        msg = _FakeMessage("/failures")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(list_failures=list)), msg))

        assert msg.sent == ["失败账为空"]

    def test_long_list_is_capped_with_a_count(self):
        rows = [_failure_row(mid=100 + i) for i in range(50)]
        msg = _FakeMessage("/failures")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(list_failures=lambda: rows)), msg))

        body = msg.sent[0]
        assert "…还有 30 条" in body
        assert "msg=119" in body          # 第 20 条（MAX_LISTED_ITEMS）还在
        assert "msg=120" not in body      # 第 21 条起不列


class TestSearchCommand:
    def _row(self, **over):
        row = {"sent_at": "2026-09-01", "media_kind": "document", "origin_title": "某频道",
               "origin_chat_id": "-1001234", "sender": "张三", "caption": "报告",
               "file_name": "a.pdf", "archived_chat_id": "-1009876543210",
               "archived_message_id": 7}
        row.update(over)
        return row

    def test_uses_db_search_and_cli_format(self):
        calls = []

        def search(query, limit):
            calls.append((query, limit))
            return [self._row()]

        msg = _FakeMessage("/search 报告 2026")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(search=search)), msg))

        assert calls == [("报告 2026", bot_commands.SEARCH_LIMIT)]
        body = msg.sent[0]
        assert "某频道" in body
        assert "https://t.me/c/9876543210/7" in body

    def test_no_keyword_replies_usage(self):
        msg = _FakeMessage("/search")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            search=lambda q, limit: pytest.fail("没关键词不该查库"))), msg))

        assert "用法" in msg.sent[0]

    def test_no_match_explains_trigram_limit(self):
        msg = _FakeMessage("/search 猫咪")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            search=lambda q, limit: [])), msg))

        assert "没搜到" in msg.sent[0]
        assert "3 个字符" in msg.sent[0]

    def test_long_result_is_truncated_to_telegram_limit(self):
        # caption 在 format_result 里截到 80 字符，撑不满 4096；file_name 不截，用它把
        # 单行撑长，20 行叠起来才真正越过上限、逼出 truncate 的省略号
        rows = [self._row(caption="长" * 200, file_name="长" * 200)
                for _ in range(bot_commands.SEARCH_LIMIT)]
        msg = _FakeMessage("/search 长")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            search=lambda q, limit: rows)), msg))

        assert len(msg.sent[0]) <= bot_commands.TELEGRAM_TEXT_LIMIT
        assert msg.sent[0].endswith("…")


class TestRetryCommand:
    def test_takes_lock_and_reports_rollback(self):
        lock = _RecordingLock()
        seen = {}

        def retry_skipped(ids):
            seen["ids"] = ids
            return RetrySummary([("-1001234567890", 100)],
                                {"-1001234567890": (200, 99)})

        msg = _FakeMessage("/retry 100")

        asyncio.run(handle_message(
            _ctx(db=SimpleNamespace(retry_skipped=retry_skipped), lock=lock), msg))

        assert (lock.entries, lock.exits) == (1, 1)
        assert seen["ids"] == [100]
        assert "已重置 1 条" in msg.sent[0]
        assert "checkpoint 回退 chat=-1001234567890: 200 → 99" in msg.sent[0]

    def test_lock_is_actually_held_while_writing(self):
        """真锁：断言写库那一刻锁是拿住的，而不只是「顺手进出了一把假锁」。"""
        lock = asyncio.Lock()
        held = []

        def retry_skipped(ids):
            held.append(lock.locked())
            return RetrySummary([], {})

        asyncio.run(handle_message(
            _ctx(db=SimpleNamespace(retry_skipped=retry_skipped), lock=lock),
            _FakeMessage("/retry")))

        assert held == [True]

    def test_no_args_means_all_skipped(self):
        seen = {}

        def retry_skipped(ids):
            seen["ids"] = ids
            return RetrySummary([("-1001234567890", 100)], {})

        asyncio.run(handle_message(
            _ctx(db=SimpleNamespace(retry_skipped=retry_skipped)), _FakeMessage("/retry")))

        assert seen["ids"] is None

    def test_non_integer_arg_replies_usage_without_touching_db(self):
        msg = _FakeMessage("/retry abc")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            retry_skipped=lambda ids: pytest.fail("参数不合法不该动库"))), msg))

        assert "用法" in msg.sent[0]

    def test_nothing_matched(self):
        msg = _FakeMessage("/retry")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            retry_skipped=lambda ids: RetrySummary([], {}))), msg))

        assert msg.sent == ["没有匹配的 skipped 行，checkpoint 未改动"]

    def test_no_rollback_is_stated_explicitly(self):
        msg = _FakeMessage("/retry")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            retry_skipped=lambda ids: RetrySummary([("-1001234567890", 100)], {}))), msg))

        assert "checkpoint 未回退" in msg.sent[0]

    def test_long_list_is_capped_with_a_count(self):
        entries = [("-1001234567890", 100 + i) for i in range(50)]
        msg = _FakeMessage("/retry")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            retry_skipped=lambda ids: RetrySummary(entries, {}))), msg))

        assert "…还有 30 条" in msg.sent[0]
        assert "下轮扫描重扫这些消息" in msg.sent[0]


class TestBackupCommand:
    def test_takes_lock_and_reports_snapshot(self):
        lock = _RecordingLock()

        async def run_backup():
            return "/data/db/backups/archive-20260920-030405.db"

        msg = _FakeMessage("/backup")

        asyncio.run(handle_message(_ctx(lock=lock, run_backup=run_backup), msg))

        assert (lock.entries, lock.exits) == (1, 1)
        assert msg.sent == ["备份完成：archive-20260920-030405.db"]

    def test_failure_is_reported_to_admin(self):
        async def boom():
            raise OSError("disk full")

        msg = _FakeMessage("/backup")

        asyncio.run(handle_message(_ctx(run_backup=boom), msg))

        assert "命令执行失败" in msg.sent[0]
        assert "disk full" in msg.sent[0]


class TestRegisterHandlers:
    def test_hooks_dispatcher_and_dispatches(self):
        """注册进去的回调真的走到 handle_message；callback handler 也挂上。"""
        registered_msg = []
        registered_cb = []

        def on_message():
            def deco(fn):
                registered_msg.append(fn)
                return fn
            return deco

        def on_callback_query():
            def deco(fn):
                registered_cb.append(fn)
                return fn
            return deco

        ctx = _ctx(db=SimpleNamespace(stats=_stats))

        bot_commands.register_handlers(
            SimpleNamespace(on_message=on_message, on_callback_query=on_callback_query), ctx)

        assert len(registered_msg) == 1 and len(registered_cb) == 1
        msg = _FakeMessage("/stats")
        asyncio.run(registered_msg[0](SimpleNamespace(), msg))
        assert "文件总数：3" in msg.sent[0]


class TestCommandMenu:
    def test_registers_all_commands_in_order(self):
        sent = []

        async def set_bot_commands(commands):
            sent.extend(commands)
            return True

        asyncio.run(bot_commands.register_command_menu(
            SimpleNamespace(set_bot_commands=set_bot_commands)))

        assert [c.command for c in sent] == [
            "stats", "search", "failures", "retry", "backup", "delete"]
        assert all(c.description for c in sent)

    def test_menu_failure_is_not_fatal(self):
        """菜单注册失败（如 flood）只 warning，命令本身照常可用。"""
        async def boom(commands):
            raise OSError("flood")

        asyncio.run(bot_commands.register_command_menu(
            SimpleNamespace(set_bot_commands=boom)))


class TestParseCallback:
    def test_action_and_arg(self):
        assert parse_callback("del:ok") == ("del", "ok")

    def test_action_without_arg(self):
        assert parse_callback("del") == ("del", "")

    def test_empty_or_none(self):
        assert parse_callback("") is None
        assert parse_callback(None) is None

    def test_bare_colon_has_no_action(self):
        assert parse_callback(":ok") is None


class TestUserAllowed:
    def test_admin_passes(self):
        assert user_allowed(SimpleNamespace(id=111), frozenset({111})) is True

    def test_non_admin_and_none_rejected(self):
        assert user_allowed(SimpleNamespace(id=999), frozenset({111})) is False
        assert user_allowed(None, frozenset({111})) is False


class TestPendingStore:
    def test_put_then_take_pops_once(self):
        store: PendingStore[PendingDelete] = PendingStore()
        payload = PendingDelete(found_ids=(100,), rollback=False)
        store.put(7, payload, requester_id=111)

        assert store.take(7, by_user=111) == payload
        assert store.take(7, by_user=111) is None      # pop-once

    def test_take_rejects_other_user(self):
        store: PendingStore[PendingDelete] = PendingStore()
        store.put(7, PendingDelete(found_ids=(100,), rollback=False), requester_id=111)

        assert store.take(7, by_user=222) is None

    def test_take_none_when_expired(self, monkeypatch):
        store: PendingStore[PendingDelete] = PendingStore(ttl_seconds=10)
        clock = [1000.0]
        monkeypatch.setattr("bot_commands.time.monotonic", lambda: clock[0])
        store.put(7, PendingDelete(found_ids=(100,), rollback=False), requester_id=111)
        clock[0] = 1011.0                                # 超过 ttl

        assert store.take(7, by_user=111) is None


def _msg_row(mid=100, chat="-1001234567890", fuid="FUID1", sender="张三", caption="报告"):
    return {"id": 1, "source_message_id": mid, "source_chat_id": chat,
            "file_unique_id": fuid, "sender": sender, "caption": caption,
            "sent_at": "2026-09-01"}


class TestParseDeleteArgs:
    def test_ids_only_defaults_no_rollback(self):
        assert parse_delete_args(["100", "105"]) == ([100, 105], False)

    def test_rollback_flag(self):
        assert parse_delete_args(["100", "rollback"]) == ([100], True)

    def test_keep_flag_is_explicit_no_rollback(self):
        assert parse_delete_args(["100", "KEEP"]) == ([100], False)

    def test_conflicting_flags_raise(self):
        with pytest.raises(ValueError):
            parse_delete_args(["100", "keep", "rollback"])

    def test_non_integer_raises(self):
        with pytest.raises(ValueError):
            parse_delete_args(["abc"])

    def test_no_ids_raises(self):
        with pytest.raises(ValueError):
            parse_delete_args(["rollback"])


class TestFormatDeletePreview:
    def test_no_rollback_says_checkpoint_unchanged(self):
        preview = DeletePreview(rows=[_msg_row(100)], missing=[], rollback=False,
                                cp_changes=[], cleared_failures=[])
        body = format_delete_preview(preview)
        assert "将删除 1 条" in body and "msg=100" in body
        assert "checkpoint 不变" in body

    def test_rollback_lists_change(self):
        preview = DeletePreview(rows=[_msg_row(300)], missing=[], rollback=True,
                                cp_changes=[("-1001234567890", 350, 299)], cleared_failures=[])
        body = format_delete_preview(preview)
        assert "checkpoint 将回退" in body
        assert "-1001234567890: 350 → 299" in body

    def test_large_rollback_is_flagged(self):
        preview = DeletePreview(rows=[_msg_row(41)], missing=[], rollback=True,
                                cp_changes=[("-1001234567890", 15000, 40)], cleared_failures=[])
        assert "⚠" in format_delete_preview(preview)

    def test_missing_and_failures_shown(self):
        preview = DeletePreview(rows=[_msg_row(100)], missing=[999], rollback=False,
                                cp_changes=[], cleared_failures=[("-1001234567890", 100)])
        body = format_delete_preview(preview)
        assert "未找到" in body and "999" in body
        assert "将清除失败账 1 条" in body


class TestFormatPurge:
    def test_rollback_reports_change(self):
        summary = PurgeSummary(1, ["FUID1"], [], {"-1001234567890": (350, 299)})
        body = format_purge(summary, PendingDelete(found_ids=(300,), rollback=True))
        assert "checkpoint 回退" in body and "350 → 299" in body

    def test_keep_states_not_rolled_back(self):
        summary = PurgeSummary(1, ["FUID1"], [], {})
        body = format_purge(summary, PendingDelete(found_ids=(300,), rollback=False))
        assert "按 keep 保留" in body

    def test_rollback_requested_but_none_moved(self):
        summary = PurgeSummary(1, [], [], {})
        body = format_purge(summary, PendingDelete(found_ids=(300,), rollback=True))
        assert "目标不小于当前值" in body


class TestDeleteCommand:
    def _db(self, rows, *, failures=(), checkpoint=350):
        return SimpleNamespace(
            find_messages_by_source_ids=lambda ids: [r for r in rows
                                                     if r["source_message_id"] in ids],
            get_failure=lambda c, m: object() if (c, m) in failures else None,
            get_checkpoint=lambda c: checkpoint,
        )

    def test_preview_attaches_buttons_and_registers_pending(self):
        pending: PendingStore[PendingDelete] = PendingStore()
        msg = _FakeMessage("/delete 300")

        asyncio.run(handle_message(_ctx(db=self._db([_msg_row(300)]), pending=pending), msg))

        assert msg.markups[0] is not None                 # 挂了按钮
        assert "将删除 1 条" in msg.sent[0]
        payload = pending.take(msg.reply_ids[0], ADMIN)
        assert payload == PendingDelete(found_ids=(300,), rollback=False)

    def test_rollback_flag_recorded_in_pending(self):
        pending: PendingStore[PendingDelete] = PendingStore()
        msg = _FakeMessage("/delete 300 rollback")

        asyncio.run(handle_message(_ctx(db=self._db([_msg_row(300)]), pending=pending), msg))

        assert pending.take(msg.reply_ids[0], ADMIN) == PendingDelete(
            found_ids=(300,), rollback=True, cp_changes=(("-1001234567890", 350, 299),))

    def test_no_match_replies_plainly_without_buttons(self):
        msg = _FakeMessage("/delete 999")

        asyncio.run(handle_message(_ctx(db=self._db([_msg_row(300)])), msg))

        assert "没找到" in msg.sent[0]
        assert msg.markups[0] is None

    def test_bad_args_reply_usage_without_touching_db(self):
        msg = _FakeMessage("/delete abc")

        asyncio.run(handle_message(_ctx(db=SimpleNamespace(
            find_messages_by_source_ids=lambda ids: pytest.fail("参数非法不该查库"))), msg))

        assert "用法" in msg.sent[0]


class TestDeleteCallback:
    def _pending_with(self, message_id, payload, requester=ADMIN):
        store: PendingStore[PendingDelete] = PendingStore()
        store.put(message_id, payload, requester)
        return store

    def test_confirm_takes_lock_and_purges(self):
        calls = []
        db = self._preview_db([_msg_row(300)], checkpoint=[350])   # 确认时重算预览，需完整库
        db.purge_messages = lambda ids, rollback: (calls.append((ids, rollback)),
                                                   PurgeSummary(1, ["F"], [], {}))[1]
        lock = _RecordingLock()
        pending = self._pending_with(
            500, PendingDelete(found_ids=(300,), rollback=True,
                               cp_changes=(("-1001234567890", 350, 299),)))
        cq = _FakeCallbackQuery("del:ok", message_id=500)

        asyncio.run(handle_callback(_ctx(db=db, lock=lock, pending=pending), cq))

        assert calls == [([300], True)]                  # 走了 purge，带对的 rollback
        assert (lock.entries, lock.exits) == (1, 1)      # 真删走锁
        assert cq.message is not None
        assert "已删除 1 条" in cq.message.edited[0]
        assert cq.answers
        assert pending.take(500, ADMIN) is None          # pop-once 已消费

    def test_cancel_edits_and_skips_purge(self):
        db = SimpleNamespace(purge_messages=lambda *a, **k: pytest.fail("取消不该删"))
        pending = self._pending_with(500, PendingDelete(found_ids=(300,), rollback=False))
        cq = _FakeCallbackQuery("del:no", message_id=500)

        asyncio.run(handle_callback(_ctx(db=db, pending=pending), cq))

        assert cq.message is not None
        assert "已取消" in cq.message.edited[0]
        assert pending.take(500, ADMIN) is None

    def test_expired_confirm_answers_invalid(self):
        db = SimpleNamespace(purge_messages=lambda *a, **k: pytest.fail("失效不该删"))
        cq = _FakeCallbackQuery("del:ok", message_id=500)      # pending 为空

        asyncio.run(handle_callback(_ctx(db=db, pending=PendingStore()), cq))

        assert any("失效" in a for a in cq.answers)

    def test_non_admin_callback_is_rejected(self):
        db = SimpleNamespace(purge_messages=lambda *a, **k: pytest.fail("非白名单不该删"))
        pending = self._pending_with(500, PendingDelete(found_ids=(300,), rollback=False))
        cq = _FakeCallbackQuery("del:ok", message_id=500, user_id=999)

        asyncio.run(handle_callback(_ctx(db=db, pending=pending), cq))

        # 未删，pending 仍在（没被别人的点击消费）
        assert pending.take(500, ADMIN) == PendingDelete(found_ids=(300,), rollback=False)

    def test_unknown_action_answered_not_purged(self):
        db = SimpleNamespace(purge_messages=lambda *a, **k: pytest.fail("未知 action 不该删"))
        cq = _FakeCallbackQuery("nope:x", message_id=500)

        asyncio.run(handle_callback(_ctx(db=db, pending=PendingStore()), cq))

        assert cq.answers                                     # 答了以清客户端转圈

    def _preview_db(self, rows, *, checkpoint):
        """带完整预览方法的假库；checkpoint 用可变容器以便确认前改动。"""
        return SimpleNamespace(
            find_messages_by_source_ids=lambda ids: [r for r in rows
                                                     if r["source_message_id"] in ids],
            get_failure=lambda c, m: None,
            get_checkpoint=lambda c: checkpoint[0],
        )

    def test_confirm_only_purges_previewed_ids_not_late_archived(self):
        """#3：预览时「未找到」的 id 即便确认前被归档，确认也只删预览命中过的，不碰它。"""
        purged = []
        db = self._preview_db([_msg_row(300)], checkpoint=[350])
        db.purge_messages = lambda ids, rollback: (purged.append(ids),
                                                   PurgeSummary(1, [], [], {}))[1]
        pending: PendingStore[PendingDelete] = PendingStore()
        ctx = _ctx(db=db, pending=pending)
        msg = _FakeMessage("/delete 300 999")            # 300 命中、999 未找到

        asyncio.run(handle_message(ctx, msg))
        assert "未找到" in msg.sent[0] and "999" in msg.sent[0]

        cq = _FakeCallbackQuery("del:ok", message_id=msg.reply_ids[0])
        asyncio.run(handle_callback(ctx, cq))

        assert purged == [[300]]                         # 只删 300，未找到的 999 不进 purge

    def test_confirm_refuses_when_rollback_span_grew_past_threshold(self):
        """#2：预览时跨度未越警戒线（无警告），确认前扫描推进 checkpoint 使跨度越线 → 拒绝。"""
        checkpoint = [350]                               # 删 300 → 预览跨度 51 < 100，不警告
        purged = []
        db = self._preview_db([_msg_row(300)], checkpoint=checkpoint)
        db.purge_messages = lambda ids, rollback: (purged.append(ids),
                                                   PurgeSummary(1, [], [], {}))[1]
        pending: PendingStore[PendingDelete] = PendingStore()
        ctx = _ctx(db=db, pending=pending)
        msg = _FakeMessage("/delete 300 rollback")

        asyncio.run(handle_message(ctx, msg))
        assert "⚠" not in msg.sent[0]                    # 预览确实没警告

        checkpoint[0] = 500                              # 确认前又扫一轮：跨度变 500-299=201 > 100
        cq = _FakeCallbackQuery("del:ok", message_id=msg.reply_ids[0])
        asyncio.run(handle_callback(ctx, cq))

        assert purged == []                              # 拒绝，没删
        assert cq.message is not None
        assert "已取消" in cq.message.edited[0] and "重新 /delete" in cq.message.edited[0]

    def test_confirm_proceeds_when_preview_already_warned(self):
        """#2 反面：预览就大跨度回退警告过（知情选择），确认即便跨度更大也照常执行，不误拦。"""
        purged = []
        db = self._preview_db([_msg_row(40)], checkpoint=[15000])
        db.purge_messages = lambda ids, rollback: (purged.append((ids, rollback)),
                                                   PurgeSummary(1, [], [], {}))[1]
        pending: PendingStore[PendingDelete] = PendingStore()
        ctx = _ctx(db=db, pending=pending)
        msg = _FakeMessage("/delete 40 rollback")

        asyncio.run(handle_message(ctx, msg))
        assert "⚠" in msg.sent[0]                        # 预览就警告过大跨度

        cq = _FakeCallbackQuery("del:ok", message_id=msg.reply_ids[0])
        asyncio.run(handle_callback(ctx, cq))

        assert purged == [([40], True)]                  # 知情选择，照常删

    def test_confirm_refuses_when_preview_planned_no_rollback_but_checkpoint_climbed(self):
        """#2 补漏：预览时 checkpoint 低于目标（显示「目标不小于当前值」、无警告），确认前
        checkpoint 被扫描推高，purge 实际会大跨度回退 —— 确认必须拦下。"""
        checkpoint = [254]                               # 低于要删的 261：预览判「不回退」
        purged = []
        db = self._preview_db([_msg_row(261)], checkpoint=checkpoint)
        db.purge_messages = lambda ids, rollback: (purged.append(ids),
                                                   PurgeSummary(1, [], [], {}))[1]
        pending: PendingStore[PendingDelete] = PendingStore()
        ctx = _ctx(db=db, pending=pending)
        msg = _FakeMessage("/delete 261 rollback")

        asyncio.run(handle_message(ctx, msg))
        assert "⚠" not in msg.sent[0]                    # 预览没警告（判定不回退）

        checkpoint[0] = 400                              # 确认前扫描把 checkpoint 推到 400
        cq = _FakeCallbackQuery("del:ok", message_id=msg.reply_ids[0])
        asyncio.run(handle_callback(ctx, cq))

        assert purged == []                              # 实际会回退 400→260（跨度 140>100），拦下
        assert cq.message is not None
        assert "已取消" in cq.message.edited[0] and "重新 /delete" in cq.message.edited[0]


class TestRollbackDrift:
    """确认时回退跨度漂移判定：以确认时实际回退计划为准，对比预览警告过的 chat。"""

    def test_actual_crosses_and_preview_did_not_warn_refuses(self):
        assert bot_commands._rollback_drift((), [("c", 101, 0)]) is not None

    def test_boundary_preview_100_not_warned_then_confirm_101_refuses(self):
        # 预览 span=100（未越线、没警告），确认 span=101 → 拦
        assert bot_commands._rollback_drift((("c", 100, 0),), [("c", 101, 0)]) is not None

    def test_preview_already_warned_proceeds(self):
        # 预览 span=200（已警告），确认 span=300 更大 → 放行（知情选择）
        assert bot_commands._rollback_drift((("c", 200, 0),), [("c", 300, 0)]) is None

    def test_actual_within_threshold_proceeds(self):
        assert bot_commands._rollback_drift((), [("c", 100, 0)]) is None

    def test_no_actual_rollback_proceeds(self):
        assert bot_commands._rollback_drift((), []) is None
