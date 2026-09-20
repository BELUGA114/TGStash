"""bot_commands：权限、命令解析、白名单与 /stats。"""
import asyncio
from types import SimpleNamespace

import bot_commands
import pytest
from bot_commands import BotContext, handle_message, parse_admin_ids, parse_command
from db import RetrySummary, Stats

ADMIN = 111


class _FakeMessage:
    """假消息：reply_text 把回复记进 sent。user_id=None 模拟频道匿名（from_user 为空）。"""

    def __init__(self, text, user_id=ADMIN):
        self.text = text
        self.from_user = None if user_id is None else SimpleNamespace(id=user_id)
        self.sent = []

    async def reply_text(self, text, **kwargs):
        self.sent.append(text)
        return SimpleNamespace(id=1)


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


def _ctx(*, db=None, lock=None, admin_ids=frozenset({ADMIN}), run_backup=None):
    async def default_backup():
        return "/data/db/backups/archive-20260920-030405.db"

    return BotContext(
        db=db if db is not None else SimpleNamespace(),
        lock=lock if lock is not None else _RecordingLock(),
        admin_ids=admin_ids,
        run_backup=run_backup or default_backup,
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
        """注册进去的回调真的走到 handle_message —— 光断言「挂上了」会漏掉闭包接错。"""
        registered = []

        def on_message():
            def deco(fn):
                registered.append(fn)
                return fn
            return deco

        ctx = _ctx(db=SimpleNamespace(stats=_stats))

        bot_commands.register_handlers(SimpleNamespace(on_message=on_message), ctx)

        assert len(registered) == 1
        msg = _FakeMessage("/stats")
        asyncio.run(registered[0](SimpleNamespace(), msg))
        assert "文件总数：3" in msg.sent[0]


class TestCommandMenu:
    def test_registers_all_commands_in_order(self):
        sent = []

        async def set_bot_commands(commands):
            sent.extend(commands)
            return True

        asyncio.run(bot_commands.register_command_menu(
            SimpleNamespace(set_bot_commands=set_bot_commands)))

        assert [c.command for c in sent] == ["stats", "search", "failures", "retry", "backup"]
        assert all(c.description for c in sent)

    def test_menu_failure_is_not_fatal(self):
        """菜单注册失败（如 flood）只 warning，命令本身照常可用。"""
        async def boom(commands):
            raise OSError("flood")

        asyncio.run(bot_commands.register_command_menu(
            SimpleNamespace(set_bot_commands=boom)))
