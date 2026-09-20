"""bot_commands：权限、命令解析、白名单与 /stats。"""
import asyncio
from types import SimpleNamespace

import pytest
from bot_commands import BotContext, handle_message, parse_admin_ids, parse_command
from db import Stats

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
