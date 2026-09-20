"""listener 侧接线：bot token 闸门、扫描轮的锁、_run_backup 返回值。"""
import asyncio
from types import SimpleNamespace

import listener
import pytest


def _ctx():
    return listener.ListenerContext(
        client=SimpleNamespace(), db=SimpleNamespace(), pipeline=SimpleNamespace(),
        receive_chat=-1001234567890, archive_chat=-1009876543210)


class TestBuildBotClient:
    def test_no_token_means_bot_disabled(self, monkeypatch):
        """未设 TG_BOT_TOKEN：不构造 bot Client，main 只跑扫描循环（零回归）。"""
        monkeypatch.setattr(listener, "TG_BOT_TOKEN", "")

        assert listener._build_bot_client() is None

    def test_builds_bot_session_with_token(self, monkeypatch, tmp_path):
        made = {}

        def fake_client(name, **kwargs):
            made["name"] = name
            made.update(kwargs)
            return SimpleNamespace(name=name)

        monkeypatch.setattr(listener, "TG_BOT_TOKEN", "123:abc")
        monkeypatch.setattr(listener, "SESSION_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "HTTP_PROXY", "")
        monkeypatch.setattr(listener, "Client", fake_client)

        client = listener._build_bot_client()

        assert client is not None
        assert made["name"] == "bot"                 # session 文件与 userbot 的 listener 分开
        assert made["bot_token"] == "123:abc"
        assert made["workdir"] == str(tmp_path)


class TestScanLoopLock:
    def test_scan_once_runs_inside_the_lock(self, monkeypatch):
        """扫描轮在锁内跑：bot 写命令不会撞上正在推进的 checkpoint。"""
        monkeypatch.setattr(listener, "SCAN_INTERVAL_SECONDS", 0)

        async def fake_maybe_backup(ctx, now, state):
            return None

        monkeypatch.setattr(listener, "_maybe_backup", fake_maybe_backup)
        monkeypatch.setattr(listener, "_write_heartbeat", lambda now: None)

        lock = asyncio.Lock()
        seen = []

        async def fake_scan_once(_ctx):
            seen.append(lock.locked())
            # 用 CancelledError 而不是普通异常退出无限循环：scan_loop 的兜底
            # except Exception 会把普通异常吞掉并继续下一轮
            raise asyncio.CancelledError

        monkeypatch.setattr(listener, "scan_once", fake_scan_once)

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(listener.scan_loop(_ctx(), lock))

        assert seen == [True], "scan_once 没有在锁内执行"


class TestRunBackupReturnsPath:
    def test_returns_snapshot_path(self, tmp_path, monkeypatch):
        """返回快照路径：bot 的 /backup 要把它回给 admin。"""
        def backup_to(dest):
            with open(dest, "w", encoding="utf-8") as f:
                f.write("snapshot")

        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_KEEP", 7)
        monkeypatch.setattr(listener, "DB_BACKUP_UPLOAD", False)

        ctx = listener.ListenerContext(
            client=SimpleNamespace(), db=SimpleNamespace(backup_to=backup_to),
            pipeline=SimpleNamespace(), receive_chat=-1, archive_chat=-2)

        dest = asyncio.run(listener._run_backup(ctx, now=1000.0))

        assert dest == str(tmp_path / "archive-19700101-001640.db")
        assert (tmp_path / "archive-19700101-001640.db").exists()
