"""设计 A：DB 备份的保留策略与编排测试。"""
import listener


class TestRetention:
    def test_keeps_newest_n_deletes_rest(self):
        """按文件名时间戳排序，只留最近 N 份，其余返回待删。"""
        paths = [
            "/b/archive-20260101-000000.db",
            "/b/archive-20260102-000000.db",
            "/b/archive-20260103-000000.db",
            "/b/archive-20260104-000000.db",
            "/b/archive-20260105-000000.db",
        ]
        to_delete = listener.backups_to_delete(paths, keep=3)
        assert to_delete == [
            "/b/archive-20260101-000000.db",
            "/b/archive-20260102-000000.db",
        ]

    def test_nothing_deleted_when_under_keep(self):
        paths = ["/b/archive-20260101-000000.db", "/b/archive-20260102-000000.db"]
        assert listener.backups_to_delete(paths, keep=7) == []

    def test_input_order_does_not_matter(self):
        """入参顺序打乱，仍按时间戳判定该删哪几份。"""
        paths = [
            "/b/archive-20260103-000000.db",
            "/b/archive-20260101-000000.db",
            "/b/archive-20260102-000000.db",
        ]
        assert listener.backups_to_delete(paths, keep=1) == [
            "/b/archive-20260101-000000.db",
            "/b/archive-20260102-000000.db",
        ]


from datetime import UTC, datetime


class TestSnapshotName:
    def test_filename_is_utc_timestamped(self):
        now = datetime(2026, 9, 19, 3, 4, 5, tzinfo=UTC).timestamp()
        assert listener._backup_filename(now) == "archive-20260919-030405.db"


class TestContextHasArchiveChat:
    def test_context_field_exists(self):
        """ListenerContext 带 archive_chat 字段，备份上传要用。"""
        import inspect
        assert "archive_chat" in inspect.signature(
            listener.ListenerContext).parameters


import asyncio
from types import SimpleNamespace


def _backup_ctx(tmp_path, *, uploaded=None):
    """假 ctx：db.backup_to 真写一个空文件，client.send_document 记录调用。"""
    def backup_to(dest):
        with open(dest, "w", encoding="utf-8") as f:
            f.write("snapshot")

    async def send_document(chat_id, path):
        if uploaded is not None:
            uploaded.append((chat_id, path))
        return SimpleNamespace(id=1)

    return listener.ListenerContext(
        client=SimpleNamespace(send_document=send_document),
        db=SimpleNamespace(backup_to=backup_to),
        pipeline=SimpleNamespace(),
        receive_chat=-1001234567890,
        archive_chat=-1009876543210,
    )


class TestMaybeBackup:
    def test_triggers_when_interval_elapsed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 100)
        monkeypatch.setattr(listener, "DB_BACKUP_UPLOAD", False)
        state = {"last_backup_at": 0.0}

        asyncio.run(listener._maybe_backup(_backup_ctx(tmp_path), now=200.0, state=state))

        assert len(listener._list_backups(str(tmp_path))) == 1
        assert state["last_backup_at"] == 200.0

    def test_skips_when_interval_not_elapsed(self, tmp_path, monkeypatch):
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 100)
        state = {"last_backup_at": 150.0}

        asyncio.run(listener._maybe_backup(_backup_ctx(tmp_path), now=200.0, state=state))

        assert listener._list_backups(str(tmp_path)) == []
        assert state["last_backup_at"] == 150.0

    def test_disabled_when_interval_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 0)
        state = {"last_backup_at": 0.0}

        asyncio.run(listener._maybe_backup(_backup_ctx(tmp_path), now=999999.0, state=state))

        assert listener._list_backups(str(tmp_path)) == []

    def test_upload_only_when_enabled(self, tmp_path, monkeypatch):
        uploaded = []
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 100)
        monkeypatch.setattr(listener, "DB_BACKUP_UPLOAD", True)
        monkeypatch.setattr(listener, "UPLOAD_COOLDOWN_SECONDS", 0)
        state = {"last_backup_at": 0.0}

        asyncio.run(listener._maybe_backup(
            _backup_ctx(tmp_path, uploaded=uploaded), now=200.0, state=state))

        assert len(uploaded) == 1
        assert uploaded[0][0] == -1009876543210

    def test_retention_deletes_oldest(self, tmp_path, monkeypatch):
        """跑到 KEEP+1 份，保留逻辑删掉最旧那份。"""
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 1)
        monkeypatch.setattr(listener, "DB_BACKUP_KEEP", 2)
        monkeypatch.setattr(listener, "DB_BACKUP_UPLOAD", False)
        ctx = _backup_ctx(tmp_path)

        for i, now in enumerate([100.0, 200.0, 300.0]):
            state = {"last_backup_at": now - 10}
            # 命名按秒取整，三次 now 落在不同秒，文件名不撞
            asyncio.run(listener._maybe_backup(ctx, now=now, state=state))

        assert len(listener._list_backups(str(tmp_path))) == 2

    def test_failure_does_not_raise(self, tmp_path, monkeypatch):
        """备份异常只吞掉记 warning，不冒泡阻塞主循环。"""
        monkeypatch.setattr(listener, "BACKUP_DIR", str(tmp_path))
        monkeypatch.setattr(listener, "DB_BACKUP_INTERVAL_SECONDS", 100)

        def boom(dest):
            raise OSError("disk full")

        ctx = listener.ListenerContext(
            client=SimpleNamespace(), db=SimpleNamespace(backup_to=boom),
            pipeline=SimpleNamespace(), receive_chat=-1, archive_chat=-2)
        state = {"last_backup_at": 0.0}

        # 不抛异常即通过
        asyncio.run(listener._maybe_backup(ctx, now=200.0, state=state))
