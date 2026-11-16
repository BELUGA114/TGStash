from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from tgstash.runtime.backups import (
    BackupConfig,
    BackupService,
    backup_filename,
    backups_to_delete,
    list_backups,
)


class TestRetention:
    def test_keeps_newest_n_deletes_rest(self):
        paths = [f"/b/archive-2026010{i}-000000.db" for i in range(1, 6)]
        assert backups_to_delete(paths, keep=3) == paths[:2]

    def test_nothing_deleted_when_under_keep(self):
        paths = ["/b/archive-20260101-000000.db", "/b/archive-20260102-000000.db"]
        assert backups_to_delete(paths, keep=7) == []

    def test_input_order_does_not_matter(self):
        paths = ["/b/archive-20260103-000000.db", "/b/archive-20260101-000000.db",
                 "/b/archive-20260102-000000.db"]
        assert backups_to_delete(paths, keep=1) == sorted(paths)[:2]


def test_filename_is_utc_timestamped():
    now = datetime(2026, 9, 19, 3, 4, 5, tzinfo=UTC).timestamp()
    assert backup_filename(now) == "archive-20260919-030405.db"


def _service(tmp_path, *, interval=100, keep=7, upload=False, uploaded=None, backup_to=None):
    def snapshot(dest):
        Path(dest).write_text("snapshot", encoding="utf-8")

    async def send_document(chat_id, path):
        if uploaded is not None:
            uploaded.append((chat_id, path))
        return SimpleNamespace(id=1)

    return BackupService(
        SimpleNamespace(backup_to=backup_to or snapshot),
        SimpleNamespace(send_document=send_document), -1009876543210,
        BackupConfig(str(tmp_path), interval, keep, upload), upload_cooldown_seconds=0)


def test_returns_snapshot_path(tmp_path):
    dest = asyncio.run(_service(tmp_path).run(now=1000.0))
    assert dest == str(tmp_path / "archive-19700101-001640.db")
    assert Path(dest).read_text(encoding="utf-8") == "snapshot"


class TestMaybeBackup:
    def test_triggers_when_interval_elapsed(self, tmp_path):
        last = asyncio.run(_service(tmp_path).maybe_run(now=200.0, last_backup_at=0.0))
        assert len(list_backups(str(tmp_path))) == 1
        assert last == 200.0

    def test_skips_when_interval_not_elapsed(self, tmp_path):
        last = asyncio.run(_service(tmp_path).maybe_run(now=200.0, last_backup_at=150.0))
        assert list_backups(str(tmp_path)) == []
        assert last == 150.0

    def test_disabled_when_interval_zero(self, tmp_path):
        last = asyncio.run(_service(tmp_path, interval=0).maybe_run(now=999999.0, last_backup_at=0.0))
        assert list_backups(str(tmp_path)) == []
        assert last == 0.0

    def test_upload_only_when_enabled(self, tmp_path):
        uploaded = []
        asyncio.run(_service(tmp_path, upload=True, uploaded=uploaded).maybe_run(200.0, 0.0))
        assert len(uploaded) == 1
        assert uploaded[0][0] == -1009876543210

    def test_retention_deletes_oldest(self, tmp_path):
        service = _service(tmp_path, interval=1, keep=2)
        for now in [100.0, 200.0, 300.0]:
            asyncio.run(service.maybe_run(now, now - 10))
        assert sorted(list_backups(str(tmp_path))) == [
            str(tmp_path / backup_filename(now)) for now in [200.0, 300.0]]

    def test_failure_waits_full_interval_before_retrying(self, tmp_path, caplog):
        calls = []

        def boom(dest):
            calls.append(dest)
            raise OSError("disk full")

        service = _service(tmp_path, backup_to=boom)
        last = asyncio.run(service.maybe_run(200.0, 0.0))
        assert last == 200.0
        assert asyncio.run(service.maybe_run(250.0, last)) == last
        assert len(calls) == 1
        assert "DB 备份失败" in caplog.text
