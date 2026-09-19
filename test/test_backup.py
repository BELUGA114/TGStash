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
