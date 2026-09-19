"""设计 C：心跳写入与 healthcheck 判定测试。"""
import pytest

import healthcheck
import listener


class TestWriteHeartbeat:
    def test_writes_epoch_seconds_single_line(self, tmp_path, monkeypatch):
        hb = tmp_path / "heartbeat"
        monkeypatch.setattr(listener, "HEARTBEAT_PATH", str(hb))

        listener._write_heartbeat(1_700_000_000.9)

        content = hb.read_text(encoding="utf-8")
        assert content.strip() == "1700000000"   # 取整秒，单行
        assert "\n" not in content.strip()

    def test_failure_does_not_raise(self, tmp_path, monkeypatch):
        """写不进去（目录不存在）只记 warning，不抛。"""
        monkeypatch.setattr(
            listener, "HEARTBEAT_PATH", str(tmp_path / "nope" / "heartbeat"))
        # 不抛异常即通过
        listener._write_heartbeat(1_700_000_000.0)


class TestIsStale:
    def test_fresh_is_not_stale(self):
        assert healthcheck.is_stale(heartbeat_ts=1000.0, now=1100.0, threshold=900) is False

    def test_expired_is_stale(self):
        assert healthcheck.is_stale(heartbeat_ts=1000.0, now=3000.0, threshold=900) is True

    def test_boundary_at_threshold_is_stale(self):
        """正好等于阈值算过期（>= threshold）。"""
        assert healthcheck.is_stale(heartbeat_ts=1000.0, now=1900.0, threshold=900) is True


class TestReadHeartbeat:
    def test_reads_epoch(self, tmp_path):
        hb = tmp_path / "heartbeat"
        hb.write_text("1700000000", encoding="utf-8")
        assert healthcheck.read_heartbeat(str(hb)) == 1700000000.0

    def test_missing_returns_none(self, tmp_path):
        assert healthcheck.read_heartbeat(str(tmp_path / "nope")) is None

    def test_garbage_returns_none(self, tmp_path):
        hb = tmp_path / "heartbeat"
        hb.write_text("not-a-number", encoding="utf-8")
        assert healthcheck.read_heartbeat(str(hb)) is None


class TestMainExitCode:
    def test_fresh_exits_zero(self, tmp_path, monkeypatch):
        hb = tmp_path / "heartbeat"
        import time
        hb.write_text(str(int(time.time())), encoding="utf-8")
        monkeypatch.setenv("SCAN_INTERVAL_SECONDS", "300")
        monkeypatch.setattr(healthcheck, "HEARTBEAT_PATH", str(hb))

        with pytest.raises(SystemExit) as exc:
            healthcheck.main()
        assert exc.value.code == 0

    def test_expired_exits_nonzero(self, tmp_path, monkeypatch):
        hb = tmp_path / "heartbeat"
        hb.write_text("1000", encoding="utf-8")   # 远古时间戳
        monkeypatch.setenv("SCAN_INTERVAL_SECONDS", "300")
        monkeypatch.setattr(healthcheck, "HEARTBEAT_PATH", str(hb))

        with pytest.raises(SystemExit) as exc:
            healthcheck.main()
        assert exc.value.code == 1

    def test_missing_exits_nonzero(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SCAN_INTERVAL_SECONDS", "300")
        monkeypatch.setattr(healthcheck, "HEARTBEAT_PATH", str(tmp_path / "nope"))

        with pytest.raises(SystemExit) as exc:
            healthcheck.main()
        assert exc.value.code == 1
