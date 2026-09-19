"""设计 C：心跳写入与 healthcheck 判定测试。"""
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
