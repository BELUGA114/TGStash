"""设计 B：retry_skipped 脚本编排测试。"""
import retry_skipped
from db import ArchiveDB

CHAT = "-1001234567890"


def _seed(tmp_path):
    """库里：checkpoint=200；入口 100/105 两条 skipped，110 一条 retrying。"""
    path = str(tmp_path / "archive.db")
    db = ArchiveDB(path)
    db.ensure_channel(CHAT, "manual_forward")
    db.set_checkpoint(CHAT, 200)
    db.increment_failure(CHAT, 100, "download", "代理断")
    db.mark_failure_skipped(CHAT, 100, "重试 3 次仍失败: download")
    db.increment_failure(CHAT, 105, "upload", "flood")
    db.mark_failure_skipped(CHAT, 105, "重试 3 次仍失败: upload")
    db.increment_failure(CHAT, 110, "verify", "临时")
    return path, db


def test_dry_run_writes_nothing(tmp_path, monkeypatch):
    path, db = _seed(tmp_path)
    monkeypatch.setattr("sys.argv", ["retry_skipped.py", "--db", path, "--dry-run"])

    retry_skipped.main()

    # skipped 仍是 skipped，checkpoint 没动
    assert db.get_failure(CHAT, 100)["status"] == "skipped"
    assert db.get_checkpoint(CHAT) == 200


def test_reset_all_rolls_back_checkpoint(tmp_path, monkeypatch):
    path, db = _seed(tmp_path)
    monkeypatch.setattr("sys.argv", ["retry_skipped.py", "--db", path])

    retry_skipped.main()

    # 两条 skipped 变回 retrying、attempts 清零
    assert db.get_failure(CHAT, 100)["status"] == "retrying"
    assert db.get_failure(CHAT, 100)["attempt_count"] == 0
    # checkpoint 回退到 min(100,105) - 1 = 99
    assert db.get_checkpoint(CHAT) == 99


def test_reset_specific_ids(tmp_path, monkeypatch):
    path, db = _seed(tmp_path)
    monkeypatch.setattr("sys.argv", ["retry_skipped.py", "105", "--db", path])

    retry_skipped.main()

    assert db.get_failure(CHAT, 100)["status"] == "skipped"      # 没选中
    assert db.get_failure(CHAT, 105)["status"] == "retrying"
    assert db.get_checkpoint(CHAT) == 104                         # 105 - 1


def test_no_skipped_leaves_checkpoint(tmp_path, monkeypatch):
    """无 skipped 行时不动 checkpoint。"""
    path = str(tmp_path / "archive.db")
    db = ArchiveDB(path)
    db.ensure_channel(CHAT, "manual_forward")
    db.set_checkpoint(CHAT, 200)
    db.increment_failure(CHAT, 110, "verify", "临时")  # 只有 retrying
    monkeypatch.setattr("sys.argv", ["retry_skipped.py", "--db", path])

    retry_skipped.main()

    assert db.get_checkpoint(CHAT) == 200


def test_checkpoint_only_moves_back(tmp_path, monkeypatch):
    """min_id - 1 不小于当前 checkpoint 时不动（只退不进）。"""
    path = str(tmp_path / "archive.db")
    db = ArchiveDB(path)
    db.ensure_channel(CHAT, "manual_forward")
    db.set_checkpoint(CHAT, 50)               # 当前 checkpoint 已经很小
    db.increment_failure(CHAT, 100, "download", "x")
    db.mark_failure_skipped(CHAT, 100, "skip")
    monkeypatch.setattr("sys.argv", ["retry_skipped.py", "--db", path])

    retry_skipped.main()

    # 100-1=99 > 50，不该把 checkpoint 前进到 99
    assert db.get_checkpoint(CHAT) == 50
    # 但行仍被重置回 retrying
    assert db.get_failure(CHAT, 100)["status"] == "retrying"
