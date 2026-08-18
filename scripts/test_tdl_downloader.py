"""
tdl_downloader.py 单元测试
使用注入的假 runner，不依赖 Go SDK、不连接 Telegram。
"""
import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stash-listener"))
from tdl_downloader import TDLDownloader, make_tme_link  # noqa: E402


def _msg(message_id, chat_id=-1001234567890):
    return SimpleNamespace(id=message_id, chat=SimpleNamespace(id=chat_id))


def _write(path, content=b"x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    return path


async def _fallback(message, path):
    return _write(path)


def _capture_runner(tmp_path, files=None):
    calls = []

    async def runner(cmd):
        calls.append(cmd)
        dest = cmd[cmd.index("-d") + 1]
        for rel in files or []:
            _write(os.path.join(dest, rel))

    return calls, runner


def test_make_tme_link_channel():
    assert make_tme_link(-1001234567890, 42) == "https://t.me/c/1234567890/42"


def test_make_tme_link_public():
    assert make_tme_link(12345, 7) == "https://t.me/c/12345/7"


def test_download_success_maps_file(tmp_path):
    calls, runner = _capture_runner(tmp_path, ["1234567890_42_photo.jpg"])
    dl = TDLDownloader(namespace="archiver", threads=4, limit=2, runner=runner)
    msg = _msg(42)

    paths = asyncio.run(dl.download([msg], str(tmp_path), _fallback))

    assert paths[42].endswith("42_photo.jpg")
    cmd = calls[0]
    assert cmd[1:3] == ["-n", "archiver"]
    assert "--threads" in cmd and "4" in cmd
    assert "--limit" in cmd and "2" in cmd
    assert "--restart" in cmd


def test_missing_file_falls_back(tmp_path):
    calls, runner = _capture_runner(tmp_path, [])
    fallback_calls = []

    async def fallback(message, path):
        fallback_calls.append((message.id, path))
        return _write(path)

    dl = TDLDownloader(runner=runner)
    msg = _msg(42)

    paths = asyncio.run(dl.download([msg], str(tmp_path), fallback))

    assert paths[42] == os.path.join(str(tmp_path), "42_fallback")
    assert fallback_calls == [(42, os.path.join(str(tmp_path), "42_fallback"))]


def test_runner_failure_falls_back(tmp_path):
    async def runner(cmd):
        raise RuntimeError("tdl failed")

    dl = TDLDownloader(runner=runner)
    msg = _msg(42)

    paths = asyncio.run(dl.download([msg], str(tmp_path), _fallback))

    assert paths[42].endswith("42_fallback")


def test_fallback_failure_returns_empty(tmp_path):
    async def runner(cmd):
        raise RuntimeError("tdl failed")

    async def fallback(message, path):
        return None

    dl = TDLDownloader(runner=runner)
    msg = _msg(42)

    paths = asyncio.run(dl.download([msg], str(tmp_path), fallback))

    assert paths == {}


def test_group_uses_single_link_and_group_flag(tmp_path):
    files = [
        "1234567890_41_41_photo.jpg",
        "1234567890_42_42_photo.jpg",
    ]
    calls, runner = _capture_runner(tmp_path, files)
    dl = TDLDownloader(runner=runner)
    msgs = [_msg(41), _msg(42)]
    links = {42: "https://t.me/some_channel/42"}

    paths = asyncio.run(dl.download(msgs, str(tmp_path), _fallback, links=links))

    assert set(paths) == {41, 42}
    cmd = calls[0]
    assert "--group" in cmd
    u_idx = cmd.index("-u")
    assert cmd[u_idx + 1:u_idx + 3] == ["https://t.me/some_channel/42", "-d"]


def test_proxy_threads_limit_template_args(tmp_path):
    async def runner(cmd):
        assert "--proxy" in cmd and "http://127.0.0.1:7890" in cmd
        assert "--threads" in cmd and "8" in cmd
        assert "--limit" in cmd and "3" in cmd
        assert "{{.DialogID}}_{{.MessageID}}_{{filenamify .FileName}}" in cmd

    dl = TDLDownloader(
        namespace="archiver",
        threads=8,
        limit=3,
        proxy="http://127.0.0.1:7890",
        runner=runner,
    )
    msg = _msg(42)

    paths = asyncio.run(dl.download([msg], str(tmp_path), _fallback))

    assert paths[42].endswith("42_fallback")


def test_fallback_paths_used(tmp_path):
    async def runner(cmd):
        pass

    expected = os.path.join(str(tmp_path), "original_name.jpg")

    async def fallback(message, path):
        assert path == expected
        return _write(path)

    dl = TDLDownloader(runner=runner)
    msg = _msg(42)

    paths = asyncio.run(dl.download(
        [msg],
        str(tmp_path),
        fallback,
        fallback_paths={42: expected},
    ))

    assert paths[42] == expected
