"""递归对照宿主机模块清单，防止镜像漏复制入口或包内模块。"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import sys
from pathlib import Path


def source_modules() -> list[str]:
    source = Path(__file__).resolve().parent.parent / "stash-listener"
    return sorted(path.relative_to(source).as_posix() for path in source.rglob("*.py")
                  if "__pycache__" not in path.parts)


def verify(app: Path, expected: list[str]) -> None:
    assert expected, "EXPECTED_MODULES 为空，冒烟脚本没拿到清单"
    missing = sorted(name for name in expected if not (app / name).is_file())
    assert not missing, f"镜像里缺少模块（Dockerfile COPY 漏了）：{missing}"
    sys.path.insert(0, str(app))

    for name in expected:
        parts = Path(name).with_suffix("").parts
        module = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
        # 登录入口会连接 Telegram 并交互，只验证能够定位到文件。
        if name == "login.py":
            assert importlib.util.find_spec(module) is not None, f"{name} 在镜像里找不到"
        else:
            importlib.import_module(module)

    importlib.import_module("pyrogram")
    importlib.import_module("PIL.Image")
    assert importlib.util.find_spec("tgcrypto") is not None, "TgCrypto 未装进镜像"

    sys.path.insert(0, str(app / "scripts"))
    for helper in ("get_chat_ids", "delete_message", "backfill_metadata", "retry_skipped", "healthcheck"):
        importlib.import_module(helper)
    print("smoke OK:", ", ".join(expected))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--app", type=Path, default=Path("/app"))
    args = parser.parse_args()
    if args.list:
        print(",".join(source_modules()))
        return
    verify(args.app, [name for name in os.environ["EXPECTED_MODULES"].split(",") if name])


if __name__ == "__main__":
    main()
