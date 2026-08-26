"""镜像冒烟：验证 COPY 清单完整、模块可导入、外部二进制齐备。

用法（CI 里 bind mount 进容器，不进镜像）：
    docker run --rm -e EXPECTED_MODULES=... \
      -v $PWD/.github/smoke.py:/smoke.py:ro img python /smoke.py
"""
import importlib
import importlib.util
import os
import pathlib
import sys

APP = pathlib.Path("/app")
sys.path.insert(0, str(APP))  # `python /smoke.py` 的 sys.path[0] 是 /，不是 /app

# 1) 宿主机 stash-listener/*.py 必须全部落进镜像 /app。
#    直接盯死历史上出过两次的 COPY 漏文件；结构化比对，改成通配 COPY 后依然有效
expected = [n for n in os.environ["EXPECTED_MODULES"].split(",") if n]
assert expected, "EXPECTED_MODULES 为空，冒烟脚本没拿到清单"
missing = sorted(n for n in expected if not (APP / n).is_file())
assert not missing, f"镜像里缺少模块（Dockerfile COPY 漏了）：{missing}"

# 2) 真正 import 一遍，模块级代码必须跑通。
#    login.py 例外：模块级 `with app:` 会真连 Telegram 并交互式要手机号
NO_IMPORT = {"login.py"}
for name in expected:
    module = name[:-3]
    if name in NO_IMPORT:
        assert importlib.util.find_spec(module) is not None, f"{name} 在镜像里找不到"
        continue
    importlib.import_module(module)

# 3) 运行时第三方依赖真的装进了 /usr/local（COPY --from=builder /install）
import pyrogram  # noqa: E402,F401
from PIL import Image  # noqa: E402,F401

assert importlib.util.find_spec("tgcrypto") is not None, "TgCrypto 未装进镜像"

# 4) scripts/ 下的辅助脚本（COPY scripts/ ./scripts/）
sys.path.insert(0, str(APP / "scripts"))
importlib.import_module("get_chat_ids")

print("smoke OK:", ", ".join(expected))
