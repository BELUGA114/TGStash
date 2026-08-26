"""
pytest 全局配置：test_reliability.py 会 import listener，
而 listener 模块级读取 TG 环境变量（TG_API_ID 等）。未提供时注入
dummy 值保证测试可独立运行；真实值仅对测试无意义（测试会 monkeypatch
掉 listener 的 app/db）。setdefault 不覆盖调用方显式设置的环境变量。
"""
import os
import tempfile

os.environ.setdefault("TG_API_ID", "12345")
os.environ.setdefault("TG_API_HASH", "testhash")
os.environ.setdefault("RECEIVE_CHAT_ID", "-1001234567890")
os.environ.setdefault("ARCHIVE_CHAT_ID", "-1009876543210")

# listener 模块级会建目录并打开 SQLite。指到临时目录，测试不再污染真实数据目录，
# 也不再依赖 /data 可写（Linux runner 上非 root 无权在 / 下建目录）
if "DATA_DIR" not in os.environ:
    os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="tgstash-test-")
