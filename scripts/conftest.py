"""
pytest 全局配置：test_reliability.py 会 import listener，
而 listener 模块级读取 TG 环境变量（TG_API_ID 等）。未提供时注入
dummy 值保证测试可独立运行；真实值仅对测试无意义（测试会 monkeypatch
掉 listener 的 app/db）。setdefault 不覆盖调用方显式设置的环境变量。
"""
import os

os.environ.setdefault("TG_API_ID", "12345")
os.environ.setdefault("TG_API_HASH", "testhash")
os.environ.setdefault("RECEIVE_CHAT_ID", "-1001234567890")
os.environ.setdefault("ARCHIVE_CHAT_ID", "-1009876543210")
