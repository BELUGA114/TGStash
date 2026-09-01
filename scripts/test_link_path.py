"""
路径二（t.me 链接）的测试：链接提取纯函数 + process_link_message 的编排分支。

假 client（get_messages / get_media_group / edit_message_text）+ 假 pipeline
就能整条跑 —— 不连 Telegram、不碰 DB、不读环境变量。
"""

import listener


class TestExtractTmeLinks:
    def test_no_link_returns_empty(self):
        assert listener.extract_tme_links("今天天气不错，没有链接") == []

    def test_empty_text_returns_empty(self):
        assert listener.extract_tme_links("") == []

    def test_plain_link(self):
        assert listener.extract_tme_links(
            "https://t.me/c/123456/78") == ["https://t.me/c/123456/78"]

    def test_strips_trailing_ascii_punctuation(self):
        """尾随的半角句读不是链接的一部分"""
        for punct in (".", ",", ";", ":", "!", "?", ")"):
            assert listener.extract_tme_links(
                f"看这个 https://t.me/c/123456/78{punct}") == ["https://t.me/c/123456/78"]

    def test_markdown_wrapped_link(self):
        assert listener.extract_tme_links(
            "见 [这条](https://t.me/somechannel/12)") == ["https://t.me/somechannel/12"]

    def test_duplicate_links_collapse_and_keep_order(self):
        text = ("https://t.me/a/1 中间 https://t.me/b/2 又是 https://t.me/a/1")
        assert listener.extract_tme_links(text) == ["https://t.me/a/1", "https://t.me/b/2"]

    def test_http_and_https_both_match(self):
        assert listener.extract_tme_links("http://t.me/a/1 https://t.me/b/2") == [
            "http://t.me/a/1", "https://t.me/b/2"]

    def test_query_string_is_kept_for_parse_to_strip(self):
        """?single 留给 parse_message_link 去掉 —— 提取阶段不该猜它的语义"""
        assert listener.extract_tme_links(
            "https://t.me/c/1/2?single") == ["https://t.me/c/1/2?single"]
