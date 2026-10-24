import logging

import pytest
from scrapy import Spider
from scrapy.exceptions import NotConfigured
from scrapy.statscollectors import StatsCollector
from scrapy.utils.test import get_crawler
from twisted.internet import defer

from provider_scrape import discord_notify
from provider_scrape.discord_notify import DiscordProgressExtension

WEBHOOK = "https://discord.com/api/webhooks/123/secret"


@pytest.fixture
def sent(monkeypatch):
    """Run sends synchronously and capture each message's content."""
    messages = []

    def fake_defer_to_thread(func, url, contents):
        try:
            func(url, contents)
        except Exception as exc:
            return defer.fail(exc)
        return defer.succeed(None)

    monkeypatch.setattr(discord_notify, "deferToThread", fake_defer_to_thread)
    monkeypatch.setattr(discord_notify, "send_discord_messages", lambda url, contents: messages.extend(contents))
    return messages


def _ext(tmp_path, webhook=WEBHOOK, **settings):
    env = tmp_path / "discord.env"
    env.write_text(f"webhook_url={webhook}\n" if webhook is not None else "", encoding="utf-8")
    crawler = get_crawler(
        Spider,
        {"DISCORD_PROGRESS_ENABLED": True, "DISCORD_ENV_FILE": str(env), **settings},
    )
    crawler.stats = StatsCollector(crawler)
    return DiscordProgressExtension.from_crawler(crawler), Spider(name="maryland")


def test_disabled_by_default(tmp_path):
    env = tmp_path / "discord.env"
    env.write_text(f"webhook_url={WEBHOOK}\n", encoding="utf-8")
    crawler = get_crawler(Spider, {"DISCORD_ENV_FILE": str(env)})
    with pytest.raises(NotConfigured):
        DiscordProgressExtension.from_crawler(crawler)


@pytest.mark.parametrize("webhook", [None, ""])
def test_missing_webhook_disables_extension(tmp_path, webhook, caplog):
    with caplog.at_level(logging.INFO), pytest.raises(NotConfigured):
        _ext(tmp_path, webhook=webhook)
    assert "No Discord webhook URL configured" in caplog.text


def test_start_progress_and_finish_messages(tmp_path, sent):
    ext, spider = _ext(tmp_path, DISCORD_PROGRESS_ITEMS=1000)

    ext.spider_opened(spider)
    for _ in range(2500):
        ext.item_scraped({}, spider)
    ext.crawler.stats.set_value("log_count/ERROR", 3)
    ext.spider_closed(spider, "finished")

    assert len(sent) == 4
    assert sent[0].startswith("**maryland**: crawl started")
    assert sent[1].startswith("**maryland**: 1,000 items scraped")
    assert sent[2].startswith("**maryland**: 2,000 items scraped")
    assert "crawl closed (reason: finished)" in sent[3]
    assert "2,500 items" in sent[3]
    assert "3 ERROR log line(s)" in sent[3]


def test_progress_interval_is_configurable(tmp_path, sent):
    ext, spider = _ext(tmp_path, DISCORD_PROGRESS_ITEMS=2)
    for _ in range(5):
        ext.item_scraped({}, spider)
    assert [m.split(": ")[1] for m in sent] == ["2 items scraped (0h 0m elapsed).", "4 items scraped (0h 0m elapsed)."]


def test_close_returns_deferred_so_scrapy_waits(tmp_path, sent):
    ext, spider = _ext(tmp_path)
    assert isinstance(ext.spider_closed(spider, "finished"), defer.Deferred)


def test_send_failure_is_logged_not_raised(tmp_path, monkeypatch, caplog):
    def fail(_url, _contents):
        raise RuntimeError("Discord returned HTTP 403")

    monkeypatch.setattr(discord_notify, "deferToThread", lambda f, *a: defer.maybeDeferred(f, *a))
    monkeypatch.setattr(discord_notify, "send_discord_messages", fail)
    ext, spider = _ext(tmp_path)
    with caplog.at_level(logging.WARNING):
        ext.spider_opened(spider)
        results = []
        ext.spider_closed(spider, "shutdown").addBoth(results.append)
    assert results == [None]  # errback consumed the failure
    assert "Discord progress message not sent: Discord returned HTTP 403" in caplog.text
    assert "secret" not in caplog.text


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(0, "0h 0m"), (3_725, "1h 2m"), (90_061, "1d 1h 1m"), (4 * 86_400 + 7_200, "4d 2h 0m")],
)
def test_format_elapsed(seconds, expected):
    assert discord_notify._format_elapsed(seconds) == expected


def test_maryland_opts_in():
    from provider_scrape.spiders.maryland import MarylandSpider

    assert MarylandSpider.custom_settings["DISCORD_PROGRESS_ENABLED"] is True
