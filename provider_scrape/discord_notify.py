"""Discord incoming-webhook notifications.

``send_discord_messages`` is the shared transport (also used by
``scripts/upload_to_huggingface.py``). ``DiscordProgressExtension`` posts crawl
progress for long-running spiders: a message when the spider opens, one every
``DISCORD_PROGRESS_ITEMS`` scraped items, and one when it closes.

The extension is registered project-wide but is a no-op unless a spider sets
``DISCORD_PROGRESS_ENABLED`` (e.g. maryland) and ``discord.env`` holds a
``webhook_url``. Sends run in a worker thread so a slow or unreachable Discord
never blocks the crawl, and a failed send is logged, never raised.
"""

import json
import logging
import os
import re
import time
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from scrapy import signals
from scrapy.exceptions import NotConfigured
from twisted.internet.threads import deferToThread

from provider_scrape.proxy_pool import load_env_file

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DISCORD_ENV_FILE = os.path.join(REPO_ROOT, "discord.env")
WEBHOOK_KEY = "webhook_url"
DISCORD_CONTENT_LIMIT = 2000
DISCORD_USER_AGENT = "Mozilla/5.0 (compatible; ChildCareProviderScraper/1.0)"

logger = logging.getLogger(__name__)


def load_webhook_url(env_file=DEFAULT_DISCORD_ENV_FILE):
    """Return the ``webhook_url`` from a ``key=value`` env file, or None."""
    return load_env_file(env_file).get(WEBHOOK_KEY) or None


def send_discord_messages(webhook_url, messages):
    """Send confirmed Discord messages, never including the secret in errors."""
    try:
        parts = urlsplit(webhook_url)
        valid_host = parts.hostname in {"discord.com", "discordapp.com"} and parts.port is None
    except ValueError:
        raise ValueError("discord.env has no valid Discord incoming webhook URL") from None
    if (
        parts.scheme != "https"
        or not valid_host
        or parts.username is not None
        or not re.fullmatch(r"/api(?:/v\d+)?/webhooks/\d+/[^/]+", parts.path)
    ):
        raise ValueError("discord.env has no valid Discord incoming webhook URL")
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query) if key != "wait"] + [("wait", "true")])
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))
    for content in messages:
        payload = json.dumps({"content": content, "allowed_mentions": {"parse": []}}).encode("utf-8")
        # Discord's Cloudflare layer rejects urllib's default Python-urllib UA (403 / 1010).
        request = Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": DISCORD_USER_AGENT},
            method="POST",
        )
        for attempt in range(3):
            try:
                with urlopen(request, timeout=10) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Discord returned HTTP {response.status}")
                break
            except HTTPError as error:
                if error.code == 429 and attempt < 2:
                    try:
                        delay = float((error.headers or {}).get("Retry-After", ""))
                    except (TypeError, ValueError):
                        delay = 0
                    if 0 < delay <= 5:
                        time.sleep(delay)
                        continue
                raise RuntimeError(f"Discord returned HTTP {error.code}") from None
            except (URLError, OSError):
                raise RuntimeError("Could not connect to Discord") from None


def _format_elapsed(seconds):
    minutes, _ = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h {minutes}m" if days else f"{hours}h {minutes}m"


class DiscordProgressExtension:
    """Post start / every-N-items / finish messages for an opted-in spider.

    Settings:
        ``DISCORD_PROGRESS_ENABLED``: opt in (default False).
        ``DISCORD_PROGRESS_ITEMS``: items between progress messages (default 1000).
        ``DISCORD_ENV_FILE``: env file holding ``webhook_url`` (default repo-root
        ``discord.env``).
    """

    def __init__(self, crawler, webhook_url, every):
        self.crawler = crawler
        self.webhook_url = webhook_url
        self.every = every
        self.items = 0
        self.started = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool("DISCORD_PROGRESS_ENABLED"):
            raise NotConfigured
        env_file = settings.get("DISCORD_ENV_FILE") or DEFAULT_DISCORD_ENV_FILE
        webhook_url = load_webhook_url(env_file)
        if not webhook_url:
            logger.info("No Discord webhook URL configured in %s; skipping Discord progress messages.", env_file)
            raise NotConfigured
        ext = cls(crawler, webhook_url, max(1, settings.getint("DISCORD_PROGRESS_ITEMS", 1000)))
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def _send(self, content):
        """Post in a worker thread; returns the Deferred (failures only logged)."""
        d = deferToThread(send_discord_messages, self.webhook_url, [content])
        d.addErrback(lambda failure: logger.warning("Discord progress message not sent: %s", failure.getErrorMessage()))
        return d

    def spider_opened(self, spider):
        self.started = time.monotonic()
        self._send(f"**{spider.name}**: crawl started ({datetime.now(UTC):%Y-%m-%d %H:%M UTC}).")

    def item_scraped(self, item, spider):
        self.items += 1
        if self.items % self.every == 0:
            self._send(f"**{spider.name}**: {self.items:,} items scraped ({self._elapsed()} elapsed).")

    def spider_closed(self, spider, reason):
        errors = self.crawler.stats.get_value("log_count/ERROR", 0) or 0
        content = (
            f"**{spider.name}**: crawl closed (reason: {reason}). "
            f"{self.items:,} items in {self._elapsed()}; {errors:,} ERROR log line(s)."
        )
        # Returned so Scrapy waits for the final message before shutting down.
        return self._send(content)

    def _elapsed(self):
        return _format_elapsed(time.monotonic() - self.started) if self.started is not None else "0h 0m"
