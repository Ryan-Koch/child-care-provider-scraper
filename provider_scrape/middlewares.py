# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from urllib.parse import urlparse

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class VaScrapeSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class VaScrapeDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ProxyPoolMiddleware:
    """Assign a per-proxy egress + download slot to rate-limited requests.

    Opt-in and spider-driven: the middleware is a pass-through unless the spider
    exposes a truthy ``proxy_pool`` (a :class:`provider_scrape.proxy_pool.ProxyPool`).
    For a request whose host matches ``spider.proxy_pool_domains`` it sets both
    ``request.meta['proxy']`` (so the built-in ``HttpProxyMiddleware`` tunnels it)
    and ``request.meta['download_slot'] = <proxy_id>`` — the latter gives each
    proxy its **own** download slot, so Scrapy's ``DOWNLOAD_DELAY`` /
    per-domain-concurrency (the single-IP cadence) is enforced independently per
    IP. N proxies ⇒ ~N parallel single-flight slots.

    Assignment respects a request-set affinity: requests carrying
    ``meta['proxy_affinity']`` (e.g. a county's ViewState-bound pagination chain)
    stick to one proxy; the rest (session-independent detail/PDF GETs) rotate
    round-robin. Requests to other hosts (e.g. the EXCELS API) are left direct on
    their own slot. An already-assigned proxy (a retry) is never reassigned, so a
    request rides out its slot's rate-limit cooldown on the same IP.

    Must run **before** ``HttpProxyMiddleware`` (default order 750) so the proxy
    it sets is honored; register it at a lower order (see settings).
    """

    def __init__(self):
        import logging

        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @staticmethod
    def _host(request):
        return (urlparse(request.url).hostname or "").lower()

    def _should_proxy(self, request, spider, pool):
        # proxy_bypass lets a request egress direct from the host IP even on a
        # pooled domain — e.g. Maryland's ~1MB inspection PDFs, which are public
        # (no session needed) and would otherwise burn metered proxy bandwidth.
        if not pool or request.meta.get("proxy") or request.meta.get("proxy_bypass"):
            return False
        domains = getattr(spider, "proxy_pool_domains", None)
        if not domains:
            return False
        host = self._host(request)
        return any(d.lower() in host for d in domains)

    def process_request(self, request, spider):
        pool = getattr(spider, "proxy_pool", None)
        if not self._should_proxy(request, spider, pool):
            return None
        affinity = request.meta.get("proxy_affinity")
        if affinity is not None:
            proxy_id, proxy_url = pool.for_key(affinity)
        else:
            proxy_id, proxy_url = pool.next_rotating()
        request.meta["proxy"] = proxy_url
        # Give each proxy its own download slot so the per-IP delay/concurrency
        # is enforced independently — this is what buys the parallel throughput.
        request.meta["download_slot"] = proxy_id
        self.logger.debug(
            "Proxy %s -> %s%s",
            proxy_id,
            request.url,
            f" (affinity={affinity})" if affinity is not None else "",
        )
        return None


class RateLimitBackoffMiddleware:
    """Cooldown-and-retry for per-IP rate-limit blocks (hard ``403`` s).

    Some licensing sites enforce a per-IP request-rate ceiling at the web-server
    layer and return a hard ``403`` once it's exceeded — e.g. Maryland's
    IIS-hosted ``checkccmd.org`` with Dynamic IP Restrictions (measured: ~2
    requests per ~20s window, site-wide per IP). Two properties make the naive
    fixes wrong:

    * ``403`` is not in Scrapy's default ``RETRY_HTTP_CODES``, so a blocked
      request is silently dropped instead of retried (a Maryland run shed ~50%
      of providers this way).
    * The block is a rolling window that only clears after a stretch of silence;
      an *immediate* retry re-trips it (below a threshold spacing the block is
      self-sustaining — every blocked request keeps the window saturated). So
      the retry has to back off the whole download slot, not just the one
      request.

    On a configured status code from a configured domain this middleware raises
    that request's download-slot delay to ``RATELIMIT_BACKOFF_COOLDOWN`` seconds
    — pausing *all* traffic to that host so the window can drain — then
    re-schedules the request. Bounded by ``RATELIMIT_BACKOFF_MAX_RETRIES``.

    Disabled by default; a spider opts in via settings, so spiders that don't
    set ``RATELIMIT_BACKOFF_ENABLED`` are unaffected (the middleware is a
    pass-through). Settings:

    * ``RATELIMIT_BACKOFF_ENABLED`` (bool, default False)
    * ``RATELIMIT_BACKOFF_HTTP_CODES`` (list, default ``[403]``)
    * ``RATELIMIT_BACKOFF_DOMAINS`` (list of host substrings; empty = all hosts)
    * ``RATELIMIT_BACKOFF_COOLDOWN`` (seconds, default 60)
    * ``RATELIMIT_BACKOFF_MAX_RETRIES`` (int, default 8)
    """

    def __init__(self, crawler):
        self.crawler = crawler
        s = crawler.settings
        self.enabled = s.getbool("RATELIMIT_BACKOFF_ENABLED", False)
        self.codes = {
            int(c) for c in s.getlist("RATELIMIT_BACKOFF_HTTP_CODES", [403])
        }
        self.domains = tuple(
            d.lower() for d in s.getlist("RATELIMIT_BACKOFF_DOMAINS", [])
        )
        self.cooldown = s.getfloat("RATELIMIT_BACKOFF_COOLDOWN", 60.0)
        self.max_retries = s.getint("RATELIMIT_BACKOFF_MAX_RETRIES", 8)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def _domain_matches(self, request):
        if not self.domains:
            return True
        host = (urlparse(request.url).hostname or "").lower()
        return any(d in host for d in self.domains)

    def process_response(self, request, response, spider):
        if (
            not self.enabled
            or response.status not in self.codes
            or not self._domain_matches(request)
        ):
            return response

        retries = request.meta.get("ratelimit_retries", 0)
        if retries >= self.max_retries:
            spider.logger.error(
                "Rate-limit %s on %s: gave up after %d cooldown retries.",
                response.status,
                request.url,
                retries,
            )
            return response

        slot_key = self._pause_slot(request)
        spider.logger.warning(
            "Rate-limit %s on %s — pausing slot %r for %.0fs, then retry %d/%d.",
            response.status,
            request.url,
            slot_key,
            self.cooldown,
            retries + 1,
            self.max_retries,
        )
        return request.replace(
            meta={**request.meta, "ratelimit_retries": retries + 1},
            dont_filter=True,
        )

    def _pause_slot(self, request):
        """Raise the request's download-slot delay for the cooldown window.

        Bumping ``slot.delay`` gates the *next* dispatch from that slot, so the
        retried request — and any other requests queued for the same host — wait
        out the cooldown. Real silence is what actually clears the rate-limit
        window (a fast retry just re-trips it). The original delay/jitter are
        restored afterwards; an overlapping trip re-arms (extends) the window
        rather than restoring early.
        """
        from twisted.internet import reactor

        engine = getattr(self.crawler, "engine", None)
        downloader = getattr(engine, "downloader", None)
        if downloader is None:
            return None
        slot_key = downloader.get_slot_key(request)
        slot = downloader.slots.get(slot_key)
        if slot is None:
            return slot_key

        # Capture the pristine delay/jitter once, on the first trip; nested
        # trips extend the pause but must not overwrite the saved values.
        if not hasattr(slot, "_ratelimit_saved"):
            slot._ratelimit_saved = (slot.delay, slot.randomize_delay)
        slot.delay = self.cooldown
        slot.randomize_delay = False

        pending = getattr(slot, "_ratelimit_restore", None)
        if pending is not None and pending.active():
            pending.cancel()
        slot._ratelimit_restore = reactor.callLater(
            self.cooldown, self._restore_slot, slot
        )
        return slot_key

    @staticmethod
    def _restore_slot(slot):
        saved = getattr(slot, "_ratelimit_saved", None)
        if saved is not None:
            slot.delay, slot.randomize_delay = saved
            del slot._ratelimit_saved
        slot._ratelimit_restore = None
