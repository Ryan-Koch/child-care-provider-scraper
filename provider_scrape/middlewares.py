# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import logging
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

    While a pool is active it also emits a periodic per-IP activity report at
    INFO (every ``PROXY_POOL_STATS_INTERVAL`` s) — each proxy's ok/blocked/err
    for the window plus totals — so a production run shows whether detail pulls
    are still fanning out across every IP, and flags (WARNING) any proxy that was
    sent work but completed nothing (the free-proxy throttle signature). Run
    totals also land in the final Scrapy stats dump under ``proxy_pool/<id>/*``.
    """

    def __init__(self, crawler=None):
        self.logger = logging.getLogger(__name__)
        self.crawler = crawler
        self.stats = crawler.stats if crawler is not None else None
        self.interval = (
            crawler.settings.getfloat("PROXY_POOL_STATS_INTERVAL", 300.0)
            if crawler is not None
            else 300.0
        )
        # Per-proxy activity, keyed by proxy_id. Seeded at spider_opened from the
        # live pool so a proxy that goes silent (0 completions under throttle)
        # still shows in the report instead of vanishing. Buckets: req (sent),
        # ok (2xx), blk (403 rate-limit), oth (other status), err (exception).
        self._counts = {}
        self._last = {}
        # Download-exception class -> count (pool-wide). The err bucket only says
        # *how many* failed; the class name says *why* (TimeoutError vs
        # ConnectionRefused vs TunnelError) — the split that distinguishes a
        # proxy-transport throttle from a target-side block, and which is
        # otherwise only visible at DEBUG.
        self._err_types = {}
        self._err_types_last = {}
        self._pool_ids = []
        self._report_task = None

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls(crawler)
        crawler.signals.connect(mw._spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(mw._spider_closed, signal=signals.spider_closed)
        return mw

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
        self._count(proxy_id, "req")
        self.logger.debug(
            "Proxy %s -> %s%s",
            proxy_id,
            request.url,
            f" (affinity={affinity})" if affinity is not None else "",
        )
        return None

    def process_response(self, request, response, spider):
        pid = request.meta.get("download_slot")
        if pid in self._counts:
            if 200 <= response.status < 300:
                self._count(pid, "ok")
            elif response.status == 403:
                self._count(pid, "blk")
            else:
                self._count(pid, "oth")
        return response

    def process_exception(self, request, exception, spider):
        pid = request.meta.get("download_slot")
        if pid in self._counts:
            self._count(pid, "err")
            etype = type(exception).__name__
            self._err_types[etype] = self._err_types.get(etype, 0) + 1
            if self.stats is not None:
                self.stats.inc_value(f"proxy_pool/exception/{etype}")
        return None

    def _bucket(self, proxy_id):
        return self._counts.setdefault(
            proxy_id, {"req": 0, "ok": 0, "blk": 0, "oth": 0, "err": 0}
        )

    def _count(self, proxy_id, key):
        """Bump an in-memory per-proxy counter and mirror it into crawler stats.

        The in-memory counts drive the periodic per-window report; the stats
        mirror lands the run totals in Scrapy's final stats dump for free.
        """
        self._bucket(proxy_id)[key] += 1
        if self.stats is not None:
            self.stats.inc_value(f"proxy_pool/{proxy_id}/{key}")

    def _spider_opened(self, spider):
        """Start the per-proxy activity report — only when a pool is active.

        Single-IP runs (no ``proxy_pool``) get no extra logging, and the report
        iterates the pool's stable id order so a quiet proxy is conspicuous by
        its zero rather than simply absent.
        """
        pool = getattr(spider, "proxy_pool", None)
        if not pool:
            return
        self._pool_ids = list(pool.ids)
        for pid in self._pool_ids:
            self._bucket(pid)
        if self.interval and self.interval > 0:
            from twisted.internet import task

            self._report_task = task.LoopingCall(self._report)
            # now=False: first report one window in, once traffic is flowing.
            self._report_task.start(self.interval, now=False)

    def _report(self):
        """Log one line of per-proxy deltas so fan-out is visible at INFO.

        The line answers "are all IPs still pulling?" at a glance: each proxy's
        ok/blocked/err for the window, plus totals. A proxy that was sent work
        but completed nothing (the free-proxy throttle signature) also gets a
        WARNING so it stands out from the steady-state line.
        """
        if not self._pool_ids:
            return
        parts = []
        totals = {"req": 0, "ok": 0, "blk": 0, "err": 0}
        quiet = []
        for pid in self._pool_ids:
            cur = self._counts.get(pid, {})
            prev = self._last.get(pid, {})
            delta = {k: cur.get(k, 0) - prev.get(k, 0) for k in self._bucket(pid)}
            self._last[pid] = dict(self._bucket(pid))
            for k in totals:
                totals[k] += delta.get(k, 0)
            parts.append(f"{pid} {delta['ok']}/{delta['blk']}/{delta['err']}")
            if delta["ok"] == 0 and (delta["req"] >= 2 or delta["err"] or delta["blk"]):
                quiet.append((pid, delta["req"], delta["err"], delta["blk"]))
        # Break the window's err count down by exception class so the log says
        # *why* things failed (e.g. "TimeoutError x18") — the transport-throttle
        # vs target-block tell — instead of just how many.
        err_types = []
        for etype in sorted(self._err_types):
            d = self._err_types[etype] - self._err_types_last.get(etype, 0)
            if d:
                err_types.append(f"{etype} x{d}")
        self._err_types_last = dict(self._err_types)
        err_detail = f" [{', '.join(err_types)}]" if err_types else ""
        self.logger.info(
            "[proxy-pool] last %ds per IP (ok/blocked/err): %s | totals: "
            "%d ok of %d sent, %d blocked, %d err%s",
            int(self.interval),
            ", ".join(parts),
            totals["ok"],
            totals["req"],
            totals["blk"],
            totals["err"],
            err_detail,
        )
        for pid, sent, err, blk in quiet:
            self.logger.warning(
                "[proxy-pool] %s completed 0 responses this window "
                "(sent %d, %d err, %d blocked) — likely throttled or unreachable.",
                pid,
                sent,
                err,
                blk,
            )

    def _spider_closed(self, spider):
        task = self._report_task
        if task is not None and task.running:
            task.stop()
        if not self._pool_ids:
            return
        parts = []
        for pid in self._pool_ids:
            b = self._bucket(pid)
            parts.append(
                f"{pid} {b['ok']} ok/{b['blk']} blocked/{b['oth']} other/{b['err']} err"
            )
        self.logger.info("[proxy-pool] final per-IP totals: %s", " | ".join(parts))


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

    It also handles a download *timeout* (via ``process_exception``) the same
    way, but only for requests that opt in with ``meta['timeout_backoff']`` — a
    saturated origin is shed load (slot paused) instead of hammered with
    immediate RETRY_TIMES-deep retries. See ``process_exception``.

    Disabled by default; a spider opts in via settings, so spiders that don't
    set ``RATELIMIT_BACKOFF_ENABLED`` are unaffected (the middleware is a
    pass-through). Settings:

    * ``RATELIMIT_BACKOFF_ENABLED`` (bool, default False)
    * ``RATELIMIT_BACKOFF_HTTP_CODES`` (list, default ``[403]``)
    * ``RATELIMIT_BACKOFF_DOMAINS`` (list of host substrings; empty = all hosts)
    * ``RATELIMIT_BACKOFF_COOLDOWN`` (seconds, default 60)
    * ``RATELIMIT_BACKOFF_MAX_RETRIES`` (int, default 8)
    * ``RATELIMIT_BACKOFF_TIMEOUT_COOLDOWN`` (seconds, default 45) — timeout path
    * ``RATELIMIT_BACKOFF_TIMEOUT_MAX_RETRIES`` (int, default 4) — timeout path
    * ``RATELIMIT_BACKOFF_TIMEOUT_EXCEPTIONS`` (list of exception class names,
      default ``["TimeoutError", "TCPTimedOutError"]``); the timeout path fires
      only for requests carrying ``meta['timeout_backoff']``.
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
        # Timeout backoff: the same cooldown-and-retry, but triggered by a
        # download *timeout* (no response) rather than a 403 — and only for
        # requests explicitly opted in with ``meta['timeout_backoff']``. Where a
        # 403 means "this IP is over its rate limit", a timeout on a fast-normally
        # endpoint means "the origin is saturated"; re-firing immediately (and
        # RETRY_TIMES-deep) just piles more concurrency onto an overloaded server
        # (congestion collapse). Pausing the slot instead sheds load so the origin
        # can drain, and the pool's other slots do the same as they trip — an
        # adaptive, per-slot concurrency cut. Bounded separately from the 403 path.
        self.timeout_cooldown = s.getfloat(
            "RATELIMIT_BACKOFF_TIMEOUT_COOLDOWN", 45.0
        )
        self.timeout_max_retries = s.getint(
            "RATELIMIT_BACKOFF_TIMEOUT_MAX_RETRIES", 4
        )
        # Matched by class name so we don't depend on which twisted timeout type
        # the handler happens to raise (TimeoutError, TCPTimedOutError, …).
        self.timeout_exceptions = frozenset(
            s.getlist(
                "RATELIMIT_BACKOFF_TIMEOUT_EXCEPTIONS",
                ["TimeoutError", "TCPTimedOutError"],
            )
        )

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

        slot_key = self._pause_slot(request, self.cooldown)
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

    def process_exception(self, request, exception, spider):
        """Cooldown-and-retry a download timeout on an opted-in request.

        Only fires for requests carrying ``meta['timeout_backoff']`` (so a
        spider chooses which request classes treat a timeout as "origin
        saturated" vs. keep the patient default retry — e.g. Maryland opts in its
        detail GETs but leaves chain-critical pagination on the standard path).
        Runs before the built-in ``RetryMiddleware`` (lower order), so under the
        cap it owns the retry (pausing the slot first); once the cap is hit it
        sets ``dont_retry`` and defers, ending the retry storm rather than handing
        the origin RETRY_TIMES more immediate hits. Non-timeout exceptions are
        left untouched for RetryMiddleware to handle as usual.
        """
        if (
            not self.enabled
            or not request.meta.get("timeout_backoff")
            or type(exception).__name__ not in self.timeout_exceptions
            or not self._domain_matches(request)
        ):
            return None

        retries = request.meta.get("timeout_retries", 0)
        if retries >= self.timeout_max_retries:
            spider.logger.warning(
                "Timeout on %s: gave up after %d cooldown retries (origin "
                "saturated).",
                request.url,
                retries,
            )
            # Stop the retry here rather than letting RetryMiddleware add more.
            request.meta["dont_retry"] = True
            return None

        slot_key = self._pause_slot(request, self.timeout_cooldown)
        spider.logger.info(
            "Timeout on %s — origin likely saturated; pausing slot %r for %.0fs, "
            "then retry %d/%d.",
            request.url,
            slot_key,
            self.timeout_cooldown,
            retries + 1,
            self.timeout_max_retries,
        )
        return request.replace(
            meta={**request.meta, "timeout_retries": retries + 1},
            dont_filter=True,
        )

    def _pause_slot(self, request, cooldown):
        """Raise the request's download-slot delay for the cooldown window.

        Bumping ``slot.delay`` gates the *next* dispatch from that slot, so the
        retried request — and any other requests queued for the same host — wait
        out the cooldown. Real silence is what actually clears the rate-limit
        window (a fast retry just re-trips it). The original delay/jitter are
        restored afterwards; an overlapping trip re-arms (extends) the window
        rather than restoring early. The pause is floored at the slot's original
        delay so a cooldown shorter than the configured spacing can never *speed
        up* dispatch below the rate-limit floor.
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
        base_delay = slot._ratelimit_saved[0]
        slot.delay = max(cooldown, base_delay)
        slot.randomize_delay = False

        pending = getattr(slot, "_ratelimit_restore", None)
        if pending is not None and pending.active():
            pending.cancel()
        slot._ratelimit_restore = reactor.callLater(
            slot.delay, self._restore_slot, slot
        )
        return slot_key

    @staticmethod
    def _restore_slot(slot):
        saved = getattr(slot, "_ratelimit_saved", None)
        if saved is not None:
            slot.delay, slot.randomize_delay = saved
            del slot._ratelimit_saved
        slot._ratelimit_restore = None
