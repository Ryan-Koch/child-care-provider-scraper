import logging
from types import SimpleNamespace

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from scrapy import Request, Spider
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.http import Response
from scrapy.settings import Settings
from scrapy.signalmanager import SignalManager
from scrapy.statscollectors import StatsCollector
from scrapy.utils.test import get_crawler
from twisted.internet.error import TimeoutError as TxTimeoutError

from provider_scrape.middlewares import (
    PlaywrightRetryMiddleware,
    ProxyPoolMiddleware,
    RateLimitBackoffMiddleware,
)
from provider_scrape.proxy_pool import build_pool, proxy_pool_alert


def _spider(pool, domains=("checkccmd.org",)):
    return SimpleNamespace(proxy_pool=pool, proxy_pool_domains=list(domains))


def _pool():
    return build_pool(["1.1.1.1:80", "2.2.2.2:81"], "u", "p", id_prefix="ws")


DETAIL = "https://www.checkccmd.org/FacilityDetail.aspx?fi=1"
EXCELS = "https://findaprogram.marylandexcels.org/api/fap/search?license=1"


def _backoff_mw(engine=None, **overrides):
    """A RateLimitBackoffMiddleware with the Maryland-style settings."""
    s = Settings()
    s.setdict(
        {
            "RATELIMIT_BACKOFF_ENABLED": True,
            "RATELIMIT_BACKOFF_DOMAINS": ["checkccmd.org"],
            "RATELIMIT_BACKOFF_TIMEOUT_COOLDOWN": 45,
            "RATELIMIT_BACKOFF_TIMEOUT_MAX_RETRIES": 4,
            **overrides,
        }
    )
    return RateLimitBackoffMiddleware(SimpleNamespace(settings=s, engine=engine))


def _backoff_spider():
    return SimpleNamespace(logger=logging.getLogger("test.backoff"))


def test_noop_without_pool():
    mw = ProxyPoolMiddleware()
    req = Request(DETAIL)
    assert mw.process_request(req, _spider(None)) is None
    assert "proxy" not in req.meta
    assert "download_slot" not in req.meta


def test_assigns_proxy_and_per_proxy_slot_for_target_host():
    mw = ProxyPoolMiddleware()
    req = Request(DETAIL)
    mw.process_request(req, _spider(_pool()))
    assert req.meta["proxy"].startswith("http://u:p@")
    # The download slot is the (cred-free) proxy id, giving each IP its own slot.
    assert req.meta["download_slot"] == "ws-0"


def test_rotating_details_round_robin_across_proxies():
    mw = ProxyPoolMiddleware()
    spider = _spider(_pool())
    slots = []
    for _ in range(4):
        req = Request(DETAIL)
        mw.process_request(req, spider)
        slots.append(req.meta["download_slot"])
    assert slots == ["ws-0", "ws-1", "ws-0", "ws-1"]


def test_affinity_pins_a_chain_to_one_proxy():
    mw = ProxyPoolMiddleware()
    spider = _spider(_pool())
    slots = []
    for _ in range(3):
        req = Request(DETAIL, meta={"proxy_affinity": "Montgomery"})
        mw.process_request(req, spider)
        slots.append(req.meta["download_slot"])
    assert slots == ["ws-0", "ws-0", "ws-0"]


def test_excels_host_stays_direct():
    mw = ProxyPoolMiddleware()
    req = Request(EXCELS)
    mw.process_request(req, _spider(_pool()))
    assert "proxy" not in req.meta
    assert "download_slot" not in req.meta


def test_existing_proxy_is_not_overridden():
    mw = ProxyPoolMiddleware()
    req = Request(DETAIL, meta={"proxy": "http://preset:9"})
    mw.process_request(req, _spider(_pool()))
    assert req.meta["proxy"] == "http://preset:9"
    assert "download_slot" not in req.meta


def test_only_configured_domains_are_proxied():
    mw = ProxyPoolMiddleware()
    # A checkccmd request is proxied; an unrelated host is not, even with a pool.
    other = Request("https://example.com/x")
    mw.process_request(other, _spider(_pool()))
    assert "proxy" not in other.meta


def test_proxy_bypass_egresses_direct_from_host():
    mw = ProxyPoolMiddleware()
    # A pooled-domain request flagged proxy_bypass (e.g. a ~1MB inspection PDF)
    # is left direct so it doesn't consume metered proxy bandwidth.
    pdf = Request(
        "https://www.checkccmd.org/PublicReports/PrintTask.aspx?t=1&d=2",
        meta={"proxy_bypass": True},
    )
    mw.process_request(pdf, _spider(_pool()))
    assert "proxy" not in pdf.meta
    assert "download_slot" not in pdf.meta


def test_counts_per_proxy_req_ok_blocked_err():
    mw = ProxyPoolMiddleware()
    spider = _spider(_pool())
    r0 = Request(DETAIL)
    r1 = Request(DETAIL)
    mw.process_request(r0, spider)  # -> ws-0
    mw.process_request(r1, spider)  # -> ws-1
    assert mw._counts["ws-0"]["req"] == 1
    assert mw._counts["ws-1"]["req"] == 1

    mw.process_response(r0, Response(DETAIL, status=200, request=r0), spider)
    mw.process_response(r1, Response(DETAIL, status=403, request=r1), spider)
    mw.process_exception(r0, Exception("timeout"), spider)

    assert mw._counts["ws-0"]["ok"] == 1
    assert mw._counts["ws-0"]["err"] == 1
    assert mw._counts["ws-1"]["blk"] == 1


TUNNEL_502 = TunnelError("Could not open CONNECT tunnel with proxy 1.1.1.1:80 [{'status': 502}]")


def _alerting_mw(**settings):
    """A ProxyPoolMiddleware on a real crawler, plus the alerts it raises."""
    crawler = SimpleNamespace(settings=Settings(settings), signals=SignalManager())
    crawler.stats = StatsCollector(crawler)
    mw = ProxyPoolMiddleware(crawler)
    alerts = []

    def capture(message, spider):
        alerts.append(message)

    crawler.signals.connect(capture, signal=proxy_pool_alert, weak=False)
    return mw, alerts


def _send(mw, spider, request=None):
    """Run a request through process_request and return it."""
    request = request or Request(DETAIL)
    mw.process_request(request, spider)
    return request


def _fail(mw, spider, request, exception=TUNNEL_502):
    """Fail a request, then return its retry as the retry middleware would."""
    mw.process_exception(request, exception, spider)
    return _send(mw, spider, request.copy())


def _succeed(mw, spider):
    """Land a response on the next proxy in rotation."""
    request = _send(mw, spider)
    mw.process_response(request, Response(DETAIL, status=200, request=request), spider)


def _set_rotation_to(pool, proxy_id):
    """Point the rotating cursor at a proxy so the next request lands on it."""
    pool._rotate = pool.ids.index(proxy_id)


def test_tunnel_failure_moves_rotating_request_to_next_proxy(caplog):
    mw, alerts = _alerting_mw()
    spider = _spider(_pool())
    request = _send(mw, spider)
    assert request.meta["download_slot"] == "ws-0"

    with caplog.at_level(logging.WARNING):
        retry = _fail(mw, spider, request)

    assert retry.meta["download_slot"] == "ws-1"
    assert retry.meta["proxy"].endswith("@2.2.2.2:81")
    assert "proxy_switched_from" not in retry.meta
    assert "switched" in caplog.text
    assert alerts == ["proxy ws-0 (1.1.1.1:80) failed to open a tunnel; switched to ws-1."]
    assert mw.stats.get_value("proxy_pool/switched") == 1


def test_timeout_keeps_the_assigned_proxy():
    mw, alerts = _alerting_mw()
    spider = _spider(_pool())
    request = _send(mw, spider)

    retry = _fail(mw, spider, request, TxTimeoutError("timed out"))

    assert retry.meta["download_slot"] == "ws-0"
    assert alerts == []
    assert mw._tunnel_streaks == {}


def test_timeout_still_gets_cooldown_retry_on_the_same_proxy():
    # Exceptions reach the pool middleware (610) first, then the backoff (560).
    mw, alerts = _alerting_mw()
    spider = _spider(_pool())
    request = _send(mw, spider, Request(DETAIL, meta={"timeout_backoff": True}))
    backoff = _backoff_mw()
    exception = TxTimeoutError("timed out")

    assert mw.process_exception(request, exception, spider) is None
    retry = backoff.process_exception(request, exception, _backoff_spider())

    assert retry.meta["timeout_retries"] == 1
    assert retry.meta["download_slot"] == "ws-0"
    assert retry.meta["proxy"] == request.meta["proxy"]
    assert alerts == []


def test_rate_limit_403_keeps_the_proxy_and_ends_its_tunnel_streak():
    mw, alerts = _alerting_mw()
    spider = _spider(_pool())
    request = _send(mw, spider, Request(DETAIL, meta={"proxy_affinity": "Howard"}))
    request = _fail(mw, spider, request)
    response = Response(DETAIL, status=403, request=request)

    assert mw.process_response(request, response, spider) is response
    retry = _backoff_mw(RATELIMIT_BACKOFF_HTTP_CODES=[403]).process_response(request, response, _backoff_spider())

    assert retry.meta["ratelimit_retries"] == 1
    assert retry.meta["download_slot"] == "ws-0"
    assert mw._tunnel_streaks == {}
    assert alerts == []


def test_affinity_request_keeps_its_proxy_on_tunnel_failure():
    mw, alerts = _alerting_mw()
    spider = _spider(_pool())
    request = _send(mw, spider, Request(DETAIL, meta={"proxy_affinity": "Howard"}))

    retry = _fail(mw, spider, request)

    assert retry.meta["download_slot"] == "ws-0"
    assert "proxy_switched_from" not in retry.meta
    assert alerts == []


def test_proxy_removed_after_repeated_failures_and_chain_repins():
    mw, alerts = _alerting_mw(PROXY_POOL_REMOVE_AFTER=2)
    pool = _pool()
    spider = _spider(pool)
    chain = _send(mw, spider, Request(DETAIL, meta={"proxy_affinity": "Howard"}))
    assert chain.meta["download_slot"] == "ws-0"
    chain = _fail(mw, spider, chain)  # First failure starts the streak.
    _set_rotation_to(pool, "ws-1")
    _succeed(mw, spider)  # Another proxy reaches the target during the streak.

    retry = _fail(mw, spider, chain)

    assert pool.ids == ["ws-1"]
    assert retry.meta["download_slot"] == "ws-1"
    assert retry.meta["proxy"].endswith("@2.2.2.2:81")
    assert mw.stats.get_value("proxy_pool/removed") == 1
    assert len(alerts) == 1
    assert alerts[0].startswith("removed proxy ws-0 (1.1.1.1:80) from the pool after 2 consecutive")
    assert "continuing with 1 proxies" in alerts[0]


def test_response_on_the_proxy_resets_its_streak():
    mw, _alerts = _alerting_mw(PROXY_POOL_REMOVE_AFTER=2)
    pool = _pool()
    spider = _spider(pool)
    chain = _send(mw, spider, Request(DETAIL, meta={"proxy_affinity": "Howard"}))
    chain = _fail(mw, spider, chain)
    mw.process_response(chain, Response(DETAIL, status=200, request=chain), spider)

    _fail(mw, spider, chain)

    assert pool.ids == ["ws-0", "ws-1"]
    assert mw._tunnel_streaks["ws-0"]["count"] == 1


def test_no_removal_when_no_proxy_is_getting_responses(caplog):
    # A target outage or bad credentials fail every proxy. Keep them all.
    mw, alerts = _alerting_mw(PROXY_POOL_REMOVE_AFTER=2)
    pool = _pool()
    spider = _spider(pool)
    request = _send(mw, spider)

    with caplog.at_level(logging.WARNING):
        for _ in range(6):
            request = _fail(mw, spider, request)

    assert pool.ids == ["ws-0", "ws-1"]
    assert mw.stats.get_value("proxy_pool/removed") is None
    assert caplog.text.count("no other proxy got a response") == 2  # Once per proxy.
    # One alert per proxy's streak, however many switches happened.
    assert len(alerts) == 2


def test_last_proxy_is_kept_and_reported_once(caplog):
    mw, alerts = _alerting_mw(PROXY_POOL_REMOVE_AFTER=2)
    pool = _pool()
    spider = _spider(pool)
    # ws-1 starts failing, ws-0 proves the target is up, then ws-0 is taken out.
    _set_rotation_to(pool, "ws-1")
    last = _send(mw, spider, Request(DETAIL))
    mw.process_exception(last, TUNNEL_502, spider)
    _set_rotation_to(pool, "ws-0")
    _succeed(mw, spider)
    pool.remove("ws-0")

    with caplog.at_level(logging.ERROR):
        for _ in range(2):
            last = _send(mw, spider, last.copy())
            mw.process_exception(last, TUNNEL_502, spider)

    assert pool.ids == ["ws-1"]
    assert caplog.text.count("it is the last one left") == 1
    assert len(alerts) == 1
    assert "it is the last one left" in alerts[0]


def test_failure_from_an_already_removed_proxy_still_switches():
    # A request in flight when its proxy was removed fails later; it must move on.
    mw, alerts = _alerting_mw()
    pool = _pool()
    spider = _spider(pool)
    request = _send(mw, spider)
    pool.remove("ws-0")

    retry = _fail(mw, spider, request)

    assert retry.meta["download_slot"] == "ws-1"
    assert alerts == []


def test_alerts_never_contain_proxy_credentials():
    mw, alerts = _alerting_mw(PROXY_POOL_REMOVE_AFTER=2)
    pool = _pool()
    spider = _spider(pool)
    chain = _send(mw, spider, Request(DETAIL, meta={"proxy_affinity": "Howard"}))
    chain = _fail(mw, spider, chain)
    _set_rotation_to(pool, "ws-1")
    _succeed(mw, spider)
    _fail(mw, spider, chain)

    rotating = _send(mw, spider)
    _fail(mw, spider, rotating)

    assert alerts
    assert not any("u:p@" in alert for alert in alerts)


def test_non_pool_response_is_not_counted():
    mw = ProxyPoolMiddleware()
    spider = _spider(_pool())
    # The EXCELS host is never proxied, so its responses carry no pool slot and
    # must not pollute the per-proxy accounting.
    ex = Request(EXCELS)
    mw.process_request(ex, spider)
    mw.process_response(ex, Response(EXCELS, status=200, request=ex), spider)
    assert mw._counts == {}


def test_report_flags_a_quiet_proxy(caplog):
    mw = ProxyPoolMiddleware()
    mw._pool_ids = ["ws-0", "ws-1"]
    for pid in mw._pool_ids:
        mw._bucket(pid)
    # ws-0 healthy (3 sent, 3 ok); ws-1 throttled (3 sent, 0 ok, 3 err).
    for _ in range(3):
        mw._count("ws-0", "req")
        mw._count("ws-0", "ok")
        mw._count("ws-1", "req")
        mw._count("ws-1", "err")

    with caplog.at_level(logging.INFO):
        mw._report()

    assert "ws-0 3/0/0" in caplog.text
    assert "ws-1 0/0/3" in caplog.text
    # The stalled IP is called out separately at WARNING.
    assert "ws-1 completed 0 responses this window" in caplog.text
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_report_breaks_down_err_by_exception_class(caplog):
    mw = ProxyPoolMiddleware()
    spider = _spider(_pool())
    mw._pool_ids = ["ws-0", "ws-1"]
    for pid in mw._pool_ids:
        mw._bucket(pid)
    r0 = Request(DETAIL)
    r1 = Request(DETAIL)
    mw.process_request(r0, spider)  # ws-0
    mw.process_request(r1, spider)  # ws-1
    mw.process_exception(r0, TxTimeoutError(), spider)
    mw.process_exception(r1, TxTimeoutError(), spider)

    with caplog.at_level(logging.INFO):
        mw._report()
    # The window line names the exception class and its count, not just "2 err".
    assert "TimeoutError x2" in caplog.text


def test_report_deltas_reset_between_windows(caplog):
    mw = ProxyPoolMiddleware()
    mw._pool_ids = ["ws-0"]
    mw._bucket("ws-0")
    mw._count("ws-0", "req")
    mw._count("ws-0", "ok")
    mw._report()  # first window consumes the initial ok
    caplog.clear()
    # No new activity -> the next window reports zero, not the cumulative total.
    with caplog.at_level(logging.INFO):
        mw._report()
    assert "ws-0 0/0/0" in caplog.text


# --- RateLimitBackoffMiddleware timeout backoff ---


def test_timeout_backoff_retries_marked_request():
    mw = _backoff_mw()
    req = Request(DETAIL, meta={"timeout_backoff": True})
    out = mw.process_exception(req, TxTimeoutError(), _backoff_spider())
    # A retry Request (not None), with the timeout-retry counter advanced.
    assert out is not None and out.meta["timeout_retries"] == 1


def test_timeout_backoff_ignores_unmarked_request():
    mw = _backoff_mw()
    # No timeout_backoff marker (e.g. a pagination postback) -> left for
    # RetryMiddleware's patient path.
    req = Request(DETAIL)
    assert mw.process_exception(req, TxTimeoutError(), _backoff_spider()) is None


def test_timeout_backoff_ignores_non_timeout_exception():
    mw = _backoff_mw()
    req = Request(DETAIL, meta={"timeout_backoff": True})
    assert mw.process_exception(req, ValueError("boom"), _backoff_spider()) is None


def test_timeout_backoff_only_on_configured_domain():
    mw = _backoff_mw()
    req = Request(EXCELS, meta={"timeout_backoff": True})  # not checkccmd
    assert mw.process_exception(req, TxTimeoutError(), _backoff_spider()) is None


def test_timeout_backoff_gives_up_and_stops_further_retries():
    mw = _backoff_mw()
    # Already at the cap: no more cooldown retries, and dont_retry is set so
    # RetryMiddleware doesn't then hammer the origin RETRY_TIMES more.
    req = Request(DETAIL, meta={"timeout_backoff": True, "timeout_retries": 4})
    out = mw.process_exception(req, TxTimeoutError(), _backoff_spider())
    assert out is None
    assert req.meta["dont_retry"] is True


def test_pause_slot_never_dips_below_base_delay():
    slot = SimpleNamespace(delay=33.0, randomize_delay=False)
    downloader = SimpleNamespace(get_slot_key=lambda r: "webshare-0", slots={"webshare-0": slot})
    mw = _backoff_mw(engine=SimpleNamespace(downloader=downloader))
    req = Request(DETAIL, meta={"download_slot": "webshare-0"})
    try:
        # Cooldown (20s) below the base spacing (33s) must not speed dispatch up.
        mw._pause_slot(req, cooldown=20.0)
        assert slot.delay == 33.0
        # A cooldown above the base does extend the pause.
        mw._pause_slot(req, cooldown=60.0)
        assert slot.delay == 60.0
    finally:
        pending = getattr(slot, "_ratelimit_restore", None)
        if pending is not None and pending.active():
            pending.cancel()


def _retry_mw(**overrides):
    s = Settings()
    s.setdict(overrides)
    return PlaywrightRetryMiddleware(s)


def _retry_spider():
    # get_retry_request() records stats via spider.crawler.
    crawler = get_crawler(Spider)
    crawler.stats = StatsCollector(crawler)
    return Spider.from_crawler(crawler, name="texas")


def test_playwright_retry_retries_playwright_error():
    req = Request("https://childcare.hhs.texas.gov/public/childcaresearch", meta={"playwright": True})
    out = _retry_mw().process_exception(req, PlaywrightTimeoutError("Timeout 30000ms exceeded"), _retry_spider())
    assert isinstance(out, Request)
    assert out.meta["retry_times"] == 1
    assert out.dont_filter is True


def test_playwright_retry_gives_up_after_retry_times():
    req = Request("https://example.com", meta={"playwright": True, "retry_times": 2})
    out = _retry_mw(RETRY_TIMES=2).process_exception(req, PlaywrightError("net::ERR_CONNECTION_RESET"), _retry_spider())
    assert out is None


def test_playwright_retry_skips_include_page_requests():
    # Left to PlaywrightErrbackMixin, which closes the page before retrying.
    req = Request("https://example.com", meta={"playwright": True, "playwright_include_page": True})
    out = _retry_mw().process_exception(req, PlaywrightTimeoutError("t/o"), _retry_spider())
    assert out is None


def test_playwright_retry_honors_dont_retry():
    req = Request("https://example.com", meta={"playwright": True, "dont_retry": True})
    assert _retry_mw().process_exception(req, PlaywrightTimeoutError("t/o"), _retry_spider()) is None


def test_playwright_retry_keeps_default_network_exceptions():
    req = Request("https://example.com")
    out = _retry_mw().process_exception(req, TxTimeoutError(), _retry_spider())
    assert isinstance(out, Request)
