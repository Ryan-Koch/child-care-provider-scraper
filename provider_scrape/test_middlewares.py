import logging
from types import SimpleNamespace

from scrapy import Request
from scrapy.http import Response

from provider_scrape.middlewares import ProxyPoolMiddleware
from provider_scrape.proxy_pool import build_pool


def _spider(pool, domains=("checkccmd.org",)):
    return SimpleNamespace(proxy_pool=pool, proxy_pool_domains=list(domains))


def _pool():
    return build_pool(["1.1.1.1:80", "2.2.2.2:81"], "u", "p", id_prefix="ws")


DETAIL = "https://www.checkccmd.org/FacilityDetail.aspx?fi=1"
EXCELS = "https://findaprogram.marylandexcels.org/api/fap/search?license=1"


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
    from twisted.internet.error import TimeoutError as TxTimeoutError

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
