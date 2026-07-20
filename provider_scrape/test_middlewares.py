from types import SimpleNamespace

from scrapy import Request

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
