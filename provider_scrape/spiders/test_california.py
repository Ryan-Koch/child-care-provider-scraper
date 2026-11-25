import unittest

from scrapy.http import TextResponse

from provider_scrape.items import ProviderItem
from provider_scrape.spiders.california import CaclSpider


class CaclSpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = CaclSpider(proxies="off")

    def test_happy_path(self):
        # Mock CSV data for the happy path scenario
        csv_data = """"Facility Type","Facility Number","Facility Name","Licensee","Facility Administrator","Facility Telephone Number","Facility Address","Facility City","Facility State","Facility Zip","County Name","Regional Office","Facility Capacity","Facility Status","License First Date","Closed Date","Citation Numbers","All Visit Dates","Inspection Visit Dates","Other Visit Dates","Complaint Info- Date, #Sub Aleg, # Inc Aleg, # Uns Aleg, # TypeA, # TypeB ...","Inspect TypeA","Inspect TypeB","Other TypeA","Other TypeB"
"CHILD CARE CENTER","123456789","HAPPY KIDS CENTER","HAPPY KIDS INC","JOHN DOE","(555) 123-4567","123 SUNNY ST","SUNNYVALE","CA","94086","SANTA CLARA","BAY AREA OFFICE","50","LICENSED","2020-01-01","","","2023-05-10","2023-05-10","","","0","0","0","0"
"CHILD CARE CENTER","987654321","PLAYFUL LEARNERS","PLAYFUL LEARNERS LLC","JANE SMITH","(555) 765-4321","456 OAK AVE","MOUNTAIN VIEW","CA","94043","SANTA CLARA","BAY AREA OFFICE","30","LICENSED","2019-03-15","","","2023-06-20","2023-06-20","","","0","0","0","0"
"""
        # Create a mock Scrapy response
        response = TextResponse(url="http://www.example.com/test.csv", body=csv_data, encoding="utf-8")

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1["provider_type"], "CHILD CARE CENTER")
        self.assertEqual(provider1["license_number"], "123456789")
        self.assertEqual(provider1["provider_name"], "HAPPY KIDS CENTER")
        self.assertEqual(provider1["license_holder"], "HAPPY KIDS INC")
        self.assertEqual(provider1["administrator"], "JOHN DOE")
        self.assertEqual(provider1["phone"], "(555) 123-4567")
        self.assertEqual(provider1["address"], "123 SUNNY ST, SUNNYVALE, CA 94086")
        self.assertEqual(provider1["county"], "SANTA CLARA")
        self.assertEqual(provider1["ca_regional_office"], "BAY AREA OFFICE")
        self.assertEqual(provider1["capacity"], "50")
        self.assertEqual(provider1["status"], "LICENSED")
        self.assertEqual(provider1["ca_license_first_date"], "2020-01-01")

        # Check second provider
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2["provider_name"], "PLAYFUL LEARNERS")
        self.assertEqual(provider2["address"], "456 OAK AVE, MOUNTAIN VIEW, CA 94043")

    def test_missing_data(self):
        # Mock CSV data with missing values
        csv_data = """"Facility Type","Facility Number","Facility Name","Licensee","Facility Administrator","Facility Telephone Number","Facility Address","Facility City","Facility State","Facility Zip","County Name","Regional Office","Facility Capacity","Facility Status","License First Date","Closed Date","Citation Numbers","All Visit Dates","Inspection Visit Dates","Other Visit Dates","Complaint Info- Date, #Sub Aleg, # Inc Aleg, # Uns Aleg, # TypeA, # TypeB ...","Inspect TypeA","Inspect TypeB","Other TypeA","Other TypeB"
"CHILD CARE CENTER","111222333","KID ZONE","","","(555) 111-2222","","SUNNYVALE","CA","94086","SANTA CLARA","BAY AREA OFFICE","","LICENSED","2021-02-01","","","","","","","0","0","0","0"
"CHILD CARE CENTER","444555666","","LEARNING TREE LLC","","","456 MAPLE AVE","","CA","94043","SANTA CLARA","BAY AREA OFFICE","25","","2018-07-20","","","","","","","0","0","0","0"
"""
        # Create a mock Scrapy response
        response = TextResponse(url="http://www.example.com/test.csv", body=csv_data, encoding="utf-8")

        # Parse the response
        results = list(self.spider.parse(response))

        # Assertions
        self.assertEqual(len(results), 2)

        # Check first provider for missing data
        provider1 = results[0]
        self.assertIsInstance(provider1, ProviderItem)
        self.assertEqual(provider1["license_holder"], "")
        self.assertEqual(provider1["administrator"], "")
        self.assertEqual(provider1["address"], ", SUNNYVALE, CA 94086")  # Address is constructed, so check for partial
        self.assertEqual(provider1["capacity"], "")

        # Check second provider for missing data
        provider2 = results[1]
        self.assertIsInstance(provider2, ProviderItem)
        self.assertEqual(provider2["provider_name"], "")
        self.assertEqual(provider2["phone"], "")
        self.assertEqual(provider2["address"], "456 MAPLE AVE, , CA 94043")  # Address is constructed
        self.assertEqual(provider2["status"], "")


if __name__ == "__main__":
    unittest.main()


# ---- California-egress proxy fallback ----

import pytest  # noqa: E402
from scrapy.http import Request  # noqa: E402
from twisted.internet.error import DNSLookupError  # noqa: E402
from twisted.python.failure import Failure  # noqa: E402

from provider_scrape.proxy_pool import ProxyPool, probe_pool, proxy_host  # noqa: E402
from provider_scrape.spiders import california as ca_mod  # noqa: E402
from provider_scrape.spiders.california import is_california_egress, select_california_proxies  # noqa: E402

GEO = {
    "http://u:p@1.1.1.1:80": {"status": "success", "countryCode": "US", "region": "MA", "city": "Boston"},
    "http://u:p@2.2.2.2:80": {"status": "success", "countryCode": "US", "region": "CA", "city": "Palo Alto"},
    "http://u:p@3.3.3.3:80": {"status": "success", "countryCode": "CA", "region": "QC", "city": "Montreal"},
    "http://u:p@4.4.4.4:80": None,
    "http://u:p@5.5.5.5:80": {"status": "success", "countryCode": "US", "region": "CA", "city": "Sacramento"},
}


def _pool():
    return ProxyPool(list(GEO), [f"webshare-{i}" for i in range(len(GEO))])


def _fake_probe(url, timeout):
    return GEO[url]


@pytest.mark.parametrize(
    "geo,expected",
    [
        ({"status": "success", "countryCode": "US", "region": "CA"}, True),
        ({"status": "success", "countryCode": "us", "region": "ca"}, True),
        ({"status": "success", "countryCode": "US", "region": "NV"}, False),
        # countryCode "CA" is Canada, not California.
        ({"status": "success", "countryCode": "CA", "region": "QC"}, False),
        ({"status": "fail", "countryCode": "US", "region": "CA"}, False),
        (None, False),
        ({}, False),
    ],
)
def test_is_california_egress(geo, expected):
    assert is_california_egress(geo) is expected


def test_select_california_proxies_keeps_pool_order():
    selected = select_california_proxies(_pool(), probe=_fake_probe)
    assert [pid for pid, _url, _geo in selected] == ["webshare-1", "webshare-4"]


def test_probe_pool_returns_every_entry_in_order():
    probed = probe_pool(_pool(), probe=_fake_probe)
    assert [pid for pid, _u, _g in probed] == [f"webshare-{i}" for i in range(5)]
    assert probed[3][2] is None


def test_proxy_host_strips_credentials():
    assert proxy_host("http://user:secret@9.9.9.9:8080") == "9.9.9.9:8080"


def _spider_with_pool(monkeypatch):
    monkeypatch.setattr(ca_mod, "probe_pool", lambda pool, **kw: probe_pool(pool, probe=_fake_probe))
    spider = CaclSpider(proxies="off")
    spider.egress_pool = _pool()
    return spider


def test_proxies_off_runs_direct_only():
    spider = CaclSpider(proxies="off")
    assert spider.egress_pool is None
    reqs = list(spider.start_requests())
    assert len(reqs) == 2
    assert all("proxy" not in r.meta for r in reqs)
    assert all(r.meta["ca_attempt"] == 0 for r in reqs)
    assert [a["label"] for a in spider._attempts] == ["host (direct)"]


def test_start_requests_tries_california_proxies_first(monkeypatch):
    spider = _spider_with_pool(monkeypatch)
    reqs = list(spider.start_requests())
    assert [a["proxy"] for a in spider._attempts] == ["http://u:p@2.2.2.2:80", "http://u:p@5.5.5.5:80", None]
    assert all(r.meta["proxy"] == "http://u:p@2.2.2.2:80" for r in reqs)
    assert all(r.meta["max_retry_times"] == 1 for r in reqs)
    assert all(r.dont_filter and r.errback == spider._on_download_error for r in reqs)
    # Credentials never leak into the log label.
    assert all("u:p" not in a["label"] for a in spider._attempts)


def test_no_california_proxy_falls_back_to_host(monkeypatch):
    monkeypatch.setattr(ca_mod, "probe_pool", lambda pool, **kw: [("webshare-0", "http://u:p@1.1.1.1:80", None)])
    spider = CaclSpider(proxies="off")
    spider.egress_pool = ProxyPool(["http://u:p@1.1.1.1:80"])
    reqs = list(spider.start_requests())
    assert [a["label"] for a in spider._attempts] == ["host (direct)"]
    assert all("proxy" not in r.meta for r in reqs)


def _fail(request):
    failure = Failure(DNSLookupError("no results for hostname lookup: www.ccld.dss.ca.gov."))
    failure.request = request
    return failure


def test_error_walks_the_chain_then_gives_up(monkeypatch):
    spider = _spider_with_pool(monkeypatch)
    req, _ = list(spider.start_requests())
    nxt = list(spider._on_download_error(_fail(req)))
    assert len(nxt) == 1
    assert nxt[0].meta["proxy"] == "http://u:p@5.5.5.5:80"
    assert nxt[0].meta["ca_attempt"] == 1
    assert nxt[0].headers.get("Referer") == req.headers.get("Referer")
    host = list(spider._on_download_error(_fail(nxt[0])))
    assert host[0].meta["ca_attempt"] == 2
    assert "proxy" not in host[0].meta
    assert "max_retry_times" not in host[0].meta
    assert list(spider._on_download_error(_fail(host[0]))) == []


def test_error_jumps_to_egress_that_already_worked(monkeypatch):
    spider = _spider_with_pool(monkeypatch)
    first, second = list(spider.start_requests())
    # `second` succeeded via the host (attempt 2) ...
    ok = TextResponse(url=second.url, body=b"", request=Request(second.url, meta={"ca_attempt": 2}), encoding="utf-8")
    list(spider.parse(ok))
    assert spider._working_attempt == 2
    # ... so `first` failing on attempt 0 skips the untested proxy and goes to host.
    nxt = list(spider._on_download_error(_fail(first)))
    assert nxt[0].meta["ca_attempt"] == 2
