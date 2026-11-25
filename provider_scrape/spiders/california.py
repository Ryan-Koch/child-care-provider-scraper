import csv
import os
from io import StringIO

import scrapy

from ..items import ProviderItem
from ..proxy_pool import load_pool, probe_pool, proxy_host

# Default location of the (git-ignored) proxy-pool env file, at the repo root —
# same convention as maryland.py / rhode_island.py. Absent ⇒ host-only mode.
DEFAULT_PROXY_ENV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "webshare.env",
)

_OFF_VALUES = ("off", "none", "false", "0", "no", "")


def is_california_egress(geo):
    """True when a :func:`probe_egress` result is a live US-California egress.

    ip-api's ``countryCode`` "CA" means *Canada*, so the US gate is essential:
    California is ``countryCode == "US"`` and ``region == "CA"``.
    """
    if not geo or geo.get("status") != "success":
        return False
    return (geo.get("countryCode") or "").upper() == "US" and (geo.get("region") or "").upper() == "CA"


def select_california_proxies(pool, timeout=12, probe=None, logger=None):
    """Probe ``pool`` and return ``[(proxy_id, proxy_url, geo), ...]`` for the
    proxies that egress in California, in pool order."""
    kwargs = {"timeout": timeout}
    if probe is not None:
        kwargs["probe"] = probe
    selected = []
    for pid, url, geo in probe_pool(pool, **kwargs):
        if is_california_egress(geo):
            selected.append((pid, url, geo))
            if logger:
                logger.info("CA egress probe: %s (%s) is California — %s", pid, proxy_host(url), geo.get("city"))
        elif logger:
            if geo is None:
                logger.info("CA egress probe: %s (%s) unreachable — skipping", pid, proxy_host(url))
            else:
                logger.debug(
                    "CA egress probe: %s (%s) not California (%s/%s) — skipping",
                    pid,
                    proxy_host(url),
                    geo.get("countryCode"),
                    geo.get("region"),
                )
    return selected


class CaclSpider(scrapy.Spider):
    """California CCLD bulk CSV exports.

    Egress: from a VPN exit outside the West Coast, ``www.ccld.dss.ca.gov``
    sometimes fails DNS outright ("DNS lookup failed: no results"). When a proxy
    pool is configured (``webshare.env`` or ``-a proxies=host:port,...``), the
    spider probes each proxy's egress location at startup and tries the ones
    that exit in California first — through an HTTPS proxy the hostname is
    resolved at the proxy, sidestepping the local resolver — moving to the next
    on any download failure, and falling back to the host (direct) egress last.
    Once one egress succeeds, the remaining downloads jump straight to it.
    Disable with ``-a proxies=off``; with no pool configured the spider runs
    direct exactly as before.
    """

    name = "california"
    allowed_domains = ["www.ccld.dss.ca.gov"]
    start_urls = [
        "https://www.ccld.dss.ca.gov/transparencyapi/api/DownloadStateData?id=ChildCareCenters&GUID=8cdb2366-1db9-4977-bf5a-06ae048b824d",
        "https://www.ccld.dss.ca.gov/transparencyapi/api/DownloadStateData?id=CHILDCAREHOMEmorethan8&GUID=8cdb2366-1db9-4977-bf5a-06ae048b824d",
    ]

    def __init__(self, *args, proxies=None, proxy_env=None, proxy_probe_timeout=12, proxy_retries=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_probe_timeout = float(proxy_probe_timeout)
        # Scrapy retries per proxy attempt (default 1 → 2 tries) so a dead proxy
        # is abandoned quickly; the host fallback keeps the global RETRY_TIMES.
        self.proxy_retries = int(proxy_retries)
        # Named egress_pool (not proxy_pool) on purpose: ProxyPoolMiddleware keys
        # its per-IP rotation/reporting off ``spider.proxy_pool``; this spider
        # pins its proxy per request itself and wants none of that.
        if proxies is not None and str(proxies).lower() in _OFF_VALUES:
            self.egress_pool = None
        else:
            self.egress_pool = load_pool(
                env_path=proxy_env or DEFAULT_PROXY_ENV,
                endpoints=None if proxies is None else proxies,
                id_prefix="webshare",
            )
        # Ordered egress attempts (California proxies, then host), built in
        # start_requests. ``_working_attempt`` is the first index that produced
        # a successful download, so later failures skip straight to it.
        self._attempts = None
        self._working_attempt = None

    def _build_attempts(self):
        attempts = []
        if self.egress_pool:
            for pid, url, _geo in select_california_proxies(
                self.egress_pool, timeout=self.proxy_probe_timeout, logger=self.logger
            ):
                attempts.append({"label": f"proxy {pid} ({proxy_host(url)})", "proxy": url})
            if attempts:
                self.logger.info("CA egress plan: %d California proxy attempt(s), then host fallback", len(attempts))
            else:
                self.logger.warning(
                    "CA egress plan: no proxy in the pool (%d) egresses in California — using host only",
                    len(self.egress_pool),
                )
        attempts.append({"label": "host (direct)", "proxy": None})
        return attempts

    def _attempt_request(self, url, index, headers):
        """Build the request for ``url`` on egress attempt ``index``, or None
        past the end of the chain."""
        if not self._attempts or index >= len(self._attempts):
            return None
        attempt = self._attempts[index]
        meta = {"ca_attempt": index, "ca_headers": headers}
        if attempt["proxy"]:
            meta["proxy"] = attempt["proxy"]
            meta["max_retry_times"] = self.proxy_retries
        return scrapy.Request(
            url,
            headers=headers,
            callback=self.parse,
            errback=self._on_download_error,
            meta=meta,
            # Every attempt re-requests the same URL, which the dupefilter would
            # otherwise drop after the first.
            dont_filter=True,
        )

    def _on_download_error(self, failure):
        request = failure.request
        index = request.meta.get("ca_attempt", 0)
        label = self._attempts[index]["label"] if self._attempts and index < len(self._attempts) else "?"
        next_index = index + 1
        if self._working_attempt is not None and self._working_attempt > index:
            next_index = self._working_attempt
        retry = self._attempt_request(request.url, next_index, request.meta.get("ca_headers"))
        if retry is None:
            self.logger.error(
                "CA download failed on every egress (last: %s) for %s: %s",
                label,
                request.url,
                failure.getErrorMessage(),
            )
            return
        self.logger.warning(
            "CA download via %s failed (%s: %s) — trying %s for %s",
            label,
            failure.type.__name__ if failure.type else "error",
            failure.getErrorMessage(),
            self._attempts[next_index]["label"],
            request.url,
        )
        yield retry

    def start_requests(self):
        headers = {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://www.ccld.dss.ca.gov/carefacilitysearch/DownloadData",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                " (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
            ),
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
        }

        self._attempts = self._build_attempts()
        for url in self.start_urls:
            yield self._attempt_request(url, 0, headers)

    def parse(self, response):
        """
        Parses the CSV response and yields a dictionary for each row.
        """
        self.logger.info(f"Parsing file from {response.url}")

        # getattr: response.meta raises AttributeError on a request-less response.
        index = (getattr(response, "meta", None) or {}).get("ca_attempt")
        if index is not None and self._attempts and index < len(self._attempts):
            if self._working_attempt is None or index < self._working_attempt:
                self._working_attempt = index
            self.logger.info("CA download succeeded via %s", self._attempts[index]["label"])

        # DownloadStateData answers 200 with an empty body when the state's export
        # is down, so an empty response looks like a clean run unless we flag it.
        if not response.text.strip():
            self.logger.error(
                f"Empty CSV body from {response.url} (HTTP {response.status}). "
                "The CA DownloadStateData endpoint is returning Content-Length: 0 - "
                "check https://www.ccld.dss.ca.gov/carefacilitysearch/ for an outage notice."
            )
            return

        # The response body is a string, so we use StringIO to treat it like a file
        csv_file = StringIO(response.text)

        # Use DictReader to easily parse the CSV into dictionaries
        reader = csv.DictReader(csv_file)

        for row in reader:
            provider = ProviderItem()
            provider["source_state"] = "California"
            provider["provider_type"] = row["Facility Type"]
            provider["license_number"] = row["Facility Number"]
            provider["provider_name"] = row["Facility Name"]
            provider["license_holder"] = row["Licensee"]
            provider["administrator"] = row["Facility Administrator"]
            provider["phone"] = row["Facility Telephone Number"]
            provider["address"] = (
                f"{row['Facility Address']}, {row['Facility City']}, {row['Facility State']} {row['Facility Zip']}"
            )
            provider["county"] = row["County Name"]
            provider["ca_regional_office"] = row["Regional Office"]
            provider["capacity"] = row["Facility Capacity"]
            provider["status"] = row["Facility Status"]
            # Have this as CA specific for now, might be able to map it better later
            provider["ca_license_first_date"] = row["License First Date"]
            provider["ca_closed_date"] = row["Closed Date"]
            provider["ca_citation_numbers"] = row["Citation Numbers"]
            provider["ca_all_visit_dates"] = row["All Visit Dates"]
            provider["ca_inspection_visit_dates"] = row["Inspection Visit Dates"]
            provider["ca_other_visit_dates"] = row["Other Visit Dates"]
            provider["ca_complaint_info"] = row[
                "Complaint Info- Date, #Sub Aleg, # Inc Aleg, # Uns Aleg, # TypeA, # TypeB ..."
            ]
            provider["ca_inspect_typea"] = row["Inspect TypeA"]
            provider["ca_inspect_typeb"] = row["Inspect TypeB"]
            provider["ca_other_typea"] = row["Other TypeA"]
            provider["ca_other_typeb"] = row["Other TypeB"]

            yield provider
