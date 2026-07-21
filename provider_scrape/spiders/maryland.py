import asyncio
import json
import logging
import os
import re

import pypdfium2 as pdfium
import scrapy
import tesserocr
from scrapy import signals
from twisted.internet import task

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.proxy_pool import load_pool

# tessdata path for tesserocr — bundled fast model
TESSDATA_DIR = os.environ.get("TESSDATA_PREFIX", "/tmp/tessdata")

# Default location of the (git-ignored) proxy-pool env file, at the repo root —
# same convention as huggingface.env. Absent ⇒ single-IP mode.
DEFAULT_PROXY_ENV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "webshare.env",
)

# Maryland EXCELS "Find a Program" public API. Keyed by license number, it
# returns the precise street address (with the house number the licensing site
# omits from HTML), lat/long coordinates, and the EXCELS rating breakdown — in
# one fast (~1-2s) JSON call on a separate, non-tarpitting domain. This replaces
# the slow inspection-report PDF download + OCR for the ~91% of inspected
# providers that participate in EXCELS. See maryland_performance_epic.
EXCELS_SEARCH_URL = (
    "https://findaprogram.marylandexcels.org/api/fap/search?license={license}"
)
EXCELS_REFERER = "https://findaprogram.marylandexcels.org/"

# Realistic browser UA applied to every request ("stealth-lite") to reduce the
# chance of tripping checkccmd.org's anti-bot protection under concurrency.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Results/pagination requests are cheap but chain-critical: each results page is
# what enqueues both the next page and that page's detail requests. Give them a
# priority well above the default (0) so a pagination postback jumps ahead of
# the large, server-throttled backlog of detail requests. Otherwise a postback
# can sit queued for hours behind the details, by which point its ASP.NET
# ViewState has gone stale and the retry silently returns the *previous* page —
# truncating that county's pagination chain (see maryland_performance_epic).
RESULTS_PRIORITY = 100

# A stale postback returns the wrong page; we re-issue the navigation toward the
# page we actually wanted. Bound those re-issues so repeated stale returns for
# one page are surfaced (logged at ERROR) and give up rather than looping.
MAX_NAV_ATTEMPTS = 5

# When a pagination postback fails terminally at the downloader level (all
# retries exhausted — its callback never runs, so self-healing can't see it),
# the errback re-issues it. Bound those restarts per county so a genuinely dead
# endpoint can't loop forever; a final give-up is left to the closed() guardrail.
MAX_CHAIN_RESTARTS = 3

# Per-request download timeouts for the idempotent, non-chain-critical fetches.
# These deliberately override the patient 180s default (kept only for the
# chain-critical pagination postbacks — see custom_settings / _navigate_to).
# A detail page or EXCELS lookup that has not responded by these thresholds is
# almost certainly tarpitted by the per-IP throttle and would time out at 180s
# anyway; cutting early frees the slot for a retry that may land in a calmer
# moment, so failing fast is net faster and — because these requests are
# idempotent and don't carry chain-critical ViewState — carries no truncation
# risk. Measured full run: 2,165 requests timed out at the 180s default,
# ~108 slot-hours. Detail pages render in ~16s nominal; EXCELS is a sub-5s JSON
# API, so these leave generous headroom over the happy path.
DETAIL_DOWNLOAD_TIMEOUT = 60
EXCELS_DOWNLOAD_TIMEOUT = 30

# All provider-detail GETs share one long-lived cookie jar instead of pinning to
# their county's pagination session. A detail page needs only *any* warm
# checkccmd session cookie plus the SearchResults referer — the ``fi`` is a
# global key, not county-scoped (verified live 2026-07-18) — so it does NOT need
# the session that ran the county search. Pinning details to the county jar was
# the 2026-07 stall: pagination holds the single 33s slot for many hours, so the
# huge detail backlog only drains afterwards, by which point every county session
# has idled out and each detail silently bounced to the search page (~70% of
# providers dropped, invisibly). One jar used continuously (a detail every ~33s)
# stays warm for the whole run, and single-flight (concurrency 1 on the host)
# means sharing it never causes ASP.NET session-lock contention.
DETAIL_COOKIEJAR = "__details__"

# The referer the licensing site requires on a detail GET (it redirects to the
# search page otherwise). RefererMiddleware sets it automatically on the normal
# path — details are followed from the SearchResults page — so we only set it
# explicitly when re-issuing a bounced detail.
SEARCH_RESULTS_REFERER = "https://www.checkccmd.org/SearchResults.aspx"

# A detail GET that lands on a non-detail page means the shared session was cold
# (the first detail of the run) or briefly expired. The bounce response itself
# sets a fresh session cookie on the shared jar, so we simply re-issue the
# request. Bounded so a genuinely bad ``fi`` can't loop, and the final give-up is
# loud (ERROR) — never the old silent drop that let the stall hide.
MAX_DETAIL_REPRIMES = 3

# Adaptive shard splitting. A (county, facility-type) search that still declares
# more than this many providers paginates too deep — deep ASP.NET "skip to page
# N" postbacks get tarpitted past 180s and truncate/deadlock. When a top-level
# type shard's first page declares over the threshold, we re-issue it split by
# license status (one sub-search per status) instead of paginating it, keeping
# every chain shallow. 700 providers is ~50 pages at 14 rows/page; the biggest
# type shard (family homes in the largest counties) is ~1,240 and splits into
# sub-shards that top out near this bound. Sub-shards are not split further.
SHARD_SPLIT_THRESHOLD = 700

# No-progress stall watchdog. Maryland runs have wedged two different ways with
# nothing above DEBUG logged: a *spin* (responses keep arriving at ~2/min but
# items/pages stay frozen, the 2026-07 50h stall) and a *deadlock* (every request
# times out, so responses flatline to zero — e.g. the proxy pool where each big
# county's deep-page pagination postback, pinned to one proxy at high priority,
# times out at 180s and monopolizes that proxy's slot while the site/proxies are
# unresponsive). This watchdog catches BOTH: forward progress is "a newly parsed
# results page OR a scraped item," and if none happens across a window — whether
# or not responses are still flowing — that window counts as stalled.
#
# Progress = pages + items on purpose: during the healthy pagination phase items
# stay ~flat by design (details are deprioritized), but new pages are progress, so
# the watchdog stays quiet then. It ALERTS (ERROR) only after STALL_ALERT_WINDOWS
# consecutive stalled windows, so a transient slow patch (search ramp-up, a burst
# of tarpitted searches that later resolve) doesn't cry wolf. If the stall
# persists to STALL_CLOSE_WINDOWS it FORCE-CLOSES the spider (unless disabled with
# ``-a stall_close=off``) so a dead run ends — flushing the items collected so far
# and firing the completeness guardrails — instead of burning hours doing nothing.
STALL_CHECK_INTERVAL = 600
STALL_ALERT_WINDOWS = 3
STALL_CLOSE_WINDOWS = 6

# Degraded-throughput floor: min forward progress (new results pages + items) per
# check window below which the run is "crawling but barely." A healthy detail
# drain is ~13 items/window and healthy pagination ~18 pages/window, while the
# proxy-throttle stalls that hide from the zero-progress watchdog trickle along at
# ~1-4/window (a 2026-07 pool run spent ~80min at ~1/window — every request timing
# out on all proxies at once — yet the zero-progress counter kept resetting on the
# trickle, so NOTHING was logged). A sustained run of sub-floor-but-nonzero windows
# now WARNs (pointing at the [proxy-pool] err lines) but never force-closes: a
# trickle means the run may still recover (that one did), so only a true, sustained
# ZERO still escalates to the STALL error + force-close.
STALL_DEGRADED_MIN_PROGRESS = 5

# Seconds between consecutive requests to the checkccmd.org host. The site now
# enforces a hard per-IP request-rate limit (IIS Dynamic IP Restrictions): a
# **trailing ~60s window that allows only 2 requests**, returning a stock IIS
# 403 once exceeded — site-wide per IP. While over the limit the block is
# self-sustaining (each blocked request keeps the window saturated); it clears
# only after ~60s of silence. Safe condition: no 3 requests within any 60s
# window, i.e. spacing must be strictly > 30s (three requests must span > 60s).
# 30.0s sits *exactly* on the failing edge (verified live: a 30s-spaced crawl
# tripped on its 3rd request), so we use 33s for ~10% margin — three requests
# span 66s. Single-flight (CONCURRENT_REQUESTS_PER_DOMAIN=1), jitter off
# (RANDOMIZE_DOWNLOAD_DELAY's 0.5x low end would dip under 30s); Scrapy's single
# slot enforces this floor between dispatches regardless of request priority.
# Tune with ``-a delay=<seconds>`` but do NOT go <= 30 without a proxy pool.
# NOTE: at ~1.8/min a full ~12k-request run is ~4-5 days from one IP — the
# per-IP wall, not concurrency, is the limiter, so a real speed-up needs IP
# rotation. See maryland_performance_epic + RateLimitBackoffMiddleware (recovers
# the occasional edge trip instead of dropping it).
DEFAULT_DELAY = 33


def extract_address_from_pdf(pdf_bytes):
    """Extract the precise address from an inspection report PDF via OCR.

    The PDFs are image-based with a consistent form layout. The ADDRESS field
    is on page 1, containing street, city, state, and zip in a row.
    """
    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        try:
            page = pdf[0]
            bitmap = page.render(scale=2)
            img = bitmap.to_pil()
        finally:
            pdf.close()

        # Crop to the ADDRESS region (consistent position across all reports)
        # At 2x scale on landscape letter (3300x2550), the ADDRESS row is ~y=1750-1950
        crop = img.crop((0, 1750, 3300, 1950))

        text = tesserocr.image_to_text(crop, path=TESSDATA_DIR)
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Find "ADDRESS:" and extract the address from the same or next line
        for i, line in enumerate(lines):
            if "ADDRESS" in line.upper():
                # Address may be after the colon on the same line
                after_colon = line.split(":", 1)[-1].strip()
                if after_colon:
                    return _format_address(after_colon)
                # Or on the next line
                if i + 1 < len(lines):
                    return _format_address(lines[i + 1])
                break

        return None
    except Exception as e:
        logging.getLogger("maryland").warning(f"OCR address extraction failed: {e}")
        return None


def _format_address(raw):
    """Format a raw OCR address string into 'street, city, state zip'."""
    # tesserocr returns: "325 N Howard Street Baltimore MD 21201"
    # Try to parse into components using the state code as anchor
    match = re.match(r"^(.+?)\s+(MD)\s+(\d{5}(?:-\d{4})?)$", raw.strip())
    if match:
        street_city = match.group(1).strip()
        state = match.group(2)
        zipcode = match.group(3)
        # The last word(s) before MD is the city — but city can be multi-word
        # so just return as "street city, MD zipcode"
        return f"{street_city}, {state} {zipcode}"
    return raw.strip()


class MarylandSpider(scrapy.Spider):
    name = "maryland"
    allowed_domains = ["checkccmd.org", "findaprogram.marylandexcels.org"]
    start_urls = ["https://www.checkccmd.org/"]

    custom_settings = {
        # checkccmd.org enforces a hard per-IP request-rate limit (see
        # DEFAULT_DELAY): crawl the licensing host strictly single-flight. Every
        # checkccmd request (search, detail, pagination, and the PDF fallback —
        # all the same IP) shares this one slot, so the whole host stays under
        # the ceiling. DOWNLOAD_DELAY is set from the ``delay`` arg in
        # from_crawler (default DEFAULT_DELAY); jitter is off so the spacing has
        # a hard floor under the limit.
        # Per-slot concurrency 1 = single-flight. In single-IP mode there is one
        # checkccmd slot; with a proxy pool each proxy gets its own download slot
        # (ProxyPoolMiddleware), so this cap applies per IP and the proxies run
        # single-flight in parallel.
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RANDOMIZE_DOWNLOAD_DELAY": False,
        # EXCELS is a separate, non-throttling JSON API — give its host its own
        # fast slot so address/coord enrichment never bottlenecks behind the
        # delayed licensing crawl. Global concurrency is high enough that EXCELS
        # runs alongside the checkccmd hits AND, under a proxy pool, every proxy
        # slot can be in flight at once (N proxies + the EXCELS slot + PDFs).
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_SLOTS": {
            "findaprogram.marylandexcels.org": {"concurrency": 4, "delay": 0.0},
        },
        # Pagination postbacks are chain-critical, so they keep the patient
        # default download timeout (180s) and retry budget rather than
        # fail-fast: a verification run with a short 45s timeout caused a
        # postback to *give up at the downloader level* (callback never runs, so
        # self-healing can't engage) and silently truncated a county at page
        # 57/106. The terminal-failure case is caught by the pagination errback
        # (_pagination_errback). AutoThrottle is intentionally NOT enabled — the
        # server's per-IP ceiling is the real limiter, and latency-driven
        # backoff only compounded the slowdown.
        "RETRY_TIMES": 10,
        "USER_AGENT": USER_AGENT,
        # Recover a per-IP rate-limit 403 instead of dropping it: pause the
        # checkccmd slot for a cooldown (real silence clears the rolling window;
        # a fast retry just re-trips it) and re-issue the request. 403 is NOT in
        # Scrapy's default RETRY_HTTP_CODES, so without this a blocked request is
        # silently lost — a prior run shed ~50% of providers exactly this way.
        # Scoped to checkccmd so the clean EXCELS API is never throttled.
        "RATELIMIT_BACKOFF_ENABLED": True,
        "RATELIMIT_BACKOFF_DOMAINS": ["checkccmd.org"],
        "RATELIMIT_BACKOFF_HTTP_CODES": [403],
        "RATELIMIT_BACKOFF_COOLDOWN": 60,
        "RATELIMIT_BACKOFF_MAX_RETRIES": 8,
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Apply the (tunable) single-flight spacing to the checkccmd host. Set
        # here rather than in custom_settings so ``-a delay=<seconds>`` works.
        crawler.settings.set("DOWNLOAD_DELAY", spider.delay, priority="spider")
        # NOTE: from_crawler runs *before* Scrapy attaches the LOG_FILE handler
        # (Crawler.crawl calls _create_spider ahead of _update_root_log_handler),
        # so anything logged here never reaches the log file. Defer the run-mode
        # banner and start the stall watchdog on spider_opened, which fires once
        # logging is fully configured.
        crawler.signals.connect(spider._log_run_mode, signal=signals.spider_opened)
        crawler.signals.connect(
            spider._start_stall_watch, signal=signals.spider_opened
        )
        return spider

    def _log_run_mode(self, *args, **kwargs):
        """Log single-IP vs proxy-pool mode. Connected to spider_opened (not done
        in from_crawler) so the line actually lands in the log file."""
        if self.proxy_pool:
            self.logger.info(
                "MarylandSpider: proxy pool ENABLED — %d egress IPs (%s); each "
                "held to single-flight DOWNLOAD_DELAY=%ss on its own slot; "
                "EXCELS direct.",
                len(self.proxy_pool),
                ", ".join(self.proxy_pool.ids),
                self.delay,
            )
        else:
            self.logger.info(
                "MarylandSpider: single-IP mode, checkccmd single-flight "
                "DOWNLOAD_DELAY=%ss (per-IP rate limit); EXCELS on its own fast "
                "slot.",
                self.delay,
            )

    # Only checkccmd.org egresses through the proxy pool; the EXCELS API is a
    # separate, non-throttling host and stays direct on its own fast slot.
    proxy_pool_domains = ["checkccmd.org"]

    def __init__(
        self,
        ocr_fallback=True,
        counties=None,
        delay=DEFAULT_DELAY,
        proxies=None,
        proxy_env=None,
        stall_close=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Seconds between consecutive checkccmd.org requests, per egress IP.
        self.delay = float(delay)
        # If the no-progress stall watchdog sees a persistent wedge, force-close
        # the spider rather than let it hang for hours. On by default; disable
        # (leave a dead run hanging, e.g. for debugging) with -a stall_close=off.
        self.stall_close = str(stall_close).lower() not in (
            "false", "0", "no", "off"
        )
        # Optional multi-IP proxy pool (opt-in). Off by default: with no
        # webshare.env and no ``-a proxies=``, the crawl egresses from the single
        # host IP exactly as before. Disable explicitly with ``-a proxies=off``
        # even when an env file is present; supply endpoints inline with
        # ``-a proxies="host:port,host:port"`` (credentials still read from the
        # env file, or embed full ``http://user:pass@host:port`` URLs).
        if proxies is not None and str(proxies).lower() in (
            "off", "none", "false", "0", "no", ""
        ):
            self.proxy_pool = None
        else:
            self.proxy_pool = load_pool(
                env_path=proxy_env or DEFAULT_PROXY_ENV,
                endpoints=None if proxies is None else proxies,
                id_prefix="webshare",
            )
        self.seen_fi = set()
        # Pages whose rows we've successfully parsed, per county. Used to avoid
        # re-extracting a page that's delivered twice (e.g. a late retry) and to
        # decide whether forward navigation still has somewhere to go.
        self.parsed_pages_by_county = {}
        # How many times we've issued a navigation postback for a given page,
        # per county. Caps stale-postback self-healing at MAX_NAV_ATTEMPTS.
        self.nav_attempts_by_county = {}
        # How many times a county's chain has been restarted from a terminal
        # downloader failure (errback). Capped at MAX_CHAIN_RESTARTS.
        self.chain_restarts_by_county = {}
        # Completeness guardrail: the provider count the results page declares
        # for each county, and how many provider rows we actually paginated
        # through. Compared at spider close so a truncated chain is loud, not a
        # silently "finished" short run (see closed()).
        self.declared_total_by_county = {}
        self.found_count_by_county = {}
        # Monotonic count of results pages whose rows we've parsed, across all
        # counties. Feeds the no-progress stall watchdog: a new page is forward
        # progress even while items are (by design) flat during pagination.
        self._pages_parsed = 0
        # Consecutive no-progress windows seen by the stall watchdog (reset by
        # _start_stall_watch at open; here so a direct _check_stall is safe).
        self._stall_windows = 0
        # Consecutive sub-floor (degraded-but-alive) windows; drives the DEGRADED
        # WARN. Same rationale for defaulting here as _stall_windows above.
        self._slow_windows = 0
        # When EXCELS has no address for a provider, fall back to the slow
        # inspection-report PDF + OCR. Disable (``-a ocr_fallback=false``) for a
        # pure-fast run that downloads zero PDFs.
        self.ocr_fallback = str(ocr_fallback).lower() not in ("false", "0", "no")
        # Optional debug filter: ``-a counties="Howard,Carroll"`` restricts the
        # crawl to counties whose dropdown label contains one of these terms
        # (case-insensitive). Used for limited verification runs.
        if counties:
            self._county_filter = [c.strip().lower() for c in str(counties).split(",")]
        else:
            self._county_filter = None

    def parse(self, response):
        """Extract form options, then launch a fresh session per county."""
        self.logger.info("Loaded search page, extracting form options...")

        self._fac_types = response.css(
            "#MainContent_ddlFacType option::attr(value)"
        ).getall()
        self._license_statuses = response.css(
            "#MainContent_ddlLicenseStatus option::attr(value)"
        ).getall()
        # (value, label) per status, for splitting an oversized shard by status.
        # The value is a comma-joined status list; the label ("Open", "Closed",
        # …) makes a readable sub-shard key.
        self._license_status_options = [
            (
                o.attrib.get("value", ""),
                (o.css("::text").get() or "").strip(),
            )
            for o in response.css("#MainContent_ddlLicenseStatus option")
            if o.attrib.get("value")
        ]
        counties = response.css(
            "#MainContent_ddlCountyList option::attr(value)"
        ).getall()
        self._cities = response.css(
            "#MainContent_ddlCityList option::attr(value)"
        ).getall()

        if self._county_filter:
            counties = [
                c
                for c in counties
                if any(term in c.lower() for term in self._county_filter)
            ]
            self.logger.info(
                f"County debug filter active — restricting to: {counties}"
            )

        self.logger.info(
            f"Form options: {len(self._fac_types)} types, "
            f"{len(self._license_statuses)} statuses, "
            f"{len(counties)} counties, {len(self._cities)} cities "
            f"— launching one search per (county, facility type) shard"
        )

        # Shard each county's search by facility type. A single county search
        # (all types) for a big county paginates 60-140 deep, and deep ASP.NET
        # "skip to page N" postbacks get tarpitted past 180s and truncate/deadlock
        # (see the 2026-07 stalls). Per-type shards keep each chain shallow, and —
        # crucially under the proxy pool — the deepest 'Homes' shards spread across
        # proxies instead of one big county monopolizing a single IP.
        #
        # Loop ORDER matters: facility type OUTER, county INNER. The proxy pool
        # assigns sticky affinity round-robin in request order, so with types
        # outer each type's shards fan across all proxies (the four deepest 'Homes'
        # shards land on four different proxies). Types inner would map every
        # 'Homes' shard to the same proxy (type index mod proxy count) and rebuild
        # the monopoly. Each shard gets its own cookie jar + fresh ViewState.
        for fac_type in self._fac_types:
            for county in counties:
                shard = self._shard_key(county, fac_type)
                self.parsed_pages_by_county[shard] = set()
                self.nav_attempts_by_county[shard] = {}
                self.chain_restarts_by_county[shard] = 0
                yield scrapy.Request(
                    "https://www.checkccmd.org/",
                    callback=self.parse_county_search,
                    cb_kwargs={
                        "county_key": shard,
                        "county": county,
                        "fac_type": fac_type,
                    },
                    meta={"cookiejar": shard, "proxy_affinity": shard},
                    dont_filter=True,
                    priority=RESULTS_PRIORITY,
                )

    @staticmethod
    def _shard_key(county, fac_type):
        """Pagination-tracking key for one (county, facility-type) search shard."""
        return f"{county} [{fac_type}]"

    def parse_county_search(self, response, county_key, county, fac_type, status=None):
        """Submit the search for one shard.

        A top-level shard (``status is None``) selects one facility type across
        all license statuses and is eligible to be split by status if it comes
        back too large. A status sub-shard selects that single status and is not
        split further.
        """
        self.logger.info(f"[{county_key}] Submitting search...")
        statuses = [status] if status is not None else self._license_statuses
        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                # One facility type per shard (vs all types) keeps the result set
                # small so pagination stays shallow.
                "ctl00$MainContent$ddlFacType": [fac_type],
                "ctl00$MainContent$ddlLicenseStatus": statuses,
                "ctl00$MainContent$ddlCountyList": [county],
                "ctl00$MainContent$ddlCityList": self._cities,
                "ctl00$MainContent$SearchButton": "SEARCH",
            },
            callback=self.parse_results,
            cb_kwargs={
                "county_key": county_key,
                "expected_page": 1,
                # Only a top-level type shard may split by status.
                "allow_split": status is None,
                "county": county,
                "fac_type": fac_type,
            },
            meta={"cookiejar": county_key, "proxy_affinity": county_key},
            dont_click=True,
            dont_filter=True,
            priority=RESULTS_PRIORITY,
        )

    def _split_shard_by_status(self, county, fac_type):
        """Re-issue an oversized type shard as one sub-search per license status.

        Each sub-shard gets its own session/jar and proxy affinity (so they fan
        across the pool), and is not split further. The oversized parent is not
        paginated — the sub-shards cover the same providers, shallower.
        """
        for status_value, status_label in self._license_status_options:
            sub = self._shard_key(county, f"{fac_type}/{status_label}")
            self.parsed_pages_by_county[sub] = set()
            self.nav_attempts_by_county[sub] = {}
            self.chain_restarts_by_county[sub] = 0
            yield scrapy.Request(
                "https://www.checkccmd.org/",
                callback=self.parse_county_search,
                cb_kwargs={
                    "county_key": sub,
                    "county": county,
                    "fac_type": fac_type,
                    "status": status_value,
                },
                meta={"cookiejar": sub, "proxy_affinity": sub},
                dont_filter=True,
                priority=RESULTS_PRIORITY,
            )

    def parse_results(
        self,
        response,
        county_key=None,
        expected_page=1,
        allow_split=False,
        county=None,
        fac_type=None,
    ):
        """Parse the search results page, with self-healing pagination.

        Pagination is a sequential chain: each results page enqueues the next.
        Under load the licensing site throttles, and a paginated postback that
        times out and is retried can come back rendering the *previous* page —
        its ASP.NET ViewState went stale. We detect that mismatch
        (``current_page != expected_page``) and re-issue the navigation toward
        the page we actually wanted, using this fresh response, bounded by
        ``MAX_NAV_ATTEMPTS``. Previously such a stale postback silently
        truncated the county's chain (the old dedup guard refused to re-request
        the page), which dropped ~60% of providers in a full run.

        ``allow_split`` (set only for a top-level type shard's first page) lets an
        oversized shard re-issue itself split by license status instead of
        paginating too deep — see ``_split_shard_by_status``.
        """
        pager_row = response.css("tr.dataPager")
        current_page = 1
        if pager_row:
            current_page_text = pager_row.css("span::text").get()
            if current_page_text and current_page_text.strip().isdigit():
                current_page = int(current_page_text.strip())

        # Self-heal a stale postback: the server rendered a different page than
        # we navigated to. Re-issue the navigation toward the page we wanted
        # (this fresh response carries a usable ViewState) without extracting
        # the wrong page's rows.
        if current_page != expected_page:
            self.logger.warning(
                f"[{county_key}] expected page {expected_page} but server "
                f"returned page {current_page} (stale postback) — re-navigating."
            )
            yield from self._navigate_to(response, county_key, expected_page)
            return

        total_text = response.css("#MainContent_lblTotalRows::text").get()
        self.logger.info(f"[{county_key}] page {current_page} — Total: {total_text}")

        total_match = re.search(r"(\d+)", total_text) if total_text else None
        declared = int(total_match.group(1)) if total_match else None

        # Adaptive split: if a top-level type shard is still too big, re-issue it
        # split by license status (shallower chains) instead of paginating deep.
        # Done on the first page only, before recording/paginating, so the parent
        # isn't double-counted — the sub-shards cover the same providers.
        if (
            allow_split
            and current_page == 1
            and county is not None
            and declared is not None
            and declared > SHARD_SPLIT_THRESHOLD
        ):
            self.logger.info(
                f"[{county_key}] declares {declared} providers "
                f"(> {SHARD_SPLIT_THRESHOLD}) — splitting by license status."
            )
            yield from self._split_shard_by_status(county, fac_type)
            return

        # Record the declared provider count for this shard (same on every page)
        # for the spider-close completeness check.
        if county_key not in self.declared_total_by_county and declared is not None:
            self.declared_total_by_county[county_key] = declared

        # Skip a page that's already been parsed (e.g. a duplicate/late retry
        # delivery): don't re-extract its rows or re-drive pagination from it.
        parsed_pages = self.parsed_pages_by_county.setdefault(county_key, set())
        if current_page in parsed_pages:
            self.logger.debug(
                f"[{county_key}] page {current_page} already parsed — skipping."
            )
            return
        parsed_pages.add(current_page)
        # Forward progress for the stall watchdog (a new page, even while items
        # stay flat during the pagination phase).
        self._pages_parsed += 1

        # Extract provider detail links, deduplicating by facility ID
        rows = response.css("#grdResults tr.rowStyle")
        # Tally provider rows actually paginated through, per county, vs the
        # declared total — the spider-close completeness guardrail.
        self.found_count_by_county[county_key] = (
            self.found_count_by_county.get(county_key, 0) + len(rows)
        )
        self.logger.info(
            f"[{county_key}] Found {len(rows)} provider rows on page {current_page}."
        )

        for row in rows:
            cols = row.css("td")
            link = cols[0].css("a::attr(href)").get() if cols else None
            if link:
                # Deduplicate by facility ID to avoid re-scraping detail pages
                # when duplicate pagination chains cause the same page to be
                # processed more than once.
                fi_match = re.search(r"fi=(\d+)", link)
                if fi_match:
                    fi = fi_match.group(1)
                    if fi in self.seen_fi:
                        continue
                    self.seen_fi.add(fi)

                # Extract fields only available on the results page
                address = (
                    cols[2].css("::text").get("").strip() if len(cols) > 2 else None
                )
                school_name = (
                    cols[4].css("::text").get("").strip() if len(cols) > 4 else None
                )
                program_type = (
                    cols[5].css("::text").get("").strip() if len(cols) > 5 else None
                )

                # Details ride one shared, self-warming session jar rather than
                # this county's pagination session (see DETAIL_COOKIEJAR): the fi
                # is a global key and a detail needs only a warm session + the
                # SearchResults referer, so pinning to the county jar bought no
                # concurrency under single-flight and only created the stall where
                # details drained after their county session had died.
                # response.follow sets the referer from this results page
                # (SearchResults.aspx) automatically.
                yield response.follow(
                    link,
                    callback=self.parse_detail,
                    cb_kwargs={
                        "address": address or None,
                        "school_name": school_name or None,
                        "program_type": program_type or None,
                    },
                    # Idempotent, non-chain-critical: fail fast (not 180s) and let
                    # RETRY_TIMES re-fetch a tarpitted page rather than tie up a
                    # per-IP slot for three minutes.
                    meta={
                        "cookiejar": DETAIL_COOKIEJAR,
                        "download_timeout": DETAIL_DOWNLOAD_TIMEOUT,
                    },
                )

        # Advance the chain to the next page (sequential, with windowed-pager
        # "..." jumps). The end of pagination is when no next link is offered.
        next_target = self._resolve_next_page(pager_row, current_page)
        if next_target and next_target not in parsed_pages:
            yield from self._navigate_to(response, county_key, next_target)

    @staticmethod
    def _resolve_next_page(pager_row, current_page):
        """Return the next page number to navigate to, or None at the last page.

        The pager renders only a window of page links; when the immediate next
        page isn't linked, a "..." link jumps to the start of the next window.
        """
        if not pager_row:
            return None
        next_page = current_page + 1
        if pager_row.css(f'a[href*="Page${next_page}"]'):
            return next_page
        # Fall back to the "..." link that jumps to the next set of pages.
        for el_link in pager_row.css("a"):
            text = el_link.css("::text").get("").strip()
            href = el_link.attrib.get("href", "")
            if text == "..." and f"Page${next_page}" not in href:
                match = re.search(r"Page\$(\d+)", href)
                if match:
                    jump_page = int(match.group(1))
                    if jump_page > current_page:
                        return jump_page
        return None

    def _navigate_to(self, response, county_key, target_page):
        """Issue a paginated postback to ``target_page``, bounded by retries.

        Shared by normal forward navigation and stale-postback self-healing, so
        repeated stale returns for the same page are capped at
        ``MAX_NAV_ATTEMPTS`` (and surfaced at ERROR) instead of looping or
        silently dying. The postback is high-priority so it resolves before its
        ViewState can go stale behind the detail-request backlog.
        """
        attempts = self.nav_attempts_by_county.setdefault(county_key, {})
        count = attempts.get(target_page, 0)
        if count >= MAX_NAV_ATTEMPTS:
            self.logger.error(
                f"[{county_key}] gave up navigating to page {target_page} after "
                f"{count} attempts (stale postbacks) — chain truncated here."
            )
            return
        attempts[target_page] = count + 1
        self.logger.info(
            f"[{county_key}] Navigating to page {target_page} (attempt {count + 1})..."
        )
        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                "__EVENTTARGET": "ctl00$MainContent$grdResults",
                "__EVENTARGUMENT": f"Page${target_page}",
            },
            callback=self.parse_results,
            errback=self._pagination_errback,
            cb_kwargs={"county_key": county_key, "expected_page": target_page},
            meta={"cookiejar": county_key, "proxy_affinity": county_key},
            dont_click=True,
            dont_filter=True,
            priority=RESULTS_PRIORITY,
        )

    def _pagination_errback(self, failure):
        """Recover a pagination postback that failed terminally.

        Retries are exhausted at the downloader level, so the callback never
        ran and the self-healing path in ``parse_results`` can't see it — the
        county's chain would otherwise die here. Re-issue the same postback
        (resetting its retry budget) so the navigation gets another full set of
        attempts; if the server returns a stale page, ``parse_results`` self-
        heals from there. Bounded by ``MAX_CHAIN_RESTARTS`` per county; a final
        give-up is surfaced loudly by ``closed()``.
        """
        request = failure.request
        county_key = request.cb_kwargs.get("county_key")
        target_page = request.cb_kwargs.get("expected_page")

        restarts = self.chain_restarts_by_county.get(county_key, 0)
        if restarts >= MAX_CHAIN_RESTARTS:
            self.logger.error(
                f"[{county_key}] pagination to page {target_page} failed "
                f"terminally and exhausted {restarts} chain restarts — chain "
                f"truncated here ({failure.value})."
            )
            return
        self.chain_restarts_by_county[county_key] = restarts + 1
        self.logger.warning(
            f"[{county_key}] pagination to page {target_page} failed terminally "
            f"({failure.value}) — restarting chain (restart {restarts + 1})."
        )
        # Reset the retry counter so the re-issued postback gets a fresh budget,
        # and keep the callback/errback so it parses and can recurse on another
        # terminal failure.
        new_meta = dict(request.meta)
        new_meta.pop("retry_times", None)
        return request.replace(
            meta=new_meta,
            dont_filter=True,
            callback=self.parse_results,
            errback=self._pagination_errback,
        )

    def _start_stall_watch(self, *args, **kwargs):
        """Begin the periodic no-progress check (on spider_opened).

        Accepts the signal's ``spider`` kwarg; ``self`` is already the spider.
        """
        self._stall_last_responses = 0
        self._stall_last_progress = 0
        self._stall_windows = 0
        self._slow_windows = 0
        self._stall_task = task.LoopingCall(self._check_stall)
        # now=False: first check one interval in, once the crawl is underway.
        self._stall_task.start(STALL_CHECK_INTERVAL, now=False)

    def _check_stall(self):
        """Alert on a persistent stall, and separately WARN on degraded throughput.

        Progress = new pages parsed + items scraped, so the healthy pagination
        phase (pages advancing, items flat) is not mistaken for a wedge. The same
        signal drives two thresholds:

        * **Degraded (WARN only):** a run of ``STALL_ALERT_WINDOWS`` windows below
          ``STALL_DEGRADED_MIN_PROGRESS`` — i.e. crawling but barely. This catches
          the proxy-throttle stalls that trickle along at ~1/window and slip past
          the zero-progress check below (a nonzero blip kept resetting it). It
          points at the ``[proxy-pool]`` err lines and NEVER closes: a trickle can
          still recover, so a slow run is surfaced, not killed.
        * **Stalled (ERROR + optional force-close):** a run of consecutive windows
          with *no* progress at all — a spin (responses flow, nothing progresses)
          or a deadlock (every request times out, responses flatline). Unchanged:
          alerts after ``STALL_ALERT_WINDOWS`` and force-closes at
          ``STALL_CLOSE_WINDOWS`` unless disabled.
        """
        stats = getattr(getattr(self, "crawler", None), "stats", None)
        if stats is None:
            return
        responses = stats.get_value("response_received_count", 0) or 0
        items = stats.get_value("item_scraped_count", 0) or 0
        progress = self._pages_parsed + items
        d_resp = responses - self._stall_last_responses
        d_prog = progress - self._stall_last_progress
        self._stall_last_responses = responses
        self._stall_last_progress = progress

        # At/above the floor is healthy — reset both streaks and we're done.
        if d_prog >= STALL_DEGRADED_MIN_PROGRESS:
            self._slow_windows = 0
            self._stall_windows = 0
            return
        # Below the floor: always a slow window; a true zero is also a stall
        # window (the only thing that ERRORs / force-closes). A nonzero trickle
        # resets the zero-streak so it can never force-close a recovering run.
        self._slow_windows += 1
        self._stall_windows = self._stall_windows + 1 if d_prog == 0 else 0

        # Sustained trickle (alive but sub-floor): WARN, never close. A true-zero
        # window falls to the STALL branch below, so we don't double-log it.
        if d_prog > 0 and self._slow_windows >= STALL_ALERT_WINDOWS:
            slow_s = self._slow_windows * STALL_CHECK_INTERVAL
            self.logger.warning(
                f"DEGRADED: only {d_prog} new pages+items in the last "
                f"~{STALL_CHECK_INTERVAL}s (below {STALL_DEGRADED_MIN_PROGRESS}/window "
                f"for ~{slow_s}s) — proxies likely throttling; see the [proxy-pool] "
                f"err lines (pages_parsed={self._pages_parsed}, items={items})."
            )

        if self._stall_windows < STALL_ALERT_WINDOWS:
            return
        stalled_s = self._stall_windows * STALL_CHECK_INTERVAL
        kind = "frozen (no responses)" if d_resp == 0 else f"spinning ({d_resp} responses)"
        self.logger.error(
            f"STALL: no new pages or items for ~{stalled_s}s — {kind} "
            f"(pages_parsed={self._pages_parsed}, items={items})."
        )
        if self.stall_close and self._stall_windows >= STALL_CLOSE_WINDOWS:
            engine = getattr(getattr(self, "crawler", None), "engine", None)
            if engine is not None and not getattr(self, "_stall_closing", False):
                self._stall_closing = True
                self.logger.error(
                    f"Stalled ~{stalled_s}s with no progress — closing the spider "
                    f"(reason=stalled) so it doesn't hang; disable with "
                    f"-a stall_close=off."
                )
                engine.close_spider(self, "stalled")

    def closed(self, reason):
        """Report per-shard completeness when the spider closes.

        Auto-connected to the ``spider_closed`` signal by Scrapy. The crawl can
        report ``finish_reason: finished`` while still having dropped most of a
        (county, facility-type) shard (a truncated pagination chain), so compare
        the provider rows we actually paginated through against the count each
        shard's results page declared and make any shortfall loud (ERROR).
        """
        stall_task = getattr(self, "_stall_task", None)
        if stall_task is not None and stall_task.running:
            stall_task.stop()

        incomplete = []
        total_declared = 0
        total_found = 0
        for shard in sorted(self.declared_total_by_county):
            declared = self.declared_total_by_county[shard]
            found = self.found_count_by_county.get(shard, 0)
            total_declared += declared
            total_found += found
            if found < declared:
                incomplete.append((shard, found, declared))

        n_shards = len(self.declared_total_by_county)
        if incomplete:
            self.logger.error(
                f"Crawl INCOMPLETE ({reason}): paginated {total_found} of "
                f"{total_declared} declared providers across {n_shards} shards; "
                f"{len(incomplete)} short:"
            )
            for shard, found, declared in incomplete:
                self.logger.error(
                    f"  [{shard}] {found}/{declared} "
                    f"({declared - found} missing)"
                )
        else:
            self.logger.info(
                f"Crawl complete ({reason}): paginated all {total_found} "
                f"declared providers across {n_shards} shards."
            )

        # Item-level completeness. The paginated-row check above only proves we
        # walked each county's result rows — NOT that those rows became items.
        # The 2026-07 stall paginated fully but dropped most details, and this
        # guardrail was blind to it. Compare items actually scraped against the
        # declared total so a detail-draining shortfall is loud too.
        stats = getattr(getattr(self, "crawler", None), "stats", None)
        if stats is not None and total_declared:
            scraped = stats.get_value("item_scraped_count", 0) or 0
            if scraped < total_declared:
                self.logger.error(
                    f"Crawl INCOMPLETE ({reason}): scraped {scraped} items but "
                    f"{total_declared} providers were declared "
                    f"({total_declared - scraped} missing) — details did not "
                    f"fully drain."
                )

    def parse_detail(self, response, address=None, school_name=None, program_type=None):
        """Parse a provider detail page.

        A detail GET that redirects away from ``FacilityDetail`` means the shared
        detail session (``DETAIL_COOKIEJAR``) was cold — the first detail of the
        run, or a rare mid-run expiry. Rather than silently dropping the provider
        (the old behavior, which let the 2026-07 stall hide), re-issue it: the
        bounce itself set a fresh session cookie on the shared jar, so the retry —
        same fi, SearchResults referer restored — succeeds. Bounded by
        ``MAX_DETAIL_REPRIMES``; an unrecoverable fi is dropped loudly (ERROR).
        """
        if "FacilityDetail" not in response.url:
            yield from self._reissue_bounced_detail(
                response, address, school_name, program_type
            )
            return

        item = ProviderItem()
        item["source_state"] = "Maryland"
        item["provider_url"] = response.url

        # Fields from the results page (not available on detail page)
        item["address"] = address
        item["md_school_name"] = school_name
        item["provider_type"] = program_type

        # Closed/suspended providers use a different panel with *Op suffixed IDs
        is_non_operating = (
            response.css("#MainContent_PnlNonOperating").get() is not None
        )

        if is_non_operating:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderNameOp"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicenseOp"
            )
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatusOp"
            )
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgramOp"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditationOp"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevelOp"
            )
        else:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderName"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicense"
            )
            item["county"] = self._get_span_text(response, "MainContent_txtCounty")
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatus"
            )
            item["phone"] = self._get_span_text(response, "MainContent_txtPhone")
            item["email"] = self._get_span_text(response, "MainContent_txtEmail")

            # Capacity - may contain total + age breakdowns separated by <br> tags
            capacity_raw = self._get_span_html(response, "MainContent_txtCapacity")
            if capacity_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", capacity_raw) if p.strip()
                ]
                if parts:
                    item["capacity"] = parts[0]
                    if len(parts) > 1:
                        item["ages_served"] = "; ".join(parts[1:])

            # Hours
            hours_raw = self._get_span_html(response, "MainContent_txtHours")
            if hours_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", hours_raw) if p.strip()
                ]
                item["hours"] = "; ".join(parts)

            # Maryland-specific fields
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgram"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditation"
            )
            item["md_fatalities"] = self._get_span_text(
                response, "MainContent_txtFatalities"
            )
            item["md_serious_injuries"] = self._get_span_text(
                response, "MainContent_txtInjuries"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevel"
            )

        # Inspections
        item["inspections"] = self._extract_inspections(response)

        first_report_url = self._get_first_report_url(response)

        # Enrich the precise address (house number for centers) + rooftop
        # coordinates from the Maryland EXCELS public API, keyed by license
        # number. Falls back to the inspection-report PDF + OCR only when EXCELS
        # has no record at all (and ocr_fallback is enabled).
        license_number = item.get("license_number")
        if license_number and str(license_number).strip().isdigit():
            yield scrapy.Request(
                EXCELS_SEARCH_URL.format(license=str(license_number).strip()),
                callback=self.parse_excels,
                cb_kwargs={"item": item, "first_report_url": first_report_url},
                headers={"Referer": EXCELS_REFERER, "Accept": "application/json"},
                # Lightweight JSON API; fail fast and retry rather than inherit
                # the 180s pagination default.
                meta={"download_timeout": EXCELS_DOWNLOAD_TIMEOUT},
                dont_filter=True,
            )
        else:
            yield from self._address_fallback(item, first_report_url)

    def _reissue_bounced_detail(self, response, address, school_name, program_type):
        """Re-issue a detail GET that bounced to the search page (cold session).

        The bounce set a fresh session cookie on the shared detail jar, so the
        re-issued request (same fi, SearchResults referer restored) succeeds.
        Bounded by ``MAX_DETAIL_REPRIMES``; a genuinely unrecoverable fi is
        dropped loudly rather than silently.
        """
        reprimes = response.meta.get("detail_reprimes", 0)
        # After a redirect, ``response.url`` is the search page; the originally
        # requested detail URL is the first entry of ``redirect_urls``.
        original = (response.meta.get("redirect_urls") or [response.url])[0]
        if "FacilityDetail" not in original or reprimes >= MAX_DETAIL_REPRIMES:
            self.logger.error(
                f"Detail dropped: {original} still bounces to {response.url} "
                f"after {reprimes} re-prime(s)."
            )
            return
        self.logger.warning(
            f"Detail session bounce for {original} -> {response.url}; re-issuing "
            f"({reprimes + 1}/{MAX_DETAIL_REPRIMES})."
        )
        yield scrapy.Request(
            original,
            callback=self.parse_detail,
            cb_kwargs={
                "address": address,
                "school_name": school_name,
                "program_type": program_type,
            },
            headers={"Referer": SEARCH_RESULTS_REFERER},
            meta={
                "cookiejar": DETAIL_COOKIEJAR,
                "download_timeout": DETAIL_DOWNLOAD_TIMEOUT,
                "detail_reprimes": reprimes + 1,
            },
            dont_filter=True,
        )

    def parse_excels(self, response, item, first_report_url=None):
        """Enrich the item with a precise address + coordinates from EXCELS.

        The EXCELS ``search?license=`` endpoint returns ``{"data": [ {...} ]}``.
        A record carries rooftop-accurate ``lat``/``long`` for both centers and
        family homes (verified: a family-home coordinate reverse-geocodes to the
        exact house, and a center coordinate matches an independent geocode of
        its house-numbered address to 0 m). So whenever EXCELS has a record we
        capture the precise location and are done — no PDF:

        * **Centers** also expose a house-numbered street address, so we adopt
          it as ``address``.
        * **Family homes** have the house number withheld from the address
          string, but the rooftop coordinate already pins the exact location
          (and the number is recoverable from it by reverse-geocoding), so the
          PDF offers no accuracy upside.

        Only a *true* EXCELS miss (no record at all) optionally falls back to the
        inspection-report PDF + OCR.
        """
        record = None
        try:
            data = json.loads(response.text).get("data")
            if isinstance(data, list) and data:
                record = data[0]
        except (ValueError, AttributeError) as e:
            self.logger.debug(
                f"EXCELS parse failed for license "
                f"{item.get('license_number')}: {e}"
            )

        if record:
            self._apply_excels_location(item, record)
            street = (record.get("streetAddress") or "").strip()
            if street and street[0].isdigit():
                # House-numbered address (centers): adopt the full address.
                item["address"] = self._compose_excels_address(record)
            # Family homes keep the results-page street-name address plus the
            # rooftop coordinates captured above.
            yield item
            return

        # True EXCELS miss: optionally OCR the inspection-report PDF.
        yield from self._address_fallback(item, first_report_url)

    @staticmethod
    def _apply_excels_location(item, record):
        """Set coordinates + structured city/state/ZIP from an EXCELS record.

        The normalization pipeline fills city/state/zip only when absent, so
        these authoritative values are not clobbered downstream.
        """
        lat = record.get("lat")
        lon = record.get("long")
        if lat is not None:
            item["latitude"] = str(lat)
        if lon is not None:
            item["longitude"] = str(lon)
        city = (record.get("city") or "").strip()
        zip_code = (record.get("zipcode") or "").strip()
        if city:
            item["city"] = city
        if (record.get("state") or "").strip().lower().startswith("maryland"):
            item["state"] = "MD"
        if zip_code:
            item["zip"] = zip_code

    def _address_fallback(self, item, first_report_url):
        """Yield the item, using PDF/OCR for a precise address when enabled.

        The ~1 MB inspection PDF is fetched with ``proxy_bypass`` so it egresses
        from the host IP (the default checkccmd slot), NOT the proxy pool —
        verified live that the PDF is public and needs no session cookie or
        referer. This keeps the pool's metered bandwidth for the small
        detail/pagination requests (a PDF is ~30x a detail page). Direct is safe:
        in pool mode the host IP carries only these PDFs, single-flight at the 33s
        host delay, well under the per-IP ceiling; in single-IP mode the flag is a
        no-op and the PDF shares the one host slot as before.
        """
        if self.ocr_fallback and first_report_url:
            yield scrapy.Request(
                first_report_url,
                callback=self.parse_inspection_pdf,
                cb_kwargs={"item": item},
                meta={"proxy_bypass": True},
                dont_filter=True,
            )
        else:
            yield item

    @staticmethod
    def _compose_excels_address(record):
        """Join EXCELS address parts into 'street, city, MD zip'."""
        street = (record.get("streetAddress") or "").strip()
        city = (record.get("city") or "").strip()
        state = (record.get("state") or "").strip()
        if state.lower().startswith("maryland"):
            state = "MD"
        zip_code = (record.get("zipcode") or "").strip()
        state_zip = " ".join(p for p in [state, zip_code] if p)
        city_state_zip = ", ".join(p for p in [city, state_zip] if p)
        return ", ".join(p for p in [street, city_state_zip] if p)

    async def parse_inspection_pdf(self, response, item):
        """Extract precise address from an inspection report PDF via OCR."""
        # Run CPU-bound OCR in a thread pool to avoid blocking the reactor
        pdf_bytes = response.body
        precise_address = await asyncio.to_thread(extract_address_from_pdf, pdf_bytes)
        if precise_address:
            self.logger.debug(
                f"OCR address for {item.get('provider_name')}: {precise_address}"
            )
            item["address"] = precise_address
        yield item

    def _get_first_report_url(self, response):
        """Get the URL of the first inspection report PDF from the detail page."""
        rows = response.css("#MainContent_grdInspection tr")
        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) >= 1:
                link = cols[0].css("a::attr(href)").get()
                if link:
                    return response.urljoin(link)
        return None

    def _extract_inspections(self, response):
        """Extract inspection records from the detail page."""
        inspections = []
        rows = response.css("#MainContent_grdInspection tr")

        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) < 7:
                continue

            insp = InspectionItem()

            report_link = cols[0].css("a::attr(href)").get()
            if report_link:
                insp["report_url"] = response.urljoin(report_link)

            insp["date"] = cols[2].css("::text").get("").strip()
            insp["type"] = cols[3].css("::text").get("").strip()

            md_regulation = cols[4].css("::text").get("").strip()
            md_finding = cols[5].css("::text").get("").strip()
            md_status = cols[6].css("::text").get("").strip()

            insp["md_regulation"] = (
                md_regulation if md_regulation and md_regulation != "\xa0" else None
            )
            insp["md_finding"] = (
                md_finding if md_finding and md_finding != "\xa0" else None
            )
            insp["md_inspection_status"] = (
                md_status if md_status and md_status != "\xa0" else None
            )

            if insp.get("date") or insp.get("type"):
                inspections.append(insp)

        return inspections

    def _get_span_text(self, response, span_id):
        """Extract text from a span element by its ID."""
        texts = response.css(f"#{span_id}::text").getall()
        if texts:
            return " ".join(t.strip() for t in texts if t.strip())
        return None

    def _get_span_html(self, response, span_id):
        """Extract inner HTML from a span element by its ID."""
        span = response.css(f"#{span_id}")
        if span:
            outer = span.get()
            if outer:
                inner = re.sub(r"^<span[^>]*>", "", outer)
                inner = re.sub(r"</span>\s*$", "", inner)
                return inner
        return None
