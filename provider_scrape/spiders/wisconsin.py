"""Spider for Wisconsin's Child Care Finder (childcarefinder.wisconsin.gov).

Site architecture (researched 2026-08)
--------------------------------------
The finder is a **Blazor Server** web app (``blazor.web.js`` +
``<!--Blazor:{"type":"server"}-->``) on IIS/ASP.NET behind an F5 load
balancer, protected by **reCAPTCHA v3** (the page head loads
``recaptcha/api.js?render=<v3 key>``; the Search button fires
``grecaptcha.execute(siteKey, {action:'submit'})`` via ``onSearchSubmit``).

Consequences that shape this spider:

* There is **no replayable JSON/HTTP API** -- the form submit and result
  pagination ride the SignalR websocket circuit, so we cannot mirror an
  underlying fetch the way Minnesota (viewstate POST) or Rhode Island (Aura
  ApexAction) do. We must drive the real UI with Playwright.
* reCAPTCHA v3 is score-based, so we reuse the Rhode Island stealth stack:
  real headed Chrome under ``xvfb-run`` with ``--enable-unsafe-swiftshader``
  and playwright-stealth. See :data:`_STEALTH_SCRIPT` and the launch options.
* The ``SearchResults?...`` and ``ProviderDetails?...`` pages render as full
  server-side HTML on GET, which is what :meth:`parse_detail` parses.

Crawl strategy
--------------
One search per county/tribe (the proven Minnesota pattern -- avoids any
statewide result cap and keeps each reCAPTCHA pass small). For each county we
tick the provider-type toggles (Group Centers + Family Providers + Day Camps)
so every regulated provider is returned, walk the paginated results in the
live browser page, and fetch each provider's detail page for the full record
plus its enforcement / monitoring / violation history as ``InspectionItem``s.

.. note::
   The reCAPTCHA-passing search submission, the result pagination control, and
   the option of handing the session to cheap HTTP GETs are verified against
   the live site in Phase 2. The HTML parsing (:func:`_rows_from_selector`,
   :meth:`parse_detail`) is fully covered by unit tests against captured
   fixtures.
"""

import platform
import re
from urllib.parse import urljoin

import scrapy
import scrapy.signals
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
from scrapy_playwright.page import PageMethod

from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.playwright_utils import PlaywrightErrbackMixin

SEARCH_URL = "https://childcarefinder.wisconsin.gov/"

# County / tribe options from the search form's `IndexModel.County` <select>.
# The value is the id the Blazor form binds; the label is the dropdown text.
# Tribal nations use ids 84-95 (there is no id 90). Value 0 (blank) is skipped.
COUNTIES = [
    (1, "Adams County"), (2, "Ashland County"), (3, "Barron County"),
    (4, "Bayfield County"), (5, "Brown County"), (6, "Buffalo County"),
    (7, "Burnett County"), (8, "Calumet County"), (9, "Chippewa County"),
    (10, "Clark County"), (11, "Columbia County"), (12, "Crawford County"),
    (13, "Dane County"), (14, "Dodge County"), (15, "Door County"),
    (16, "Douglas County"), (17, "Dunn County"), (18, "Eau Claire County"),
    (19, "Florence County"), (20, "Fond Du Lac County"), (21, "Forest County"),
    (22, "Grant County"), (23, "Green County"), (24, "Green Lake County"),
    (25, "Iowa County"), (26, "Iron County"), (27, "Jackson County"),
    (28, "Jefferson County"), (29, "Juneau County"), (30, "Kenosha County"),
    (31, "Kewaunee County"), (32, "La Crosse County"), (33, "Lafayette County"),
    (34, "Langlade County"), (35, "Lincoln County"), (36, "Manitowoc County"),
    (37, "Marathon County"), (38, "Marinette County"), (39, "Marquette County"),
    (40, "Milwaukee County"), (41, "Monroe County"), (42, "Oconto County"),
    (43, "Oneida County"), (44, "Outagamie County"), (45, "Ozaukee County"),
    (46, "Pepin County"), (47, "Pierce County"), (48, "Polk County"),
    (49, "Portage County"), (50, "Price County"), (51, "Racine County"),
    (52, "Richland County"), (53, "Rock County"), (54, "Rusk County"),
    (55, "St. Croix County"), (56, "Sauk County"), (57, "Sawyer County"),
    (58, "Shawano County"), (59, "Sheboygan County"), (60, "Taylor County"),
    (61, "Trempealeau County"), (62, "Vernon County"), (63, "Vilas County"),
    (64, "Walworth County"), (65, "Washburn County"), (66, "Washington County"),
    (67, "Waukesha County"), (68, "Waupaca County"), (69, "Waushara County"),
    (70, "Winnebago County"), (71, "Wood County"), (72, "Menominee County"),
    (84, "Menominee Tribe"), (85, "Red Cliff Tribe"),
    (86, "Stockbridge-Munsee Tribe"), (87, "Potawatomi Tribe"),
    (88, "Lac Du Flambeau Tribe"), (89, "Bad River Tribe"),
    (91, "Sokaogon Tribe"), (92, "Oneida Nation"), (93, "Ho Chunk Nation"),
    (94, "Lac Courte Oreilles Tribe"), (95, "St. Croix Tribe"),
]

# Provider-type toggles in the "Your Child Care Needs" section. Ticking all
# three returns every regulated provider type for the selected county.
PROVIDER_TYPE_TOGGLES = [
    "#IncludeGroupCenters",
    "#IncludeFamilyProviders",
    "#IncludeDayCamps",
]

_CANVAS_PATCH = """
const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(...args) {
    const ctx = this.getContext('2d');
    if (ctx) {
        const imageData = ctx.getImageData(0, 0, this.width, this.height);
        for (let i = 0; i < imageData.data.length; i += 4) {
            imageData.data[i] ^= 1;
        }
        ctx.putImageData(imageData, 0, 0);
    }
    return origToDataURL.apply(this, args);
};
"""

_HW_PATCH = """
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
"""

# WebGL vendor/renderer masking is OS-conditional, exactly as in Rhode Island:
# on the GPU-less Linux server (xvfb + SwiftShader) we hand reCAPTCHA an honest
# "Google Inc. (Intel)" software-GL string; on a dev Mac we leave stealth's
# default off so the native Metal renderer flows through unchanged.
_WEBGL_VENDOR_LINUX = "Google Inc. (Intel)"
_WEBGL_RENDERER_LINUX = (
    "ANGLE (Intel, Intel(R) UHD Graphics 630 (0x00003E9B) Direct3D11 "
    "vs_5_0 ps_5_0, D3D11)"
)

_stealth_kwargs = dict(
    navigator_languages_override=("en-US", "en"),
    webgl_vendor=False,
)
if platform.system() == "Linux":
    _stealth_kwargs.update(
        webgl_vendor=True,
        webgl_vendor_override=_WEBGL_VENDOR_LINUX,
        webgl_renderer_override=_WEBGL_RENDERER_LINUX,
    )

_STEALTH_SCRIPT = Stealth(**_stealth_kwargs).script_payload

assert "webdriver" in _STEALTH_SCRIPT, (
    "playwright-stealth script_payload missing webdriver patch — check version"
)

_CITY_STATE_ZIP = re.compile(r"^(.*?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$")

# Results pager caption, e.g. "Showing 1 - 50 out of 93". Drives page logging
# and the deterministic end-of-results stop condition.
_SHOWING = re.compile(r"Showing\s+(\d+)\s*-\s*(\d+)\s+out\s+of\s+(\d+)", re.I)


def _showing(selector):
    """Parse the "Showing lo - hi out of total" caption into (lo, hi, total).

    Returns None when the caption is absent (e.g. a no-results page).
    """
    caption = selector.xpath("string(//p[contains(., 'out of')][1])").get()
    if not caption:
        return None
    match = _SHOWING.search(caption)
    return tuple(int(g) for g in match.groups()) if match else None


def _clean(value):
    """Collapse whitespace; return None for empty/whitespace-only strings."""
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _string(node):
    """XPath string() of a selector node, whitespace-cleaned."""
    if node is None:
        return None
    return _clean(node.xpath("string(.)").get())


def _labeled(root, label):
    """Value of the sibling div following a Bold label div in the same row.

    The provider-details block renders each datum as
    ``<div class="col-* Bold">Label</div><div class="col-*">Value</div>``.
    """
    parts = root.xpath(
        ".//div[contains(@class,'Bold')][normalize-space(.)=$l]"
        "/following-sibling::div[1]//text()",
        l=label,
    ).getall()
    return _clean(" ".join(parts))


def _next_row_value(root, label):
    """Value that lives in the row *after* a Bold label's row (not a sibling).

    Used for "Ages Served", where the value is a full-width row below the
    label rather than a same-row sibling cell.
    """
    parts = root.xpath(
        ".//div[contains(@class,'Bold')][normalize-space(.)=$l]"
        "/ancestor::div[contains(@class,'row')][1]"
        "/following-sibling::div[contains(@class,'row')][1]//text()",
        l=label,
    ).getall()
    return _clean(" ".join(parts))


def _split_city_state_zip(line):
    """Split "Madison WI 53717-1013" into (city, state, zip). None on no match."""
    if not line:
        return None, None, None
    match = _CITY_STATE_ZIP.match(line)
    if not match:
        return None, None, None
    return match.group(1).strip() or None, match.group(2), match.group(3)


def _rows_from_selector(selector, base_url, county):
    """Extract one stub dict per provider row from a SearchResults page.

    Robust to whether the rows are wrapped in ``<table>``/``<tbody>``: rows are
    selected by the presence of a ``ProviderDetails`` link. ``base_url`` is used
    to absolutise the relative detail hrefs.
    """
    stubs = []
    rows = selector.xpath("//tr[.//a[contains(@href,'ProviderDetails')]]")
    for row in rows:
        anchor = row.xpath(".//a[contains(@href,'ProviderDetails')]")
        href = anchor.xpath("./@href").get()
        if not href:
            continue
        cells = row.xpath("./td")
        provider_type = _clean(cells[1].css("div::text").get()) if len(cells) > 1 else None
        rating = row.css("td div.text-nowrap::attr(title)").get()
        address = ", ".join(
            p for p in (_clean(t) for t in row.css("address::text").getall()) if p
        )
        stubs.append({
            "detail_url": _absolute(base_url, href),
            "provider_name": _clean(anchor.xpath("string(.)").get()),
            "provider_type": provider_type,
            "wi_youngstar_rating": _clean(rating),
            "address": address or None,
            "county": county,
        })
    return stubs


def _absolute(base_url, href):
    """Join a relative href to base_url (detail/report links are relative)."""
    return urljoin(base_url, href)


class StealthContextMiddleware:
    """Apply playwright-stealth + fingerprint patches at the browser-context
    level (identical approach to the Minnesota / Rhode Island spiders).
    """

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        crawler.signals.connect(
            mw.spider_opened, signal=scrapy.signals.spider_opened
        )
        return mw

    def spider_opened(self, spider):
        from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler

        handlers = spider.crawler.engine.downloader.handlers._handlers
        handler = handlers.get("https")
        if not isinstance(handler, ScrapyPlaywrightDownloadHandler):
            spider.logger.warning(
                "StealthContextMiddleware: scrapy-playwright handler not "
                "found; stealth patches NOT applied."
            )
            return

        original = handler._create_browser_context
        # Capture the spider from spider_opened for logging: the handler calls
        # _create_browser_context during engine start with spider=None (see
        # scrapy_playwright.handler._launch), so the `spider` argument below
        # can't be relied on to be set — using it directly raises
        # AttributeError: 'NoneType' object has no attribute 'logger'.
        log_spider = spider

        async def patched_create_context(name, context_kwargs=None, spider=None):
            wrapper = await original(
                name, context_kwargs=context_kwargs, spider=spider
            )
            await wrapper.context.add_init_script(_STEALTH_SCRIPT)
            await wrapper.context.add_init_script(_CANVAS_PATCH)
            await wrapper.context.add_init_script(_HW_PATCH)
            log_spider.logger.info(
                "StealthContextMiddleware: stealth patches applied to "
                "context '%s' (browser %s)",
                name,
                wrapper.context.browser.version,
            )
            return wrapper

        handler._create_browser_context = patched_create_context
        spider.logger.info(
            "StealthContextMiddleware: patched _create_browser_context."
        )


class WisconsinSpider(PlaywrightErrbackMixin, scrapy.Spider):
    name = "wisconsin"
    allowed_domains = ["childcarefinder.wisconsin.gov"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 90 * 1000,
        "DOWNLOADER_MIDDLEWARES": {
            "provider_scrape.spiders.wisconsin.StealthContextMiddleware": 100,
        },
        # headless:False + real Chrome under xvfb-run is what yields a passable
        # reCAPTCHA v3 score (see the Rhode Island spider for the full rationale
        # behind --enable-unsafe-swiftshader on GPU-less Linux).
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,
            "channel": "chrome",
            "args": (
                ["--ozone-platform=x11", "--enable-unsafe-swiftshader"]
                if platform.system() == "Linux" else []
            ) + (
                ["--window-size=1440,900", "--force-device-scale-factor=2"]
                if platform.system() == "Darwin" else []
            ),
            "timeout": 30 * 1000,
        },
        # IMPORTANT: scrapy-playwright reads PLAYWRIGHT_CONTEXTS (plural). There
        # is NO PLAYWRIGHT_CONTEXT_ARGS setting — writing context options there
        # silently drops them (that bug hid Minnesota's timezone). The default
        # context is created at startup from this dict.
        #
        # timezone_id is America/New_York to match our single Eastern VPN exit,
        # NOT Wisconsin's Central zone: reCAPTCHA v3 weighs IP-geo vs. timezone
        # consistency, and a TZ/IP mismatch is exactly what failed RI's v3. If
        # the run ever moves to a Central-time exit, change this to match.
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
                "viewport": {"width": 1440, "height": 900},
                "device_scale_factor": 2,
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }
        },
    }

    def __init__(
        self,
        counties=None,
        max_providers=None,
        settle_ms=6000,
        results_timeout=30000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Optional comma-separated county-id filter for debug runs, e.g.
        # `-a counties=40,13` to crawl only Milwaukee and Dane.
        if counties:
            wanted = {int(c) for c in str(counties).split(",")}
            self._counties = [(cid, n) for (cid, n) in COUNTIES if cid in wanted]
        else:
            self._counties = list(COUNTIES)
        self.max_providers = int(max_providers) if max_providers else None
        self.settle_ms = int(settle_ms)
        # How long to wait for provider rows before concluding a county is
        # empty. Non-empty results render in a few seconds; empty counties/tribes
        # (many tribal entries) render an empty page with no rows and no "no
        # results" text, so we must bound this rather than wait indefinitely.
        self.results_timeout_ms = int(results_timeout)
        self._provider_count = 0

    def start_requests(self):
        """One Playwright search per county/tribe."""
        for county_id, county_name in self._counties:
            yield scrapy.Request(
                SEARCH_URL,
                callback=self.parse_county,
                errback=self.errback_close_page,
                dont_filter=True,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_retry": True,
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_load_state", "domcontentloaded",
                            timeout=60000,
                        ),
                        # Let the Blazor circuit + reCAPTCHA v3 script settle so
                        # the score window is open before we submit.
                        PageMethod("wait_for_timeout", self.settle_ms),
                    ],
                    "county_id": county_id,
                    "county_name": county_name,
                },
            )

    async def parse_county(self, response):
        """Drive the search form for one county, then walk the result pages.

        Phase 2 (live): submit the form (passing reCAPTCHA v3), then loop the
        paginated results in the same browser page, yielding a detail request
        per provider. Detail pages are fetched with Playwright by default so
        they inherit the winning fingerprint + session.
        """
        page = response.meta["playwright_page"]
        county_id = response.meta["county_id"]
        county_name = response.meta["county_name"]
        try:
            await self._submit_search(page, county_id, county_name)
            page_num = 0
            total_rows = 0
            while True:
                page_num += 1
                selector = scrapy.Selector(text=await page.content())
                stubs = _rows_from_selector(selector, page.url, county_name)
                showing = _showing(selector)
                self.logger.info(
                    "[%s] results page %d: %d providers (%s)",
                    county_name, page_num, len(stubs),
                    "showing %d-%d of %d" % showing if showing else "no count",
                )
                for stub in stubs:
                    if self.max_providers and self._provider_count >= self.max_providers:
                        self.logger.info(
                            "max_providers=%d reached; stopping.",
                            self.max_providers,
                        )
                        return
                    self._provider_count += 1
                    yield self._detail_request(stub)
                total_rows += len(stubs)
                # Stop when the caption says we've shown the last row, else turn
                # the page over the Blazor circuit.
                if showing and showing[1] >= showing[2]:
                    break
                if not await self._go_to_next_page(page, county_name, page_num):
                    break
            self.logger.info(
                "[%s] done: %d pages, %d providers",
                county_name, page_num, total_rows,
            )
        finally:
            await page.close()

    async def _submit_search(self, page, county_id, county_name):
        """Select the county, tick provider-type toggles, and click Search.

        Verified live (2026-08): the three provider-type toggles bind over the
        circuit, reCAPTCHA v3 passes, and Search navigates to
        ``SearchResults?County=<id>&...``. (Whether the advanced
        Licensed/Certified toggles further widen coverage is still open.)
        """
        await page.select_option(
            "select[name='IndexModel.County']", value=str(county_id)
        )
        for toggle in PROVIDER_TYPE_TOGGLES:
            locator = page.locator(toggle)
            if await locator.count():
                await locator.check(force=True)
        self.logger.info("[%s] submitting search", county_name)
        await page.click("#btnSearch")
        # The reCAPTCHA v3 execute + Blazor submit navigates to SearchResults.
        await page.wait_for_url("**/SearchResults*", timeout=90000)
        try:
            await page.wait_for_selector(
                "a[href*='ProviderDetails']", timeout=self.results_timeout_ms
            )
        except PlaywrightTimeoutError:
            # Empty counties/tribes render a SearchResults page with no rows and
            # no "no results" text -- there is nothing row-shaped to wait for.
            # Don't burn the full timeout (or wedge the browser): confirm the
            # results component rendered (its Back-to-Search / Save buttons are
            # present on both empty and populated pages) so we can tell a
            # genuinely empty county from a stalled render, then let parse_county
            # read zero rows and finish this county cleanly.
            rendered = bool(
                await page.locator("button.childcaresearch-button").count()
            )
            self.logger.warning(
                "[%s] no provider rows within %ds (results rendered=%s) — "
                "treating as 0 providers",
                county_name, self.results_timeout_ms // 1000, rendered,
            )

    async def _go_to_next_page(self, page, county_name, page_num):
        """Advance to the next results page in-browser. Returns False at the end.

        The live pager is a bare Blazor ``<button>Next ></button>`` (plus
        First/Prev/Last) that turns the page over the SignalR circuit -- no URL
        change, no page query param. There are two identical pagers (above and
        below the table); clicking the first is enough.
        """
        next_button = page.locator("button", has_text="Next").first
        if not await next_button.count():
            return False
        first_href = await page.locator(
            "a[href*='ProviderDetails']"
        ).first.get_attribute("href")
        await next_button.click()
        try:
            # Wait until the first provider link changes (page turned over the
            # circuit) rather than a fixed sleep.
            await page.wait_for_function(
                "(prev) => {"
                "  const a = document.querySelector(\"a[href*='ProviderDetails']\");"
                "  return a && a.getAttribute('href') !== prev;"
                "}",
                arg=first_href,
                timeout=30000,
            )
        except Exception:
            self.logger.warning(
                "[%s] next-page wait timed out after page %d; stopping.",
                county_name, page_num,
            )
            return False
        return True

    def _detail_request(self, stub):
        return scrapy.Request(
            stub["detail_url"],
            callback=self.parse_detail,
            errback=self.errback_close_page,
            dont_filter=True,
            meta={
                "playwright": True,
                "playwright_retry": True,
                "playwright_page_methods": [
                    PageMethod(
                        "wait_for_selector",
                        "#providerDetailsCollapsible",
                        timeout=60000,
                    ),
                ],
                "stub": stub,
            },
        )

    def parse_detail(self, response):
        """Parse a ProviderDetails page into a ProviderItem + inspections."""
        stub = response.meta.get("stub", {})
        item = ProviderItem()
        item["source_state"] = "Wisconsin"
        item["provider_url"] = response.url
        item["county"] = stub.get("county")

        pd = response.css("#providerDetailsCollapsible")

        # Name / street / city-state-zip / contact live in the left column.
        left = pd.css(".col-md-3")
        item["provider_name"] = (
            _clean(left.css(".col-12.Bold::text").get())
            or stub.get("provider_name")
        )
        col12 = [_clean(t) for t in left.css(".col-12::text").getall()]
        col12 = [t for t in col12 if t]
        street = None
        city_state_zip = None
        for line in col12[1:]:  # skip the name
            if line == "Contact Information":
                break
            if _CITY_STATE_ZIP.match(line):
                city_state_zip = line
            elif street is None:
                street = line
        parts = [p for p in (street, city_state_zip) if p]
        item["address"] = ", ".join(parts) if parts else stub.get("address")
        city, state, zip_code = _split_city_state_zip(city_state_zip)
        item["city"] = city
        item["state"] = state or "WI"
        item["zip"] = zip_code
        item["phone"] = _clean(left.css("a[href^='tel:']::text").get())
        item["administrator"] = _next_row_value(left, "Contact Information") \
            or _labeled(left, "Contact Information")

        # Identifiers + regulation.
        item["license_number"] = _labeled(pd, "Provider #")
        item["wi_location_number"] = _labeled(pd, "Location #")
        # Certified (non-licensed) providers have no facility number; the page
        # renders "N/A" there, which we normalise to None.
        facility_number = _labeled(pd, "Facility #")
        if facility_number and facility_number.strip().upper() == "N/A":
            facility_number = None
        item["wi_facility_number"] = facility_number
        # Some providers have no current regulation; the Regulation Type field
        # then holds a notice sentence rather than a type. Capture it as status
        # and fall back to the results-list type so provider_type stays a real
        # category value (and doesn't pollute facility_category).
        regulation = _labeled(pd, "Regulation Type")
        if regulation and regulation.lower().startswith("no active license"):
            item["status"] = regulation
            regulation = None
        item["provider_type"] = regulation or stub.get("provider_type")
        item["license_holder"] = _labeled(pd, "Applicant/Licensee")

        item["ages_served"] = _next_row_value(pd, "Ages Served")
        item["wi_months_open"] = _labeled(pd, "Months Open")
        item["capacity"] = _labeled(pd, "Day Capacity")
        item["wi_night_capacity"] = _labeled(pd, "Night Capacity")
        item["hours"] = self._parse_hours(response)

        # YoungStar quality rating (state-specific per the field playbook).
        ys = response.css("#youngstarDetailsCollapsible")
        item["wi_youngstar_rating"] = (
            _clean(ys.css("div.text-nowrap::attr(title)").get())
            or stub.get("wi_youngstar_rating")
        )
        item["wi_unique_services"] = [
            t for t in (_clean(x) for x in ys.css("table td::text").getall()) if t
        ] or None

        # Accreditation (common field).
        item["accreditation"] = _clean(
            response.css("#FacilityHeader_Accreditation .card-text::text").get()
        )

        self._parse_provider_reported(response, item)

        inspections = self._parse_inspections(response)
        item["inspections"] = inspections
        violations = [i for i in inspections if i.get("type") == "Violation"]
        item["deficiencies"] = len(violations) if violations else None

        yield item

    def _parse_hours(self, response):
        """Compose the Mon-Fri / Sat-Sun grid into a single string."""
        parts = [
            _clean(x)
            for x in response.xpath(
                "//div[@id='divHoursOfOperation']"
                "//div[contains(@class,'Bold')][normalize-space(.)='Hours']"
                "/following-sibling::div//text()"
            ).getall()
        ]
        parts = [p for p in parts if p]
        if not parts:
            return None
        # parts alternate day-range, time e.g. ["Mon-Fri", "6:00AM - 6:00PM",
        # "Sat-Sun", "Closed"].
        pairs = []
        for i in range(0, len(parts) - 1, 2):
            pairs.append(f"{parts[i]} {parts[i + 1]}")
        return "; ".join(pairs) if pairs else None

    def _parse_provider_reported(self, response, item):
        # Always define the fields so an absent section yields None, not a
        # missing key.
        item["wi_special_care_types"] = None
        item["wi_program_philosophy"] = None
        item["wi_vacancies"] = None
        item["wi_waitlist"] = None
        prd = response.css("#providerReportedDetailsCollapsible")
        if not prd:
            return
        item["wi_special_care_types"] = [
            t for t in (_clean(x) for x in prd.css("#typesOfCare td::text").getall())
            if t
        ] or None
        item["wi_program_philosophy"] = self._text_after_h3(prd, "Program Philosophy")
        item["wi_vacancies"] = self._text_after_h3(prd, "Vacancies")
        item["wi_waitlist"] = self._text_after_h3(prd, "Waitlist")

    @staticmethod
    def _text_after_h3(root, title):
        """Value that follows an <h3> heading within its parent block.

        The value may be an element (``<span>``/``<div>``) or a bare text node,
        and Blazor scatters ``<!--!-->`` comment placeholders in between. Rather
        than walk fragile sibling nodes, take the heading's parent string and
        strip the heading title prefix — the parent blocks for Philosophy /
        Vacancies / Waitlist contain only the heading and its value.
        """
        full = _string(root.xpath(".//h3[normalize-space(.)=$t]/parent::*", t=title))
        if not full:
            return None
        if full.startswith(title):
            full = full[len(title):]
        return _clean(full)

    def _parse_inspections(self, response):
        """Build the inspections list from the three desktop history tables."""
        inspections = []
        inspections.extend(self._parse_enforcement(response))
        inspections.extend(self._parse_monitoring(response))
        inspections.extend(self._parse_violations(response))
        return inspections

    @staticmethod
    def _desktop_rows(response, section_id):
        """Data rows of the desktop (non-mobile) table within a section.

        Each history section renders twice — a desktop ``.d-md-block`` table and
        a ``.d-md-none`` mobile accordion. We take the desktop table only so
        rows aren't double-counted.
        """
        return response.css(
            f"#{section_id} .d-md-block table.Grid"
        ).xpath(".//tr[td]")

    def _parse_enforcement(self, response):
        out = []
        for row in self._desktop_rows(response, "EnforcementSection"):
            cells = row.xpath("./td")
            if len(cells) < 6:
                continue
            insp = InspectionItem()
            insp["type"] = "Enforcement"
            insp["date"] = _string(cells[1])
            insp["wi_enforcement_type"] = _string(cells[2])
            insp["wi_appeal"] = _string(cells[3])
            insp["wi_decision"] = _string(cells[4])
            insp["wi_description"] = _string(cells[5])
            out.append(dict(insp))
        return out

    def _parse_monitoring(self, response):
        out = []
        for row in self._desktop_rows(response, "VisitSection"):
            cells = row.xpath("./td")
            if len(cells) < 5:
                continue
            insp = InspectionItem()
            insp["type"] = "Monitoring"
            insp["date"] = _string(cells[1])
            insp["original_status"] = _string(cells[2])
            summary = cells[3].css("a::attr(href)").get()
            correction = cells[4].css("a::attr(href)").get()
            insp["report_url"] = _absolute(response.url, summary) if summary else None
            insp["wi_correction_plan_url"] = (
                _absolute(response.url, correction) if correction else None
            )
            out.append(dict(insp))
        return out

    def _parse_violations(self, response):
        out = []
        for row in self._desktop_rows(response, "ViolationSection"):
            cells = row.xpath("./td")
            if len(cells) < 4:
                continue
            insp = InspectionItem()
            insp["type"] = "Violation"
            insp["date"] = _string(cells[0])
            insp["wi_rule_number"] = _string(cells[1])
            rule_link = cells[1].css("a::attr(href)").get()
            insp["report_url"] = rule_link
            insp["wi_rule_summary"] = _string(cells[2])
            insp["wi_description"] = _string(cells[3])
            out.append(dict(insp))
        return out
