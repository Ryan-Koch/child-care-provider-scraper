import csv
import io
import random
from urllib.parse import quote

import scrapy
import scrapy.signals
from playwright_stealth import Stealth
from scrapy_playwright.page import PageMethod

from provider_scrape.items import ProviderItem

# Base URL for county results pages — all filter params default to False/All
# so every provider in the county is returned. Server-side filtering happens
# via `con=<CountyName>`; `co=<id>` alone is NOT a filter and will render all
# ~22,000 statewide providers (5.6MB viewstate that then blows past ASP.NET's
# maxRequestLength on the csvdownload POST → HTTP 500). Always pass both.
_RESULTS_BASE = (
    "https://licensinglookup.dhs.state.mn.us/Results.aspx?"
    "a=False&cdtrt=False&crfcc=False&crfmhc=False&e=0&dsfpv=False"
    "&hcbsbss=False&crfss=False&sils62=False&irts=False&qrtp61=False"
    "&crfsc=False&afcfads=False&ppy40=False&rsfsls=False&crsrc=False"
    "&ppy62=False&ppy61=False&dsfeds=False&sn=All&irtsrcs=False"
    "&cdtcwct=False&hcbsihss=False&crssls=False&hcbsics=False"
    "&locked=False&adcrem29=False&sils40=False&ci=All&hcbsds=False"
    "&crfgrs=False&crfts=False&rsfrs=false&hcbsrss=False&cdtsamht=False"
    "&hcbsses=False&hcbsiss=False&crfrt=False&crscr=False&crfprtf=False"
    "&cdtidat=False&stcse40=False&qrtp40=False&crsaost=False&cdtat=False"
    "&rcs40=False&dsfess=False&crfcdc=False&rcs=False&stcse62=False"
    "&stcse61=False&qrtp62=False&crfmhlock=False&dsfees=False&tn=All"
    "&z=&mhc=False&crfd=False&cdtnrt=False&sils61=False&s=All"
    "&afcaost=False&t=All&dsfdth=False&n=&l="
)

_CANVAS_PATCH = """
const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;

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


def _results_url(county_id, county_name):
    """Build the per-county Results.aspx URL.

    `con=<name>` is the real server-side filter; `co=<id>` alone would
    return every provider in MN. Both are sent to match the form the site
    itself builds when you submit the Search page.
    """
    return (
        f"{_RESULTS_BASE}"
        f"&con={quote(county_name, safe='')}"
        f"&co={county_id}"
    )


# Minnesota counties from the Search.aspx `ddlCounty` dropdown. Order matches
# the dropdown (not alphabetical/numeric — the site lists multi-county
# aggregates like "Faribault & Martin" adjacent to their first member). IDs
# 88, 92, 93 are tribal/joint-county aggregates.
COUNTIES = [
    (1, "Aitkin"),
    (2, "Anoka"),
    (3, "Becker"),
    (4, "Beltrami"),
    (5, "Benton"),
    (6, "Big Stone"),
    (7, "Blue Earth"),
    (8, "Brown"),
    (9, "Carlton"),
    (10, "Carver"),
    (11, "Cass"),
    (12, "Chippewa"),
    (13, "Chisago"),
    (14, "Clay"),
    (15, "Clearwater"),
    (16, "Cook"),
    (17, "Cottonwood"),
    (18, "Crow Wing"),
    (19, "Dakota"),
    (20, "Dodge"),
    (21, "Douglas"),
    (22, "Faribault"),
    (92, "Faribault & Martin"),
    (23, "Fillmore"),
    (24, "Freeborn"),
    (25, "Goodhue"),
    (26, "Grant"),
    (27, "Hennepin"),
    (28, "Houston"),
    (29, "Hubbard"),
    (30, "Isanti"),
    (31, "Itasca"),
    (32, "Jackson"),
    (33, "Kanabec"),
    (34, "Kandiyohi"),
    (35, "Kittson"),
    (36, "Koochiching"),
    (37, "Lac Qui Parle"),
    (38, "Lake"),
    (39, "Lake of the Woods"),
    (40, "Le Sueur"),
    (41, "Lincoln"),
    (88, "Lincoln & Lyon & Murray"),
    (42, "Lyon"),
    (44, "Mahnomen"),
    (45, "Marshall"),
    (46, "Martin"),
    (43, "McLeod"),
    (47, "Meeker"),
    (48, "Mille Lacs"),
    (93, "MNPrairie"),
    (49, "Morrison"),
    (50, "Mower"),
    (51, "Murray"),
    (52, "Nicollet"),
    (53, "Nobles"),
    (54, "Norman"),
    (55, "Olmsted"),
    (56, "Otter Tail"),
    (57, "Pennington"),
    (58, "Pine"),
    (59, "Pipestone"),
    (60, "Polk"),
    (61, "Pope"),
    (62, "Ramsey"),
    (63, "Red Lake"),
    (64, "Redwood"),
    (65, "Renville"),
    (66, "Rice"),
    (67, "Rock"),
    (68, "Roseau"),
    (70, "Scott"),
    (71, "Sherburne"),
    (72, "Sibley"),
    (69, "St. Louis"),
    (73, "Stearns"),
    (74, "Steele"),
    (75, "Stevens"),
    (76, "Swift"),
    (77, "Todd"),
    (78, "Traverse"),
    (79, "Wabasha"),
    (80, "Wadena"),
    (81, "Waseca"),
    (82, "Washington"),
    (83, "Watonwan"),
    (84, "Wilkin"),
    (85, "Winona"),
    (86, "Wright"),
    (87, "Yellow Medicine"),
]

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_STEALTH_SCRIPT = Stealth(
    navigator_user_agent_override=_UA,
    navigator_platform_override="MacIntel",
    navigator_languages_override=("en-US", "en"),
    webgl_vendor_override="Intel Inc.",
    webgl_renderer_override="Intel Iris OpenGL Engine",
).script_payload

assert "webdriver" in _STEALTH_SCRIPT, (
    "playwright-stealth script_payload missing webdriver patch — check version"
)


# Columns in the TSV returned by the csvdownload POST, in order.
CSV_COLUMNS = [
    "License Number",
    "License Type",
    "Name of Program",
    "AddressLine1",
    "AddressLine2",
    "AddressLine3",
    "City",
    "State",
    "Zip",
    "County",
    "Phone",
    "License Status",
    "License Holder",
    "Capacity",
    "Type Of License",
    "Restrictions",
    "Services",
    "Licensing Authority",
    "Initial Effective Date",
    "Current Effective Date",
    "Expiration Date",
    "License Holder Lives Onsite",
    "EmailAddress",
]


def _human_delay(lo=800, hi=2000):
    """Return a PageMethod that pauses for a random duration in [lo, hi] ms."""
    return PageMethod("wait_for_timeout", random.randint(lo, hi))


def _compose_address(row):
    """Join the split address columns from the TSV into a single string."""
    street_parts = [
        (row.get(k) or "").strip()
        for k in ("AddressLine1", "AddressLine2", "AddressLine3")
    ]
    street = ", ".join(p for p in street_parts if p)

    city = (row.get("City") or "").strip()
    state = (row.get("State") or "").strip()
    zip_code = (row.get("Zip") or "").strip()
    state_zip = " ".join(p for p in [state, zip_code] if p)
    city_state_zip = ", ".join(p for p in [city, state_zip] if p)

    full = ", ".join(p for p in [street, city_state_zip] if p)
    return full or None


def _row_to_item(row):
    """Convert one TSV row (as a dict keyed by header) into a ProviderItem."""
    item = ProviderItem()
    item["source_state"] = "Minnesota"
    item["provider_url"] = None
    item["license_number"] = (row.get("License Number") or "").strip() or None
    item["provider_type"] = (row.get("License Type") or "").strip() or None
    item["provider_name"] = (row.get("Name of Program") or "").strip() or None
    item["address"] = _compose_address(row)
    item["county"] = (row.get("County") or "").strip() or None
    item["phone"] = (row.get("Phone") or "").strip() or None
    item["status"] = (row.get("License Status") or "").strip() or None
    item["license_holder"] = (row.get("License Holder") or "").strip() or None
    item["capacity"] = (row.get("Capacity") or "").strip() or None
    item["mn_type_of_license"] = (row.get("Type Of License") or "").strip() or None
    item["mn_restrictions"] = (row.get("Restrictions") or "").strip() or None
    item["mn_licensed_to_provide"] = (row.get("Services") or "").strip() or None
    item["license_begin_date"] = (
        row.get("Initial Effective Date") or ""
    ).strip() or None
    item["mn_last_renewed_date"] = (
        row.get("Current Effective Date") or ""
    ).strip() or None
    item["license_expiration"] = (row.get("Expiration Date") or "").strip() or None
    item["mn_license_holder_onsite"] = (
        row.get("License Holder Lives Onsite") or ""
    ).strip() or None
    item["email"] = (row.get("EmailAddress") or "").strip() or None
    return item


def _parse_csv_body(body, logger=None):
    """Parse the CSV body from the csvdownload POST into a list of dict rows.

    The server returns comma-separated values with double-quoted fields and a
    header row (lines also carry a trailing comma). A tab-delimiter pass is
    kept as a defensive fallback. Returns an empty list if the body is empty
    or the header does not match the expected schema.
    """
    if not body:
        return []

    for delimiter in (",", "\t"):
        reader = csv.reader(io.StringIO(body), delimiter=delimiter)
        rows = list(reader)
        if not rows:
            continue
        header = [h.strip() for h in rows[0]]
        if "License Number" in header and "Name of Program" in header:
            return [dict(zip(header, r)) for r in rows[1:] if r]

    if logger:
        logger.warning("csvdownload body did not look like the expected CSV/TSV")
    return []


# In-browser fetch that mirrors the __doPostBack the csvdownload anchor would
# trigger. Runs in the page's JS realm, so it uses Chromium's network stack
# and the live cookie jar (including Imperva's JS-set __uzmd etc.) — which
# Playwright's APIRequestContext cannot match. Hidden inputs are serialized
# in document order; __EVENTTARGET/__EVENTARGUMENT are overridden in place.
_CSV_DOWNLOAD_SCRIPT = """
async ({url, eventTarget}) => {
    const required = ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION'];
    const body = new URLSearchParams();
    const hidden = document.querySelectorAll('form input[type="hidden"]');
    hidden.forEach((el) => {
        if (!el.name) return;
        let value = el.value;
        if (el.name === '__EVENTTARGET') value = eventTarget;
        else if (el.name === '__EVENTARGUMENT') value = '';
        body.append(el.name, value);
    });
    const missing = required.filter((n) => !body.get(n));
    if (missing.length > 0) {
        return {error: 'missing_tokens', missing: missing};
    }
    if (!body.get('__EVENTTARGET')) {
        body.append('__EVENTTARGET', eventTarget);
        body.append('__EVENTARGUMENT', '');
    }
    let resp;
    try {
        resp = await fetch(url, {
            method: 'POST',
            body: body,
            credentials: 'include',
            mode: 'same-origin',
            redirect: 'follow',
        });
    } catch (e) {
        return {error: 'fetch_failed', detail: String(e)};
    }
    return {
        status: resp.status,
        contentType: resp.headers.get('content-type') || '',
        contentDisposition: resp.headers.get('content-disposition') || '',
        body: await resp.text(),
        viewstateSize: (body.get('__VIEWSTATE') || '').length,
        inputCount: hidden.length,
    };
}
"""


class StealthContextMiddleware:
    """Downloader middleware that applies playwright-stealth at the browser
    context level.
    """

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        crawler.signals.connect(mw.spider_opened, signal=scrapy.signals.spider_opened)
        return mw

    def spider_opened(self, spider):
        from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler

        handlers = spider.crawler.engine.downloader.handlers._handlers
        handler = handlers.get("https")

        if not isinstance(handler, ScrapyPlaywrightDownloadHandler):
            spider.logger.warning(
                "StealthContextMiddleware: scrapy-playwright handler not found "
                "for https — stealth context patch not applied."
            )
            return

        original = handler._create_browser_context

        async def patched_create_context(name, context_kwargs=None, spider=None):
            wrapper = await original(name, context_kwargs=context_kwargs, spider=spider)
            await wrapper.context.add_init_script(_STEALTH_SCRIPT)
            await wrapper.context.add_init_script(_CANVAS_PATCH)
            await wrapper.context.add_init_script(_HW_PATCH)
            spider.logger.debug(
                "StealthContextMiddleware: stealth init script added to context '%s'",
                name,
            )

            browser_version = wrapper.context.browser.version
            spider.logger.info(
                "StealthContextMiddleware: browser version: %s (channel=chrome expected)",
                browser_version,
            )

            return wrapper

        handler._create_browser_context = patched_create_context
        spider.logger.info(
            "StealthContextMiddleware: patched _create_browser_context on "
            "scrapy-playwright handler."
        )


class MinnesotaSpider(scrapy.Spider):
    """Spider for Minnesota DHS child care provider licensing data.

    One request per county: load the Results.aspx page in Playwright, capture
    the ASP.NET viewstate tokens, then POST `__EVENTTARGET=csvdownload` via
    the same browser context to receive the full county provider export as
    TSV. Each row becomes one ProviderItem — no detail pages are fetched.
    """

    name = "minnesota"

    custom_settings = {
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60 * 1000,
        "USER_AGENT": _UA,
        "DOWNLOADER_MIDDLEWARES": {
            "provider_scrape.spiders.minnesota.StealthContextMiddleware": 100,
        },
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "channel": "chrome",
            "timeout": 20 * 1000,
        },
        "PLAYWRIGHT_CONTEXT_ARGS": {
            "ignore_https_errors": True,
            "user_agent": _UA,
            "viewport": {"width": 1920, "height": 1080},
            "locale": "en-US",
            "timezone_id": "America/Chicago",
        },
    }

    def __init__(self, county_delay=120, counties=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Seconds to wait between per-county requests (randomized ±50% by
        # scrapy's RANDOMIZE_DOWNLOAD_DELAY). At 120s × 90 counties ≈ 3 hours.
        self.county_delay = float(county_delay)
        if counties:
            wanted = {int(c) for c in str(counties).split(",")}
            self._debug_counties = [
                (cid, name) for (cid, name) in COUNTIES if cid in wanted
            ]
        else:
            self._debug_counties = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.settings.set("DOWNLOAD_DELAY", spider.county_delay, priority="spider")
        spider.logger.info(
            "MinnesotaSpider: DOWNLOAD_DELAY set to %ss per county (±50%% jitter)",
            spider.county_delay,
        )
        return spider

    def start_requests(self):
        """Yield one Playwright request per county results page.

        The callback captures viewstate tokens from the rendered HTML, then
        performs a POST in the same browser context to download the CSV
        export for that county.
        """
        counties = getattr(self, "_debug_counties", None) or COUNTIES
        for county_id, county_name in counties:
            yield scrapy.Request(
                _results_url(county_id, county_name),
                callback=self.parse_county,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle", timeout=90000),
                        _human_delay(500, 3000),
                    ],
                    "county_id": county_id,
                    "county_name": county_name,
                },
            )

    async def parse_county(self, response):
        """Fire the ASP.NET `csvdownload` postback from inside the browser.

        `page.evaluate` runs a fetch() in the page's JS realm, which means
        Chromium's own network stack, TLS fingerprint, and full cookie jar
        (including Imperva's JS-set __uzmd/__uzmc). Playwright's own
        `APIRequestContext` shares cookies but not the fingerprint, and
        Imperva rejects it with a Radware CAPTCHA page.
        """
        county_id = response.meta["county_id"]
        county_name = response.meta.get("county_name", "")
        page = response.meta["playwright_page"]

        status = None
        content_type = ""
        content_disposition = ""
        body_text = ""
        viewstate_size = 0
        input_count = 0

        try:
            if not await page.locator("a#csvdownload").count():
                self.logger.info(
                    "[county %s/%s] No csvdownload link — zero providers",
                    county_id,
                    county_name,
                )
                return

            post_url = page.url
            self.logger.info(
                "[county %s/%s] Firing csvdownload postback via in-page "
                "fetch (url=%s)",
                county_id,
                county_name,
                post_url,
            )

            result = await page.evaluate(
                _CSV_DOWNLOAD_SCRIPT,
                {"url": post_url, "eventTarget": "csvdownload"},
            )

            if result.get("error"):
                self.logger.warning(
                    "[county %s/%s] cannot submit postback: error=%s "
                    "detail=%s",
                    county_id,
                    county_name,
                    result.get("error"),
                    result.get("missing") or result.get("detail"),
                )
                return

            status = result.get("status")
            content_type = result.get("contentType", "") or ""
            content_disposition = result.get("contentDisposition", "") or ""
            body_text = result.get("body", "") or ""
            viewstate_size = result.get("viewstateSize", 0) or 0
            input_count = result.get("inputCount", 0) or 0
        finally:
            await page.close()

        self.logger.info(
            "[county %s/%s] csvdownload response: status=%s type=%r "
            "disposition=%r body=%d bytes "
            "(hidden_inputs=%d, viewstate=%d bytes)",
            county_id,
            county_name,
            status,
            content_type,
            content_disposition,
            len(body_text),
            input_count,
            viewstate_size,
        )

        if not body_text:
            self.logger.warning(
                "[county %s/%s] empty CSV body; skipping",
                county_id,
                county_name,
            )
            return

        head = body_text[:500].lower()
        if "radware captcha" in head or "<html" in head:
            self.logger.warning(
                "[county %s/%s] body looks like HTML/bot challenge; head=%r",
                county_id,
                county_name,
                body_text[:300],
            )
            return

        rows = _parse_csv_body(body_text, logger=self.logger)
        self.logger.info(
            "[county %s/%s] parsed %d provider rows",
            county_id,
            county_name,
            len(rows),
        )

        for row in rows:
            yield _row_to_item(row)
