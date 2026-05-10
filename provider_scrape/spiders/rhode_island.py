import asyncio
import json
import platform
import random
from urllib.parse import quote, urlencode

import scrapy
import scrapy.signals
from playwright_stealth import Stealth
from scrapy_playwright.page import PageMethod

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_PAGE_URL = (
    "https://earlylearningprograms.dhs.ri.gov/s/?language=en_US"
)
AURA_ENDPOINT_PATH = "/s/sfsites/aura?r=1&aura.ApexAction.execute=1"
DETAIL_PAGE_URL_TEMPLATE = (
    "https://earlylearningprograms.dhs.ri.gov/s/program-detail"
    "?language=en_US&pid={pid}&lang=en"
)

# Search pre-filters: ticking every age group satisfies the "at least one
# criteria" requirement and matches the form's behavior of returning the
# full provider catalog. We deliberately omit `includeClosedProviders`.
AGE_GROUPS = [
    "Infant (6 weeks up to 18 months)",
    "Toddler (18 months up to 36 months)",
    "Preschool (3 - 4 yrs)",
    "Pre-K (4 - 5 yrs)",
    "School Age (5 yrs and in Kindergarten through 16 yrs)",
]

# Day order used when collapsing scheduleOfOperationData into a string.
_DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
              "Saturday", "Sunday"]
_DAY_ABBR = {
    "Monday": "Mon",
    "Tuesday": "Tue",
    "Wednesday": "Wed",
    "Thursday": "Thu",
    "Friday": "Fri",
    "Saturday": "Sat",
    "Sunday": "Sun",
}

# We deliberately do NOT override navigator.userAgent or apply the
# webgl_vendor patch here. The fingerprint audit (-a audit=1) showed
# those produced cross-checkable inconsistencies that reCAPTCHA v3
# weighs heavily:
#   - navigator.userAgent reported Chrome/124 while navigator.appVersion
#     and navigator.userAgentData.brands reported the real Chrome version
#     (e.g. 147) — instant "this UA is spoofed" signal.
#   - playwright-stealth's webgl_vendor patch defaults to "Intel Inc." /
#     "Intel Iris OpenGL Engine" — wrong on Apple Silicon, where real
#     Chrome reports "Google Inc. (Apple)" / "ANGLE (Apple, Apple M…)".
#     `webgl_vendor=False` disables that patch entirely so the real GPU
#     strings flow through.
# Stealth still patches navigator.webdriver, plugin arrays, etc. — the
# things that genuinely *are* "wrong" by default.
_STEALTH_SCRIPT = Stealth(
    navigator_platform_override="MacIntel",
    navigator_languages_override=("en-US", "en"),
    webgl_vendor=False,
).script_payload

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

# navigator.deviceMemory is bucketed and capped at 8 GB by the spec so 8
# is the natural value to report on any modern machine. We don't patch
# hardwareConcurrency anymore — letting the real CPU core count surface
# avoids another inconsistency (e.g. an M-series Mac reporting 8 when
# its Metal renderer would suggest 10+).
_HW_PATCH = """
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
"""

# In-page POST that mirrors the Aura ApexAction.execute call the program
# detail page makes natively. Running inside the page's JS realm reuses
# Chromium's TLS fingerprint and the Salesforce session cookies — which
# the lightning runtime sets via JS during the search page load.
_DETAIL_FETCH_SCRIPT = """
async ({url, body}) => {
    let resp;
    try {
        resp = await fetch(url, {
            method: 'POST',
            body: body,
            credentials: 'include',
            mode: 'same-origin',
            headers: {
                'Content-Type':
                    'application/x-www-form-urlencoded;charset=UTF-8',
                'X-SFDC-LDS-Endpoints':
                    'ApexActionController.execute:'
                    + 'RICS_ViewProgramDetailsController.getProgramDetails',
            },
        });
    } catch (e) {
        return {error: 'fetch_failed', detail: String(e)};
    }
    return {status: resp.status, body: await resp.text()};
}
"""


def extract_search_results(payload):
    """Pull the searchResults list out of an Aura ApexAction.execute response.

    Returns [] for any malformed/non-success response so the caller can log
    the body and bail without crashing.
    """
    if not isinstance(payload, dict):
        return []
    actions = payload.get("actions") or []
    if not actions:
        return []
    action = actions[0]
    if action.get("state") != "SUCCESS":
        return []
    inner = (action.get("returnValue") or {}).get("returnValue") or {}
    return inner.get("searchResults") or []


def extract_detail_payload(payload):
    """Return the inner returnValue dict from a getProgramDetails response,
    or None if the response is missing/unsuccessful."""
    if not isinstance(payload, dict):
        return None
    actions = payload.get("actions") or []
    if not actions:
        return None
    action = actions[0]
    if action.get("state") != "SUCCESS":
        return None
    return (action.get("returnValue") or {}).get("returnValue")


def format_hours(schedule_data):
    """Collapse scheduleOfOperationData.data into a 'Mon 7:30 AM-5:00 PM; ...'
    string. Returns None when no day is selected.
    """
    if not schedule_data:
        return None
    by_day = {row.get("name"): row for row in schedule_data if row.get("name")}
    parts = []
    for day in _DAY_ORDER:
        row = by_day.get(day)
        if not row or not row.get("isSelected"):
            continue
        start = row.get("startTimeStr")
        end = row.get("endTimeStr")
        if not start or not end:
            continue
        parts.append(f"{_DAY_ABBR[day]} {start}-{end}")
    return "; ".join(parts) if parts else None


def format_age_group_capacity(age_group_wrapper):
    """Reduce ageGroupServedWrapper to {age_group, classrooms, capacity}
    rows for groups the program is licensed to serve. Returns None when the
    list is empty or every entry is unselected.
    """
    if not age_group_wrapper:
        return None
    rows = []
    for entry in age_group_wrapper:
        if not entry.get("isSelected"):
            continue
        rows.append({
            "age_group": entry.get("name"),
            "classrooms": entry.get("numberOfClassRooms"),
            "capacity": entry.get("totalCapacity"),
        })
    return rows or None


def format_ages_served(acc_availability):
    """Reduce accAvailability to a simple list of age-group names.

    The standard cross-state `ages_served` field is a list of strings
    (see NJ, CO, etc.). RI's source ships a richer per-age-group slot
    structure, but for the shared field we want only the names so
    downstream consumers can compare across states. The full structure
    is still preserved on `ri_availability`.
    """
    if not acc_availability:
        return None
    names = []
    seen = set()
    for entry in acc_availability:
        age = entry.get("ageGroup")
        if age and age not in seen:
            names.append(age)
            seen.add(age)
    return names or None


def format_availability(acc_availability):
    """Reduce accAvailability to {age_group, slot_info} rows, dropping empty
    entries. Returns None when nothing is reported."""
    if not acc_availability:
        return None
    rows = []
    for entry in acc_availability:
        age = entry.get("ageGroup")
        slot = entry.get("slotInfo")
        if not age and not slot:
            continue
        rows.append({"age_group": age, "slot_info": slot})
    return rows or None


def _empty_to_none(val):
    """Normalize empty/placeholder values to None.

    The source API renders missing fields as the literal string "--" (e.g.
    `ccapStatus` and `ccapExpirationDate` for non-CCAP providers). Treat
    that as a null sentinel so it doesn't leak into the output.
    """
    if val is None:
        return None
    if isinstance(val, str):
        stripped = val.strip()
        if not stripped or stripped == "--":
            return None
    if isinstance(val, list) and len(val) == 0:
        return None
    return val


def _summarize_compliance(domains):
    """Roll up domain-level compliance into a single 'compliant/total' string,
    or None if the visit reports no domains."""
    if not domains:
        return None
    compliant = 0
    total = 0
    for d in domains:
        items = d.get("items") or []
        for item in items:
            total += 1
            if not item.get("isNonCompliant"):
                compliant += 1
    if total == 0:
        return None
    return f"{compliant}/{total}"


def build_inspections(visits):
    """Map lstVisits to a list of InspectionItem (lightweight: no per-domain
    detail). Returns [] when there are no visits."""
    if not visits:
        return []
    inspections = []
    for v in visits:
        ins = InspectionItem()
        ins["date"] = _empty_to_none(v.get("visitDateFormatted"))
        ins["type"] = _empty_to_none(v.get("name"))
        ins["report_url"] = _empty_to_none(v.get("visitDownloadURL"))
        # Prefer the visit's own compliance summary string when present
        # (e.g. '87/87'); otherwise compute one from per-domain items.
        compliance_str = _empty_to_none(v.get("compliance"))
        if compliance_str and compliance_str != "--":
            ins["ri_compliance"] = compliance_str
        else:
            ins["ri_compliance"] = _summarize_compliance(v.get("domains"))
        ins["ri_licensor"] = _empty_to_none(v.get("licensor"))
        inspections.append(ins)
    return inspections


def build_item(summary, detail):
    """Merge a search-result summary dict with the per-program detail dict
    into a ProviderItem. `detail` may be None when the per-program fetch
    failed; in that case we return what the search alone gave us."""
    item = ProviderItem()
    item["source_state"] = "Rhode Island"

    # Detail URL (decoded URL the page would link to). Use the same form
    # the site itself produces so this is reproducible from a captured pid.
    pid = summary.get("id")
    item["provider_url"] = (
        DETAIL_PAGE_URL_TEMPLATE.format(pid=quote(pid, safe=""))
        if pid else None
    )

    # Summary-derived fields
    item["provider_name"] = _empty_to_none(summary.get("accName"))
    item["address"] = _empty_to_none(summary.get("accAddress"))
    item["latitude"] = _empty_to_none(summary.get("latitude"))
    item["longitude"] = _empty_to_none(summary.get("longitude"))
    item["phone"] = _empty_to_none(summary.get("accPhone"))
    item["email"] = _empty_to_none(summary.get("accEmail"))
    item["provider_type"] = _empty_to_none(summary.get("accType"))
    item["status"] = _empty_to_none(summary.get("accLicenseStatus"))
    item["scholarships_accepted"] = _empty_to_none(summary.get("isCCAPType"))

    item["ri_brightstars_rating"] = summary.get("programRating")
    item["ri_license_decision"] = _empty_to_none(
        summary.get("accLicenseDecision")
    )
    item["ri_is_lea"] = summary.get("isLea")

    if detail is None:
        item["inspections"] = []
        return item

    acc_wrap = (detail.get("programDetailWrap") or {}).get("accWrap") or {}
    schedule = (detail.get("scheduleOfOperationData") or {}).get("data")
    age_group_wrapper = detail.get("ageGroupServedWrapper") or []
    visits = detail.get("lstVisits") or []

    # accWrap usually carries a richer / more-canonical version of the
    # summary fields. Prefer it when present.
    item["provider_name"] = (
        _empty_to_none(acc_wrap.get("accName")) or item["provider_name"]
    )
    item["address"] = (
        _empty_to_none(acc_wrap.get("accAddress")) or item["address"]
    )
    item["phone"] = _empty_to_none(acc_wrap.get("accPhone")) or item["phone"]
    item["email"] = _empty_to_none(acc_wrap.get("accEmail")) or item["email"]
    item["provider_type"] = (
        _empty_to_none(acc_wrap.get("accType")) or item["provider_type"]
    )
    item["status"] = (
        _empty_to_none(acc_wrap.get("accLicenseStatus")) or item["status"]
    )

    item["capacity"] = acc_wrap.get("capacity")
    item["languages"] = _empty_to_none(acc_wrap.get("languageSpoken"))
    item["administrator"] = _empty_to_none(acc_wrap.get("contactPerson"))
    item["license_begin_date"] = _empty_to_none(
        acc_wrap.get("originalLicenseStartDate")
    )
    item["license_expiration"] = _empty_to_none(
        acc_wrap.get("licenseExpirationDate")
    )
    item["provider_website"] = _empty_to_none(acc_wrap.get("website"))
    item["hours"] = format_hours(schedule)
    item["ages_served"] = format_ages_served(
        acc_wrap.get("accAvailability")
    )

    # RI-specific
    item["ri_most_recently_renewed"] = _empty_to_none(
        acc_wrap.get("currentLicenseStartDate")
    )
    item["ri_ccap_status"] = _empty_to_none(acc_wrap.get("ccapStatus"))
    item["ri_ccap_expiration_date"] = _empty_to_none(
        acc_wrap.get("ccapExpirationDate")
    )
    item["ri_head_start"] = _empty_to_none(acc_wrap.get("headStart"))
    item["ri_state_prek"] = _empty_to_none(acc_wrap.get("riStatePreK"))
    item["ri_provider_contact_name"] = _empty_to_none(
        acc_wrap.get("providerContactName")
    )
    item["ri_provider_email"] = _empty_to_none(acc_wrap.get("providerEmail"))
    item["ri_services_offered"] = _empty_to_none(
        acc_wrap.get("servicesOffered")
    )
    item["ri_age_group_capacity"] = format_age_group_capacity(
        age_group_wrapper
    )
    item["ri_availability"] = format_availability(
        acc_wrap.get("accAvailability")
    )
    # accWrap.programRating is the canonical value when present; fall back
    # to the search summary if missing.
    item["ri_brightstars_rating"] = (
        acc_wrap.get("programRating")
        if acc_wrap.get("programRating") is not None
        else item["ri_brightstars_rating"]
    )
    item["ri_license_decision"] = (
        _empty_to_none(acc_wrap.get("accLicenseDecision"))
        or item["ri_license_decision"]
    )

    item["inspections"] = build_inspections(visits)
    return item


def build_detail_message(pid):
    """Construct the URL-encoded `message` body for a single
    RICS_ViewProgramDetailsController.getProgramDetails call.

    The action `id` field ("1;a") is required by the Aura framework but its
    exact value doesn't seem to matter — the lightning runtime uses it only
    to correlate request/response client-side.
    """
    message = {
        "actions": [
            {
                "id": "1;a",
                "descriptor": (
                    "aura://ApexActionController/ACTION$execute"
                ),
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": "RICS_ViewProgramDetailsController",
                    "method": "getProgramDetails",
                    "params": {
                        "programId": pid,
                        "language": "English__c",
                    },
                    "cacheable": False,
                    "isContinuation": False,
                },
            }
        ]
    }
    return json.dumps(message, separators=(",", ":"))


def build_detail_post_body(pid, aura_context):
    """URL-encode the full POST body for the per-program detail call.

    `aura_context` is the JSON blob captured verbatim from the search
    request — it carries the framework UID (fwuid) which the server uses to
    validate the request belongs to the currently deployed app version.
    """
    page_uri = (
        f"/s/program-detail?language=en_US&pid={quote(pid, safe='')}"
        f"&lang=en"
    )
    return urlencode({
        "message": build_detail_message(pid),
        "aura.context": aura_context,
        "aura.pageURI": page_uri,
        "aura.token": "null",
    })


class StealthContextMiddleware:
    """Apply playwright-stealth + a couple of fingerprint patches at the
    browser-context level. Same pattern as new_jersey/minnesota."""

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

        async def patched_create_context(name, context_kwargs=None, spider=None):
            wrapper = await original(
                name, context_kwargs=context_kwargs, spider=spider
            )
            await wrapper.context.add_init_script(_STEALTH_SCRIPT)
            await wrapper.context.add_init_script(_CANVAS_PATCH)
            await wrapper.context.add_init_script(_HW_PATCH)
            spider.logger.info(
                "StealthContextMiddleware: stealth patches applied to "
                "context '%s'",
                name,
            )
            return wrapper

        handler._create_browser_context = patched_create_context


class RhodeIslandSpider(scrapy.Spider):
    """Spider for https://earlylearningprograms.dhs.ri.gov/s/.

    Phase 1 (Playwright): load the search page, tick all 5 age-group
    checkboxes, click Search, capture the providerSearch Aura response.
    The page's reCAPTCHA v3 supplies a token automatically — so long as
    the browser fingerprint scores well enough we don't see the visible
    challenge.

    Phase 2 (in-page fetch): the captured search request gives us the
    `aura.context` blob. We then fire one POST per pid against the same
    Aura endpoint — `RICS_ViewProgramDetailsController.getProgramDetails`
    — using `page.evaluate` so the calls inherit the live cookie jar and
    Chromium TLS fingerprint.
    """

    name = "rhode_island"
    allowed_domains = ["earlylearningprograms.dhs.ri.gov"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 90 * 1000,
        "DOWNLOADER_MIDDLEWARES": {
            "provider_scrape.spiders.rhode_island.StealthContextMiddleware": 100,
        },
        # `headless: False` + real Chrome via xvfb-run is what gets us a
        # passable reCAPTCHA v3 score. Headless leaks fingerprint signals
        # that drop the score below the visible-challenge threshold.
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,
            "channel": "chrome",
            # Window/DPR flags: in headed mode Playwright's `viewport`
            # context arg has no effect on the actual window size. We
            # pass `--window-size` and `--force-device-scale-factor` to
            # Chrome directly. `--ozone-platform=x11` is added on Linux
            # only — on macOS the flag is a no-op for Chrome's Cocoa
            # backend but in some Chrome versions can interfere with
            # subsequent arg parsing.
            "args": (
                ["--ozone-platform=x11"]
                if platform.system() == "Linux" else []
            ) + [
                "--window-size=1440,900",
                "--force-device-scale-factor=2",
            ],
            "timeout": 30 * 1000,
        },
        # IMPORTANT: scrapy-playwright reads PLAYWRIGHT_CONTEXTS (plural)
        # — there is NO `PLAYWRIGHT_CONTEXT_ARGS` setting. The fingerprint
        # audit revealed our viewport/locale/timezone were being silently
        # dropped because we wrote them to the wrong key. The handler
        # creates the default context at startup from this dict.
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
                # No user_agent override — let real Chrome's native UA
                # flow through unchanged. See _STEALTH_SCRIPT comment.
                #
                # 1440x900 is the canonical macOS laptop viewport.
                # device_scale_factor=2 makes the page render as Retina.
                "viewport": {"width": 1440, "height": 900},
                "device_scale_factor": 2,
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }
        },
    }

    def __init__(
        self,
        detail_delay_min=0.5,
        detail_delay_max=1.5,
        max_providers=None,
        manual_captcha=False,
        manual_timeout=300,
        audit=False,
        search_retries=2,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.detail_delay_min = float(detail_delay_min)
        self.detail_delay_max = float(detail_delay_max)
        self.max_providers = int(max_providers) if max_providers else None
        # Scrapy passes -a args as strings; treat any truthy non-zero string
        # as enabling manual captcha mode.
        if isinstance(manual_captcha, str):
            self.manual_captcha = manual_captcha.strip().lower() not in (
                "", "0", "false", "no", "off",
            )
        else:
            self.manual_captcha = bool(manual_captcha)
        self.manual_timeout = int(manual_timeout)
        if isinstance(audit, str):
            self.audit = audit.strip().lower() not in (
                "", "0", "false", "no", "off",
            )
        else:
            self.audit = bool(audit)
        self.search_retries = int(search_retries)

    def start_requests(self):
        yield scrapy.Request(
            SEARCH_PAGE_URL,
            callback=self.parse_search_page,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod(
                        "wait_for_load_state", "domcontentloaded",
                        timeout=60000,
                    ),
                    # Give the LWC + reCAPTCHA v3 script time to settle so
                    # the score window is open by the time we click Search.
                    PageMethod("wait_for_timeout", 6000),
                ],
            },
        )

    async def parse_search_page(self, response):
        page = response.meta["playwright_page"]
        try:
            if self.audit:
                await self._dump_fingerprint(page)
                return
            await self._humanize_warmup(page)
            await self._tick_age_groups(page)
            # A short idle after the checkbox interactions, plus a couple of
            # mouse moves, gives reCAPTCHA v3 a few more behavior samples.
            await self._post_form_jitter(page)
            search_results, aura_context = await self._submit_search(page)

            if not search_results:
                self.logger.error(
                    "RI search returned no results — possible reCAPTCHA "
                    "block or response shape change. Aborting."
                )
                return
            if not aura_context:
                self.logger.error(
                    "RI search captured but aura.context could not be "
                    "extracted — cannot fetch detail pages."
                )
                return

            total = len(search_results)
            if self.max_providers and self.max_providers < total:
                self.logger.info(
                    "RI search: limiting %d → %d providers (max_providers)",
                    total,
                    self.max_providers,
                )
                search_results = search_results[: self.max_providers]
                total = len(search_results)

            self.logger.info("RI search succeeded: %d providers", total)

            detail_url = (
                "https://earlylearningprograms.dhs.ri.gov" + AURA_ENDPOINT_PATH
            )
            for idx, summary in enumerate(search_results, start=1):
                pid = summary.get("id")
                detail = None
                if pid:
                    detail = await self._fetch_detail(
                        page, detail_url, pid, aura_context
                    )
                else:
                    self.logger.warning(
                        "Search result missing id; yielding summary-only "
                        "item: %r",
                        summary.get("accName"),
                    )

                yield build_item(summary, detail)

                if idx == 1 or idx % 25 == 0 or idx == total:
                    self.logger.info("RI detail progress: %d/%d", idx, total)

                # Stay polite between detail calls.
                if idx < total:
                    await asyncio.sleep(
                        random.uniform(
                            self.detail_delay_min, self.detail_delay_max
                        )
                    )
        finally:
            await page.close()

    async def _dump_fingerprint(self, page):
        """Print the bot-detection signals reCAPTCHA v3 cares about.

        Run with `-a audit=1` to inspect what stealth is/isn't masking
        without involving the search at all. We log a JSON blob so it's
        greppable; pair with a manual diff against a real Chrome session
        on the same machine.
        """
        probe = """
        async () => {
            const cdpProps = Object.keys(window).filter(
                (k) => k.startsWith('cdc_') || k === '$cdc_asdjflasutopfhvcZLmcfl_'
            );
            return {
                userAgent: navigator.userAgent,
                appVersion: navigator.appVersion,
                platform: navigator.platform,
                vendor: navigator.vendor,
                languages: navigator.languages,
                language: navigator.language,
                webdriver: navigator.webdriver,
                hardwareConcurrency: navigator.hardwareConcurrency,
                deviceMemory: navigator.deviceMemory,
                maxTouchPoints: navigator.maxTouchPoints,
                pluginsLength: navigator.plugins.length,
                pluginNames: Array.from(navigator.plugins).map(p => p.name),
                mimeTypesLength: navigator.mimeTypes.length,
                permissionsApi: typeof navigator.permissions !== 'undefined',
                connection: navigator.connection ? {
                    effectiveType: navigator.connection.effectiveType,
                    rtt: navigator.connection.rtt,
                    downlink: navigator.connection.downlink,
                } : null,
                userAgentData: navigator.userAgentData ? {
                    brands: navigator.userAgentData.brands,
                    mobile: navigator.userAgentData.mobile,
                    platform: navigator.userAgentData.platform,
                } : null,
                screen: {
                    width: screen.width, height: screen.height,
                    availWidth: screen.availWidth, availHeight: screen.availHeight,
                    colorDepth: screen.colorDepth, pixelDepth: screen.pixelDepth,
                },
                window: {
                    innerWidth: window.innerWidth,
                    innerHeight: window.innerHeight,
                    outerWidth: window.outerWidth,
                    outerHeight: window.outerHeight,
                    devicePixelRatio: window.devicePixelRatio,
                },
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                timezoneOffset: new Date().getTimezoneOffset(),
                cdpHooks: cdpProps,
                hasChrome: typeof window.chrome !== 'undefined',
                hasChromeRuntime: !!(window.chrome && window.chrome.runtime),
                hasChromeLoadTimes: !!(
                    window.chrome && typeof window.chrome.loadTimes === 'function'
                ),
                webglVendor: (() => {
                    try {
                        const c = document.createElement('canvas');
                        const gl = c.getContext('webgl');
                        const dbg = gl.getExtension('WEBGL_debug_renderer_info');
                        return gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL);
                    } catch (e) { return 'ERR:' + e.message; }
                })(),
                webglRenderer: (() => {
                    try {
                        const c = document.createElement('canvas');
                        const gl = c.getContext('webgl');
                        const dbg = gl.getExtension('WEBGL_debug_renderer_info');
                        return gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL);
                    } catch (e) { return 'ERR:' + e.message; }
                })(),
                permissionsState: await (async () => {
                    try {
                        const p = await navigator.permissions.query(
                            {name: 'notifications'}
                        );
                        return {
                            notifications: p.state,
                            // Real Chrome returns 'denied' for notifications when
                            // Notification.permission is 'default' — they should
                            // match. Headless/automated browsers often disagree.
                            notificationApi: Notification.permission,
                        };
                    } catch (e) { return 'ERR:' + e.message; }
                })(),
            };
        }
        """
        info = await page.evaluate(probe)
        self.logger.info("FINGERPRINT_AUDIT %s", json.dumps(info, indent=2))

    async def _humanize_warmup(self, page):
        """Generate behavior signals before any captcha-protected click.

        reCAPTCHA v3 weights time-on-page and mouse/keyboard activity.
        Submitting Search ~5s after page load with zero interaction tends
        to produce a sub-threshold score. Spending ~20s wandering the
        cursor, scrolling, and focusing/blurring the program-name input
        pushes the score over the threshold reliably.

        Baseline was raised from ~10s to ~20s after observing intermittent
        v3 failures on the shorter dwell. The retry loop in `_submit_search`
        also helps, but spending the time up-front means we usually don't
        need it.
        """
        # Random non-linear mouse path
        for _ in range(random.randint(6, 10)):
            x = random.randint(150, 1700)
            y = random.randint(150, 900)
            try:
                await page.mouse.move(
                    x, y, steps=random.randint(15, 35),
                )
            except Exception as e:
                self.logger.debug("warmup mouse.move failed: %s", e)
                break
            await asyncio.sleep(random.uniform(0.3, 0.8))

        # Light scroll, idle, then back up
        try:
            await page.mouse.wheel(0, random.randint(150, 350))
            await asyncio.sleep(random.uniform(0.8, 1.5))
            await page.mouse.wheel(0, -random.randint(50, 200))
        except Exception as e:
            self.logger.debug("warmup scroll failed: %s", e)
        await asyncio.sleep(random.uniform(0.6, 1.2))

        # Focus + tab away the program-name input (real keystroke event)
        try:
            name_input = page.locator('input[name="accName"]').first
            if await name_input.count() > 0:
                await name_input.click(timeout=3000)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                await page.keyboard.press("Tab")
        except Exception as e:
            self.logger.debug("warmup focus failed: %s", e)

        await asyncio.sleep(random.uniform(5.0, 8.0))
        self.logger.info("RI: warm-up phase complete")

    async def _post_form_jitter(self, page):
        """A short stir after ticking checkboxes — a couple of mouse moves
        plus a small idle so the click-Search timestamp isn't suspiciously
        adjacent to the checkbox flips."""
        for _ in range(random.randint(2, 4)):
            x = random.randint(300, 1600)
            y = random.randint(300, 800)
            try:
                await page.mouse.move(x, y, steps=random.randint(10, 25))
            except Exception:
                break
            await asyncio.sleep(random.uniform(0.15, 0.45))
        await asyncio.sleep(random.uniform(1.0, 2.0))

    async def _tick_age_groups(self, page):
        """Check every `input[name="ageGroup"]` checkbox.

        SLDS hides the real <input> visually and renders a styled
        <span class="slds-checkbox_faux"> inside the <label>. LWC's two-way
        binding only fires on the label click — clicking the input
        directly (incl. Playwright's `check()`) leaves the bound state
        unchanged, which is the "Clicking the checkbox did not change its
        state" error. So we look up each input's id and click its
        `label[for=...]` instead.
        """
        await page.wait_for_selector(
            'input[name="ageGroup"]', timeout=30000
        )
        checkboxes = page.locator('input[name="ageGroup"]')
        count = await checkboxes.count()
        if count != len(AGE_GROUPS):
            self.logger.warning(
                "Expected %d ageGroup checkboxes, page has %d",
                len(AGE_GROUPS),
                count,
            )
        ticked = 0
        for i in range(count):
            cb = checkboxes.nth(i)
            cb_id = await cb.get_attribute("id")
            if not cb_id:
                self.logger.warning(
                    "ageGroup checkbox %d missing id; skipping", i
                )
                continue
            await page.locator(f'label[for="{cb_id}"]').click()
            if not await cb.is_checked():
                self.logger.warning(
                    "ageGroup checkbox %d (id=%s) did not toggle on after "
                    "label click",
                    i,
                    cb_id,
                )
                continue
            ticked += 1
        self.logger.info("RI: ticked %d/%d age-group checkboxes", ticked, count)
        if ticked == 0:
            raise RuntimeError(
                "Could not tick any ageGroup checkboxes — search would "
                "fail the 'at least one criteria' validation."
            )

    async def _submit_search(self, page):
        """Click Search, retrying on v3-failure responses, and return
        (searchResults, aura_context).

        reCAPTCHA v3 scores are non-deterministic — even with a clean
        fingerprint and a healthy warm-up, the same machine will
        occasionally land a sub-threshold score. To absorb that variance
        we retry up to `search_retries` times, fully reloading the page
        between attempts. Reload is necessary because after a v3
        failure the page swaps in the visible v2 widget; clicking
        Search again would invoke v2 (which requires a manual solve),
        not a fresh v3 token.

        On the final v3 failure, if `manual_captcha` is enabled we fall
        through to the v2 wait loop so the operator can solve it by hand.
        """
        aura_context = None
        v3_failed = False
        max_attempts = self.search_retries + 1
        for attempt in range(1, max_attempts + 1):
            results, aura_context, v3_failed = await self._click_and_capture(
                page
            )
            if results:
                if attempt > 1:
                    self.logger.info(
                        "RI search succeeded on retry %d/%d",
                        attempt,
                        max_attempts,
                    )
                return results, aura_context
            if not v3_failed:
                # Shape change or other failure — retrying won't help.
                break
            if attempt >= max_attempts:
                break
            self.logger.warning(
                "RI search v3 failed (attempt %d/%d); reloading page and "
                "retrying with fresh warm-up",
                attempt,
                max_attempts,
            )
            await self._reset_and_warm_up(page)

        if v3_failed and self.manual_captcha:
            self.logger.warning("=" * 70)
            self.logger.warning(
                "reCAPTCHA v3 failed after %d attempts. SOLVE THE VISIBLE",
                max_attempts,
            )
            self.logger.warning(
                "CHALLENGE in the browser window. Waiting up to %d seconds...",
                self.manual_timeout,
            )
            self.logger.warning("=" * 70)
            return await self._wait_for_manual_search(page)
        return [], aura_context

    async def _reset_and_warm_up(self, page):
        """Reload the search page and re-run the warm-up/form-prep cycle.

        Used between v3 retries. A full reload (rather than just another
        click) puts the lightning component back into v3 mode after it
        had switched to rendering the v2 fallback widget.
        """
        try:
            await page.reload(
                wait_until="domcontentloaded", timeout=60000,
            )
        except Exception as e:
            self.logger.warning("Page reload failed during retry: %s", e)
            return
        await page.wait_for_timeout(random.randint(3000, 5000))
        # Extra cool-down so v3 doesn't see two execute() calls back-to-back.
        await asyncio.sleep(random.uniform(5.0, 9.0))
        await self._humanize_warmup(page)
        await self._tick_age_groups(page)
        await self._post_form_jitter(page)

    async def _click_and_capture(self, page):
        """Click the Search button once and parse the resulting Aura response.

        Returns (results, aura_context, v3_failed). The v3_failed flag
        tells the retry loop whether retrying is worthwhile — True means
        the captcha verdict was sub-threshold (transient, retry might
        succeed), False means either success or a non-captcha failure
        (shape change, network issue) where retrying won't help.
        """
        search_button = page.get_by_role("button", name="Search").first
        async with page.expect_response(
            _is_provider_search_response,
            timeout=90000,
        ) as resp_info:
            await search_button.click()
        resp = await resp_info.value
        return await self._parse_search_response(resp)

    async def _wait_for_manual_search(self, page):
        """Block until the next providerSearch response shows up.

        The page resubmits search automatically when the user finishes the
        visible v2 challenge — we just listen.
        """
        try:
            async with page.expect_response(
                _is_provider_search_response,
                timeout=self.manual_timeout * 1000,
            ) as resp_info:
                pass
            resp = await resp_info.value
        except Exception as e:
            self.logger.error(
                "Timed out (or failed) waiting for manual captcha solve: %s",
                e,
            )
            return [], None
        results, aura_context, _ = await self._parse_search_response(resp)
        if results:
            self.logger.info(
                "Manual captcha solve succeeded; %d results", len(results),
            )
        else:
            self.logger.error(
                "Manual captcha solve still produced no results — giving up."
            )
        return results, aura_context

    async def _parse_search_response(self, resp):
        """Decode a providerSearch Aura response.

        Returns (results, aura_context, v3_failed). v3_failed is True when
        the responseWrap explicitly flagged the v3 captcha as the reason
        for an empty result set; False on success or any other failure.
        Logs structured details so the failure mode (captcha vs. shape
        change) is obvious.
        """
        body = await resp.text()
        post_data = resp.request.post_data or ""
        aura_context = _extract_form_field(post_data, "aura.context")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            self.logger.error(
                "RI search response not JSON: %s; head=%r", e, body[:500]
            )
            return [], aura_context, False

        results = extract_search_results(payload)
        if results:
            return results, aura_context, False

        v3_failed = False
        actions = (payload or {}).get("actions") or []
        if not actions:
            self.logger.error(
                "RI search response had no actions; body head=%r",
                body[:500],
            )
        else:
            a = actions[0]
            state = a.get("state")
            errors = a.get("error") or []
            inner = (
                (a.get("returnValue") or {}).get("returnValue") or {}
            )
            response_wrap = inner.get("responseWrap") or {}
            v3_failed = bool(response_wrap.get("isV3Failed"))
            self.logger.error(
                "RI search returned no rows: state=%s "
                "responseWrap=%r errors=%r body_head=%r",
                state,
                response_wrap,
                errors,
                body[:500],
            )
        return [], aura_context, v3_failed

    async def _fetch_detail(self, page, url, pid, aura_context):
        body = build_detail_post_body(pid, aura_context)
        try:
            result = await page.evaluate(
                _DETAIL_FETCH_SCRIPT,
                {"url": url, "body": body},
            )
        except Exception as e:
            self.logger.warning(
                "Detail fetch threw for pid=%s: %s", pid, e
            )
            return None

        if result.get("error"):
            self.logger.warning(
                "Detail fetch failed for pid=%s: %s (%s)",
                pid,
                result.get("error"),
                result.get("detail"),
            )
            return None

        if result.get("status") != 200:
            self.logger.warning(
                "Detail HTTP %s for pid=%s; head=%r",
                result.get("status"),
                pid,
                (result.get("body") or "")[:200],
            )
            return None

        try:
            data = json.loads(result["body"])
        except json.JSONDecodeError as e:
            self.logger.warning(
                "Detail JSON parse failed for pid=%s: %s", pid, e
            )
            return None

        return extract_detail_payload(data)


def _is_provider_search_response(response):
    """Predicate: true for responses to the RICS_ProviderSearch Aura POST."""
    if "/s/sfsites/aura" not in response.url:
        return False
    request = response.request
    if request.method != "POST":
        return False
    return "RICS_ProviderSearch" in (request.post_data or "")


def _extract_form_field(post_data, field):
    """Pull a single field's value out of a form-urlencoded POST body.

    Implemented manually rather than via `parse_qs` because the values are
    JSON blobs that may contain `&`/`=` after URL-decoding round-trips,
    and `parse_qs` would also collapse repeated keys. We just want the raw
    URL-encoded value as the page sent it.
    """
    if not post_data:
        return None
    needle = f"{field}="
    pieces = post_data.split("&")
    for piece in pieces:
        if piece.startswith(needle):
            from urllib.parse import unquote
            return unquote(piece[len(needle):])
    return None
