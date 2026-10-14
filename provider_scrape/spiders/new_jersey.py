import json
from collections import Counter
from urllib.parse import urlencode

import scrapy
import scrapy.signals
from playwright_stealth import Stealth
from scrapy_playwright.page import PageMethod

from provider_scrape.items import ProviderItem

API_HOST = "https://www.childcarenj.gov"
SEARCH_URL = f"{API_HOST}/Search"
API_PATH = "/Services/GetProviders.aspx"

# Filter params held constant; only pageSize/currentPage vary. All booleans
# off + empty programTypes/rating yields the unfiltered provider set.
_STATIC_PARAMS = {
    "zipcode": "",
    "county": "",
    "programName": "",
    "camp": "false",
    "center": "false",
    "home": "false",
    "preschool": "false",
    "rating": "",
    "programTypes": "",
    "subsidised": "false",
    "mccynplus": "false",
    "sortKey": "ProgramName",
    "sortDirection": "0",
}

# JSON day suffix → short label used in the formatted hours string
_DAY_LABELS = [
    ("Sunday", "Sun"),
    ("Monday", "Mon"),
    ("Tuesday", "Tue"),
    ("Wednesday", "Wed"),
    ("Thursday", "Thu"),
    ("Friday", "Fri"),
    ("Saturday", "Sat"),
]

# Per-age-group tuition cadence mapping: (item suffix, JSON suffix)
_TUITION_AGE_GROUPS = [
    ("infant", "Infant"),
    ("toddler", "Toddler"),
    ("preschool", "Preschool"),
    ("school_age", "SchoolAge"),
]
_TUITION_CADENCES = [
    ("hourly", "Hourly"),
    ("daily", "Daily"),
    ("weekly", "Weekly"),
    ("monthly", "Monthly"),
]


# --------------------------------------------------------------------------- #
# Code -> label lookup tables (bug_07)
#
# Captured from an inline `looupTable` JS object (sic -- typo in the site's
# own source; providerdetails.aspx spells it `lookupTable`) embedded directly
# in childcarenj.gov's page HTML -- not a separate JS bundle or lookup API.
# First recovered from an Internet Archive snapshot (2026-05-17), then
# refreshed from the live /Search page on 2026-10-15 via a webshare.env
# proxy (this project's direct IP is Cloudflare-WAF-blocked; the spider's
# fingerprint got through unmodified once proxied). The live table only
# added special schedules 7-12. See
# tasks/common_fields_epic/bug_07_nj_lookups.json for both captures.
#
# Codes the live table itself does NOT define (meal options 7/8,
# environmental features 6-13, accreditation 28, yearly schedule 0) have no
# public label source: the details page doesn't render those sections at
# all. They fall through to "Unknown (code N)" by design rather than being
# guessed.
#
# These are STATIC tables, not a live fetch: the table lives in
# challenge-gated HTML, not a dedicated lookup endpoint, so fetching it live
# would mean re-solving Cloudflare on every spider run just for this --
# far more fragile than committing a dict that rarely changes.
#
# Two nj_* fields have NO label source anywhere (checked this table, the
# public UI, and a general web search) and are deliberately left as raw
# values only, with no *_label sibling added -- guessing would violate
# bug_07's "never invent a mapping without a source" rule:
#   - nj_facility_type (NJFacilityType): not rendered anywhere in the public
#     UI and not present in the lookup table; codes 1-4 observed.
#   - nj_mccyn_plus (MCCYNPlusInd): a '1'/'2' indicator with no documented
#     meaning found anywhere.
# --------------------------------------------------------------------------- #

_LICENSE_TYPE_LABELS = {1: "Licensed", 2: "Registered", 3: "License exempt"}

_PROGRAM_FACILITY_TYPE_LABELS = {
    1: "Licensed Child Care Center",
    2: "Registered Family Child Care Provider",
    3: "Group care",
}

_QUALITY_RATING_LABELS = {
    1: "QRIS level 1",
    2: "QRIS level 2",
    3: "QRIS level 3",
    4: "QRIS level 4",
    5: "QRIS level 5",
    6: "QRIS level 6",
}

_ACCREDITATION_LABELS = {
    1: "National Association for the Education of Young Children",
    2: "National Association for Family Child Care",
    3: "Council on Accreditation",
    4: "American Camp Association",
    5: "National Early Childhood Program Accreditation",
    6: "National Accreditation Commission for Early Care and Education Programs",
    7: "Association for Christian Schools International",
    8: "American Montessori Society",
    9: "Accredited Professional Preschool Learning Environment",
    10: "Association Montessori International/USA",
    11: "Association of Waldorf Schools of North America",
    12: "Commission on Accreditation of Rehabilitation Facilities: Child and Youth Services Standards",
    13: "Green Apple Accreditation Services",
    14: "International Christian Accrediting",
    15: "Council on Accreditation- After School Programs",
    16: "Academy for Early Childhood Program Accreditation",
    17: "National Council for Private School Accreditation",
    18: "Montessori School Accreditation Commission",
    19: "Southern Association of Colleges and Schools",
    20: "Other Type of National Accreditation",
}

_YEARLY_SCHEDULE_LABELS = {1: "Full year", 2: "School year", 3: "Summer only"}

_FEDERAL_PROGRAM_LABELS = {
    1: "Participates in Head Start",
    2: "Participates in state preschool program",
    3: "Accepts state-funded child care assistance; e.g., contracts, vouchers, scholarships",
    4: "Accepts private scholarships; e.g., United Way scholarships",
}

_MEAL_OPTION_LABELS = {
    1: "Breakfast",
    2: "Lunch",
    3: "Dinner",
    4: "Morning snack",
    5: "Afternoon snack",
    6: "Child and Adult Care Food Program",
}

_SPECIAL_NEEDS_TRAINING_LABELS = {
    1: "Developmental delay",
    2: "Emotional or behavioral",
    3: "Hearing impairment",
    4: "Visual impairment",
    5: "Physical challenges",
    6: "Special health needs",
    7: "Speech impairment",
}

_TRANSPORTATION_LABELS = {
    1: "On school transportation route",
    2: "Near public transportation",
    3: "Program transportation provided",
}

_SPECIAL_SCHEDULE_LABELS = {
    1: "Evening",
    2: "Overnight",
    3: "Weekends",
    4: "Holidays",
    5: "Sick care",
    6: "Drop-in care",
    7: "Special Needs",
    8: "Night Care",
    9: "Temporary Care",
    10: "Emergency Care",
    11: "Intermittent Care",
    12: "Rotating Shifts",
}

_DISCOUNT_LABELS = {
    1: "Discount provided for siblings",
    2: "Discounts provided for employees",
    3: "Discounts provided on a sliding scale",
    4: "Discounts provided for parents in military service",
}

_FEE_LABELS = {
    1: "Registration fee",
    2: "Late fee",
    3: "Extended care fee",
    4: "Waitlist fee",
    5: "Materials fee",
    6: "Activities fee",
}

_ENVIRONMENTAL_FEATURE_LABELS = {
    1: "No pets",
    2: "Fenced-in play area",
    3: "Handicap accessible",
    4: "Separated play areas",
    5: "Smoke-free environment",
}


def _resolve_code(code, table, field_name, unmapped_counts):
    """Map one raw code to its label, or an identifiable placeholder.

    ``None`` passes straight through (nothing to resolve, not "unknown"). A
    code absent from `table` is tallied in `unmapped_counts` -- a ``Counter``
    keyed by ``(field_name, code)`` -- so ``NewJerseySpider.closed`` can log
    one aggregate warning with counts, instead of a line per record.
    """
    if code is None:
        return None
    if code in table:
        return table[code]
    unmapped_counts[(field_name, code)] += 1
    return f"Unknown (code {code})"


def _resolve_codes(codes, table, field_name, unmapped_counts):
    """List version of `_resolve_code`. Preserves every element -- never drops."""
    if codes is None:
        return None
    return [_resolve_code(code, table, field_name, unmapped_counts) for code in codes]


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

# In-browser fetch. Runs inside the page's JS realm so Chromium's cookie jar
# (including the `cf_clearance` set on /Search) and TLS fingerprint are both
# used — Playwright's APIRequestContext shares cookies but not fingerprint
# and Cloudflare often rejects it.
_FETCH_SCRIPT = """
async (url) => {
    const r = await fetch(url, {
        headers: {'X-Requested-With': 'XMLHttpRequest'},
        credentials: 'include',
    });
    return {status: r.status, body: await r.text()};
}
"""


def build_api_path(page_size, current_page=0):
    """Build a GetProviders.aspx URL (site-relative) for given page size/number."""
    params = dict(_STATIC_PARAMS)
    params["pageSize"] = str(page_size)
    params["currentPage"] = str(current_page)
    return f"{API_PATH}?{urlencode(params)}"


def _trim_seconds(time_str):
    """'07:30:00' → '07:30'. Empty/None passes through as ''."""
    if not time_str:
        return ""
    parts = time_str.split(":")
    return ":".join(parts[:2]) if len(parts) >= 2 else time_str


def format_hours(facility):
    """Collapse the 14 DailyOpeningTime*/DailyClosingTime* fields into a
    'Mon 07:30-18:30; Tue 07:30-18:30' string. Returns None if every day
    is blank.
    """
    parts = []
    for long_name, short_name in _DAY_LABELS:
        open_t = _trim_seconds(facility.get(f"DailyOpeningTime{long_name}"))
        close_t = _trim_seconds(facility.get(f"DailyClosingTime{long_name}"))
        if open_t and close_t:
            parts.append(f"{short_name} {open_t}-{close_t}")
    return "; ".join(parts) if parts else None


def build_address(facility):
    """Join the split address fields into a single string.

    Some records have an empty `ProgramAddressStreetNumber` with the whole
    street address packed into `ProgramAddressStreetName` (e.g. '163 Cherry
    Ave'), so we join only non-empty fragments.
    """
    street_parts = [
        (facility.get("ProgramAddressStreetNumber") or "").strip(),
        (facility.get("ProgramAddressStreetName") or "").strip(),
    ]
    street = " ".join(p for p in street_parts if p)
    city = (facility.get("ProgramCity") or "").strip()
    state = (facility.get("ProgramState") or "").strip()
    zip_code = (facility.get("ProgramZipCode") or "").strip()
    state_zip = " ".join(p for p in [state, zip_code] if p)
    city_state_zip = ", ".join(p for p in [city, state_zip] if p)
    full = ", ".join(p for p in [street, city_state_zip] if p)
    return full or None


def _empty_to_none(val):
    """Normalize blank strings and empty lists to None; leave other values alone."""
    if val is None:
        return None
    if isinstance(val, str) and not val.strip():
        return None
    if isinstance(val, list) and len(val) == 0:
        return None
    return val


def _build_social_media(facility):
    """Collapse the four ProgramSocialMediaLink* fields into a dict, or
    return None if every field is blank."""
    social = {
        "twitter": (facility.get("ProgramSocialMediaLinkTwitter") or "").strip() or None,
        "facebook": (facility.get("ProgramSocialMediaLinkFacebook") or "").strip() or None,
        "instagram": (facility.get("ProgramSocialMediaLinkInstagram") or "").strip() or None,
        "youtube": (facility.get("ProgramSocialMediaLinkYouTube") or "").strip() or None,
    }
    if any(v for v in social.values()):
        return social
    return None


def build_item(facility, unmapped_counts=None):
    """Map one Facilities[] JSON record to a ProviderItem.

    `unmapped_counts` is a `Counter` the caller uses to tally any raw code
    that falls outside the bug_07 lookup tables above, keyed by
    `(field_name, code)` -- see `NewJerseySpider.closed`. When omitted (e.g.
    a unit test that only cares about one record) a throwaway `Counter` is
    used, so the "Unknown (code N)" fallback still works correctly, just
    without cross-record aggregation.
    """
    if unmapped_counts is None:
        unmapped_counts = Counter()

    item = ProviderItem()
    item["source_state"] = "New Jersey"
    item["provider_url"] = None

    item["provider_name"] = (facility.get("ProgramName") or "").strip() or None
    item["address"] = build_address(facility)
    item["county"] = _empty_to_none(facility.get("ProgramCounty"))
    item["email"] = _empty_to_none(facility.get("ProgramEmail"))
    item["phone"] = _empty_to_none(facility.get("ProgramPhoneNumber"))
    item["provider_website"] = _empty_to_none(facility.get("ProgramWebsiteLink"))
    item["capacity"] = facility.get("ProgramLicensedCapacityTotal")
    item["license_number"] = _empty_to_none(facility.get("LicenseNumber"))
    item["languages"] = _empty_to_none(facility.get("LanguagesSpokenByStaff"))
    item["ages_served"] = _empty_to_none(facility.get("AgesLicensedToServe"))
    item["hours"] = format_hours(facility)

    item["nj_unique_program_id"] = _empty_to_none(facility.get("UniqueProgramID"))
    item["nj_program_facility_type"] = facility.get("ProgramFacilityType")
    item["nj_program_facility_type_label"] = _resolve_code(
        item["nj_program_facility_type"], _PROGRAM_FACILITY_TYPE_LABELS, "program_facility_type", unmapped_counts
    )
    # NJFacilityType: no label source found anywhere (see module docstring
    # above) -- raw code only, deliberately no *_label sibling.
    item["nj_facility_type"] = facility.get("NJFacilityType")
    item["nj_license_type"] = facility.get("LicenseType")
    item["nj_license_type_label"] = _resolve_code(
        item["nj_license_type"], _LICENSE_TYPE_LABELS, "license_type", unmapped_counts
    )
    item["nj_quality_rating"] = facility.get("QualityRating")
    item["nj_quality_rating_label"] = _resolve_code(
        item["nj_quality_rating"], _QUALITY_RATING_LABELS, "quality_rating", unmapped_counts
    )
    item["nj_accreditation"] = _empty_to_none(facility.get("Accreditation"))
    item["nj_accreditation_labels"] = _resolve_codes(
        item["nj_accreditation"], _ACCREDITATION_LABELS, "accreditation", unmapped_counts
    )
    item["nj_yearly_schedule"] = facility.get("YearlySchedule")
    item["nj_yearly_schedule_label"] = _resolve_code(
        item["nj_yearly_schedule"], _YEARLY_SCHEDULE_LABELS, "yearly_schedule", unmapped_counts
    )
    item["nj_doh_id"] = _empty_to_none(facility.get("DohID"))
    item["nj_phone_extension"] = _empty_to_none(facility.get("ProgramPhoneNumberExtension"))
    item["nj_participation_programs"] = _empty_to_none(facility.get("ParticipationInLocalStateOrFederalPrograms"))
    item["nj_participation_programs_labels"] = _resolve_codes(
        item["nj_participation_programs"], _FEDERAL_PROGRAM_LABELS, "participation_programs", unmapped_counts
    )
    item["nj_curriculum"] = _empty_to_none(facility.get("CurriculumUsed"))
    item["nj_child_assessment"] = _empty_to_none(facility.get("ChildAssessmentUsed"))
    item["nj_environmental_features"] = _empty_to_none(facility.get("EnvironmentalFeatures"))
    item["nj_environmental_features_labels"] = _resolve_codes(
        item["nj_environmental_features"], _ENVIRONMENTAL_FEATURE_LABELS, "environmental_features", unmapped_counts
    )
    item["nj_meal_options"] = _empty_to_none(facility.get("MealOptions"))
    item["nj_meal_options_labels"] = _resolve_codes(
        item["nj_meal_options"], _MEAL_OPTION_LABELS, "meal_options", unmapped_counts
    )
    item["nj_special_needs_training"] = _empty_to_none(facility.get("TrainingAndExperienceToSupportSpecialNeeds"))
    item["nj_special_needs_training_labels"] = _resolve_codes(
        item["nj_special_needs_training"], _SPECIAL_NEEDS_TRAINING_LABELS, "special_needs_training", unmapped_counts
    )
    item["nj_transportation"] = _empty_to_none(facility.get("TransportationOptions"))
    item["nj_transportation_labels"] = _resolve_codes(
        item["nj_transportation"], _TRANSPORTATION_LABELS, "transportation", unmapped_counts
    )
    item["nj_special_schedules"] = _empty_to_none(facility.get("SpecialSchedules"))
    item["nj_special_schedules_labels"] = _resolve_codes(
        item["nj_special_schedules"], _SPECIAL_SCHEDULE_LABELS, "special_schedules", unmapped_counts
    )
    item["nj_discounts"] = _empty_to_none(facility.get("DiscountsAvailable"))
    item["nj_discounts_labels"] = _resolve_codes(item["nj_discounts"], _DISCOUNT_LABELS, "discounts", unmapped_counts)
    item["nj_fees"] = _empty_to_none(facility.get("AdditionalDepositsAndFees"))
    item["nj_fees_labels"] = _resolve_codes(item["nj_fees"], _FEE_LABELS, "fees", unmapped_counts)
    # MCCYNPlusInd: no label source found anywhere (see module docstring
    # above) -- raw '1'/'2' value only, deliberately no *_label sibling.
    item["nj_mccyn_plus"] = _empty_to_none(facility.get("MCCYNPlusInd"))
    item["nj_social_media"] = _build_social_media(facility)

    # Additive (D2): provider_type feeds facility_category_from_type in the
    # normalization pipeline. NJ never set this before bug_07, so
    # facility_category was never derived for any NJ record. Sourced from the
    # same programTypes table as nj_program_facility_type_label -- see
    # normalization.FACILITY_CATEGORY_BUCKETS for the center/family_home
    # entries this now maps to.
    item["provider_type"] = item["nj_program_facility_type_label"]

    for age_out, age_in in _TUITION_AGE_GROUPS:
        for cadence_out, cadence_in in _TUITION_CADENCES:
            item[f"nj_tuition_{age_out}_{cadence_out}"] = facility.get(f"Tuition{age_in}{cadence_in}")

    return item


class StealthContextMiddleware:
    """Downloader middleware that applies playwright-stealth and a couple
    of navigator fingerprint patches at the browser-context level.
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
                "StealthContextMiddleware: scrapy-playwright handler not found; stealth patches NOT applied."
            )
            return

        original = handler._create_browser_context

        async def patched_create_context(name, context_kwargs=None, spider=None):
            wrapper = await original(name, context_kwargs=context_kwargs, spider=spider)
            await wrapper.context.add_init_script(_STEALTH_SCRIPT)
            await wrapper.context.add_init_script(_CANVAS_PATCH)
            await wrapper.context.add_init_script(_HW_PATCH)
            spider.logger.info(
                "StealthContextMiddleware: stealth patches applied to context '%s'",
                name,
            )
            return wrapper

        handler._create_browser_context = patched_create_context


class NewJerseySpider(scrapy.Spider):
    """Spider for https://www.childcarenj.gov/Search.

    Uses a stealth Playwright browser to clear Cloudflare on /Search, then
    calls `GetProviders.aspx` from inside the same page context. First a
    pageSize=1 probe to read `ResultCount`, then one big fetch with
    `pageSize = ResultCount + page_size_buffer` to grab everything in two
    requests. If the server caps pageSize, paginates with
    `fallback_page_size` instead.
    """

    name = "new_jersey"
    allowed_domains = ["childcarenj.gov"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "RETRY_TIMES": 3,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60 * 1000,
        "USER_AGENT": _UA,
        "DOWNLOADER_MIDDLEWARES": {
            "provider_scrape.spiders.new_jersey.StealthContextMiddleware": 100,
        },
        # `channel=chrome` asks Playwright to use installed Google Chrome
        # rather than bundled chromium — Cloudflare is much more willing to
        # hand out `cf_clearance` to real Chrome. Requires `playwright
        # install chrome` on the host.
        #
        # `headless: False` is required: even Chrome's new headless leaks
        # enough fingerprint signals that CF will 200 the page load but
        # 403 the XHR to GetProviders.aspx. On Linux servers, run via
        # `xvfb-run` to provide a virtual display (run_spiders.sh does
        # this automatically when xvfb-run is on PATH).
        #
        # `--ozone-platform=x11` forces Chrome to use X11 instead of
        # Wayland. Without it, on a Wayland desktop Chrome connects to
        # the real compositor (popping a window on the user's screen);
        # on a server where xvfb-run only provides X11, Chrome would
        # otherwise fail to start because its compiled-in Ozone default
        # is Wayland. Harmless on X11-only systems and on macOS.
        #
        # `--enable-unsafe-swiftshader` restores software WebGL under
        # xvfb. Chrome 150 dropped the automatic SwiftShader fallback for
        # WebGL, so on a GPU-less server `canvas.getContext('webgl')`
        # returns null — the page reports *no* WebGL at all, a strong
        # headless/bot signal Cloudflare fingerprints. The flag lets a
        # real (software) GL context and honest renderer string flow
        # through again. Harmless where a GPU exists: it only permits
        # SwiftShader as a fallback, it doesn't force it.
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,
            "channel": "chrome",
            "args": ["--ozone-platform=x11", "--enable-unsafe-swiftshader"],
            "timeout": 30 * 1000,
        },
        # scrapy-playwright reads PLAYWRIGHT_CONTEXTS (plural) — there is
        # no `PLAYWRIGHT_CONTEXT_ARGS` setting. Until this fix the kwargs
        # below were being silently dropped and the default context ran
        # with zero kwargs. NJ tolerates that because Cloudflare grants
        # `cf_clearance` based on the JS challenge, not on viewport /
        # locale / timezone consistency. RI surfaced the bug because
        # reCAPTCHA v3 weighs those signals.
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "ignore_https_errors": True,
                "user_agent": _UA,
                "viewport": {"width": 1920, "height": 1080},
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }
        },
    }

    def __init__(
        self,
        page_size_buffer=100,
        fallback_page_size=200,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.page_size_buffer = int(page_size_buffer)
        self.fallback_page_size = int(fallback_page_size)
        # bug_07: tallies raw codes not found in the lookup tables above,
        # keyed by (field_name, code) -- see closed().
        self.unmapped_code_counts = Counter()

    def start_requests(self):
        # `networkidle` doesn't work here — Cloudflare insights + GA keep the
        # network noisy, so it never quiesces. `domcontentloaded` is enough
        # to know the HTML is parsed, and the short fixed wait gives the CF
        # managed-challenge JS a chance to stash the `cf_clearance` cookie
        # before we fire the API fetch.
        #
        # The site shows a first-visit popup that intercepts page interactions
        # until dismissed. Reloading once after the initial wait gets us past
        # it without needing to identify and click the close button — the
        # dismissal cookie is set during the first view, so the reload renders
        # the page popup-free. `cf_clearance` survives the reload, so we
        # don't have to re-pay the Cloudflare challenge.
        yield scrapy.Request(
            SEARCH_URL,
            callback=self.parse_search_page,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded", timeout=60000),
                    PageMethod("wait_for_timeout", 5000),
                    PageMethod("reload", wait_until="domcontentloaded", timeout=60000),
                    PageMethod("wait_for_timeout", 3000),
                ],
            },
        )

    async def parse_search_page(self, response):
        """Probe for ResultCount, then either one-shot or paginate."""
        page = response.meta["playwright_page"]
        try:
            probe = await page.evaluate(_FETCH_SCRIPT, build_api_path(1))
            if probe.get("status") != 200:
                self.logger.error(
                    "Probe call returned HTTP %s; body head=%r",
                    probe.get("status"),
                    (probe.get("body") or "")[:300],
                )
                return

            try:
                probe_data = json.loads(probe["body"])
            except json.JSONDecodeError as e:
                self.logger.error("Probe JSON parse failed: %s", e)
                return

            result_count = int(probe_data.get("ResultCount") or 0)
            self.logger.info("NJ probe: ResultCount=%s (pageSize=1)", result_count)
            if result_count == 0:
                self.logger.warning(
                    "ResultCount is 0 — nothing to scrape (payload head=%r)",
                    probe["body"][:300],
                )
                return

            big_page_size = result_count + self.page_size_buffer
            self.logger.info(
                "Single-shot fetch: pageSize=%s (ResultCount=%s + buffer=%s)",
                big_page_size,
                result_count,
                self.page_size_buffer,
            )
            big = await page.evaluate(_FETCH_SCRIPT, build_api_path(big_page_size))
            if big.get("status") != 200:
                self.logger.error(
                    "Single-shot call returned HTTP %s; falling back to pagination",
                    big.get("status"),
                )
                async for item in self._paginate(page, result_count):
                    yield item
                return

            try:
                big_data = json.loads(big["body"])
            except json.JSONDecodeError as e:
                self.logger.error(
                    "Single-shot JSON parse failed: %s; falling back to pagination",
                    e,
                )
                async for item in self._paginate(page, result_count):
                    yield item
                return

            facilities = big_data.get("Facilities") or []
            if len(facilities) >= result_count:
                self.logger.info(
                    "Single-shot success: got %d facilities for ResultCount=%d",
                    len(facilities),
                    result_count,
                )
                for raw in facilities:
                    yield build_item(raw, self.unmapped_code_counts)
            else:
                self.logger.warning(
                    "Server capped pageSize: got %d of %d; switching to paginated fetch (pageSize=%d)",
                    len(facilities),
                    result_count,
                    self.fallback_page_size,
                )
                async for item in self._paginate(page, result_count):
                    yield item
        finally:
            await page.close()

    async def _paginate(self, page, result_count):
        """Fetch all records by walking pages of `fallback_page_size`."""
        seen = 0
        current_page = 0
        while seen < result_count:
            path = build_api_path(
                page_size=self.fallback_page_size,
                current_page=current_page,
            )
            resp = await page.evaluate(_FETCH_SCRIPT, path)
            if resp.get("status") != 200:
                self.logger.error(
                    "Pagination page %d returned HTTP %s; stopping at %d/%d",
                    current_page,
                    resp.get("status"),
                    seen,
                    result_count,
                )
                return
            try:
                data = json.loads(resp["body"])
            except json.JSONDecodeError as e:
                self.logger.error(
                    "Pagination page %d JSON parse failed: %s; stopping at %d/%d",
                    current_page,
                    e,
                    seen,
                    result_count,
                )
                return

            facilities = data.get("Facilities") or []
            if not facilities:
                self.logger.info(
                    "Pagination empty page %d after %d/%d records — stopping",
                    current_page,
                    seen,
                    result_count,
                )
                return

            seen_after = seen + len(facilities)
            self.logger.info(
                "Pagination: page %d got %d records (seen %d/%d)",
                current_page,
                len(facilities),
                seen_after,
                result_count,
            )
            for raw in facilities:
                yield build_item(raw, self.unmapped_code_counts)

            seen = seen_after
            current_page += 1

    def closed(self, reason):
        """bug_07: summarize any code -> label lookup misses in one place.

        Per-record logging would spam thousands of identical lines for a
        single stale/new code, so misses are tallied during the crawl (see
        `_resolve_code`/`_resolve_codes`) and reported here as one aggregate
        warning with counts -- never silently dropped.
        """
        if not self.unmapped_code_counts:
            return
        fields = sorted({field for field, _ in self.unmapped_code_counts})
        detail = ", ".join(
            f"{field}={code}({count}x)" for (field, code), count in sorted(self.unmapped_code_counts.items())
        )
        self.logger.warning(
            "NJ: %d unmapped code(s) across %d field(s) fell back to 'Unknown "
            "(code N)' -- see tasks/common_fields_epic/bug_07_nj_lookups.json "
            "for the captured table and known staleness notes. Fields: %s. "
            "Counts: %s",
            len(self.unmapped_code_counts),
            len(fields),
            ", ".join(fields),
            detail,
        )
