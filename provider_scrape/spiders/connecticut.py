"""Connecticut child care provider spider.

Source: https://www.211childcare.org -- Connecticut's 211 Child Care directory
(United Way of CT / CT Office of Early Childhood). A Rails app backing a React
SPA, with no JavaScript, cookies, tokens, or CAPTCHA required -- a plain GET
with a browser UA returns clean JSON (see connecticut_plan.md Sec 1-2).

This is *not* a search -> detail crawl. `GET /providers.json` (the map/list
search) is hard-capped at 300 rows and, worse, each call returns a different
**random sample** -- there is no pagination to walk and no way to enumerate
the dataset through it (plan Sec 3.1). A town-name frontier sweep looks
complete but silently drops ~10% of providers (records with no
`type_of_provider`, and mailing cities the geocoder can't resolve -- plan
Sec 3.2); that approach is kept only as the optional `-a verify=1` coverage
audit below, not as the primary crawl.

The site *does* expose a per-id detail endpoint (`GET /providers/{id}.json`)
that works for every provider, searchable or not, and is complete by
construction. So the crawl is a dense id sweep:

  Phase 1 -- discover the id ceiling (`max_id`) by probing upward from a
             known baseline in blocks, stopping after a long run of misses
             (ids are sparse, not literally sequential).
  Phase 2 -- GET /providers/{id}.json for id in 1..max_id. A missing id
             returns the literal body `null` at HTTP 200 -- there is no 404.
  Phase 3 -- (default ON, `-a violations=0` to skip) for every inspection
             harvested in Phase 2's embedded `inspections[]`, GET
             /inspections/{id}.json for the violations + document detail the
             summary omits, and merge it back into that InspectionItem before
             the provider item is yielded (hold-and-join, plan Sec 4.3).

Per Ryan's decisions (2026-08-20, plan Sec 1):
  D-1: sweep all ids and emit every record, including the ~34% CT hides from
       its own public search (flagged via `ct_searchable`/`status`).
  D-2: the Phase 3 violations/documents tier is default-ON, not opt-in.
  D-3: `status` is "Listed"/"Not Listed", derived from `searchable`.

See tasks/connecticut_epic/connecticut_plan.md for the full recon writeup.
"""

import re
from urllib.parse import quote

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://www.211childcare.org"
PROVIDER_URL = BASE_URL + "/providers/{}.json"
INSPECTION_URL = BASE_URL + "/inspections/{}.json"
SEARCH_URL = BASE_URL + "/providers.json"
# A real, stable, client-rendered per-provider page (plan Sec 6.1) -- there is
# no per-provider GET detail page to point at instead (the JSON API is it).
LISTING_URL_TMPL = BASE_URL + "/listings/{}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Live baseline (2026-08-20): max_id 7691, ~91.5% dense -- ids move as CT adds
# providers, so this is a discovery *seed*, never hardcoded as the ceiling
# (plan Sec 4.1).
KNOWN_MAX_ID = 7691
# The top of the id range is genuinely patchy (a 32-wide gap was observed at
# 7657-7688), so discovery probes in blocks and only stops after a long run of
# consecutive misses -- ~8x the largest observed natural gap.
DISCOVERY_BLOCK_SIZE = 50
MISS_STREAK_LIMIT = 250

# Baseline unique count (2026-08-20: ~7,030 of 7,691 ids resolve). Warn in
# closed() if a full run falls far short -- the Indiana/Kentucky precedent.
EXPECTED_MIN_PROVIDERS = 6000

# license_number prefix -> license_type label (plan Sec 5.6). A clean,
# perfectly correlated taxonomy over the full searchable population -- no
# off-diagonal cells. Records with no license_number (586/4,114 searchable)
# get no ct_license_type.
LICENSE_PREFIX_MAP = {
    "DCFH": "Licensed Family Child Care Home",
    "DCCC": "Licensed Child Care Center",
    "YCYC": "Licensed Youth Camp",
    "DCGH": "Licensed Group Child Care Home",
    "DCEX": "License-Exempt Child Care Program",
    "YCEX": "Exempt Youth Camp",
}

# shifts[].population_by_age[].group -> which age-group boolean(s) it sets
# (plan Sec 6.2). "Toddler/Preschool" is a combined band that sets both.
AGE_GROUP_FLAGS = {
    "Infant": ("infant",),
    "Toddler": ("toddler",),
    "Toddler/Preschool": ("toddler", "preschool"),
    "Preschool": ("preschool",),
    "School Age": ("school",),
}
# Age order for rendering `ages_served` (youngest to oldest).
AGE_GROUP_ORDER = ("Infant", "Toddler", "Toddler/Preschool", "Preschool", "School Age")

# Monday-Sunday order for rendering shifts[].schedule, whose dict keys arrive
# in arbitrary order (a real record starts with "Friday" -- plan Sec 5.7).
DAY_ORDER = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

# The eight rate buckets on shifts[].rates_by_age[].rates (plan Sec 6.4);
# values are decimal strings, e.g. "322.00".
RATE_BUCKETS = (
    "full_time_daily",
    "full_time_hourly",
    "full_time_monthly",
    "full_time_weekly",
    "part_time_daily",
    "part_time_hourly",
    "part_time_monthly",
    "part_time_weekly",
)

# The complete set of Connecticut's 169 towns (CT has no county government --
# these are the state's only general-purpose municipalities). Seed list for
# the optional `-a verify=1` town-frontier audit (plan Sec 3.2/4.4); NOT used
# for the primary crawl. Deliberately incomplete for enumeration purposes --
# some providers report a mailing city (e.g. "Uncasville", "Winsted") that is
# a village inside one of these towns, not a town itself, and the search
# endpoint returns zero rows for those -- that gap is expected and is exactly
# what the id sweep exists to cover (plan Sec 3.2).
CT_TOWNS = (
    "Andover",
    "Ansonia",
    "Ashford",
    "Avon",
    "Barkhamsted",
    "Beacon Falls",
    "Berlin",
    "Bethany",
    "Bethel",
    "Bethlehem",
    "Bloomfield",
    "Bolton",
    "Bozrah",
    "Branford",
    "Bridgeport",
    "Bridgewater",
    "Bristol",
    "Brookfield",
    "Brooklyn",
    "Burlington",
    "Canaan",
    "Canterbury",
    "Canton",
    "Chaplin",
    "Cheshire",
    "Chester",
    "Clinton",
    "Colchester",
    "Colebrook",
    "Columbia",
    "Cornwall",
    "Coventry",
    "Cromwell",
    "Danbury",
    "Darien",
    "Deep River",
    "Derby",
    "Durham",
    "Eastford",
    "East Granby",
    "East Haddam",
    "East Hampton",
    "East Hartford",
    "East Haven",
    "East Lyme",
    "Easton",
    "East Windsor",
    "Ellington",
    "Enfield",
    "Essex",
    "Fairfield",
    "Farmington",
    "Franklin",
    "Glastonbury",
    "Goshen",
    "Granby",
    "Greenwich",
    "Griswold",
    "Groton",
    "Guilford",
    "Haddam",
    "Hamden",
    "Hampton",
    "Hartford",
    "Hartland",
    "Harwinton",
    "Hebron",
    "Kent",
    "Killingly",
    "Killingworth",
    "Lebanon",
    "Ledyard",
    "Lisbon",
    "Litchfield",
    "Lyme",
    "Madison",
    "Manchester",
    "Mansfield",
    "Marlborough",
    "Meriden",
    "Middlebury",
    "Middlefield",
    "Middletown",
    "Milford",
    "Monroe",
    "Montville",
    "Morris",
    "Naugatuck",
    "New Britain",
    "New Canaan",
    "New Fairfield",
    "New Hartford",
    "New Haven",
    "Newington",
    "New London",
    "New Milford",
    "Newtown",
    "Norfolk",
    "North Branford",
    "North Canaan",
    "North Haven",
    "North Stonington",
    "Norwalk",
    "Norwich",
    "Old Lyme",
    "Old Saybrook",
    "Orange",
    "Oxford",
    "Plainfield",
    "Plainville",
    "Plymouth",
    "Pomfret",
    "Portland",
    "Preston",
    "Prospect",
    "Putnam",
    "Redding",
    "Ridgefield",
    "Rocky Hill",
    "Roxbury",
    "Salem",
    "Salisbury",
    "Scotland",
    "Seymour",
    "Sharon",
    "Shelton",
    "Sherman",
    "Simsbury",
    "Somers",
    "Southbury",
    "Southington",
    "South Windsor",
    "Sprague",
    "Stafford",
    "Stamford",
    "Sterling",
    "Stonington",
    "Stratford",
    "Suffield",
    "Thomaston",
    "Thompson",
    "Tolland",
    "Torrington",
    "Trumbull",
    "Union",
    "Vernon",
    "Voluntown",
    "Wallingford",
    "Warren",
    "Washington",
    "Waterbury",
    "Waterford",
    "Watertown",
    "Westbrook",
    "West Hartford",
    "West Haven",
    "Weston",
    "Westport",
    "Wethersfield",
    "Willington",
    "Wilton",
    "Winchester",
    "Windham",
    "Windsor",
    "Windsor Locks",
    "Wolcott",
    "Woodbridge",
    "Woodbury",
    "Woodstock",
)


def extend_miss_streak(results, start, end, max_hit, trailing_miss):
    """Scan resolved ids ``[start, end]`` in order, updating discovery state.

    Pure and side-effect-free: ``results`` maps id -> bool (True if that id
    resolved to a provider); ``max_hit``/``trailing_miss`` are the running
    state from any prior blocks. Returns the updated ``(max_hit,
    trailing_miss)``. Kept free of Scrapy/network so it is trivially unit
    testable (plan Sec 4.1).
    """
    for id_ in range(start, end + 1):
        if results.get(id_):
            max_hit = id_
            trailing_miss = 0
        else:
            trailing_miss += 1
    return max_hit, trailing_miss


def clean_zip_full(raw):
    """Whitespace-clean a raw zip value (keeps a -4 suffix), or None.

    Source zips arrive as bare 5-digit, ZIP+4, and with stray leading spaces
    or a trailing tab (plan Sec 5.5). This is the value embedded in the
    composed `address` string -- contrast `zip5_from_clean`.
    """
    if not isinstance(raw, str):
        return None
    cleaned = re.sub(r"\s+", "", raw)
    return cleaned or None


def pad_zip_base(cleaned_zip):
    """Restore a ZIP's lost leading zero, or return the value unchanged.

    Some records publish the zip as a number, so ``06516`` arrives as ``6516``
    (plan Sec 10.1). Every Connecticut ZIP is ``06xxx``/``069xx``, so a 4-digit
    base is unambiguously a dropped leading zero. Only the base is padded --
    any ``-nnnn`` +4 suffix is left alone.
    """
    if not cleaned_zip:
        return cleaned_zip
    base, sep, plus4 = cleaned_zip.partition("-")
    if base.isdigit() and len(base) == 4:
        base = "0" + base
    return base + sep + plus4


def zip5_from_clean(cleaned_zip):
    """First 5 digits of an already-whitespace-cleaned, zero-padded zip."""
    if not cleaned_zip:
        return None
    digits = re.sub(r"\D", "", cleaned_zip)
    if len(digits) < 5:
        return None
    return digits[:5]


# Document `description` values that count as "the report for this visit",
# most-preferred first. A follow-up visit publishes only a "Follow-up
# Inspection Report" -- for that visit it *is* the report, so it is the
# fallback (plan Sec 10.1). Deliberately excludes "Inspection Report
# Addendum" (a supplement, not the report), "Corrective Action Plan", and
# "Legal Resolution"; every document stays reachable via `ct_documents`.
REPORT_DOC_PREFERENCE = ("Inspection Report", "Follow-up Inspection Report")


# The sentence the source puts where a street address would go when the
# provider has opted out of publishing one (plan Sec 10.1). Matched
# case-insensitively on the prefix -- the apostrophe is a plain ASCII one in
# every observed record, but the prefix stops short of it anyway.
SUPPRESSED_ADDRESS_PREFIX = "this provider"


def is_suppressed_address(street):
    """True when `street` is the "address has been hidden" sentinel."""
    if not isinstance(street, str):
        return False
    normalized = re.sub(r"\s+", " ", street).strip().lower()
    return normalized.startswith(SUPPRESSED_ADDRESS_PREFIX) and "hidden" in normalized


def compose_address(street, city, zip_full):
    """Compose the full `"street, city, CT zip"` address string (plan Sec 6.1).

    The source `address` field is street-only; this builds the common item's
    multi-part `address`. Never guesses a missing piece.
    """
    line_parts = [p for p in (street, city) if p]
    state_zip = "CT " + zip_full if zip_full else "CT"
    if line_parts:
        return ", ".join(line_parts) + ", " + state_zip
    return state_zip if zip_full else None


def ct_license_type_from_number(license_number):
    """Derive the license_type label from a license_number prefix, or None."""
    if not license_number:
        return None
    prefix = license_number.split(".", 1)[0].strip().upper()
    return LICENSE_PREFIX_MAP.get(prefix)


def ages_from_shifts(shifts):
    """Derive ``(ages_served, age_flags)`` from the union of every shift's
    ``population_by_age`` (plan Sec 6.2). A multi-shift provider's bands are
    unioned (a real record splits School Age into its own shift, plan Sec 8
    test 10). ``group: null`` entries are skipped. **No fallback** to the
    age_range_min/max weeks fields when shifts is empty -- see Sec 5.8: the
    38 records with age_range_max: 0 are the *same* 38 with shifts: [], so
    that fallback is broken in exactly the case that would trigger it.
    """
    if not shifts:
        return None, {}
    flags = {"infant": False, "toddler": False, "preschool": False, "school": False}
    labels_by_group = {}
    for shift in shifts:
        for band in shift.get("population_by_age") or []:
            group = band.get("group")
            if not group:
                continue
            labels_by_group.setdefault(group, band.get("label"))
            for flag in AGE_GROUP_FLAGS.get(group, ()):
                flags[flag] = True
    ordered_labels = [labels_by_group[g] for g in AGE_GROUP_ORDER if labels_by_group.get(g)]
    ages_served = ", ".join(ordered_labels) or None
    active_flags = {k: v for k, v in flags.items() if v}
    return ages_served, active_flags


def _strip_seconds(value):
    """ "07:15:00" -> "07:15"; anything else passed through unchanged."""
    if isinstance(value, str) and value.count(":") == 2:
        return value.rsplit(":", 1)[0]
    return value


def first_schedule(shifts):
    """The first shift with a populated `schedule`, or None.

    Multiple shifts on one provider commonly repeat the same weekly window
    (verified: id 970's two shifts share an identical Mon-Fri 07:15-17:30
    schedule) -- the first populated one is the representative one.
    """
    for shift in shifts or []:
        schedule = shift.get("schedule")
        if schedule:
            return schedule
    return None


def schedule_rows(schedule):
    """Monday->Sunday ``[(day, open, close)]`` rows from a schedule dict."""
    rows = []
    for day in DAY_ORDER:
        window = schedule.get(day)
        if not window:
            continue
        rows.append((day, _strip_seconds(window.get("start_time")), _strip_seconds(window.get("end_time"))))
    return rows


def format_hours(schedule):
    """Render a schedule dict into a compact human string, or None.

    Collapses to "Monday-Friday 07:15-17:30" when every listed day shares the
    same open/close, else lists each day "Monday 07:15-17:30; ...".
    """
    if not schedule:
        return None
    rows = schedule_rows(schedule)
    if not rows:
        return None
    times = {(o, c) for _, o, c in rows}
    if len(times) == 1 and len(rows) > 1:
        o, c = rows[0][1], rows[0][2]
        return f"{rows[0][0]}-{rows[-1][0]} {o}-{c}".strip()
    return "; ".join(f"{day} {o}-{c}".strip() for day, o, c in rows)


def rates_from_shifts(shifts):
    """[{age_group, label, <non-null rate buckets>}] from rates_by_age (plan
    Sec 6.4). Only the ~48% of providers with at least one populated rate
    bucket contribute an entry; the seven all-null buckets are omitted per
    entry too."""
    out = []
    for shift in shifts or []:
        for band in shift.get("rates_by_age") or []:
            rates = band.get("rates") or {}
            populated = {k: rates[k] for k in RATE_BUCKETS if rates.get(k) is not None}
            if not populated:
                continue
            entry = {"age_group": band.get("group"), "label": band.get("label")}
            entry.update(populated)
            out.append(entry)
    return out


class ConnecticutSpider(scrapy.Spider):
    name = "connecticut"
    allowed_domains = ["www.211childcare.org"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, max_id=None, violations=1, concurrency=8, verify=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_id_override = int(max_id) if max_id else None
        self.do_violations = str(violations).strip().lower() not in ("0", "false")
        self.concurrency = int(concurrency)
        self.verify_audit = str(verify).strip().lower() not in ("0", "false", "")

        self.max_id = None
        self.missing = 0
        self.emitted = 0
        self.provider_failures = 0
        self.inspection_fetch_count = 0
        self.inspection_detail_failures = 0
        self.pending = {}  # provider_id -> {"item": ..., "outstanding": n}
        self.sweep_ids = set()  # provider ids the id sweep actually emitted

        # -a verify=1 town-frontier audit state (off by default, plan Sec 4.4).
        self.town_ids = set()
        self.towns_queried = set()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Apply `-a concurrency=N` to the *crawler's* settings.

        Mutating ``self.custom_settings`` from ``__init__`` is a silent
        no-op: ``Crawler.__init__`` reads ``custom_settings`` off the
        **class** before any spider instance exists. ``from_crawler`` runs
        early enough (before settings are frozen / the engine reads
        concurrency) for `crawler.settings.set(...)` to actually take effect
        -- see maryland.py / kentucky.py for the same fix (plan Sec 4.5).
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        for key in ("CONCURRENT_REQUESTS", "CONCURRENT_REQUESTS_PER_DOMAIN"):
            crawler.settings.set(key, spider.concurrency, priority="spider")
        return spider

    def _get(self, url, callback, meta=None, errback=None):
        return scrapy.Request(
            url, callback=callback, errback=errback, headers=HEADERS, meta=meta or {}, dont_filter=True
        )

    # --- Phase 1: max_id discovery -------------------------------------- #

    def start_requests(self):
        if self.max_id_override:
            self.max_id = self.max_id_override
            self.logger.info(
                "Connecticut: max_id override=%d, skipping discovery",
                self.max_id,
            )
            yield from self._provider_sweep_requests()
        else:
            self.logger.info(
                "Connecticut: discovering max_id starting from KNOWN_MAX_ID=%d",
                KNOWN_MAX_ID,
            )
            self._discovery_results = {}
            self._discovery_max_hit = 0
            self._discovery_trailing_miss = 0
            yield from self._discovery_block_requests(KNOWN_MAX_ID)

        if self.verify_audit:
            self.logger.info(
                "Connecticut: verify audit ON -- sweeping %d seed towns",
                len(CT_TOWNS),
            )
            for town in CT_TOWNS:
                self.towns_queried.add(town.strip().lower())
                yield self._town_search_request(town)

    def _discovery_block_requests(self, block_start):
        self._discovery_block_start = block_start
        self._discovery_block_end = block_start + DISCOVERY_BLOCK_SIZE - 1
        self._discovery_pending = DISCOVERY_BLOCK_SIZE
        for id_ in range(block_start, self._discovery_block_end + 1):
            yield self._get(
                PROVIDER_URL.format(id_),
                self.parse_discovery,
                meta={"discovery_id": id_},
                errback=self.discovery_errback,
            )

    def parse_discovery(self, response):
        id_ = response.meta["discovery_id"]
        exists = response.json() is not None
        yield from self._record_discovery(id_, exists)

    def discovery_errback(self, failure):
        id_ = failure.request.meta.get("discovery_id")
        self.logger.warning(
            "Connecticut: discovery request for id %s failed (%s) -- treating as a miss",
            id_,
            failure.value,
        )
        yield from self._record_discovery(id_, False)

    def _record_discovery(self, id_, exists):
        self._discovery_results[id_] = exists
        self._discovery_pending -= 1
        if self._discovery_pending > 0:
            return
        self._discovery_max_hit, self._discovery_trailing_miss = extend_miss_streak(
            self._discovery_results,
            self._discovery_block_start,
            self._discovery_block_end,
            self._discovery_max_hit,
            self._discovery_trailing_miss,
        )
        if self._discovery_trailing_miss >= MISS_STREAK_LIMIT:
            self.max_id = self._discovery_max_hit
            self.logger.info(
                "Connecticut: discovery found ceiling max_id=%d (%d consecutive misses)",
                self.max_id,
                self._discovery_trailing_miss,
            )
            if self.max_id < KNOWN_MAX_ID:
                self.logger.warning(
                    "Connecticut: discovered max_id=%d is BELOW the known "
                    "baseline %d -- the API likely changed shape, not that "
                    "CT lost providers",
                    self.max_id,
                    KNOWN_MAX_ID,
                )
            yield from self._provider_sweep_requests()
            return
        yield from self._discovery_block_requests(self._discovery_block_end + 1)

    # --- Phase 2: provider sweep ----------------------------------------- #

    def _provider_sweep_requests(self):
        self.logger.info("Connecticut: sweeping ids 1..%d", self.max_id)
        for id_ in range(1, self.max_id + 1):
            yield self._get(
                PROVIDER_URL.format(id_),
                self.parse_provider,
                meta={"provider_id": id_},
                errback=self.provider_errback,
            )

    def provider_errback(self, failure):
        provider_id = failure.request.meta.get("provider_id")
        self.provider_failures += 1
        self.logger.warning(
            "Connecticut: provider %s request failed after retries (%s)",
            provider_id,
            failure.value,
        )

    def parse_provider(self, response):
        provider_id = response.meta["provider_id"]
        data = response.json()
        if data is None:
            # ~600 of these are normal (plan Sec 4.2) -- no log, no retry.
            self.missing += 1
            return

        item = self._item_from_provider(data, provider_id)
        raw_inspections = data.get("inspections") or []
        summaries = self._summary_inspections(raw_inspections)
        if summaries:
            item["inspections"] = summaries
            counts = [s["ct_violations_count"] for s in summaries if s.get("ct_violations_count") is not None]
            if counts:
                item["deficiencies"] = sum(counts)

        if not raw_inspections or not self.do_violations:
            yield self._emit_provider(provider_id, item)
            return

        # Hold-and-join (plan Sec 4.3): stash the half-built item and fire one
        # request per inspection id; the item is yielded once every detail
        # call has resolved (success, null, or errback -- all three paths
        # decrement the counter, guarding against a provider that never gets
        # emitted).
        self.pending[provider_id] = {
            "item": item,
            "outstanding": len(raw_inspections),
        }
        for insp in raw_inspections:
            insp_id = insp.get("id")
            if insp_id is None:
                yield from self._maybe_emit_pending(provider_id)
                continue
            yield self._get(
                INSPECTION_URL.format(insp_id),
                self.parse_inspection_detail,
                meta={"provider_id": provider_id, "inspection_id": insp_id},
                errback=self.inspection_errback,
            )

    def _emit_provider(self, provider_id, item):
        self.sweep_ids.add(provider_id)
        self.emitted += 1
        return item

    def _item_from_provider(self, data, provider_id):
        item = ProviderItem()
        item["source_state"] = "Connecticut"
        item["provider_url"] = LISTING_URL_TMPL.format(provider_id)
        item["ct_provider_id"] = provider_id

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        business_name = data.get("business_name")
        put("provider_name", business_name)
        put("license_number", data.get("license_number"))
        put("ct_provider_uid", data.get("provider_uid"))

        # license_holder only when the naive first/last split differs from
        # business_name (plan Sec 5.5) -- most records are a naive split of
        # the business name, not a real person's name.
        first = data.get("first_name") or ""
        last = data.get("last_name") or ""
        candidate = re.sub(r"\s+", " ", f"{first} {last}").strip()
        business_norm = re.sub(r"\s+", " ", business_name or "").strip()
        if candidate and candidate != business_norm:
            item["license_holder"] = candidate

        put("provider_type", data.get("type_of_provider"))
        type_of_care = data.get("type_of_care")
        if type_of_care and type_of_care != data.get("type_of_provider"):
            put("ct_type_of_care", type_of_care)

        searchable = bool(data.get("searchable"))
        item["ct_searchable"] = searchable
        item["status"] = "Listed" if searchable else "Not Listed"

        item["ct_licensed"] = bool(data.get("license"))
        put("ct_license_type", ct_license_type_from_number(data.get("license_number")))
        put("ct_elevate_membership_level", data.get("elevate_membership_level"))

        # --- address ---
        street = data.get("address")
        city = data.get("city")
        zip_full = pad_zip_base(clean_zip_full(data.get("zip")))
        zip5 = zip5_from_clean(zip_full)
        put("city", city)
        item["state"] = "CT"
        if zip5:
            item["zip"] = zip5
        # A provider can opt out of publishing a street address; the source
        # then carries a sentence where the street belongs (plan Sec 10.1).
        # Flag it and drop it rather than composing it into `address` --
        # city/zip/coordinates are real and are kept (ks_address_suppressed
        # precedent).
        suppressed = is_suppressed_address(street)
        item["ct_address_suppressed"] = suppressed
        if not suppressed:
            put("address", compose_address(street, city, zip_full))
        put("phone", data.get("phone_number"))
        put("email", data.get("email"))
        put("provider_website", data.get("website"))

        lat = data.get("latitude")
        lng = data.get("longitude")
        if lat and lng:  # drop the pair when either is 0.0/null (Sec 5.4)
            item["latitude"] = str(lat)
            item["longitude"] = str(lng)

        put("capacity", data.get("capacity"))
        put("ct_capacity_three_and_under", data.get("capacity_three_and_under"))
        put("ct_capacity_full_time", data.get("capacity_full_time"))
        put("ct_capacity_school_aged", data.get("capacity_school_aged"))

        ages_served, age_flags = ages_from_shifts(data.get("shifts"))
        put("ages_served", ages_served)
        for field, value in age_flags.items():
            item[field] = value

        schedule = first_schedule(data.get("shifts"))
        put("hours", format_hours(schedule))
        if schedule:
            item["ct_schedule"] = [{"day": d, "open": o, "close": c} for d, o, c in schedule_rows(schedule)]
        rates = rates_from_shifts(data.get("shifts"))
        if rates:
            item["ct_rates"] = rates

        if data.get("age_range_min") is not None:
            item["ct_age_range_min_weeks"] = data["age_range_min"]
        if data.get("age_range_max"):  # 0 means missing, not "birth" (Sec 5.8)
            item["ct_age_range_max_weeks"] = data["age_range_max"]

        if data.get("financial_assistance") is not None:
            item["scholarships_accepted"] = bool(data["financial_assistance"])
        if data.get("accepting_referrals") is not None:
            item["ct_accepting_referrals"] = bool(data["accepting_referrals"])
        if data.get("administers_meds") is not None:
            item["ct_administers_meds"] = bool(data["administers_meds"])
        if data.get("wheelchair") is not None:
            item["ct_wheelchair_accessible"] = bool(data["wheelchair"])

        accreditations = data.get("accreditations") or []
        put("ct_accreditations", accreditations)
        item["ct_head_start"] = bool(data.get("headstart_funding")) or ("Head Start" in accreditations)

        put("languages", data.get("languages"))
        put("ct_school_districts", data.get("school_districts"))
        put("ct_transportation", data.get("transportation"))
        put("ct_education_levels", data.get("education_levels"))
        put("ct_special_needs", data.get("special_needs"))
        put("ct_date_established", data.get("date_established"))
        put("ct_oec_contact_id", data.get("oec_contact_id"))

        return item

    @staticmethod
    def _summary_inspections(raw_inspections):
        """Build InspectionItems from a provider record's embedded summary
        rows -- free (zero extra requests), plan Sec 6.3."""
        out = []
        for insp in raw_inspections:
            entry = InspectionItem()
            if insp.get("visited_on"):
                entry["date"] = insp["visited_on"]
            if insp.get("visit_type"):
                entry["type"] = insp["visit_type"]
            if insp.get("id") is not None:
                entry["ct_inspection_id"] = insp["id"]
            if insp.get("case_uid") is not None:
                entry["ct_case_uid"] = insp["case_uid"]
            if insp.get("status"):
                entry["ct_inspection_status"] = insp["status"]
            if insp.get("severity"):
                entry["ct_severity"] = insp["severity"]
            if insp.get("reason"):
                entry["ct_reason"] = insp["reason"]
            if insp.get("resolution"):
                entry["ct_resolution"] = insp["resolution"]
            if insp.get("violations_count") is not None:
                entry["ct_violations_count"] = insp["violations_count"]
            if insp.get("document_count") is not None:
                entry["ct_document_count"] = insp["document_count"]
            out.append(entry)
        return out

    # --- Phase 3: inspection detail fan-out (default ON) ------------------ #

    def parse_inspection_detail(self, response):
        provider_id = response.meta["provider_id"]
        inspection_id = response.meta["inspection_id"]
        self.inspection_fetch_count += 1
        # Everything from the JSON decode through the merge is guarded: this
        # callback owns one inspection's *detail*, but the provider item is
        # only released once every outstanding inspection has reported back
        # (Sec 4.3). An escaping exception would therefore strand the whole
        # provider forever -- losing a parent record over one bad inspection.
        # Degrade instead: count it, log it, and always fall through to the
        # decrement below so the parent still gets emitted (with that one
        # inspection left at summary-only detail).
        try:
            data = response.json()
            if data is not None:
                self._merge_inspection_detail(provider_id, inspection_id, data)
            else:
                self.inspection_detail_failures += 1
                self.logger.warning(
                    "Connecticut: inspection %s (provider %s) returned a null detail body",
                    inspection_id,
                    provider_id,
                )
        except Exception:
            self.inspection_detail_failures += 1
            self.logger.exception(
                "Connecticut: inspection %s detail failed to parse for "
                "provider %s -- keeping the provider, dropping this "
                "inspection's violation/document detail",
                inspection_id,
                provider_id,
            )
        yield from self._maybe_emit_pending(provider_id)

    def inspection_errback(self, failure):
        provider_id = failure.request.meta.get("provider_id")
        inspection_id = failure.request.meta.get("inspection_id")
        self.inspection_detail_failures += 1
        # Same contract as parse_inspection_detail: the logging must never be
        # what strands the parent, so the decrement stays outside the guard.
        try:  # noqa: SIM105
            self.logger.warning(
                "Connecticut: inspection %s detail request failed for provider %s (%s)",
                inspection_id,
                provider_id,
                failure.value,
            )
        except Exception:  # pragma: no cover -- defensive only
            pass
        yield from self._maybe_emit_pending(provider_id)

    def _merge_inspection_detail(self, provider_id, inspection_id, data):
        pending = self.pending.get(provider_id)
        if not pending:
            return
        for insp_item in pending["item"].get("inspections") or []:
            if insp_item.get("ct_inspection_id") == inspection_id:
                self._apply_inspection_detail(insp_item, data)
                break

    @staticmethod
    def _apply_inspection_detail(insp_item, data):
        # NOTE the source API's field names are swapped: `description` holds
        # the regulation cite and `statute` holds the requirement text (plan
        # Sec 6.3). violations_count is the authoritative "any real
        # violations" signal -- a clean inspection's `violations[]` still
        # carries one "No Violations" sentinel row, which this filters out.
        violations_count = data.get("violations_count") or 0
        raw_violations = data.get("violations") or []
        if violations_count and raw_violations:
            insp_item["ct_violations"] = [
                {"regulation": v.get("description"), "category": v.get("category"), "statute": v.get("statute")}
                for v in raw_violations
            ]
        documents = data.get("documents") or []
        if documents:
            insp_item["ct_documents"] = [
                {
                    "description": d.get("description"),
                    "document_type": d.get("document_type"),
                    "visited_on": d.get("visited_on"),
                    "link": d.get("link"),
                }
                for d in documents
            ]
            for wanted in REPORT_DOC_PREFERENCE:
                link = next((d["link"] for d in documents if d.get("description") == wanted and d.get("link")), None)
                if link:
                    insp_item["report_url"] = link
                    break

    def _maybe_emit_pending(self, provider_id):
        pending = self.pending.get(provider_id)
        if pending is None:
            return
        pending["outstanding"] -= 1
        if pending["outstanding"] > 0:
            return
        del self.pending[provider_id]
        yield self._emit_provider(provider_id, pending["item"])

    # --- optional: -a verify=1 town-frontier coverage audit (plan Sec 4.4) #

    def _town_search_request(self, town):
        url = f"{SEARCH_URL}?town={quote(town)}"
        return self._get(url, self.parse_town_search, meta={"town": town}, errback=self.town_search_errback)

    def town_search_errback(self, failure):
        self.logger.warning(
            "Connecticut: verify-audit town search failed for %r (%s)",
            failure.request.meta.get("town"),
            failure.value,
        )

    def parse_town_search(self, response):
        """Harvest ids + newly-seen town names from a town search response.

        Trusts each record's own ``town`` field (not the query param) to
        discover new towns to enqueue -- safe even when the server
        fuzzy-matched a bogus/typo'd query to a different town (plan
        Sec 5.2), since every record it returns genuinely does carry that
        town in its own data.
        """
        records = response.json() or []
        for record in records:
            rid = record.get("id")
            if rid is not None:
                self.town_ids.add(rid)
            new_town = record.get("town")
            if not new_town:
                continue
            key = new_town.strip().lower()
            if key and key not in self.towns_queried:
                self.towns_queried.add(key)
                yield self._town_search_request(new_town.strip())

    # --- shutdown ---------------------------------------------------------- #

    def closed(self, reason):
        self.logger.info(
            "Connecticut: finished (%s) -- max_id=%s, %d providers emitted, "
            "%d missing ids, %d provider request failures, %d inspection "
            "details fetched, %d inspection detail failures, %d items still "
            "pending at shutdown",
            reason,
            self.max_id,
            self.emitted,
            self.missing,
            self.provider_failures,
            self.inspection_fetch_count,
            self.inspection_detail_failures,
            len(self.pending),
        )
        if self.emitted < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Connecticut: only %d providers emitted (< %d baseline) -- possible incomplete crawl",
                self.emitted,
                EXPECTED_MIN_PROVIDERS,
            )
        if self.pending:
            # closed() cannot re-inject items into the pipeline -- this is a
            # diagnostic-only flush/count (plan Sec 4.3/10): under normal
            # operation every pending provider is resolved via
            # parse_inspection_detail or inspection_errback, so this should
            # be 0. A nonzero count here means a request was dropped without
            # invoking either.
            self.logger.warning(
                "Connecticut: %d provider(s) still pending at shutdown "
                "(inspection detail requests never resolved) -- their items "
                "were NOT emitted: %s",
                len(self.pending),
                sorted(self.pending)[:50],
            )
        if self.verify_audit:
            missing_from_sweep = self.town_ids - self.sweep_ids
            if missing_from_sweep:
                self.logger.warning(
                    "Connecticut: verify audit found %d id(s) the "
                    "town-frontier search knows about but the id sweep "
                    "missed: %s",
                    len(missing_from_sweep),
                    sorted(missing_from_sweep)[:50],
                )
            else:
                self.logger.info(
                    "Connecticut: verify audit found no gaps in the id sweep (%d town-search ids, all covered)",
                    len(self.town_ids),
                )
