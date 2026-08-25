"""Indiana child care provider spider.

Source: the FSSA provider map at
https://secure.in.gov/apps/fssa/providersearch/map -- an Angular SPA backed by a
clean JSON API (no cookie/referer/token/CAPTCHA required).

Two endpoints, a classic search -> detail pattern:

  * ``childCareSearch`` (POST) enumerates providers inside a map bounding box.
    It reports ``totalResults`` (the true match count) and returns up to
    ``pageSize`` provider summaries plus ``providerGroups`` (map clusters -- a
    display overlay we ignore; they are fully redundant with the paginated
    ``providers`` list).
  * ``search/id`` (POST) returns the full record for one ``{providerId,
    locationId}``.

Because a single generous statewide bounding box reports the whole-state
``totalResults`` (widening the box does not increase it) and pagination is
complete (the union of every page's provider ids equals ``totalResults`` with no
duplicates), enumeration is just: one statewide box, page through it. No
adaptive geographic grid is needed (contrast north_dakota.py).

Each unique provider is then fetched from ``search/id`` for the full record
(license dates, capacity by age, hours, inspections, ...).
"""

import json
import math

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://secure.in.gov/apps/fssa/providersearch/api/providers/childCareSearch"
DETAIL_URL = "https://secure.in.gov/apps/fssa/providersearch/api/providers/search/id"
# The map is an SPA with a POST-only detail; there is no per-provider GET URL.
# Emit the map base as `provider_url`; the real identity is `in_provider_id` /
# `in_location_id` (DC POST-only precedent).
MAP_URL = "https://secure.in.gov/apps/fssa/providersearch/map"

# Generous statewide bounding box (padded beyond Indiana's borders). A wide box
# is safe: `totalResults` reflects the real dataset, not the box area.
SEARCH_AREA = {
    "northEast": {"lat": 41.90, "lng": -84.60},
    "southWest": {"lat": 37.70, "lng": -88.20},
}
# Any interior point; only influences the (unused) `distance` sort key.
CENTER = {"LAT": 39.90, "LNG": -86.20}
PAGE_SIZE = 250  # the app's page size; larger works but 5000 -> HTTP 500.

# Baseline unique count (calibrated live 2026-08-18: 3,978). Warn if a run falls
# far short -- a sign the API shape or coverage changed.
EXPECTED_MIN_PROVIDERS = 3500

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"),
    "Accept": "application/json, text/plain, */*",
}
POST_HEADERS = {
    **HEADERS,
    "Content-Type": "application/json;charset=utf-8",
    "Origin": "https://secure.in.gov",
    "Referer": MAP_URL,
}

# Age-band label words -> approximate age in years. Indiana expresses licensed
# ages as ordinal words (Infant, Toddler, Two ... Eighteen) plus "30 Months".
AGE_WORD_TO_YEARS = {
    "infant": 0,
    "toddler": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "30 months": 2.5,
}


def join_names(people):
    """Join a list of ``{name: ...}`` dicts into a ``", "`` string, or None."""
    if not people:
        return None
    names = [p.get("name", "").strip() for p in people if p.get("name")]
    return ", ".join(n for n in names if n) or None


def title_county(name):
    """Title-case an ALL-CAPS county name (``"MARION"`` -> ``"Marion"``)."""
    if not isinstance(name, str) or not name.strip():
        return None
    return name.strip().title()


def _band_years(label):
    """Approximate age in years for a band label word; None if unrecognized."""
    if not label:
        return None
    return AGE_WORD_TO_YEARS.get(label.strip().lower())


def ages_from_bands(licensed_ages):
    """Derive (ages_served string, age-group boolean dict) from licensedAges.

    ``licensed_ages`` is the API's list of ``{startAge, endAge, quantity}`` (or
    None). Returns ``(ages_served_or_None, {infant, toddler, preschool,
    school})`` where each flag is True if any band overlaps that group's age
    span. An empty/None input yields ``(None, {})`` (no flags set).
    """
    if not licensed_ages:
        return None, {}
    labels = []
    flags = {"infant": False, "toddler": False, "preschool": False, "school": False}
    for band in licensed_ages:
        start_label = band.get("startAge")
        end_label = band.get("endAge")
        labels.append(f"{start_label}-{end_label}" if end_label else start_label)
        start = _band_years(start_label)
        if start is None:
            continue
        end = _band_years(end_label)
        if end is None:
            end = start
        if start <= 0:
            flags["infant"] = True
        if start <= 1 <= end:
            flags["toddler"] = True
        if start <= 5 and end >= 2:
            flags["preschool"] = True
        if end >= 6:
            flags["school"] = True
    ages_served = ", ".join(str(x) for x in labels if x) or None
    active = {k: v for k, v in flags.items() if v}
    return ages_served, active


def format_schedule(schedule):
    """Render the weekly schedule into a compact human string, or None.

    Collapses to ``"Monday-Friday 6:30 AM-6:00 PM"`` when every listed day
    shares the same open/close; otherwise lists each day ``"Monday 6:30 AM-6:00
    PM; ..."``.
    """
    if not schedule:
        return None
    rows = [(s.get("dayOfWeek"), s.get("openTime"), s.get("closeTime")) for s in schedule if s.get("dayOfWeek")]
    if not rows:
        return None
    times = {(o, c) for _, o, c in rows}
    if len(times) == 1 and len(rows) > 1:
        o, c = rows[0][1], rows[0][2]
        span = f"{rows[0][0]}-{rows[-1][0]}"
        return f"{span} {o}-{c}".strip()
    parts = []
    for day, o, c in rows:
        window = f" {o}-{c}".rstrip() if o or c else ""
        parts.append(f"{day}{window}".strip())
    return "; ".join(parts)


class IndianaSpider(scrapy.Spider):
    name = "indiana"
    allowed_domains = ["secure.in.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = set()  # provider ids already scheduled for detail
        self.page_count = 0

    # --- enumeration (search) ------------------------------------------- #

    def _search_request(self, page):
        body = {
            "categoryIds": [],  # [] == all; the key MUST be present (else 400)
            "coordinates": CENTER,
            "searchArea": SEARCH_AREA,
            "pageNumber": page,
            "pageSize": PAGE_SIZE,
        }
        return scrapy.Request(
            SEARCH_URL,
            method="POST",
            body=json.dumps(body),
            headers=POST_HEADERS,
            callback=self.parse_search,
            meta={"page": page},
            dont_filter=True,
        )

    def start_requests(self):
        yield self._search_request(1)

    def parse_search(self, response):
        data = response.json()
        page = response.meta["page"]
        self.page_count += 1
        providers = data.get("providers") or []

        # On page 1, learn the total and fan out the remaining pages.
        if page == 1:
            total = data.get("totalResults") or 0
            num_pages = math.ceil(total / PAGE_SIZE) if total else 0
            self.logger.info(
                "Indiana: totalResults=%d -> %d pages of %d; fanning out pages 2..%d",
                total,
                num_pages,
                PAGE_SIZE,
                num_pages,
            )
            for p in range(2, num_pages + 1):
                yield self._search_request(p)

        for prov in providers:
            pid = prov.get("id")
            if not pid or pid in self.seen:
                continue
            self.seen.add(pid)
            loc = prov.get("location") or {}
            coords = loc.get("coordinates") or {}
            yield scrapy.Request(
                DETAIL_URL,
                method="POST",
                body=json.dumps(
                    {
                        "providerId": pid,
                        "locationId": loc.get("id"),
                        "coordinates": {"LAT": coords.get("lat"), "LNG": coords.get("lng")},
                    }
                ),
                headers=POST_HEADERS,
                callback=self.parse_detail,
                meta={"provider_id": pid, "location_id": loc.get("id")},
                dont_filter=True,
            )

        self.logger.info(
            "Indiana: search page %d parsed (%d providers); %d unique providers so far",
            page,
            len(providers),
            len(self.seen),
        )

    # --- detail (per provider) ------------------------------------------ #

    def parse_detail(self, response):
        prov = response.json().get("provider") or {}
        loc = prov.get("location") or {}
        pid = prov.get("id") or response.meta.get("provider_id")
        lid = loc.get("id") or response.meta.get("location_id")

        item = ProviderItem()
        item["source_state"] = "Indiana"
        item["provider_url"] = MAP_URL

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        # --- identity ---
        put("provider_name", prov.get("name"))
        put("license_number", pid)  # Indiana has no license number
        put("in_provider_id", pid)
        put("in_location_id", lid)
        put("license_holder", join_names(loc.get("applicants")))

        # --- type / status ---
        put("provider_type", loc.get("providerType"))
        put("in_ptq_level", loc.get("ptqLevel"))
        if loc.get("isTemporarilyClosed"):
            item["status"] = "Temporary Closure"
            item["in_is_temporarily_closed"] = True
            put("in_temporarily_closed_message", loc.get("temporarilyClosedMessage"))
        else:
            put("status", loc.get("status"))  # "Open"

        # --- address (homes suppress line1/city/state; keep zip/coords/county) ---
        put("address", loc.get("line1"))
        put("city", loc.get("city"))
        item["state"] = "IN"  # the Indiana licensing DB -- unambiguous
        put("zip", loc.get("zipCode"))
        counties = loc.get("counties") or []
        if counties:
            put("county", title_county(counties[0].get("name")))
        coords = loc.get("coordinates") or {}
        if coords.get("lat") is not None:
            item["latitude"] = str(coords["lat"])
        if coords.get("lng") is not None:
            item["longitude"] = str(coords["lng"])
        put("phone", loc.get("phoneNumber"))

        # --- license ---
        lic = loc.get("license") or {}
        put("license_type", lic.get("typeDescription"))
        put("license_begin_date", lic.get("effectiveDate"))
        put("license_expiration", lic.get("terminationDate"))

        # --- capacity / ages (capacity = sum of per-band quantities) ---
        licensed_ages = loc.get("licensedAges") or []
        if licensed_ages:
            total_cap = sum(b.get("quantity") or 0 for b in licensed_ages)
            if total_cap:
                item["capacity"] = total_cap
            item["in_licensed_ages"] = [
                {"start_age": b.get("startAge"), "end_age": b.get("endAge"), "quantity": b.get("quantity")}
                for b in licensed_ages
            ]
        ages_served, age_flags = ages_from_bands(licensed_ages)
        put("ages_served", ages_served)
        for field, value in age_flags.items():
            item[field] = value

        # --- programs / subsidy / quality-adjacent ---
        item["scholarships_accepted"] = bool(loc.get("isCcdf"))
        item["in_is_ccdf"] = bool(loc.get("isCcdf"))
        programs = [p.get("programDescription") for p in (loc.get("programs") or []) if p.get("programDescription")]
        put("in_programs", programs)
        put("accreditation", [a.get("name") for a in (loc.get("accreditations") or []) if a.get("name")])

        # --- schedule / hours ---
        put("hours", format_schedule(loc.get("schedule")))
        if loc.get("schedule"):
            item["in_schedule"] = [
                {"day": s.get("dayOfWeek"), "open": s.get("openTime"), "close": s.get("closeTime")}
                for s in loc["schedule"]
            ]

        # --- health / complaints ---
        if loc.get("healthViolationCount") is not None:
            item["in_health_violation_count"] = loc["healthViolationCount"]
        complaints = loc.get("complaints") or []
        if complaints:
            item["in_complaints"] = [
                {"complaint_date": c.get("complaintDate"), "issue": c.get("issue"), "closed_date": c.get("closedDate")}
                for c in complaints
            ]

        # --- inspections ---
        inspections = self._parse_inspections(loc.get("inspections") or [])
        if inspections:
            item["inspections"] = inspections

        yield item

    @staticmethod
    def _parse_inspections(raw):
        out = []
        for ins in raw:
            entry = InspectionItem()
            if ins.get("surveyDate"):
                entry["date"] = ins["surveyDate"]
            if ins.get("departmentDescription"):
                entry["type"] = ins["departmentDescription"]
            rule = ins.get("centerRule") or {}
            if rule.get("code"):
                entry["in_rule_code"] = rule["code"]
            if rule.get("description"):
                entry["in_rule_description"] = rule["description"]
            if ins.get("noncomplianceStatement"):
                entry["in_noncompliance"] = ins["noncomplianceStatement"]
            if ins.get("isHealthViolation") is not None:
                entry["in_is_health_violation"] = ins["isHealthViolation"]
            if ins.get("correctionDate"):
                entry["in_correction_date"] = ins["correctionDate"]
            out.append(entry)
        return out

    def closed(self, reason):
        self.logger.info(
            "Indiana: finished (%s) -- %d search pages, %d unique providers",
            reason,
            self.page_count,
            len(self.seen),
        )
        if len(self.seen) < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Indiana: only %d providers found (< %d baseline) -- possible incomplete crawl",
                len(self.seen),
                EXPECTED_MIN_PROVIDERS,
            )
