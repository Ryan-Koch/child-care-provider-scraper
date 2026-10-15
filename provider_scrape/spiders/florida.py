import asyncio
import json
import re
from collections import Counter
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse

import scrapy
from scrapy_playwright.page import PageMethod

from ..items import InspectionItem, ProviderItem

LANDING_URL = "https://caressearch.myflfamilies.com/"
SEARCH_API_BASE = "https://caresapi.myflfamilies.com/api/publicSearch/Search"
SEARCH_API_PATH = "caresapi.myflfamilies.com/api/publicSearch/Search"
SPA_SEARCH_URL = "https://caressearch.myflfamilies.com/PublicSearch/Search"

# Florida's 67 counties. Names match what the public-search API expects;
# Miami-Dade is hyphenated and the St. counties keep the period.
FL_COUNTIES = [
    "Alachua",
    "Baker",
    "Bay",
    "Bradford",
    "Brevard",
    "Broward",
    "Calhoun",
    "Charlotte",
    "Citrus",
    "Clay",
    "Collier",
    "Columbia",
    "DeSoto",
    "Dixie",
    "Duval",
    "Escambia",
    "Flagler",
    "Franklin",
    "Gadsden",
    "Gilchrist",
    "Glades",
    "Gulf",
    "Hamilton",
    "Hardee",
    "Hendry",
    "Hernando",
    "Highlands",
    "Hillsborough",
    "Holmes",
    "Indian River",
    "Jackson",
    "Jefferson",
    "Lafayette",
    "Lake",
    "Lee",
    "Leon",
    "Levy",
    "Liberty",
    "Madison",
    "Manatee",
    "Marion",
    "Martin",
    "Miami-Dade",
    "Monroe",
    "Nassau",
    "Okaloosa",
    "Okeechobee",
    "Orange",
    "Osceola",
    "Palm Beach",
    "Pasco",
    "Pinellas",
    "Polk",
    "Putnam",
    "Santa Rosa",
    "Sarasota",
    "Seminole",
    "St. Johns",
    "St. Lucie",
    "Sumter",
    "Suwannee",
    "Taylor",
    "Union",
    "Volusia",
    "Wakulla",
    "Walton",
    "Washington",
]


# bug_12: FL's per-county search is a loose free-text match -- a county
# search also returns any provider whose address/city/name/DBA happens to
# contain that county's name as a substring (e.g. "Orange" matches "Orange
# Park", "Dixie" matches "Old Dixie Hwy", "Gulf" matches "Gulf Coast ABLE
# Academy"). Sweeping all 67 counties therefore returns the same provider
# more than once under different (sometimes wrong) counties, which in turn
# makes every inspection on that provider look duplicated. See
# tasks/common_fields_epic/bug_12_florida_inspection_id_duplicates.md and
# bug_12_fl_dup_analysis.json for the full evidence.
#
# FL license numbers also embed the county as a 2-letter code immediately
# after a 2-digit DCF service-district number (e.g. "C11MD0431" -> district
# 11, county code "MD" -> Miami-Dade). The same code recurs across
# districts for different counties (e.g. "MA" alone is Manatee, Martin,
# Marion, or Madison depending on the district), so the (district, code)
# *pair* is what's authoritative, not the code alone. This map was derived
# on 2026-10-15 from hf_sample/florida.json: of the 585 license_numbers the
# search sweep returned under more than one county, 474 had exactly one
# candidate county whose name did NOT appear as a substring of the
# provider's own address/city/name/DBA (i.e. an unambiguous "true" county,
# since the substring match is the search's own false-positive signature).
# Grouping those 474 resolved truths by (district, code) was internally
# consistent for every combo but one: (06, "PI") had 37 Pinellas rows
# against a single "St. Johns" row -- that lone row is itself explained by
# the same search bug, not a second meaning of the code. This table is not
# a complete map of all FL (district, code) pairs, only the ones observed;
# it is used strictly as tier 2 of ``choose_surviving_county`` below, below
# the API record's own ``county`` field (tier 1).
FL_LICENSE_DISTRICT_COUNTY_CODES = {
    ("01", "ES"): "Escambia",
    ("01", "OK"): "Okaloosa",
    ("01", "SR"): "Santa Rosa",
    ("01", "WA"): "Walton",
    ("02", "FR"): "Franklin",
    ("02", "GA"): "Gadsden",
    ("02", "JE"): "Jefferson",
    ("02", "LE"): "Leon",
    ("02", "WK"): "Wakulla",
    ("03", "CO"): "Columbia",
    ("03", "MA"): "Madison",
    ("03", "TA"): "Taylor",
    ("04", "CL"): "Clay",
    ("04", "DU"): "Duval",
    ("04", "NA"): "Nassau",
    ("05", "CI"): "Citrus",
    ("05", "HE"): "Hernando",
    ("05", "LA"): "Lake",
    ("05", "MA"): "Marion",
    ("05", "SU"): "Sumter",
    ("06", "PA"): "Pasco",
    ("06", "PI"): "Pinellas",
    ("07", "FL"): "Flagler",
    ("07", "PU"): "Putnam",
    ("07", "SJ"): "St. Johns",
    ("07", "VO"): "Volusia",
    ("08", "AL"): "Alachua",
    ("08", "BR"): "Bradford",
    ("09", "OR"): "Orange",
    ("09", "OS"): "Osceola",
    ("10", "HA"): "Hardee",
    ("10", "HI"): "Highlands",
    ("10", "PO"): "Polk",
    ("11", "MD"): "Miami-Dade",
    ("12", "DE"): "DeSoto",
    ("12", "MA"): "Manatee",
    ("12", "SA"): "Sarasota",
    ("13", "HI"): "Hillsborough",
    ("14", "BA"): "Bay",
    ("14", "CA"): "Calhoun",
    ("14", "JA"): "Jackson",
    ("14", "WS"): "Washington",
    ("15", "PB"): "Palm Beach",
    ("16", "MO"): "Monroe",
    ("17", "BO"): "Broward",
    ("18", "BR"): "Brevard",
    ("18", "SE"): "Seminole",
    ("19", "IR"): "Indian River",
    ("19", "MA"): "Martin",
    ("19", "SL"): "St. Lucie",
    ("20", "CO"): "Collier",
    ("20", "GL"): "Glades",
    ("20", "LE"): "Lee",
}

# Matches a FL license number's leading type letter, 2-digit district, and
# 2-letter county code, e.g. "C11MD0431" -> ("11", "MD").
_LICENSE_DISTRICT_COUNTY_RE = re.compile(r"^[A-Za-z](\d{2})([A-Za-z]{2})\d+$")


def _license_county_from_code(license_number):
    """Return the county implied by a FL license number's embedded
    district/county code, or ``None`` if the number doesn't match the
    pattern or the (district, code) pair isn't in
    ``FL_LICENSE_DISTRICT_COUNTY_CODES``."""
    if not license_number:
        return None
    match = _LICENSE_DISTRICT_COUNTY_RE.match(license_number)
    if not match:
        return None
    district, code = match.group(1), match.group(2).upper()
    return FL_LICENSE_DISTRICT_COUNTY_CODES.get((district, code))


def _provider_own_county(record):
    """Return the county the raw API record claims for itself (its own
    ``county`` key), independent of which per-county search found it. This
    is FL's authoritative value -- see the tier-1 note on
    ``choose_surviving_county``. ``None`` if the record has no usable
    value."""
    value = record.get("county") if isinstance(record, dict) else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _county_name_in_text(county, *text_fields):
    """True if ``county``'s name appears (case-insensitively) anywhere in
    the given text fields -- FL's free-text search false-positive
    signature."""
    blob = " ".join(str(field) for field in text_fields if field).lower()
    return county.lower() in blob


def choose_surviving_county(candidate_counties, record, license_number=None):
    """Decide which county should survive for a provider whose license was
    returned by more than one of FL's per-county searches (bug_12 cause A).

    ``candidate_counties`` are the county values the duplicate rows
    carried, in first-seen sweep order; ``record`` is the (shared) raw API
    record; ``license_number`` is its ``licenseNumber``.

    Returns ``(county, tier)``, where tier is:
      1 -- the API record carries its own ``county`` field. Authoritative:
           it bypasses the free-text search entirely, so it's used even
           when it doesn't match any search candidate.
      2 -- the license number's embedded district/county code names one of
           the candidates.
      3 -- exactly one candidate's county name is NOT a substring of the
           provider's own address/city/name/DBA (the substring match is
           the search's false-positive signature, so the non-matching one
           is the real hit).
      4 -- none of the above resolved it; keep the first candidate
           (deterministic FL_COUNTIES sweep order). Callers should log
           this at debug -- it means the true county couldn't be
           determined from any available signal.
    """
    candidates = list(dict.fromkeys(candidate_counties))  # de-dup, keep first-seen order

    own_county = _provider_own_county(record)
    if own_county:
        return own_county, 1

    code_county = _license_county_from_code(license_number)
    if code_county and code_county in candidates:
        return code_county, 2

    text_fields = (
        record.get("fullAddress"),
        record.get("city"),
        record.get("providerName"),
        record.get("dba"),
    )
    non_matches = [c for c in candidates if not _county_name_in_text(c, *text_fields)]
    if len(non_matches) == 1:
        return non_matches[0], 3

    return candidates[0], 4


def dedupe_providers(records_with_county):
    """Collapse bug_12 cause-A duplicates: the same ``licenseNumber`` found
    under more than one county search. ``records_with_county`` is an
    iterable of ``(county, raw_record)`` pairs in first-seen sweep order.

    Returns ``(resolved, tier_counts)``:
      * ``resolved`` -- a list of ``(county, raw_record)`` pairs, one per
        distinct license number, in first-seen order, with ``county``
        chosen by ``choose_surviving_county`` when more than one copy was
        found.
      * ``tier_counts`` -- a ``Counter`` of how many duplicate groups were
        resolved via each tier (1-4); only groups with more than one copy
        are counted (a license seen once needs no tie-break).
    """
    groups = {}
    order = []
    for county, record in records_with_county:
        # license_number should always be present (bug_12 evidence: every
        # provider has one); fall back to the record's identity so a
        # missing one never gets silently merged with an unrelated record.
        key = record.get("licenseNumber") or id(record)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append((county, record))

    tier_counts = Counter()
    resolved = []
    for key in order:
        entries = groups[key]
        if len(entries) == 1:
            resolved.append(entries[0])
            continue
        counties = [county for county, _ in entries]
        license_number = key if isinstance(key, str) else None
        county, tier = choose_surviving_county(counties, entries[0][1], license_number)
        tier_counts[tier] += 1
        resolved.append((county, entries[0][1]))

    return resolved, tier_counts


def dedupe_inspection_reports(reports):
    """Drop bug_12 cause-B duplicates: a report whose ``id`` repeats
    earlier in the same provider's report list (FL's backend occasionally
    emits the identical row twice, consecutively). Reports with no ``id``
    are never collapsed -- each is kept independently, since ``None`` is
    not a real identity.

    Returns ``(kept_reports, dropped_count)``.
    """
    seen_ids = set()
    kept = []
    dropped = 0
    for report in reports:
        report_id = report.get("id")
        if report_id and report_id in seen_ids:
            dropped += 1
            continue
        if report_id:
            seen_ids.add(report_id)
        kept.append(report)
    return kept, dropped


def _num(value):
    """Some numeric fields come as ``{"source": "5.0", "parsedValue": 5}``
    instead of a plain number. Unwrap when needed; pass through otherwise.
    """
    if isinstance(value, dict) and "parsedValue" in value:
        return value["parsedValue"]
    return value


def build_county_url(county):
    qs = urlencode({"searchText": county, "tag": "Counties"}, quote_via=quote)
    return f"{SEARCH_API_BASE}?{qs}"


def build_spa_search_url(county):
    return f"{SPA_SEARCH_URL}?{urlencode({'term': county}, quote_via=quote)}"


def response_matches_county(resp_url, county):
    """True iff the response URL is the public-search API call whose
    ``searchText`` query parameter decodes to the given county name."""
    if SEARCH_API_PATH not in resp_url:
        return False
    try:
        qs = parse_qs(urlparse(resp_url).query)
    except Exception:
        return False
    search_text = qs.get("searchText", [""])[0]
    return unquote(search_text).strip().lower() == county.strip().lower()


class FloridaSpider(scrapy.Spider):
    name = "florida"
    allowed_domains = [
        "caressearch.myflfamilies.com",
        "caresapi.myflfamilies.com",
    ]

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "CONCURRENT_REQUESTS": 4,
        "ROBOTSTXT_OBEY": False,
    }

    # Timeouts and pacing (tunable via -a flags).
    NAV_TIMEOUT_MS = 30_000
    RESPONSE_TIMEOUT_MS = 25_000
    BETWEEN_COUNTY_SLEEP_S = 0.4

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # bug_12 counters, reported in closed().
        self._provider_dupe_rows_dropped = 0
        self._inspection_dupes_dropped = 0
        self._county_tier_counts = Counter()

    async def start(self):
        yield scrapy.Request(
            url=LANDING_URL,
            callback=self.parse_landing,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                ],
            },
        )

    async def parse_landing(self, response):
        """For each county, drive the SPA itself: navigate to its search URL
        and capture the response body of the API call the SPA fires. This
        sidesteps the API's session-binding (Sec-Fetch-*, TLS fingerprint,
        anti-CSRF, or whatever is encoded in "Claims were not found"); we
        never reissue the call ourselves.

        bug_12: the per-county search is a loose free-text match, so the
        same provider (licenseNumber) can come back under more than one
        county. Rather than emit a ProviderItem per hit, every raw record
        is buffered (with the county search that found it) until the whole
        sweep finishes, then ``dedupe_providers`` collapses duplicates and
        picks the one true county (see its docstring and
        ``choose_surviving_county``). Buffering is a dict-grouping pass
        over ~6.7k small records -- cheap, no extra awaits -- so it can't
        introduce the kind of unbounded-await stall seen elsewhere in this
        codebase. Items are still emitted even if the sweep aborts partway
        through: the emit step runs unconditionally after the sweep's
        try/except, not nested inside it.
        """
        page = response.meta["playwright_page"]
        buffered_records = []  # (county, raw_record), first-seen sweep order

        try:
            total = len(FL_COUNTIES)
            successes = 0
            for index, county in enumerate(FL_COUNTIES, start=1):
                self.logger.info(f"[{index}/{total}] navigating SPA for {county}")

                api_resp = await self._fetch_county_via_spa(page, county)
                if api_resp is None:
                    continue
                status, body = api_resp

                if status != 200:
                    self.logger.error(f"[{index}/{total}] {county}: HTTP {status} — first 200 chars: {body[:200]}")
                    continue

                try:
                    data = json.loads(body)
                except json.JSONDecodeError as exc:
                    self.logger.error(f"[{index}/{total}] {county}: bad JSON: {exc}")
                    continue

                if not isinstance(data, list) or not data:
                    self.logger.warning(f"[{index}/{total}] {county}: empty/unexpected payload")
                    continue

                records = data[0].get("publicSearches") or []
                self.logger.info(f"[{index}/{total}] {county}: {len(records)} providers")
                successes += 1

                for record in records:
                    buffered_records.append((county, record))

                await asyncio.sleep(self.BETWEEN_COUNTY_SLEEP_S)

            self.logger.info(f"Finished: {successes}/{total} counties returned data")
        except Exception:
            self.logger.exception(
                f"parse_landing: county sweep aborted early; emitting "
                f"{len(buffered_records)} buffered provider rows collected so far"
            )

        try:
            resolved, tier_counts = dedupe_providers(buffered_records)
            self._provider_dupe_rows_dropped += len(buffered_records) - len(resolved)
            self._county_tier_counts.update(tier_counts)

            for county, record in resolved:
                yield self.parse_provider(record, county)
        finally:
            await page.close()

    async def _fetch_county_via_spa(self, page, county):
        """Navigate the SPA to its per-county search URL and return the
        ``(status, body)`` of the corresponding API call. Returns ``None``
        if the response could not be captured."""
        search_url = build_spa_search_url(county)

        def matcher(resp):
            return response_matches_county(resp.url, county)

        try:
            async with page.expect_response(matcher, timeout=self.RESPONSE_TIMEOUT_MS) as resp_info:
                await page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=self.NAV_TIMEOUT_MS,
                )
            api_resp = await resp_info.value
        except Exception as exc:
            self.logger.error(f"{county}: failed waiting for API response: {exc}")
            return None

        try:
            status = api_resp.status
            body = await api_resp.text()
        except Exception as exc:
            self.logger.error(f"{county}: failed reading API response body: {exc}")
            return None

        return status, body

    def parse_provider(self, data, county):
        item = ProviderItem()
        item["source_state"] = "Florida"
        # bug_12: the per-county search is a loose text match, so `county`
        # (the search that found this record) can be wrong even for a
        # provider found only once. Prefer the record's own county.
        item["county"] = _provider_own_county(data) or county

        item["provider_name"] = data.get("providerName")
        item["fl_dba"] = data.get("dba")
        item["provider_type"] = data.get("providerType")
        item["license_number"] = data.get("licenseNumber")
        item["fl_license_status"] = data.get("licenseStatus")
        item["status"] = data.get("providerStatus")
        item["license_expiration"] = data.get("licenseExpirationDate")
        item["license_begin_date"] = data.get("originationDate")
        item["fl_alternate_provider_number"] = data.get("alternateProviderNumber")

        phone = data.get("providerPhone")
        item["phone"] = str(phone) if phone is not None else None
        item["email"] = data.get("emailAddress")
        item["capacity"] = data.get("capacity")
        item["address"] = data.get("fullAddress")
        item["fl_city"] = data.get("city")
        item["fl_zip_code"] = data.get("zipCode")
        item["latitude"] = data.get("latitude")
        item["longitude"] = data.get("longitude")

        item["hours"] = {
            day: data.get(f"{day}Hours")
            for day in (
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            )
        }

        item["fl_display_address_on_web"] = data.get("displayAddressOnWeb")
        item["fl_display_email_on_web"] = data.get("displayEmailOnWeb")
        item["fl_display_phone_on_web"] = data.get("displayPhoneOnWeb")

        item["fl_is_religious_exempt"] = data.get("isReligiousExempt")
        item["fl_is_faith_based"] = data.get("isFaithBased")
        item["fl_is_head_start"] = data.get("isHeadStart")
        item["fl_is_offering_school_readiness"] = data.get("isOfferingSchoolReadiness")
        item["fl_is_vpk"] = data.get("isVPK")
        item["fl_is_gold_seal"] = data.get("isGoldSeal")
        item["fl_is_public_school"] = data.get("isPublicSchool")

        item["fl_vpk_composite_score"] = _num(data.get("compositesScore"))
        item["fl_wels_rating_date"] = data.get("welsRatingDate")
        item["fl_vpk_school_year_composite_score"] = _num(data.get("vpkSchoolYearCompositeScore"))
        item["fl_vpk_school_year_wels_rating_date"] = data.get("vpkSchoolYearWelsRatingDate")
        item["fl_vpk_summer_composite_score"] = _num(data.get("vpkSummerCompositeScore"))
        item["fl_vpk_summer_wels_rating_date"] = data.get("vpkSummerWelsRatingDate")

        item["fl_is_trauma_badge"] = data.get("isTraumaBadge")
        item["fl_is_inclusion_badge"] = data.get("isInclusionBadge")
        item["fl_is_dll_badge"] = data.get("isDualLanguageLearnersBadge")
        item["fl_is_infant_toddler_badge"] = data.get("isInfantToddlerBadge")
        item["fl_trauma_badge_date"] = data.get("traumaBadgeDate")
        item["fl_inclusion_badge_date"] = data.get("inclusionBadgeDate")
        item["fl_dll_badge_date"] = data.get("dualLanguageLearnersBadgeDate")
        item["fl_infant_toddler_badge_date"] = data.get("infantToddlerBadgeDate")

        services = data.get("service") or []
        item["fl_services"] = [s.get("name") for s in services]
        item["infant"] = "Infant Care" in item["fl_services"]

        programs = data.get("program") or []
        item["fl_programs"] = [p.get("name") for p in programs]

        item["fl_gold_seal"] = data.get("goldSeal")
        vpk = data.get("vpk") or {}
        item["fl_vpk_accreditation"] = vpk.get("accreditation")
        item["fl_vpk_classrooms"] = vpk.get("classRoom")
        item["fl_vpk_curriculum"] = vpk.get("curriculum")
        item["fl_vpk_instructor_credentials"] = vpk.get("instructorCredential")

        item["inspections"] = self.parse_inspections(data.get("inspection") or [])
        return item

    def parse_inspections(self, year_buckets):
        """bug_12 cause B: FL's backend occasionally repeats the identical
        inspection report twice, consecutively, within one provider's own
        report list. ``dedupe_inspection_reports`` drops exact repeats of a
        non-null ``id`` (never collapsing reports that have no ``id`` at
        all, since ``None`` isn't a real identity). De-duping is done
        across the provider's whole history (all year buckets flattened,
        in order), not reset per bucket, in case a repeat ever straddles a
        year boundary."""
        all_reports = []
        for bucket in year_buckets or []:
            all_reports.extend(bucket.get("inspectionReport") or [])

        kept_reports, dropped = dedupe_inspection_reports(all_reports)
        self._inspection_dupes_dropped += dropped

        inspections = []
        for report in kept_reports:
            ins = InspectionItem()
            ins["date"] = report.get("inspectionDate")
            ins["fl_has_violation"] = report.get("hasViolation")
            ins["fl_inspection_id"] = report.get("id")
            inspections.append(ins)
        return inspections

    def closed(self, reason):
        tier_summary = ", ".join(f"tier{tier}={count}" for tier, count in sorted(self._county_tier_counts.items()))
        self.logger.info(
            f"Florida: finished ({reason}) -- bug_12 de-dup: "
            f"{self._provider_dupe_rows_dropped} duplicate provider rows dropped "
            f"(county tie-break tiers: {tier_summary or 'none needed'}); "
            f"{self._inspection_dupes_dropped} duplicate inspection reports dropped"
        )
