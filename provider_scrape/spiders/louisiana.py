"""Louisiana child care provider spider.

Source: https://louisianaschools.com -- the Louisiana Department of
Education's public "Louisiana School & Center Finder". A JavaScript SPA backed
by a clean, cookieless JSON API (no cookie/referer/token/CAPTCHA/rate limit
observed) -- the same shape as ``indiana.py``, our closest structural template.

Two plain HTTP GETs, no Playwright, no pagination:

  * ``schoolbysearchparams`` returns the ENTIRE statewide list in one response
    (~3,327 records, no query params/cookies required). Each record carries a
    ``profileType`` of ``EE`` (early-ed / child care), ``BOTH`` (a school that
    also runs an early-ed program), or ``K12`` (pure school -- skipped).
  * ``GetSchoolDetailByUniqueId/{uniqueId}`` returns the full record for one
    provider. ``entityId == uniqueId`` on every record and the detail endpoint
    is keyed by exactly that ``uniqueId`` -- no id translation needed.

The "catch" (Ryan, 2026-09-08): LA mixes public schools into this dataset. We
keep ``EE`` + ``BOTH`` and skip ``K12``, but we do NOT drop the unlicensed
public-school pre-K rows found within ``BOTH`` (~85% of it) -- the search
summary carries no license info at all, so there is no way to pre-filter, and
Ryan wants the full EE+BOTH population (~2,534) emitted with the licensed/
unlicensed distinction captured in the fields (``license_number``/
``la_licensed``/``la_public_school_status``) rather than by dropping rows. See
tasks/louisiana_story/louisiana_plan.md Sec 4.1 for the full decision log.

Louisiana publishes no license status, no capacity, no license dates, and no
structured inspection records (only external report URLs) -- those common
fields are simply left unset (plan Sec 4.3/4.6).
"""

import re

import scrapy

from provider_scrape.items import ProviderItem

SEARCH_URL = "https://louisianaschools.com/api/School/schoolbysearchparams"
DETAIL_URL = "https://louisianaschools.com/api/SchoolDetail/GetSchoolDetailByUniqueId/{unique_id}"
PROVIDER_URL_TEMPLATE = "https://louisianaschools.com/{unique_id}/ec-school-about"

# profileType values we keep. K12 (pure schools, incl. two "Nation"/"LA"
# garbage aggregate rows) is skipped entirely (plan Sec 2.2/4.1).
KEEP_PROFILE_TYPES = ("EE", "BOTH")

# Baseline unique count (calibrated live 2026-09-08: 2,534 EE+BOTH out of 3,327
# total). Warn if a run falls far short -- a sign the API shape/coverage
# changed.
EXPECTED_MIN_PROVIDERS = 2300

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"),
    "Accept": "application/json, text/plain, */*",
}

# earlyEdLicenseType -> provider_type (plan Sec 4.2). "R (Family Home)" is
# LA's registered family child care home vocab entry; Roman numerals I/II/III
# are church/nonprofit, for-profit, and publicly-funded centers respectively
# (all facility-based -> facility_category "center").
LICENSE_TYPE_TO_PROVIDER_TYPE = {
    "i": "Type I",
    "ii": "Type II",
    "iii": "Type III",
    "r (family home)": "Family Child Care Home",
}

# earlyEdGrades tokens (split on "/") -> the common age-group flag they set.
# NOTE: use earlyEdGrades, NOT gradeservedcurrentsy -- the latter is the
# whole-school grade span on a BOTH record and would wrongly set `school` for
# a public-school pre-K program that has no early-ed school-age care at all
# (plan Sec 5.6).
GRADE_TOKEN_TO_AGE_FLAG = {
    "infant": "infant",
    "toddler": "toddler",
    "prek": "preschool",
    "pk": "preschool",
    "school age": "school",
}

# Case-insensitive affirmative tokens for LA's inconsistently-cased binary
# flags ("YES"/"NO" on earlyEdCcapAvailableBinary vs "Yes"/"No" elsewhere --
# plan Sec 5.3).
_AFFIRMATIVE = {"yes", "y"}

_WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def compose_provider_type(sa):
    """Compose ``provider_type`` from the early-ed license/public-school fields.

    Preference order (plan Sec 4.2): a mapped ``earlyEdLicenseType`` first (the
    licensed/registered child care vocab); else a set
    ``earlyEdPublicSchoolStatus`` (e.g. "Public School" -- the unlicensed
    public-school pre-K case); else, for a record the source tags
    ``entityType == "school"`` with neither, a private/parochial/special school
    running a PreK program -> "Private School"; else ``None`` (leave
    provider_type unset). An unmapped, non-empty license type is
    passed through as ``f"Type {raw}"`` so the normalization "other" fallback +
    warning surfaces it for a future vocab extension rather than silently
    dropping it.
    """
    license_type = (sa.get("earlyEdLicenseType") or "").strip()
    if license_type:
        mapped = LICENSE_TYPE_TO_PROVIDER_TYPE.get(license_type.lower())
        if mapped:
            return mapped
        return f"Type {license_type}"
    public_school_status = (sa.get("earlyEdPublicSchoolStatus") or "").strip()
    if public_school_status:
        return public_school_status
    # Anything still here is a school (the source tags every record
    # entityType="school") with no early-ed license and no public-school
    # status: empirically a private/parochial/special school running a PreK/
    # early-ed program -- both the ~210 BOTH private schools and the lone EE
    # "special school" (506099). Map it to "Private School" (already ->
    # facility_category "center") for parity with how public-school pre-K
    # classifies. Gate on the source's own entityType so a malformed/empty
    # record (no entityType) still falls through to unset. Ryan, 2026-09-08.
    if (sa.get("entityType") or "").strip().lower() == "school":
        return "Private School"
    return None


def age_flags_from_grades(early_ed_grades):
    """Derive the infant/toddler/preschool/school booleans from earlyEdGrades.

    ``early_ed_grades`` is a "/"-joined string like ``"Infant/Toddler/PreK"``.
    Returns a dict of only the flags that were actually set (True) so callers
    can assign them directly onto the item without clobbering anything.
    """
    if not early_ed_grades:
        return {}
    flags = {}
    for token in early_ed_grades.split("/"):
        field = GRADE_TOKEN_TO_AGE_FLAG.get(token.strip().lower())
        if field:
            flags[field] = True
    return flags


def format_hours(sa):
    """Join the non-empty earlyEdMonday..earlyEdSunday day strings.

    Each populated day field already includes its own day label (e.g.
    ``"Mon: 07:30am to 04:00pm"``), so this just filters and joins with "; ".
    """
    days = [sa.get(f"earlyEd{day}") for day in _WEEKDAYS]
    days = [d.strip() for d in days if d and d.strip()]
    return "; ".join(days) or None


def is_affirmative(value):
    """Case-insensitive "Yes"/"YES" check for LA's inconsistently-cased binary
    flags (plan Sec 5.3). Empty/None/"No"/"NO" -> False."""
    return isinstance(value, str) and value.strip().lower() in _AFFIRMATIVE


def is_real_coordinate(latitude, longitude):
    """True unless both values are missing or the ``(0, 0)`` null-island
    sentinel (observed live on one record, e.g. VIR001 -- a non-empty but
    obviously-wrong placeholder, distinct from the empty-string "no data"
    case already handled by plan Sec 2.4). Louisiana is nowhere near (0, 0),
    so treating it as "no coordinate" rather than emitting a bogus point is a
    safe, conservative guard.
    """
    if not latitude or not longitude:
        return False
    try:
        return (float(latitude), float(longitude)) != (0.0, 0.0)
    except (TypeError, ValueError):
        return True  # not a plain float -- let it through unchanged


def normalize_parish(value):
    """Normalize the LA school-district name to a bare, consistent `county`.

    The source field is the (parish-aligned) school-district name and is
    inconsistently suffixed -- the same district appears as both "Acadia" and
    "Acadia Parish", and the set includes non-parish municipal districts (e.g.
    "Zachary", plus a bogus "Zachary Parish"). We strip a trailing " Parish"
    so every value is a bare name ("Acadia", "Caddo", "West Baton Rouge"),
    matching the codebase's bare-county norm (cf. Indiana "Marion") and never
    inventing a fake parish. Returns None for empty input.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    return re.sub(r"\s+Parish$", "", value.strip(), flags=re.IGNORECASE) or None


class LouisianaSpider(scrapy.Spider):
    name = "louisiana"
    allowed_domains = ["louisianaschools.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 90,  # detail payloads reach ~245 KB
        "ROBOTSTXT_OBEY": False,  # robots.txt is "Disallow:" (allow-all) anyway
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen = set()  # uniqueIds already scheduled for detail
        self.kept = 0
        self.skipped = 0
        self.licensed_count = 0
        self.unlicensed_count = 0

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, headers=HEADERS, callback=self.parse_search, dont_filter=True)

    def parse_search(self, response):
        data = response.json()
        records = data.get("result") or []

        profile_counts = {}
        for record in records:
            profile_type = record.get("profileType")
            profile_counts[profile_type] = profile_counts.get(profile_type, 0) + 1
        self.logger.info("Louisiana: profileType histogram over %d records: %s", len(records), profile_counts)

        for record in records:
            if record.get("profileType") not in KEEP_PROFILE_TYPES:
                self.skipped += 1
                continue
            unique_id = record.get("uniqueId")
            if not unique_id or unique_id in self.seen:
                continue
            self.seen.add(unique_id)
            self.kept += 1
            yield scrapy.Request(
                DETAIL_URL.format(unique_id=unique_id),
                headers=HEADERS,
                callback=self.parse_detail,
                meta={"summary": record},
                dont_filter=True,
            )

        self.logger.info(
            "Louisiana: search parsed -- %d EE+BOTH kept, %d K12 skipped",
            self.kept,
            self.skipped,
        )

    def parse_detail(self, response):
        result = response.json().get("result") or {}
        sa = result.get("schoolAbout") or {}
        lp = result.get("leftPanel") or {}
        summary = response.meta["summary"]
        unique_id = summary.get("uniqueId")

        item = ProviderItem()
        item["source_state"] = "Louisiana"
        item["provider_url"] = PROVIDER_URL_TEMPLATE.format(unique_id=unique_id)

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            if value is not None:
                item[key] = value

        # --- identity ---
        put("provider_name", sa.get("earlyEdName") or summary.get("name"))
        # License info is detail-only (plan Sec 5.1); unset when the record has
        # no early-ed license number (unlicensed public-school pre-K, Sec 4.7).
        put("license_number", sa.get("earlyEdLicenseNumber"))
        put("la_unique_id", unique_id)
        tips_number = (sa.get("earlyEdTipsNumber") or "").strip()
        if tips_number and tips_number != "0":  # "0"/"" means absent (Sec 6.3)
            put("la_tips_number", tips_number)
        put("la_licensed", sa.get("earlyEdLicensed"))
        put("la_profile_type", summary.get("profileType"))
        put("la_public_school_status", sa.get("earlyEdPublicSchoolStatus"))

        # --- type / category (facility_category is derived by the pipeline) ---
        put("provider_type", compose_provider_type(sa))
        put("administrator", sa.get("earlyEdDirectorName") or sa.get("principalName"))

        # --- address ---
        street = (sa.get("addressStreet") or "").strip()
        city = (sa.get("addressCity") or "").strip()
        state = (sa.get("addressState") or "").strip()
        zip_code = (sa.get("addressPostalCode") or "").strip()
        address_parts = [p for p in (street, city) if p]
        state_zip = " ".join(p for p in (state, zip_code) if p)
        if state_zip:
            address_parts.append(state_zip)
        put("address", ", ".join(address_parts) if address_parts else None)
        put("city", city)
        put("state", state)
        put("zip", zip_code)
        put("county", normalize_parish(sa.get("districtName") or summary.get("districtParishName")))

        # --- coordinates (state-published, Sec 4.5) ---
        latitude = sa.get("addressLatitude") or summary.get("latitude")
        longitude = sa.get("addressLongitude") or summary.get("longitude")
        if is_real_coordinate(latitude, longitude):
            item["latitude"] = str(latitude).strip()
            item["longitude"] = str(longitude).strip()
            item["geocode_source"] = "state"

        # --- contact ---
        put("phone", sa.get("earlyEdPhoneNumber") or sa.get("phoneNumber"))
        put("email", sa.get("earlyEdEmail"))
        put("provider_website", sa.get("website"))

        # --- hours / ages ---
        put("hours", format_hours(sa))
        put("ages_served", sa.get("earlyEdGrades"))
        for field, value in age_flags_from_grades(sa.get("earlyEdGrades")).items():
            item[field] = value

        # --- programs / subsidy ---
        if sa.get("earlyHeadStart"):
            item["head_start"] = True
        put("transportation", sa.get("earlyEdTransportationBinaryFormatted"))
        item["scholarships_accepted"] = is_affirmative(sa.get("earlyEdCcapAvailableBinary"))
        item["la_before_care"] = is_affirmative(sa.get("beforeCareBinary"))
        item["la_after_care"] = is_affirmative(sa.get("afterCareBinary"))
        item["la_night_care"] = is_affirmative(sa.get("earlyEdNightCareBinary"))

        # --- quality (state-specific per the field-mapping playbook) ---
        put("la_star_rating", summary.get("starRating"))
        put("la_performance_rating", summary.get("siteRating") or lp.get("siteRating"))
        put("la_performance_score", summary.get("siteScore") or lp.get("siteScore"))

        # --- external report URLs (no structured inspections, Sec 4.6) ---
        # NOTE: `inspectionList` is deliberately NOT emitted -- it is the same
        # static "license-exempt-providers monitoring checklist" PDF on every
        # record, not a per-provider link (plan Sec 5.4).
        put("la_inspection_url", sa.get("inspectionUrl"))
        put("la_serious_injuries_url", sa.get("seriousInjuriesUrl"))
        put("la_performance_report_url", sa.get("earlyEdSchoolReport"))

        if item.get("license_number") or item.get("la_licensed"):
            self.licensed_count += 1
        else:
            self.unlicensed_count += 1

        yield item

    def closed(self, reason):
        self.logger.info(
            "Louisiana: finished (%s) -- %d EE+BOTH kept, %d K12 skipped, "
            "%d licensed, %d unlicensed public-school pre-K",
            reason,
            self.kept,
            self.skipped,
            self.licensed_count,
            self.unlicensed_count,
        )
        if self.kept < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Louisiana: only %d providers kept (< %d baseline) -- possible incomplete crawl",
                self.kept,
                EXPECTED_MIN_PROVIDERS,
            )
