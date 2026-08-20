"""Iowa child care provider spider.

Two sources, joined on one key -- neither is sufficient alone:

  * **C3** (``search.iachildcareconnect.org``) -- Iowa Child Care Connect, the
    referral/search map. A single stateless ``POST`` to ``/Map/pins`` returns
    the entire state (~3,208 records) in one response: rich contact,
    location, coordinates, hours, ages, languages, live vacancy counts, and
    the IQ4K quality level. It has no license type, no status, no capacity.
  * **Titan** (``secureapp.dhs.state.ia.us``) -- Iowa HHS's public Child Care
    Reports app. Supplies exactly what C3 lacks: license type (an 8-value
    vocabulary), status, and the inspection/complaint report lists.

The join key is C3's ``licenseId`` == Titan's ``ProviderID``, both plain
integers (never the zero-padded ``paddedLicenseId`` display form -- see
``tasks/iowa_epic/iowa_plan.md`` §5.9). The union of both sources is emitted,
not just the intersection: two whole license categories (``Exempt from
Licensing`` and ``In-Home``) are 100% Titan-only (§4.2, §11).

Three phases:

  1. **Titan paging** (``parse_titan``) -- ``GetProviderComplianceReports``,
     50 rows/page, 68 pages. Chained strictly sequentially (one page in
     flight at a time, ``PageIndex + 1`` requested from the previous page's
     callback) because concurrent paging silently drops rows (measured: a
     6-way-concurrent harvest lost 350/3,370 rows with zero duplicates and no
     error -- see plan §5.1). Once the join dict is complete, the last page's
     callback fires the C3 pins request.
  2. **C3 pins** (``parse_pins``) -- one POST, the whole state. Each pin is
     joined against the Titan dict (`int` keys both sides) and built into a
     ``ProviderItem``. Titan-only providers (never seen in the pins array)
     are unioned in afterward.
  3. **Report-list fan-out** (``parse_reports``) -- for every provider with a
     nonzero compliance/complaint/checklist count, a keyed
     ``GetProviderComplaintAndComplicanceReportList`` lookup (not a
     stateful cursor -- safe to run concurrently, measured 0 errors at 8
     workers). Providers with all three counts at zero are finalized without
     a request (§4.4).

See ``tasks/iowa_epic/iowa_plan.md`` for the full research writeup this
spider implements.
"""
import logging
import re
from datetime import datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

logger = logging.getLogger(__name__)

PINS_URL = "https://search.iachildcareconnect.org/Map/pins"
TITAN_SEARCH_URL = (
    "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/"
    "GetProviderComplianceReports"
)
TITAN_REPORTS_URL = (
    "https://secureapp.dhs.state.ia.us/dhs_titan_public/ChildCare/"
    "GetProviderComplaintAndComplicanceReportList"
)
# Mirrors DownloadG360CAPRICAReport in the site's own
# Scripts/Childcare/ComplianceReport.js (plan §6.5).
DOCUMENT_BASE_URL = (
    "https://secureapp.dhs.state.ia.us/dhs_titan_public/DocumentRepository"
)
# C3 is an SPA with a POST-only pins endpoint -- there is no per-provider
# page. Emit the map base as `provider_url` (the Indiana / Washington DC
# POST-only precedent); the real identity is `license_number`.
MAP_URL = "https://search.iachildcareconnect.org/Map"

CENTRAL = ZoneInfo("America/Chicago")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"
    ),
    "Accept": "application/json, text/plain, */*",
}

# Baseline unique count (calibrated live 2026-08-19: 3,372). Warn if a run
# falls far short -- a sign the API shape or coverage changed.
EXPECTED_MIN_PROVIDERS = 3000

# agesServed tokens -> the item's four boolean age-group fields (plan §6.2).
# Note the token arrives over the wire JSON-escaped as "Before&After
# School"; json.loads decodes it to a plain "&" before this ever sees it, so
# match on the decoded string (§5.6).
AGE_TOKEN_TO_FIELD = {
    "Infant(0-12)": "infant",
    "Infant(13-23)": "infant",
    "Toddler(2yo)": "toddler",
    "Preschool(3yo)": "preschool",
    "Preschool(4-5yo)": "preschool",
    "Before&After School": "school",
    "School Age": "school",
}

# The 7 age bands C3 reports openings for, each as a `<prefix>FtOpenings` /
# `<prefix>PtOpenings` pair (14 flat keys total) -- folded into one list per
# the nd_vacancies_by_age precedent (plan §6.3).
OPENINGS_AGE_BANDS = [
    ("infant0to12Mo", "Infant (0-12 mo.)"),
    ("infant13to23Mo", "Infant (13-23 mo.)"),
    ("toddler2Yo", "Toddler (2 yo.)"),
    ("preschool3Yo", "Preschool (3 yo.)"),
    ("preschool4to5Yo", "Preschool (4-5 yo.)"),
    ("beforeAfterSchool", "Before & After School"),
    ("schoolAgeFullTime", "School Age Full Time"),
]

DAY_LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
              "Saturday", "Sunday"]
DAY_KEYS = ["mondayHours", "tuesdayHours", "wednesdayHours", "thursdayHours",
            "fridayHours", "saturdayHours", "sundayHours"]

# Titan report list -> InspectionItem.type when FileTypeDescription is null
# (§5.13: FileTypeDescription is authoritative and usually present; this is
# only a fallback).
LIST_NAME_TO_TYPE = {
    "ComplianceReportList": "Compliance Report",
    "ComplaintReportList": "Complaint Report",
    "RegulationCheckListReportList": "Regulation CheckList",
}

_TITAN_DATE_RE = re.compile(r"^/Date\((-?\d+)\)/$")
_COUNTY_SUFFIX_RE = re.compile(r"\s+County$", re.IGNORECASE)


def titan_search_url(page):
    """Build a ``GetProviderComplianceReports`` URL for one page (0-based).

    All filters blank == every provider; ``pageSize`` is fixed server-side at
    50 regardless of what is requested (plan §2.2).
    """
    params = {"ProviderID": "", "ProviderName": "", "City": "", "County": "",
              "PageIndex": page, "TypeOfCare": ""}
    return f"{TITAN_SEARCH_URL}?{urlencode(params)}"


def reports_url(provider_id, type_of_care_id):
    """Build a ``GetProviderComplaintAndComplicanceReportList`` URL."""
    return (f"{TITAN_REPORTS_URL}?providerID={provider_id}"
            f"&TypeOfCareID={type_of_care_id}")


def ages_served_flags(ages_served):
    """Derive the infant/toddler/preschool/school booleans from ``agesServed``.

    ``ages_served`` is C3's comma-separated 7-token vocabulary string. Returns
    a dict with only the flags that should be ``True`` (unset flags are simply
    absent, matching every other spider's ``put()`` convention). An
    unrecognized token is logged and skipped rather than raising.
    """
    flags = {}
    if not ages_served:
        return flags
    for token in ages_served.split(","):
        token = token.strip()
        if not token:
            continue
        field = AGE_TOKEN_TO_FIELD.get(token)
        if field is None:
            logger.warning("Iowa: unrecognized agesServed token %r", token)
            continue
        flags[field] = True
    return flags


def openings_by_age(pin):
    """Fold C3's 14 flat per-age opening counts into one list (plan §6.3).

    Always returns all 7 age bands (even when every count is 0) so the shape
    is uniform across records.
    """
    return [
        {"ageGroup": label,
         "fullTime": pin.get(f"{prefix}FtOpenings") or 0,
         "partTime": pin.get(f"{prefix}PtOpenings") or 0}
        for prefix, label in OPENINGS_AGE_BANDS
    ]


def format_hours(formatted):
    """Render C3's ``formattedHoursOfOperation`` into a compact human string.

    Collapses to ``"Monday-Friday 6:00 AM - 6:00 PM"`` when every day that has
    hours shares the same string; otherwise lists each day individually. Days
    with a null value (closed / not listed) are omitted. Returns ``None`` when
    every day is null (plan §4.6 -- clock hours live here, not in
    ``hoursOfOperation``, which is a service-type vocabulary).
    """
    if not formatted:
        return None
    rows = [(day, formatted.get(key)) for day, key in zip(DAY_LABELS, DAY_KEYS)
            if formatted.get(key)]
    if not rows:
        return None
    times = {t for _, t in rows}
    if len(times) == 1 and len(rows) > 1:
        return f"{rows[0][0]}-{rows[-1][0]} {rows[0][1]}"
    if len(rows) == 1:
        return f"{rows[0][0]} {rows[0][1]}"
    return "; ".join(f"{day} {t}" for day, t in rows)


def parse_titan_epoch(value):
    """Parse a Titan ``/Date(<ms>)/`` wrapper into an aware Central datetime.

    The site's own ``moment()`` call renders these in the browser's local
    timezone; Iowa is Central, so epoch milliseconds are converted in
    ``America/Chicago`` rather than UTC (plan §5.12 -- using UTC would shift
    late-evening reports onto the wrong calendar day, which matters because
    the report URL embeds this date and the server picks a *different
    document* for a wrong date with no error).
    """
    if not isinstance(value, str):
        return None
    match = _TITAN_DATE_RE.match(value.strip())
    if not match:
        logger.warning("Iowa: unrecognized Titan date format %r", value)
        return None
    return datetime.fromtimestamp(int(match.group(1)) / 1000, tz=CENTRAL)


def titan_iso_date(value):
    """``/Date(ms)/`` -> ``YYYY-MM-DD`` in America/Chicago, or None."""
    dt = parse_titan_epoch(value)
    return dt.strftime("%Y-%m-%d") if dt else None


def titan_mdy_date(value):
    """``/Date(ms)/`` -> ``MM/DD/YYYY`` in America/Chicago, or None.

    This is the format ``ViewCAPRICADocument`` / the legacy report URL expect
    for their ``createdDate`` query parameter.
    """
    dt = parse_titan_epoch(value)
    return dt.strftime("%m/%d/%Y") if dt else None


def build_report_url(rec):
    """Build the document URL for one Titan report record (plan §6.5).

    Only the CAPRICA shape has ever been observed live (473/473 sampled
    records); the G360 and legacy branches are implemented from the site's
    own JS and log a warning if ever exercised, since they are otherwise
    untested against a real response.

    ``CrypticID`` / ``CrypticFormID`` arrive already URL-encoded (e.g.
    ``"jzxFNdvCTp4%3d"``) -- built by concatenation, never re-``urlencode``'d,
    or the literal ``%3d`` would become ``%253d`` and break the link (§5.14).
    """
    if rec.get("IsG360Report"):
        logger.warning(
            "Iowa: IsG360Report report encountered (undocumented live) -- "
            "FileID=%r", rec.get("FileID"),
        )
        return f"{DOCUMENT_BASE_URL}/ViewComplianceDocument/?CrypticFileID={rec.get('FileID')}"
    if rec.get("IsLegacy"):
        logger.warning(
            "Iowa: IsLegacy report encountered (undocumented live) -- "
            "CrypticID=%r", rec.get("CrypticID"),
        )
        # NB: the legacy shape keys off VersionTime, not CreatedDate -- that
        # asymmetry is in the site's own JS, not a typo (plan §6.5).
        created = titan_mdy_date(rec.get("VersionTime"))
        return (f"{DOCUMENT_BASE_URL}/ProviderLegacyDocumentReport/"
                f"?providerID={rec.get('CrypticID')}&createdDate={created}")
    created = titan_mdy_date(rec.get("CreatedDate"))
    return (f"{DOCUMENT_BASE_URL}/ViewCAPRICADocument/"
            f"?Id={rec.get('CrypticID')}&formID={rec.get('CrypticFormID')}"
            f"&createdDate={created}")


def build_inspection(rec, list_name):
    """Build one ``InspectionItem`` from a Titan report record.

    ``type`` comes from ``FileTypeDescription`` when present -- it is
    authoritative even when it disagrees with the list the record came from
    (12/473 sampled records self-describe as "Compliance Report" while
    sitting in ``RegulationCheckListReportList``, plan §5.13) -- falling back
    to the list-name mapping only when it is null.
    """
    entry = InspectionItem()
    entry["type"] = rec.get("FileTypeDescription") or LIST_NAME_TO_TYPE[list_name]
    date = titan_iso_date(rec.get("CreatedDate"))
    if date:
        entry["date"] = date
    entry["report_url"] = build_report_url(rec)
    return entry


def build_provider_item(c3, titan):
    """Build a ``ProviderItem`` from a C3 pin, a Titan search row, or both.

    ``c3`` and ``titan`` are the raw dicts from their respective sources; at
    least one is present (a provider with neither would never have been
    discovered). See plan §6.1 for the full field mapping and §4.2 for why
    the union -- not just the intersection -- of the two sources is built
    here: C3-only rows omit ``provider_type``/``facility_category``/
    ``status``; Titan-only rows omit ``address``/coordinates/``phone`` (C3 is
    the only source for all of those).
    """
    item = ProviderItem()
    item["source_state"] = "Iowa"
    item["provider_url"] = MAP_URL

    def put(key, value):
        if isinstance(value, str):
            value = value.strip() or None
        elif isinstance(value, list):
            value = value or None
        if value is not None:
            item[key] = value

    # --- identity (license_number is the shared join key, unpadded -- §4.3) ---
    if c3 is not None:
        put("provider_name", c3.get("businessName"))
        if c3.get("licenseId") is not None:
            item["license_number"] = str(c3["licenseId"])
        put("ia_padded_license_id", c3.get("paddedLicenseId"))
    elif titan is not None:
        put("provider_name", titan.get("ProviderName"))
        if titan.get("ProviderID") is not None:
            item["license_number"] = str(titan["ProviderID"])

    # --- type / status / report counts (Titan only) ---
    if titan is not None:
        put("provider_type", titan.get("TypeOfCareDesc"))
        put("status", titan.get("StatusCode"))
        put("ia_compliance_report_count", titan.get("ComplianceCount"))
        put("ia_complaint_count", titan.get("ComplaintCount"))
        put("ia_regulation_checklist_count", titan.get("RegulationCheckListCount"))

    # --- address / coordinates / contact (C3 only -- §5.10) ---
    if c3 is not None:
        put("address", c3.get("address"))
        put("city", c3.get("city"))
        item["state"] = "IA"  # every C3 fullAddress contains ", IA " (§5.10)
        if c3.get("zipCode") is not None:
            item["zip"] = str(c3["zipCode"])  # zipCode is an int (§5.4)
        if c3.get("latitude") is not None:
            item["latitude"] = str(c3["latitude"])
        if c3.get("longitude") is not None:
            item["longitude"] = str(c3["longitude"])
        put("phone", c3.get("phoneNumber"))
        put("email", c3.get("email"))
        put("provider_website", c3.get("website"))
        put("ia_business_type", c3.get("businessType"))
        put("ia_region", c3.get("region"))
        item["ia_referral_listed"] = bool(c3.get("referral"))

    # --- county: prefer C3's spelling (stripped of " County"), else Titan's
    #     bare name as-is (§5.3; Titan also drops the apostrophe in O'Brien,
    #     another reason to prefer C3 when the row is joined) ---
    if c3 is not None and c3.get("county"):
        put("county", _COUNTY_SUFFIX_RE.sub("", c3["county"].strip()))
    elif titan is not None and titan.get("County"):
        put("county", titan["County"])

    # --- ages / hours / vacancies / vocab (C3 only) ---
    if c3 is not None:
        put("ages_served", c3.get("agesServed"))
        for field, value in ages_served_flags(c3.get("agesServed")).items():
            item[field] = value
        put("hours", format_hours(c3.get("formattedHoursOfOperation")))
        put("ia_care_types", c3.get("hoursOfOperation"))
        put("languages", c3.get("languages"))
        put("transportation", c3.get("transportation"))
        put("meals", c3.get("cacfpParticipation"))
        item["scholarships_accepted"] = bool(c3.get("acceptsCCA"))
        total_openings = c3.get("totalOpenings")
        if total_openings is not None:
            item["ia_total_openings"] = total_openings
            item["accepting_new_children"] = total_openings > 0
        item["ia_openings_by_age"] = openings_by_age(c3)
        put("ia_openings_as_of", c3.get("openingsAsOfDt"))
        put("ia_days_of_operation", c3.get("daysOfOperation"))
        put("ia_serves_special_needs", c3.get("servesSpecialNeeds"))
        put("ia_iq4k_level", c3.get("iQ4KLevel"))

    return item


class IowaSpider(scrapy.Spider):
    name = "iowa"
    allowed_domains = ["search.iachildcareconnect.org",
                        "secureapp.dhs.state.ia.us"]

    custom_settings = {
        # 8 is for the Phase-3 report-list fan-out (verified safe at 8
        # workers / 0 errors, plan §2.3). The Titan page chain in
        # parse_titan self-serializes -- each page is requested from the
        # previous page's callback, so only one is ever in flight regardless
        # of this setting -- so raising it here does NOT reintroduce the
        # §5.1 pagination bug.
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 180,  # Titan pages measured up to 27s (§5.1)
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_MAXSIZE": 0,  # the 6.1 MB pins payload; 0 == no cap
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.licensing = {}  # int(ProviderID) -> Titan search row
        self.titan_pages = 0
        self.both_count = 0
        self.c3_only_count = 0
        self.titan_only_count = 0
        self.report_request_count = 0
        self.item_count = 0

    # --- Phase 2: Titan paging (sequential -- §5.1) ---------------------- #

    def start_requests(self):
        yield self._titan_request(0)

    def _titan_request(self, page):
        return scrapy.Request(
            titan_search_url(page),
            headers=HEADERS,
            callback=self.parse_titan,
            meta={"page": page},
            dont_filter=True,
        )

    def parse_titan(self, response):
        data = response.json()
        page = response.meta["page"]
        self.titan_pages += 1
        rows = data.get("ComplianceList") or []
        total_pages = data.get("totalPage") or 0
        absolute_total = data.get("absoluteTotal") or 0

        for row in rows:
            pid = row.get("ProviderID")
            if pid is None:
                continue
            self.licensing[int(pid)] = row

        self.logger.info(
            "Iowa: Titan page %d/%d -> %d rows (running total %d)",
            page + 1, total_pages, len(rows), len(self.licensing),
        )

        next_page = page + 1
        if next_page < total_pages:
            yield self._titan_request(next_page)
            return

        # Last page processed -- validate the harvest before moving on. A
        # silent short harvest would quietly blank provider_type/status on
        # hundreds of rows, exactly the failure mode measured under
        # concurrency (§5.1), so this must never pass unnoticed.
        if len(self.licensing) < absolute_total:
            self.logger.warning(
                "Iowa: Titan SHORT HARVEST -- collected %d unique providers "
                "across %d pages, but absoluteTotal is %d. provider_type / "
                "status will be missing on the difference.",
                len(self.licensing), self.titan_pages, absolute_total,
            )
        self.logger.info(
            "Iowa: Titan paging complete -- %d unique providers across %d "
            "pages; fetching the C3 pins (whole-state POST)",
            len(self.licensing), self.titan_pages,
        )
        yield scrapy.Request(
            PINS_URL, method="POST", headers=HEADERS,
            callback=self.parse_pins, dont_filter=True,
        )

    # --- Phase 1: C3 pins + join ----------------------------------------- #

    def parse_pins(self, response):
        pins = response.json()
        self.logger.info("Iowa: C3 pins received -- %d records", len(pins))

        joined_ids = set()
        for pin in pins:
            license_id = pin.get("licenseId")
            titan_row = None
            if license_id is not None:
                license_id = int(license_id)
                joined_ids.add(license_id)
                titan_row = self.licensing.get(license_id)
            if titan_row is not None:
                self.both_count += 1
            else:
                self.c3_only_count += 1
            item = build_provider_item(pin, titan_row)
            yield from self._finalize_or_fetch(item, titan_row)

        titan_only_ids = sorted(set(self.licensing) - joined_ids)
        self.titan_only_count = len(titan_only_ids)
        for pid in titan_only_ids:
            titan_row = self.licensing[pid]
            item = build_provider_item(None, titan_row)
            yield from self._finalize_or_fetch(item, titan_row)

        self.logger.info(
            "Iowa: join complete -- %d both, %d C3-only, %d Titan-only "
            "(%d union); dispatching report-list requests (Phase 3)",
            self.both_count, self.c3_only_count, self.titan_only_count,
            self.both_count + self.c3_only_count + self.titan_only_count,
        )

    def _finalize_or_fetch(self, item, titan_row):
        """Yield the finished item directly, or fetch its report list first.

        Providers with all three report counts at zero (200/3,370 measured,
        §2.3/§4.4) get no request -- there is nothing to fetch.
        """
        if titan_row is None:
            self.item_count += 1
            yield item
            return
        counts = (titan_row.get("ComplianceCount") or 0,
                  titan_row.get("ComplaintCount") or 0,
                  titan_row.get("RegulationCheckListCount") or 0)
        if not any(counts):
            self.item_count += 1
            yield item
            return
        self.report_request_count += 1
        yield scrapy.Request(
            reports_url(titan_row["ProviderID"], titan_row.get("TypeOfCareID")),
            headers=HEADERS,
            callback=self.parse_reports,
            meta={"item": item, "provider_id": titan_row["ProviderID"],
                  "expected_counts": counts},
            dont_filter=True,
        )

    # --- Phase 3: report-list fan-out (concurrent -- §2.3) --------------- #

    def parse_reports(self, response):
        data = response.json()
        item = response.meta["item"]
        pid = response.meta["provider_id"]
        expected_counts = response.meta["expected_counts"]

        lists = {
            name: data.get(name) or [] for name in (
                "ComplianceReportList", "ComplaintReportList",
                "RegulationCheckListReportList",
            )
        }
        actual_counts = tuple(len(lists[name]) for name in (
            "ComplianceReportList", "ComplaintReportList",
            "RegulationCheckListReportList",
        ))
        if actual_counts != expected_counts:
            self.logger.warning(
                "Iowa: report count mismatch for provider %s -- search row "
                "said (compliance=%d, complaint=%d, checklist=%d), fetched "
                "(%d, %d, %d)",
                pid, *expected_counts, *actual_counts,
            )

        inspections = [
            build_inspection(rec, name)
            for name, records in lists.items()
            for rec in records
        ]
        if inspections:
            item["inspections"] = inspections

        self.item_count += 1
        yield item

    def closed(self, reason):
        expected = self.both_count + self.c3_only_count + self.titan_only_count
        self.logger.info(
            "Iowa: finished (%s) -- %d items emitted (%d both, %d C3-only, "
            "%d Titan-only), %d report-list requests dispatched",
            reason, self.item_count, self.both_count, self.c3_only_count,
            self.titan_only_count, self.report_request_count,
        )
        if self.item_count < expected:
            self.logger.warning(
                "Iowa: only %d items emitted but the join produced %d -- "
                "possible dropped items", self.item_count, expected,
            )
        if self.item_count < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Iowa: only %d items emitted (< %d baseline) -- possible "
                "incomplete crawl", self.item_count, EXPECTED_MIN_PROVIDERS,
            )
