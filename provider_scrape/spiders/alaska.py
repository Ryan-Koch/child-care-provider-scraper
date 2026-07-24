"""Alaska child care providers, read directly from the AKCCIS JSON API.

Alaska decommissioned findccprovider.health.alaska.gov (now 502ing) and
migrated to AKCCIS (akccis.com), an Angular SPA whose map view is backed by
three public JSON endpoints:

  * ``POST /server/api/Facility/Search``               -> full state roster
  * ``GET  /server/api/Facility/GetSearchFacilityById?facilityGenId={id}``
                                                       -> same-shape single record
  * ``GET  /server/api/Inspection/GetFacilityInspectionTasksPublicView``
         ``?facilityGenId={id}``                       -> visit-level inspections

The Search endpoint returns every provider (~700) in ~1.4MB in one call, so
no pagination or geographic enumeration is needed (contrast ``north_dakota``).
We fan out one inspection GET per provider. Deeper deficiency detail is not
publicly exposed by AKCCIS -- the visit-level ``compliance: "C"/"NC"`` is the
finding-level signal we ship. See ``docs/alaska_field_mapping.md`` for the
full field-mapping decision log.
"""
import json
from datetime import datetime

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

# The .NET default DateTime -- AKCCIS emits this as a "no date" sentinel on
# nested objects that have no real value (agesServed.effectiveDate is the
# common case). Recognized and returned as None.
_NET_DEFAULT_DATE = "0001-01-01"

# Visit-level compliance codes AKCCIS ships on GetFacilityInspectionTasksPublicView.
_COMPLIANCE = {"C": "In Compliance", "NC": "Non-Compliance"}


def _clean(value):
    """Trim to a non-empty string, or ``None``. Numeric values are stringified."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _yesno(flag):
    """Convert an AKCCIS ``true`` / ``false`` -> ``"Yes"`` / ``"No"``.

    Matches the shape the previous spider emitted for ``acceptsCCAP``.
    ``None`` in -> ``None`` out; strings pass through cleaned.
    """
    if flag is True:
        return "Yes"
    if flag is False:
        return "No"
    return _clean(flag)


def _stringify_coordinate(value):
    """Coordinates are strings on ``ProviderItem`` (``normalization`` requires
    it). Preserve full precision -- never cast to ``float``.
    """
    if value is None or value == "":
        return None
    return str(value)


def _iso_date(value):
    """Accept AKCCIS's two date shapes and return ``YYYY-MM-DD`` (or ``None``).

    Shapes observed:
      * .NET-encoded ISO ``2025-06-01T08:00:00Z`` (roster + license objects).
      * US ``6/23/2026 1:00 PM`` (inspection endpoint's ``visitDate``).

    ``0001-01-01T00:00:00Z`` (the .NET default) becomes ``None``.
    Unparseable / empty values become ``None``.
    """
    text = _clean(value)
    if not text:
        return None
    if "T" in text:
        date_part = text.split("T", 1)[0]
        if date_part.startswith(_NET_DEFAULT_DATE):
            return None
        try:
            return datetime.strptime(date_part, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    # US shape from the inspection endpoint -- strip the time portion first.
    date_part = text.split(" ", 1)[0]
    try:
        return datetime.strptime(date_part, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _format_months(months):
    """Format an integer month count as ``"12 Years, 11 Months"`` etc.

    Drops the months clause when zero (36 -> ``"3 Years"``); uses singular
    for a value of exactly one (1 -> ``"1 Year"``).
    """
    total = int(round(months))
    if total < 12:
        return f"{total} {'Month' if total == 1 else 'Months'}"
    years, remainder = divmod(total, 12)
    year_word = "Year" if years == 1 else "Years"
    if remainder == 0:
        return f"{years} {year_word}"
    month_word = "Month" if remainder == 1 else "Months"
    return f"{years} {year_word}, {remainder} {month_word}"


def _months_to_age(start, end):
    """Format ``(0.0, 155.0)`` -> ``'0 Months - 12 Years, 11 Months'``.

    Missing start or end -> ``None``. Inverted ranges (``start > end``)
    also -> ``None`` (defensive; not observed in practice).
    """
    if start is None or end is None:
        return None
    if start > end:
        return None
    return f"{_format_months(start)} - {_format_months(end)}"


def _overlaps(range_start, range_end, other_start, other_end):
    """True if inclusive ranges ``[range_start, range_end]`` and
    ``[other_start, other_end]`` overlap.
    """
    return range_start <= other_end and other_start <= range_end


def _age_flags(start, end):
    """Return ``{"infant", "toddler", "preschool", "school"}`` booleans for
    the given month range.

    Thresholds (inclusive months):
      * infant     0-11
      * toddler   12-35
      * preschool 36-59
      * school    60+  (no upper bound)

    Missing values or inverted ranges -> ``{}`` (caller sets nothing).
    """
    if start is None or end is None or start > end:
        return {}
    return {
        "infant":    _overlaps(start, end, 0, 11),
        "toddler":   _overlaps(start, end, 12, 35),
        "preschool": _overlaps(start, end, 36, 59),
        "school":    end >= 60,
    }


def _expand_compliance(code):
    """AKCCIS visit ``compliance`` code -> human label.

    ``"C"`` -> ``"In Compliance"``, ``"NC"`` -> ``"Non-Compliance"``; any
    other non-empty value passes through unchanged.
    """
    text = _clean(code)
    if not text:
        return None
    return _COMPLIANCE.get(text, text)


def _build_address(street, street2, city, state, zip_code):
    """Assemble ``"STREET STREET2 CITY, ST ZIP"`` tolerating empty pieces.

    Empty-string ``address2`` is common in AKCCIS -- concatenation must skip
    it or the result gets a double space (``"35095 Huntington Drive  Soldotna,
    AK 99669"``).
    """
    street = _clean(street) or ""
    street2 = _clean(street2) or ""
    left = " ".join(part for part in (street, street2) if part).strip()
    city = _clean(city) or ""
    state = _clean(state) or ""
    if city and state:
        mid = f"{city}, {state}"
    else:
        mid = city or state
    right = _clean(zip_code) or ""
    combined = " ".join(part for part in (left, mid, right) if part).strip()
    return combined or None


# --------------------------------------------------------------------------- #
# Spider
# --------------------------------------------------------------------------- #

class AlaskaSpider(scrapy.Spider):
    """Alaska child care providers via the AKCCIS public API."""

    name = "alaska"
    allowed_domains = ["akccis.com"]

    SITE = "https://akccis.com"
    SEARCH_URL = f"{SITE}/server/api/Facility/Search"
    INSPECTION_URL = (
        f"{SITE}/server/api/Inspection/GetFacilityInspectionTasksPublicView"
    )
    # Deep link into the AKCCIS map view for a single facility.
    PROFILE_URL = f"{SITE}/client/map?facilityGenId={{}}"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64; rv:152.0) "
            "Gecko/20100101 Firefox/152.0"
        ),
        "Accept": "application/json, text/plain, */*",
        # The AKCCIS API server has returned 403 without a Referer in some
        # environments during testing -- always include it.
        "Referer": f"{SITE}/client/map",
    }

    custom_settings = {
        # Plain JSON over HTTP -- override the project-wide scrapy-playwright
        # download handler so no chromium is launched.
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
        # Polite across the ~700 per-provider inspection calls.
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.25,
        "RETRY_TIMES": 3,
    }

    # Sanity floor: warn loudly at close if the emit count drops far below
    # what the API has been returning (700 today). Not a hard failure -- the
    # WARNING is easier to spot in the log than a small item count.
    _MIN_EXPECTED_ITEMS = 500

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._items_emitted = 0
        self._with_inspections = 0
        self._processed_inspections = 0

    # --- request flow --------------------------------------------------- #

    def start_requests(self):
        # Empty JSON array = no filters = full state roster in one response.
        # The POST body must be a JSON *array*; the API rejects an object.
        yield scrapy.Request(
            self.SEARCH_URL,
            method="POST",
            body=b"[]",
            headers={
                **self.HEADERS,
                "Content-Type": "application/json",
                "Origin": self.SITE,
            },
            callback=self.parse_search,
            dont_filter=True,
        )

    def parse_search(self, response):
        roster = json.loads(response.text)
        self.logger.info("Alaska: roster returned %d records", len(roster))
        for record in roster:
            facility_id = record.get("facilityGenId")
            if not facility_id:
                self.logger.warning(
                    "Alaska: roster row missing facilityGenId, skipping "
                    "(facilityName=%r)", record.get("facilityName"))
                continue
            yield scrapy.Request(
                f"{self.INSPECTION_URL}?facilityGenId={facility_id}",
                callback=self.parse_inspection,
                errback=self.errback_inspection,
                headers=self.HEADERS,
                cb_kwargs={"roster": record},
            )

    def parse_inspection(self, response, roster):
        events = json.loads(response.text) or []
        self._processed_inspections += 1
        if events:
            self._with_inspections += 1
        if self._processed_inspections % 100 == 0:
            self.logger.info(
                "Alaska: processed %d inspection responses (%d had visits)",
                self._processed_inspections, self._with_inspections)
        item = self.build_item(roster, events)
        self._items_emitted += 1
        yield item

    def errback_inspection(self, failure):
        """Inspection fetch failed after retries -- emit the provider with
        an empty inspection list rather than dropping it entirely."""
        roster = failure.request.cb_kwargs.get("roster", {})
        self.logger.warning(
            "Alaska: inspection fetch failed for facilityGenId=%s: %r. "
            "Emitting provider without inspections.",
            roster.get("facilityGenId"), failure.value)
        self._processed_inspections += 1
        item = self.build_item(roster, [])
        self._items_emitted += 1
        yield item

    def closed(self, reason):
        if self._items_emitted < self._MIN_EXPECTED_ITEMS:
            self.logger.warning(
                "Alaska: only %d items emitted (expected >= %d) -- API may "
                "be degraded or the search response was truncated",
                self._items_emitted, self._MIN_EXPECTED_ITEMS)
        else:
            self.logger.info(
                "Alaska: emitted %d items (%d with inspections)",
                self._items_emitted, self._with_inspections)

    # --- item assembly -------------------------------------------------- #

    def build_item(self, roster, inspection_events):
        item = ProviderItem()
        item["source_state"] = "Alaska"

        facility_id = _clean(roster.get("facilityGenId"))
        if facility_id:
            item["ak_facility_gen_id"] = facility_id
            item["provider_url"] = self.PROFILE_URL.format(facility_id)

        item["provider_name"] = _clean(roster.get("facilityName"))
        item["license_number"] = _clean(roster.get("licenseNumber"))
        item["phone"] = _clean(roster.get("phoneNumber"))

        # licensedCapacity is already an int on the wire -- no parsing needed
        # (contrast the old spider, which stripped digits from "60 Children").
        item["capacity"] = roster.get("licensedCapacity")

        item["administrator"] = _clean(roster.get("facilityAdmin"))

        # doingBusinessAs is used inconsistently by AKCCIS: on ~510/572
        # populated records it holds the licensee / owner name (not a true
        # DBA). Route it to license_holder rather than inventing a `dba`
        # common field (see docs/alaska_field_mapping.md). Skip the ~62
        # identity cases where it just duplicates facilityName.
        dba = _clean(roster.get("doingBusinessAs"))
        name = _clean(roster.get("facilityName"))
        if dba and dba != name:
            item["license_holder"] = dba

        item["provider_type"] = _clean(roster.get("facilityType"))
        item["status"] = _clean(roster.get("providerStatus"))
        item["status_date"] = _iso_date(
            roster.get("providerStatusEffectiveDate"))

        # Current license period. futureLicense / expiredLicense are ignored
        # (no ProviderItem field exposes license history -- a project-wide
        # decision, not AK-specific).
        license_obj = roster.get("license") or {}
        item["license_begin_date"] = _iso_date(license_obj.get("effectiveDate"))
        item["license_expiration"] = _iso_date(license_obj.get("endDate"))

        item["scholarships_accepted"] = _yesno(roster.get("isCCAP"))

        # Address components. Setting all three of city/state/zip explicitly
        # makes the normalization pipeline skip its best-effort address parse.
        city = _clean(roster.get("city"))
        # stateDescAbbr is always "AK" for this spider but read it from the
        # source rather than hard-coding.
        state = _clean(roster.get("stateDescAbbr"))
        zip_code = _clean(roster.get("zipCode"))
        item["address"] = _build_address(
            roster.get("address"), roster.get("address2"),
            city, state, zip_code)
        if city:
            item["city"] = city
        if state:
            item["state"] = state
        if zip_code:
            item["zip"] = zip_code

        item["county"] = _clean(roster.get("county"))
        item["latitude"] = _stringify_coordinate(roster.get("latitude"))
        item["longitude"] = _stringify_coordinate(roster.get("longitude"))

        # Ages: use agesAcceptedMonths* (the state-authorized range) rather
        # than license.startAge/endAge (occasionally diverges on ~5 records).
        ages_start = roster.get("agesAcceptedMonthsStart")
        ages_end = roster.get("agesAcceptedMonthsEnd")
        item["ages_served"] = _months_to_age(ages_start, ages_end)
        for group, flag in _age_flags(ages_start, ages_end).items():
            item[group] = flag

        # AK-specific enrichment (see docs/alaska_field_mapping.md).
        facility_number = roster.get("facilityNumber")
        if facility_number is not None:
            item["ak_facility_number"] = str(facility_number)
        item["ak_legacy_license_number"] = _clean(
            roster.get("legacyLicenseNumber"))
        item["ak_vendor_id"] = _clean(roster.get("vendorId"))
        item["ak_facility_subtype"] = _clean(
            roster.get("facilityTypeSubTypeDescription"))
        item["ak_license_type"] = _clean(roster.get("licenseType"))
        item["ak_licensing_specialist"] = _clean(
            roster.get("facilityLicSpecialist"))

        item["inspections"] = self.build_inspections(inspection_events)
        return item

    @staticmethod
    def build_inspections(events):
        """Map AKCCIS visit rows -> ``InspectionItem`` list, deduped.

        AKCCIS does not currently emit duplicate visit rows in the observed
        data, but the fingerprint (date/purpose/compliance/visit-type/
        specialist) is cheap insurance and matches the old spider's shape.
        Future-dated visits (scheduled inspections) are emitted as-is;
        filtering to completed-only is a downstream decision.
        """
        inspections = []
        seen = set()
        for event in events or []:
            date = _iso_date(event.get("visitDate"))
            purpose = _clean(event.get("purposeOfVisit"))
            original_status = _expand_compliance(event.get("compliance"))
            visit_type = _clean(event.get("visitType"))
            specialist = _clean(event.get("licensingSpecialist"))

            fingerprint = (date, purpose, original_status, visit_type,
                           specialist)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)

            insp = InspectionItem()
            insp["date"] = date
            insp["type"] = purpose
            insp["original_status"] = original_status
            if visit_type:
                insp["ak_visit_type"] = visit_type
            if specialist:
                insp["ak_licensing_specialist"] = specialist
            inspections.append(insp)
        return inspections
