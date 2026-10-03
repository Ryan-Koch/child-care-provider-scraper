"""Wyoming child care provider spider.

Source: Wyoming DFS Child Care Shopping Portal, https://childcare.dfs.wyo.gov/
(a React SPA over a JSON API at /shoppingportal/shopping/*). See
tasks/wyoming/wyoming_plan.md for the recon writeup this spider implements.
No cookies or Playwright are needed -- only `content-type: application/json`.

Three phases:

  Phase 1  POST providerSelection   one per ZIP, sweeping 82001..83199. It is
                                     a ~50 mile *radius* search, so results
                                     overlap heavily; dedupe on facilityId.
                                     ZIPs missing from the server's table
                                     answer HTTP 400 ZIP_INVALID and are
                                     skipped.
  Phase 2  POST facilityDetails     one per unique facility -> ProviderItem.
                                     Only starts once the whole sweep is done
                                     (the ZIP-centroid rule needs the full
                                     search union, plan Sec 3.1).
  Phase 3  GET  reports/visits|inspections|violations/{facilityId}
                                     chained per facility (default ON, `-a
                                     reports=0` to skip) -> InspectionItems.

Caveats:

  * `externalId` is an "eligibility lead" id minted by the portal's /home/
    pre-screener, not a session. The default works with no cookie; override
    with `-a lead_id=<id>`. If it expires every request answers
    `eliglead.not.found` and the spider closes itself (plan Risk #1) --
    mint a new one by running the pre-screener at /home/ and copying the id
    from the /shopping/lead/<id> URL.
  * License-exempt providers (`POST listLicenseExemptProviders`, ~115 named
    individuals at home addresses, no license id, capacity 0) are a known
    source that is deliberately NOT scraped (decision 2026-10-02).
  * This lists the providers the portal publishes (~495 licensed). Whether
    that is every licensed Wyoming provider is unverified (plan Risk #2).
  * About a third of the facilities carry a ZIP centroid instead of a
    geocode; those coordinates are dropped (plan Sec 3.1).
"""

import json
import re

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.http import JsonRequest

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://childcare.dfs.wyo.gov"
API_URL = BASE_URL + "/shoppingportal/shopping"
SEARCH_URL = API_URL + "/providerSelection"
DETAIL_URL = API_URL + "/facilityDetails"
REPORT_URL = API_URL + "/reports/{kind}/{facility_id}"
PROVIDER_URL = BASE_URL + "/shopping/"

DEFAULT_LEAD_ID = "ty2vm5ezyb60fs5"
ZIP_RANGE_START = 82001
ZIP_RANGE_END = 83199  # inclusive

# Live baseline (2026-10-02): 495 licensed facilities. Logged as a caveat, not
# a hard gate (plan Risk #2).
EXPECTED_PROVIDER_COUNT = 495

TEST_FACILITY_RE = re.compile(r"^ECARES Test", re.IGNORECASE)

LEAD_NOT_FOUND = "eliglead.not.found"
ZIP_INVALID = "ZIP_INVALID"

MEAL_SERVICES = {"meals and snacks provided", "child and adult food program"}
TRANSPORT_SERVICE = "provides transportation"

INSPECTION_TYPE_MAP = {
    "FIRE_INSPECTION": "Fire Inspection",
    "FOOD_SAFETY_AND_OR_SANITATION_INSPECTION": "Food Safety and/or Sanitation Inspection",
}

PROGRESS_EVERY_ZIPS = 100
PROGRESS_EVERY_FACILITIES = 50

_WS_RE = re.compile(r"\s+")


def clean(value):
    """Trim a string to None-or-non-empty; pass through anything else."""
    if isinstance(value, str):
        text = _WS_RE.sub(" ", value).strip()
        return text or None
    return value


def number(value):
    """Return a plain number from either of the API's two numeric shapes.

    Live responses carry plain floats; the story's captured samples carry the
    compound form ``{"source": "50.0", "parsedValue": 50}`` (plan Sec 3.6).
    """
    if isinstance(value, dict):
        return value.get("parsedValue")
    return value


def format_age(months):
    """Format an age bound in months: "N months" below 24 or when not a whole
    number of years, else "N years" (plan Sec 3.3)."""
    months = number(months)
    if months is None:
        return None
    months = int(months)
    if months < 24 or months % 12:
        return f"{months} months"
    return f"{months // 12} years"


def format_ages(age_range):
    """Human string like "0 months - 12 years", or None if either bound is missing."""
    if not isinstance(age_range, dict):
        return None
    low = format_age(age_range.get("minAge"))
    high = format_age(age_range.get("maxAge"))
    if low is None or high is None:
        return None
    return f"{low} - {high}"


def format_address(address):
    """ "address1[, address2], City, WY 82001"; missing parts are skipped."""
    if not isinstance(address, dict):
        return None
    street = ", ".join(p for p in (clean(address.get("address1")), clean(address.get("address2"))) if p)
    state_zip = " ".join(p for p in (clean(address.get("state")), clean(address.get("zip"))) if p)
    parts = [p for p in (street, clean(address.get("city")), state_zip) if p]
    return ", ".join(parts) or None


def hours_schedule(hours):
    """[{day, start, end}] for the days that have a start time."""
    schedule = []
    for entry in hours or []:
        if entry.get("startTime"):
            schedule.append({"day": entry.get("weekday"), "start": entry["startTime"], "end": entry.get("endTime")})
    return schedule


def format_hours(schedule):
    """ "Monday 06:00-18:00; Tuesday ..." -- closed days are omitted."""
    return "; ".join(f"{d['day']} {d['start']}-{d['end']}" for d in schedule)


def person_name(person):
    """ "First Last", title-cased only when the source is all-lowercase."""
    name = clean(" ".join(p for p in (person.get("firstname"), person.get("lastname")) if p))
    if name and name.islower():
        name = name.title()
    return name


def parse_directors(directors):
    """Return [{name, title}], deduped case-insensitively, in source order."""
    seen = set()
    result = []
    for person in directors or []:
        name = person_name(person)
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        result.append({"name": name, "title": clean(person.get("title"))})
    return result


def fix_mojibake(text):
    """Repair UTF-8 text that was decoded as cp1252 (e.g. ``â€œ`` -> ``"``).

    Only touched when the telltale ``â€`` / ``Ã`` is present. A few UTF-8 bytes
    (0x81, 0x8D, 0x8F, 0x90, 0x9D) have no cp1252 character and survive as
    C1 control characters, so each character falls back to latin-1 -- a bare
    ``encode("cp1252")`` fails on the closing curly quote.
    """
    if not isinstance(text, str) or ("â€" not in text and "Ã" not in text):
        return text
    raw = bytearray()
    for char in text:
        try:
            raw += char.encode("cp1252")
        except UnicodeEncodeError:
            try:
                raw += char.encode("latin-1")
            except UnicodeEncodeError:
                return text
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return text


def parse_violation_details(details):
    return [
        {
            "regulation": clean(fix_mojibake(d.get("regulation"))),
            "compliance_due_date": clean(d.get("complianceDueDate")),
            "compliance_achieved_date": clean(d.get("complianceAchievedDate")),
            "action_plan_due_date": clean(d.get("complianceActionPlanDueDate")),
            "action_plan_completed_date": clean(d.get("complianceActionPlanCompletedDate")),
        }
        for d in details or []
    ]


def visit_inspection(row):
    inspection = InspectionItem()
    inspection["date"] = clean(row.get("visitDate"))
    visit_type = clean(row.get("visitType")) or "Visit"
    inspection["type"] = visit_type.replace("Renewal / Validation Visit", "Renewal/Validation Visit")
    inspection["report_url"] = clean(row.get("visitFormURL"))
    inspection["wy_record_type"] = "visit"
    inspection["wy_violation_found"] = row.get("violation")
    return inspection


def inspection_inspection(row):
    inspection = InspectionItem()
    inspection["date"] = clean(row.get("inspectionDate"))
    raw_type = clean(row.get("inspectionType"))
    if raw_type in INSPECTION_TYPE_MAP:
        raw_type = INSPECTION_TYPE_MAP[raw_type]
    elif raw_type and re.fullmatch(r"[A-Z_]+", raw_type):
        raw_type = raw_type.replace("_", " ").capitalize()
    inspection["type"] = raw_type or "Inspection"
    inspection["report_url"] = clean(row.get("inspectionFormURL"))
    inspection["wy_record_type"] = "inspection"
    return inspection


def notice_inspection(row):
    """Return (InspectionItem, deficiency_count) for one compliance notice."""
    details = parse_violation_details(row.get("violationDetails"))
    inspection = InspectionItem()
    inspection["date"] = clean(row.get("visitDate"))
    inspection["type"] = "Compliance Notice"
    inspection["report_url"] = clean(row.get("complianceNoticeUrl"))
    inspection["wy_record_type"] = "compliance_notice"
    inspection["wy_violations"] = details
    declared = clean(row.get("numberOfNonComplianceViolations"))
    try:
        declared = int(declared)
    except (TypeError, ValueError):
        declared = None
    # The listed details win over the declared (string) count if they disagree.
    return inspection, len(details), declared


def program_entry(program):
    entry = {
        "name": clean(program.get("programName")),
        "type": clean(program.get("programType")),
        "type_of_care": clean(program.get("typeOfCare")),
        "min_age_months": (program.get("ageGroup") or {}).get("minAge"),
        "max_age_months": (program.get("ageGroup") or {}).get("maxAge"),
        "languages": [clean(x) for x in program.get("programLanguages") or [] if clean(x)],
        "accepting_new_enrollment": program.get("acceptingNewEnrollment"),
        "rates": [
            {"rate": number(r.get("rate")), "period": r.get("period"), "type": r.get("type")}
            for r in program.get("programRates") or []
        ],
        "availability_start": program.get("programAvailabilityStartDate"),
        "availability_end": program.get("programAvailabilityEndDate"),
    }
    # Descriptions are free text and often a placeholder like "." -- keep only
    # real text.
    for key, source in (("description", "programDescription"), ("philosophy", "programPhilosophy")):
        text = clean(program.get(source))
        if text and re.search(r"\w", text):
            entry[key] = text
    return entry


def normalize_address_key(address):
    return clean((address or {}).get("address1") or "").lower()


def find_centroid_points(search_records):
    """Return the set of (lat, lon) points shared by different street addresses.

    The portal returns a ZIP centroid for ungeocoded addresses, so a point
    that several different streets share is not a real geocode (plan Sec 3.1).
    Two facilities at the *same* street and point are a legitimate geocode.
    """
    streets = {}
    for record in search_records:
        address = record.get("facilityAddress") or {}
        point = (address.get("lat"), address.get("lon"))
        if point[0] is None or point[1] is None:
            continue
        streets.setdefault(point, set()).add(normalize_address_key(address))
    return {point for point, names in streets.items() if len(names) > 1}


def build_item(facility, search_record, centroid_points):
    """Build a ProviderItem from a detail `facility` dict plus its search record.

    ``facility`` may be empty (detail request failed) -- the search record
    carries enough to emit a partial item.
    """
    search_record = search_record or {}
    facility = facility or {}
    item = ProviderItem()

    facility_id = search_record.get("facilityId") or facility.get("facilityExternalId")
    item["wy_facility_id"] = facility_id
    item["provider_url"] = PROVIDER_URL
    item["provider_name"] = clean(facility.get("facilityName") or search_record.get("facilityName"))
    license_id = clean(facility.get("licenseId")) or clean(search_record.get("licenseId"))
    if license_id:
        item["license_number"] = license_id
    item["provider_type"] = clean(facility.get("facilityType") or search_record.get("facilityType"))

    address = facility.get("facilityAddress") or search_record.get("facilityAddress") or {}
    item["address"] = format_address(address)
    lat, lon = address.get("lat"), address.get("lon")
    if lat is not None and lon is not None:
        if (lat, lon) in centroid_points:
            item["wy_coordinates_approximate"] = True
        else:
            item["latitude"] = lat
            item["longitude"] = lon

    contact = facility.get("facilityContact") or {}
    if clean(contact.get("phone")):
        item["phone"] = clean(contact["phone"])
    if clean(contact.get("email")):
        item["email"] = clean(contact["email"])
    if clean(facility.get("facilityWebsite")):
        item["provider_website"] = clean(facility["facilityWebsite"])
    if facility.get("noOfChildrenServed") is not None:
        item["capacity"] = facility["noOfChildrenServed"]

    directors = parse_directors(facility.get("facilityDirectors"))
    if directors:
        item["administrator"] = ", ".join(d["name"] for d in directors)
        item["wy_directors"] = directors

    schedule = hours_schedule(facility.get("facilityHours"))
    if schedule:
        item["hours"] = format_hours(schedule)
        item["wy_schedule"] = schedule

    ages = format_ages(facility.get("facilityAgeRange"))
    if ages:
        item["ages_served"] = ages

    subsidy = facility.get("acceptingSubsidy", search_record.get("acceptingSubsidies"))
    if subsidy is not None:
        item["scholarships_accepted"] = subsidy
    accreditations = [clean(a) for a in facility.get("accreditations") or [] if clean(a)]
    if accreditations:
        item["accreditation"] = ", ".join(accreditations)

    service_names = [clean(s.get("serviceName")) for s in facility.get("services") or []]
    service_names = [s for s in service_names if s]
    if service_names:
        item["wy_services"] = service_names
    meals = [s for s in service_names if s.lower() in MEAL_SERVICES]
    if meals:
        item["meals"] = ", ".join(meals)

    # The search record's bool wins; the service name only corroborates it.
    if search_record.get("providesTransport") is not None:
        item["transportation"] = search_record["providesTransport"]
    elif any(s.lower() == TRANSPORT_SERVICE for s in service_names):
        item["transportation"] = True

    programs = list(facility.get("programs") or []) + list(facility.get("otherPrograms") or [])
    if programs:
        item["wy_programs"] = [program_entry(p) for p in programs]
        item["accepting_new_children"] = any(p.get("acceptingNewEnrollment") for p in programs)
        languages = []
        for program in programs:
            for language in program.get("programLanguages") or []:
                language = clean(language)
                if language and language not in languages:
                    languages.append(language)
        if languages:
            item["languages"] = ", ".join(languages)

    if "preEnrollmentVisitRequired" in facility:
        item["wy_pre_enrollment_visit_required"] = facility["preEnrollmentVisitRequired"]
    if facility.get("chargingRegistrationFee"):
        fee = number(facility.get("registrationFee"))
        if fee is not None:
            item["wy_registration_fee"] = fee
    if "weekendCare" in search_record:
        item["wy_weekend_care"] = search_record["weekendCare"]
    if "eveningCare" in search_record:
        item["wy_evening_care"] = search_record["eveningCare"]
    return item


def response_json(response):
    try:
        return json.loads(response.text)
    except ValueError:
        return {}


class WyomingSpider(scrapy.Spider):
    name = "wyoming"
    allowed_domains = ["childcare.dfs.wyo.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, lead_id=DEFAULT_LEAD_ID, reports=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lead_id = str(lead_id).strip()
        # `-a reports=0` skips Phase 3, mirroring New Hampshire's `visits=0`.
        self.do_reports = str(reports).strip().lower() not in ("0", "false")

        self.zips = [str(z) for z in range(ZIP_RANGE_START, ZIP_RANGE_END + 1)]
        self.zips_pending = len(self.zips)
        self.zips_done = 0
        self.zips_valid = 0
        self.facilities = {}  # facilityId -> search record (first seen)
        self.centroid_points = set()
        self.lead_error_logged = False

        self.details_done = 0
        self.details_total = 0
        self.items_emitted = 0
        self.detail_failures = 0
        self.report_failures = 0
        self.test_records_skipped = 0
        self.notices_total = 0
        self.inspections_total = 0

    # ------------------------------------------------------------------ #
    # Phase 1 -- ZIP sweep
    # ------------------------------------------------------------------ #

    def start_requests(self):
        self.logger.info(
            "Wyoming Phase 1: sweeping %d ZIPs (%s..%s) against providerSelection",
            len(self.zips),
            self.zips[0],
            self.zips[-1],
        )
        for zip_code in self.zips:
            yield JsonRequest(
                SEARCH_URL,
                data={"memberId": "1", "zip": zip_code, "isAnonymous": True, "externalId": self.lead_id},
                callback=self.parse_search,
                errback=self.search_failed,
                cb_kwargs={"zip_code": zip_code},
                meta={"handle_httpstatus_list": [400]},
                dont_filter=True,
            )

    def _lead_expired(self, body):
        return body.get("errorMessage") == LEAD_NOT_FOUND

    def _close_for_lead(self):
        if not self.lead_error_logged:
            self.lead_error_logged = True
            self.logger.error(
                "Wyoming: the lead id %r was rejected (%s). Mint a new one by running the "
                "pre-screener at %s/home/ and copy the id from the /shopping/lead/<id> URL, "
                "then re-run with `-a lead_id=<id>`.",
                self.lead_id,
                LEAD_NOT_FOUND,
                BASE_URL,
            )
        raise CloseSpider("wyoming_lead_expired")

    def parse_search(self, response, zip_code):
        body = response_json(response)
        if response.status == 400:
            if self._lead_expired(body):
                self._close_for_lead()
            codes = [e.get("errorCode") for e in body.get("validationErrors") or []]
            if ZIP_INVALID in codes:
                self.logger.debug("Wyoming: ZIP %s is not in the portal's ZIP table, skipping", zip_code)
            else:
                self.logger.warning("Wyoming: ZIP %s returned HTTP 400: %s", zip_code, response.text[:300])
        else:
            self.zips_valid += 1
            for record in (body.get("data") or {}).get("facilities") or []:
                facility_id = record.get("facilityId")
                if facility_id and facility_id not in self.facilities:
                    self.facilities[facility_id] = record
        yield from self._zip_finished()

    def search_failed(self, failure):
        self.logger.warning("Wyoming: search request failed: %s", failure.value)
        yield from self._zip_finished()

    def _zip_finished(self):
        """Count a finished sweep request; the last one starts Phase 2."""
        self.zips_pending -= 1
        self.zips_done += 1
        if self.zips_done % PROGRESS_EVERY_ZIPS == 0 and self.zips_pending:
            self.logger.info(
                "Wyoming Phase 1: %d/%d ZIPs done, %d valid, %d unique facilities so far",
                self.zips_done,
                len(self.zips),
                self.zips_valid,
                len(self.facilities),
            )
        if self.zips_pending == 0:
            yield from self._start_details()

    # ------------------------------------------------------------------ #
    # Phase 2 -- facility details
    # ------------------------------------------------------------------ #

    def _start_details(self):
        self.logger.info(
            "Wyoming Phase 1 complete: %d/%d ZIPs valid, %d unique facilities (baseline %d)",
            self.zips_valid,
            len(self.zips),
            len(self.facilities),
            EXPECTED_PROVIDER_COUNT,
        )
        self.centroid_points = find_centroid_points(self.facilities.values())
        affected = sum(
            1
            for r in self.facilities.values()
            if ((r.get("facilityAddress") or {}).get("lat"), (r.get("facilityAddress") or {}).get("lon"))
            in self.centroid_points
        )
        self.logger.info(
            "Wyoming: %d shared ZIP-centroid points cover %d facilities; their coordinates will be dropped",
            len(self.centroid_points),
            affected,
        )

        targets = []
        for facility_id, record in self.facilities.items():
            if TEST_FACILITY_RE.match(record.get("facilityName") or ""):
                self.test_records_skipped += 1
                self.logger.info("Wyoming: skipping test record %s (%s)", facility_id, record.get("facilityName"))
                continue
            targets.append((facility_id, record))
        self.details_total = len(targets)
        self.logger.info("Wyoming Phase 2: requesting details for %d facilities", self.details_total)
        for facility_id, record in targets:
            yield JsonRequest(
                DETAIL_URL,
                data={"facilityId": facility_id, "memberId": "1", "externalId": self.lead_id, "flowType": "ANONYMOUS"},
                callback=self.parse_detail,
                errback=self.detail_failed,
                cb_kwargs={"record": record},
                meta={"handle_httpstatus_list": [400]},
                dont_filter=True,
            )

    def _detail_progress(self):
        self.details_done += 1
        if self.details_done % PROGRESS_EVERY_FACILITIES == 0 or self.details_done == self.details_total:
            self.logger.info("Wyoming Phase 2: %d/%d facility details done", self.details_done, self.details_total)

    def parse_detail(self, response, record):
        body = response_json(response)
        if response.status == 400:
            if self._lead_expired(body):
                self._close_for_lead()
            self.logger.warning(
                "Wyoming: detail for %s returned HTTP 400: %s", record.get("facilityId"), response.text[:300]
            )
            self.detail_failures += 1
            self._detail_progress()
            yield from self._finish(build_item({}, record, self.centroid_points))
            return
        facility = (body.get("data") or {}).get("facility") or {}
        self._detail_progress()
        item = build_item(facility, record, self.centroid_points)
        if self.do_reports:
            yield self._report_request("visits", item)
        else:
            yield from self._finish(item)

    def detail_failed(self, failure):
        record = failure.request.cb_kwargs["record"]
        self.logger.warning("Wyoming: detail request for %s failed: %s", record.get("facilityId"), failure.value)
        self.detail_failures += 1
        self._detail_progress()
        yield from self._finish(build_item({}, record, self.centroid_points))

    # ------------------------------------------------------------------ #
    # Phase 3 -- reports (visits -> inspections -> violations)
    # ------------------------------------------------------------------ #

    def _report_request(self, kind, item):
        item.setdefault("inspections", [])
        callback = {
            "visits": self.parse_visits,
            "inspections": self.parse_inspections,
            "violations": self.parse_violations,
        }[kind]
        return scrapy.Request(
            REPORT_URL.format(kind=kind, facility_id=item["wy_facility_id"]),
            callback=callback,
            errback=self.report_failed,
            cb_kwargs={"item": item},
            dont_filter=True,
        )

    @staticmethod
    def _rows(response, key):
        return (response_json(response).get("data") or {}).get(key) or []

    def parse_visits(self, response, item):
        item["inspections"].extend(visit_inspection(r) for r in self._rows(response, "visits"))
        yield self._report_request("inspections", item)

    def parse_inspections(self, response, item):
        item["inspections"].extend(inspection_inspection(r) for r in self._rows(response, "inspections"))
        yield self._report_request("violations", item)

    def parse_violations(self, response, item):
        deficiencies = 0
        for row in self._rows(response, "violations"):
            inspection, count, declared = notice_inspection(row)
            if declared is not None and declared != count:
                self.logger.warning(
                    "Wyoming: %s notice %s declares %s violations but lists %d; using the list",
                    item["wy_facility_id"],
                    inspection["date"],
                    declared,
                    count,
                )
            item["inspections"].append(inspection)
            deficiencies += count
            self.notices_total += 1
        item["deficiencies"] = deficiencies
        yield from self._finish(item)

    def report_failed(self, failure):
        """A failed report request must not drop the provider."""
        item = failure.request.cb_kwargs["item"]
        self.report_failures += 1
        self.logger.warning(
            "Wyoming: report request %s failed (%s); emitting %s with what it has",
            failure.request.url,
            failure.value,
            item.get("wy_facility_id"),
        )
        yield from self._finish(item)

    def _finish(self, item):
        self.items_emitted += 1
        self.inspections_total += len(item.get("inspections") or [])
        if self.do_reports and (self.items_emitted % PROGRESS_EVERY_FACILITIES == 0):
            self.logger.info(
                "Wyoming Phase 3: %d/%d facilities complete (reports fetched)",
                self.items_emitted,
                self.details_total,
            )
        yield item

    def closed(self, reason):
        self.logger.info(
            "Wyoming: finished (%s) -- %d items emitted (%d/%d unique facilities found, %d test records "
            "skipped), %d inspections, %d compliance notices, %d detail failures, %d report failures",
            reason,
            self.items_emitted,
            len(self.facilities),
            EXPECTED_PROVIDER_COUNT,
            self.test_records_skipped,
            self.inspections_total,
            self.notices_total,
            self.detail_failures,
            self.report_failures,
        )
        if self.items_emitted and self.items_emitted < EXPECTED_PROVIDER_COUNT * 0.9:
            self.logger.warning(
                "Wyoming: only %d items emitted (< 90%% of the %d baseline observed 2026-10-02) -- "
                "possible incomplete crawl",
                self.items_emitted,
                EXPECTED_PROVIDER_COUNT,
            )
