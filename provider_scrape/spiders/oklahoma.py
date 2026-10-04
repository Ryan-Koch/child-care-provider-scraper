"""Oklahoma child care provider spider.

Source: OKDHS "Child Care Locator", https://childcarefind.okdhs.org/ (a
Next.js pages-router app with ISR caching). See tasks/oklahoma/oklahoma_plan.md
for the recon writeup this spider implements.

Two phases, plain HTTP (no cookies, no Playwright):

  Phase 1  GET /providers                 one unfiltered request returns every
                                          provider in the state (~2,600, ~5.5
                                          MB, no pagination) as JSON embedded in
                                          ``<script id="__NEXT_DATA__">``.
  Phase 2  GET /providers/<vendorId>      one per provider -> ProviderItem built
                                          from the search record plus the detail
                                          JSON (contact, ages, hours, star level,
                                          monitoring visits, complaints).

Quirks worth knowing:

  * Never read the rendered markup; its CSS classes are build hashes. Both pages
    carry everything in ``__NEXT_DATA__`` -> ``props.pageProps``.
  * Detail pages often answer a *stale cached 404* on the first hit
    (``page == "/404"``, HTTP 404, empty pageProps); ISR regenerates the page
    in the background so a later request succeeds. The callback re-yields the
    request with ``dont_filter`` and a lower priority (back of the queue), up to
    MAX_DETAIL_ATTEMPTS. Providers that never load still yield a search-only
    item flagged ``ok_detail_unavailable``.
  * An unknown vendorId answers HTTP 200 with empty pageProps, so "ready" means
    ``pageProps.vendorId`` is present, not just a 200.
  * "View Full Report" pages (/licensing-history/...) are deliberately not
    fetched; the detail JSON already carries the non-compliance text.
"""

import asyncio
import json

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://childcarefind.okdhs.org"
SEARCH_URL = BASE_URL + "/providers"
DETAIL_URL = BASE_URL + "/providers/{}"
REPORT_URL = BASE_URL + "/licensing-history/{}/{}"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Cold-cache 404s: one early request is usually enough, a few need many tries
# (one provider took ~8 attempts over ~30 s in recon). Decision D1.
MAX_DETAIL_ATTEMPTS = 20

# Backoff before re-requesting a detail page that answered a stale 404:
# 2s, 4s, 8s, 16s, then 30s. Back-to-back retries (even deprioritized ones)
# burned the whole attempt budget once the queue was nearly empty, because ISR
# needs real time to regenerate the page.
RETRY_DELAY_BASE = 2
RETRY_DELAY_MAX = 30

PROGRESS_EVERY = 250

# Live baseline (2026-10-03): 2,631 providers. Used only for a completeness
# warning at the end of a run, never as a gate.
EXPECTED_PROVIDER_COUNT = 2631

FACILITY_TYPES = {
    "childcare-center": "Child Care Center",
    "childcare-home": "Family Child Care Home",
}

# Chip text the site shows for each `hours` tag (from the app's JS bundle).
CARE_TYPE_LABELS = {
    "year-round": "Year Round",
    "school-year": "School Year Only",
    "daytime": "Daytime Hours",
    "evening": "Evening Hours",
    "weekend": "Weekend Hours",
    "overnight": "Overnight Hours",
    "drop-in": "Drop In",
    "summer": "Summer Only",
    "school-vacation": "School Vacation Only",
    "sick-care": "Sick Care",
}

# `agesAccepted` code -> (age group label shown on the page, age flag).
AGE_GROUPS = {
    "zero-month": ("Infants (0-11 months)", "infant"),
    "twelve-month": ("Toddlers (12-23 months)", "toddler"),
    "two-year": ("Preschool (24-48 months; 2-4 yrs.)", "preschool"),
    "three-year": ("Preschool (24-48 months; 2-4 yrs.)", "preschool"),
    "four-year": ("Preschool (24-48 months; 2-4 yrs.)", "preschool"),
    "five-year": ("School-age (5 years-older)", "school"),
}
AGE_FLAGS = ("infant", "toddler", "preschool", "school")


def retry_delay(attempt):
    """Seconds to wait after the ``attempt``-th failed request (1-based)."""
    return min(RETRY_DELAY_BASE**attempt, RETRY_DELAY_MAX)


def clean(value):
    """Collapse whitespace; return None for empty/missing values."""
    if value is None:
        return None
    text = " ".join(str(value).replace("\xa0", " ").split())
    return text or None


def _put(item, key, value):
    """Set ``item[key]`` only when ``value`` is non-empty."""
    if value not in (None, "", [], {}):
        item[key] = value


def flag(value):
    """The source's "True"/"False" strings (or real bools) -> bool; None if unknown."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower() if value is not None else ""
    if text in ("true", "false"):
        return text == "true"
    return None


def page_props(response):
    """``props.pageProps`` from the page's __NEXT_DATA__ JSON, or None."""
    raw = response.css("script#__NEXT_DATA__::text").get()
    if not raw:
        return None
    try:
        props = json.loads(raw)["props"]["pageProps"]
    except (ValueError, KeyError, TypeError):
        return None
    return props if isinstance(props, dict) else None


def is_detail_ready(response, props):
    """False for the stale-cached 404 (or anything that lacks the provider)."""
    return response.status == 200 and bool(props) and bool(props.get("vendorId"))


def format_address(record):
    return ", ".join(line for line in (clean(x) for x in record.get("addressLines") or []) if line) or None


def format_hours(schedule):
    """ "Monday 6:30AM-6:00PM; Tuesday ..." -- closed days are omitted."""
    return "; ".join(
        f"{d['day']} {d['hours'].replace(' - ', '-')}" for d in schedule if d.get("hours") and d.get("day")
    )


def build_schedule(hours_of_operation):
    """[{day, hours}] with hours None for closed days."""
    return [
        {"day": clean(entry.get("weekday")), "hours": clean(entry.get("hours"))}
        for entry in hours_of_operation or []
        if clean(entry.get("weekday"))
    ]


def care_type_labels(tags):
    labels = []
    for tag in tags or []:
        label = CARE_TYPE_LABELS.get(tag) or str(tag).replace("-", " ").title()
        if label not in labels:
            labels.append(label)
    return labels


def _coord(value):
    return None if value is None else str(value)


def apply_coordinates(item, record):
    coords = record.get("coordinates") or {}
    _put(item, "latitude", _coord(coords.get("latitude")))
    _put(item, "longitude", _coord(coords.get("longitude")))


def apply_ages(item, codes):
    """ages_served plus the four age flags; all set whenever codes are present."""
    if not codes:
        return
    labels = []
    for code in codes:
        label = AGE_GROUPS.get(code, (None, None))[0]
        if label and label not in labels:
            labels.append(label)
    _put(item, "ages_served", ", ".join(labels))
    served = {AGE_GROUPS[c][1] for c in codes if c in AGE_GROUPS}
    for key in AGE_FLAGS:
        item[key] = key in served


def visit_inspection(vendor_id, visit):
    """One monitoring visit -> InspectionItem."""
    inspection = InspectionItem()
    date = clean(visit.get("visitDate"))
    purpose = clean(visit.get("purpose"))
    visit_type = clean(visit.get("visitType"))
    _put(inspection, "date", date)
    _put(inspection, "type", f"{purpose} ({visit_type})" if purpose and visit_type else purpose or visit_type)
    count, total = visit.get("complianceCount"), visit.get("complianceTotal")
    if count is not None and total is not None:
        inspection["original_status"] = f"{count} of {total} areas in compliance"
    if date:
        inspection["report_url"] = REPORT_URL.format(vendor_id, date)
    _put(inspection, "ok_visit_type", visit_type)
    _put(inspection, "ok_visit_purpose", purpose)
    if count is not None:
        inspection["ok_compliance_count"] = count
    if total is not None:
        inspection["ok_compliance_total"] = total
    inspection["ok_noncompliances"] = [t for t in (clean(x) for x in visit.get("noncompliancesObserved") or []) if t]
    return inspection


def complaint_inspection(complaint):
    """One substantiated complaint -> InspectionItem."""
    inspection = InspectionItem()
    # "2026-07-31T00:00:00-05:00[America/Chicago]" -> "2026-07-31"
    _put(inspection, "date", (clean(complaint.get("dateReceived")) or "")[:10])
    inspection["type"] = "Complaint"
    allegations = complaint.get("allegations") or []
    findings = []
    for allegation in allegations:
        finding = clean(allegation.get("allegationFindings"))
        if finding and finding not in findings:
            findings.append(finding)
    _put(inspection, "original_status", "; ".join(findings))
    inspection["ok_allegations"] = [
        {
            "requirement": clean(a.get("requirement")),
            "requirement_description": clean(a.get("requirementDescription")),
            "noncompliance_observed": clean(a.get("noncomplianceObserved")),
            "plan_to_correct": clean(a.get("planToCorrect")),
            "occurrence_date": clean(a.get("occurrenceDate")),
            "findings": clean(a.get("allegationFindings")),
        }
        for a in allegations
    ]
    return inspection


def build_inspections(vendor_id, detail):
    """Visits and complaints, newest first."""
    inspections = [visit_inspection(vendor_id, v) for v in detail.get("monitoringVisits") or []]
    inspections += [complaint_inspection(c) for c in detail.get("complaints") or []]
    inspections.sort(key=lambda i: i.get("date") or "", reverse=True)
    return inspections


def count_deficiencies(detail):
    """Non-compliance statements across visits plus complaint allegations."""
    visits = sum(len(v.get("noncompliancesObserved") or []) for v in detail.get("monitoringVisits") or [])
    allegations = sum(len(c.get("allegations") or []) for c in detail.get("complaints") or [])
    return visits + allegations


def build_search_item(record, logger=None):
    """A ProviderItem from the search record alone."""
    vendor_id = record["vendorId"]
    item = ProviderItem()
    item["source_state"] = "Oklahoma"
    item["provider_url"] = DETAIL_URL.format(vendor_id)
    item["license_number"] = vendor_id
    _put(item, "provider_name", clean(record.get("name")))
    _put(item, "ok_doing_business_as", clean(record.get("officialDoingBusinessAs")))

    slug = record.get("facilityType")
    if slug and slug not in FACILITY_TYPES and logger:
        logger.warning("Oklahoma: unknown facilityType %r for %s; keeping the raw slug", slug, vendor_id)
    _put(item, "provider_type", FACILITY_TYPES.get(slug, slug))

    _put(item, "address", format_address(record))
    apply_coordinates(item, record)
    _put(item, "ok_care_types", care_type_labels(record.get("hours")))
    subsidy = flag(record.get("isSubsidyAccepted"))
    if subsidy is not None:
        item["scholarships_accepted"] = subsidy
    return item


def enrich_item(item, detail):
    """Layer the detail page's fields onto a search-built item."""
    vendor_id = detail["vendorId"]
    _put(item, "provider_name", clean(detail.get("name")))
    _put(item, "ok_doing_business_as", clean(detail.get("officialDoingBusinessAs")))
    _put(item, "address", format_address(detail))
    apply_coordinates(item, detail)
    _put(item, "phone", clean(detail.get("phoneNumber")))
    _put(item, "email", clean(detail.get("emailAddress")))
    _put(item, "administrator", clean(detail.get("directorFullName")))
    _put(item, "ok_administrator_title", clean(detail.get("directorPosition")))
    if detail.get("licenseCapacity") is not None:
        item["capacity"] = int(detail["licenseCapacity"])

    schedule = build_schedule(detail.get("hoursOfOperation"))
    _put(item, "hours", format_hours(schedule))
    _put(item, "ok_schedule", schedule)
    _put(item, "ok_care_types", care_type_labels(detail.get("hours")))
    apply_ages(item, detail.get("agesAccepted"))

    subsidy = flag(detail.get("isSubsidyAccepted"))
    if subsidy is not None:
        item["scholarships_accepted"] = subsidy
    _put(item, "ok_subsidy_contract_number", clean(detail.get("contractNumber")))

    star = clean(detail.get("starLevelCode"))
    if star and star.isdigit():
        item["ok_star_level"] = int(star)
    _put(item, "ok_licensing_specialist", clean(detail.get("workerFullName")))
    _put(item, "ok_licensing_specialist_phone", clean(detail.get("workerPhoneNumberFormatted")))
    for source, target in (
        ("denialSent", "ok_denial_sent"),
        ("revocationSent", "ok_revocation_sent"),
        ("emergencyIssued", "ok_emergency_issued"),
    ):
        value = flag(detail.get(source))
        if value is not None:
            item[target] = value

    item["deficiencies"] = count_deficiencies(detail)
    _put(item, "inspections", build_inspections(vendor_id, detail))
    return item


class OklahomaSpider(scrapy.Spider):
    name = "oklahoma"
    allowed_domains = ["childcarefind.okdhs.org"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 3,  # network errors / 5xx only; stale 404s are retried in the callback
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": USER_AGENT,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.details_total = 0
        self.details_done = 0
        self.ok_first_try = 0
        self.ok_after_retry = 0
        self.gave_up = 0
        self.retries = 0

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_search)

    # ------------------------------------------------------------------ #
    # Phase 1 -- search page
    # ------------------------------------------------------------------ #

    def parse_search(self, response):
        props = page_props(response)
        records = props.get("childcareProviders") if props else None
        if not isinstance(records, list):
            self.logger.error("Oklahoma: no childcareProviders in __NEXT_DATA__ at %s; nothing to crawl", response.url)
            return

        unique = {}
        for record in records:
            vendor_id = record.get("vendorId") if isinstance(record, dict) else None
            if vendor_id and vendor_id not in unique:
                unique[vendor_id] = record
        breakdown = {}
        for record in unique.values():
            slug = record.get("facilityType")
            breakdown[slug] = breakdown.get(slug, 0) + 1
        self.logger.info(
            "Oklahoma: search page lists %d records, %d unique vendorIds; facilityType breakdown %s",
            len(records),
            len(unique),
            breakdown,
        )
        self.details_total = len(unique)

        for vendor_id, record in unique.items():
            yield self.detail_request(vendor_id, record, attempt=1)

    # ------------------------------------------------------------------ #
    # Phase 2 -- detail pages
    # ------------------------------------------------------------------ #

    def detail_request(self, vendor_id, record, attempt):
        return scrapy.Request(
            DETAIL_URL.format(vendor_id),
            callback=self.parse_detail,
            errback=self.detail_failed,
            cb_kwargs={"record": record, "attempt": attempt},
            meta={"handle_httpstatus_list": [404]},
            # Retries go behind fresh requests so ISR can regenerate the page.
            priority=-attempt,
            # Retries re-request the same URL.
            dont_filter=True,
        )

    async def parse_detail(self, response, record, attempt):
        vendor_id = record["vendorId"]
        props = page_props(response)

        if not is_detail_ready(response, props):
            if attempt < MAX_DETAIL_ATTEMPTS:
                self.retries += 1
                self.logger.debug(
                    "Oklahoma: detail %s not ready (HTTP %s), attempt %d/%d; retrying in %ds",
                    vendor_id,
                    response.status,
                    attempt,
                    MAX_DETAIL_ATTEMPTS,
                    retry_delay(attempt),
                )
                # An async callback and asyncio.sleep suit this project's asyncio
                # reactor: the wait is non-blocking and per page, so it works even
                # with an empty queue, while first attempts never pass through it.
                await asyncio.sleep(retry_delay(attempt))
                yield self.detail_request(vendor_id, record, attempt + 1)
                return
            yield self.give_up(record, f"HTTP {response.status} after {attempt} attempts")
            return

        if attempt == 1:
            self.ok_first_try += 1
        else:
            self.ok_after_retry += 1
        item = enrich_item(build_search_item(record, self.logger), props)
        self.note_done()
        yield item

    def detail_failed(self, failure):
        """Network-level failure after Scrapy's own retries: keep the search data."""
        record = failure.request.cb_kwargs["record"]
        yield self.give_up(record, repr(failure.value))

    def give_up(self, record, reason):
        self.gave_up += 1
        self.logger.warning(
            "Oklahoma: giving up on detail %s (%s); emitting search-only item", record["vendorId"], reason
        )
        item = build_search_item(record, self.logger)
        item["ok_detail_unavailable"] = True
        self.note_done()
        return item

    def note_done(self):
        self.details_done += 1
        if self.details_done % PROGRESS_EVERY == 0:
            self.logger.info(
                "Oklahoma: %d/%d details done (%d retries so far, %d gave up)",
                self.details_done,
                self.details_total,
                self.retries,
                self.gave_up,
            )

    def closed(self, reason):
        self.logger.info(
            "Oklahoma: finished (%s) -- %d/%d details done: %d ok on first try, %d ok after retry, "
            "%d gave up; %d retry requests",
            reason,
            self.details_done,
            self.details_total,
            self.ok_first_try,
            self.ok_after_retry,
            self.gave_up,
            self.retries,
        )
        if self.details_total and reason == "finished" and self.details_total < EXPECTED_PROVIDER_COUNT * 0.9:
            self.logger.warning(
                "Oklahoma: search listed only %d providers (< 90%% of the %d baseline observed 2026-10-03)",
                self.details_total,
                EXPECTED_PROVIDER_COUNT,
            )
