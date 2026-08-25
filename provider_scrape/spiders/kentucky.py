"""Kentucky child care provider spider.

Source: the kynect benefits "Find Child Care" map at
https://kynect.ky.gov/benefits/s/child-care-provider -- a Salesforce Aura
community backed by a clean JSON-over-Aura Apex endpoint that works
cookieless, tokenless, and without a browser. This is the project's first
direct Aura-API spider (contrast montana.py, which drives its Aura community
with Playwright because its Apex methods are not directly reachable --
Kentucky's are, so no browser is used here; see kentucky_plan.md Sec 5.7).

One endpoint, two Apex methods (``SSP_ChildCareProviderSearchController``):

  * ``getChildCareProviderDetails`` -- the search. Returns full provider
    summaries (name, license #, type, status, address, coordinates, phone,
    stars, age flags, hours) for **one exact ZIP code**. There is no
    statewide/county/city/name search the server will actually serve -- every
    non-ZIP variant exceeds the server's ~7s deadline and gets its connection
    reset (plan Sec 2.7). So the spider sweeps the complete Kentucky ZIP space,
    40000-42799 (2,800 requests), and dedupes on ``ProviderId``.
  * ``fetchBrightwheelDetailsForProvider`` -- the detail. Returns capacity, the
    inspection history, a per-age cost table, and any open regulatory action.
    Keyed by ``(providerId, licenseNumber)`` -- both are required, or the
    connection resets exactly like the Sec 2.7 timeout (plan Sec 5.5).

See tasks/kentucky_epic/kentucky_plan.md for the full recon writeup (a live,
statewide 2,800-ZIP enumeration and a headless-browser audit of the search UI).
"""

import json
import re
from urllib.parse import urlencode

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

AURA_URL = "https://kynect.ky.gov/benefits/s/sfsites/aura?r=12&aura.ApexAction.execute=1"
SEARCH_PAGE_URL = "https://kynect.ky.gov/benefits/s/child-care-provider"
# The map is a single Aura route with no per-provider deep link and a
# POST-only detail; this is the Indiana/DC/Kansas precedent (plan Sec 4.3).
AURA_PAGE_URI = "/benefits/s/child-care-provider?origin=program-page&language=en_US"

# Salesforce framework build id baked into every request's aura.context.
# Captured from a browser DevTools session against the search page on
# 2026-08-20; Kentucky's org will bump this a few times a year on Salesforce
# release upgrades (plan Sec 5.6). When it goes stale the endpoint answers
# every action with an Aura ERROR state instead of data -- see closed().
FWUID = "OUcwT3JDYUZld21JQ2ZOckR1VnppUWtVMjdnTGFERUU2S3FfSVdrcU92bkExNC4xOTIuODM4ODYwOA"

AURA_CONTEXT = {
    "mode": "PROD",
    "fwuid": FWUID,
    "app": "siteforce:communityApp",
    "loaded": {"APPLICATION@markup://siteforce:communityApp": "1706_8wJLrETnpOGvg7aPJCutcg"},
    "dn": [],
    "globals": {},
    "uad": True,
}

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Origin": "https://kynect.ky.gov",
    "Referer": f"{SEARCH_PAGE_URL}?origin=program-page&language=en_US",
}

# ``queryData`` for the search call -- send all 20 keys; only ``zipCode5``
# varies (plan Sec 2.2). ``zipCode5`` is an exact match, not a radius or a
# prefix: one request per ZIP is the only way to enumerate.
QUERY_DATA_BASE = {
    "latitude": "",
    "longitude": "",
    "providerName": None,
    "licenseNumber": None,
    "providerIDValues": None,
    "isFavoriteSearch": False,
    "DestinationLatitude": None,
    "DestinationLongitude": None,
    "SourceAddressDetails": None,
    "DestinationAddressDetails": None,
    "SourceCountyName": None,
    "SourceCity": None,
    "DestinationCountyName": None,
    "DestinationCity": None,
    "alongTheRouteLocations": None,
    "SearchCriteria": "ZipCode",
    "sourceState": None,
    "destinationState": None,
    "distance": None,
}

# Kentucky's USPS ZIPs live entirely inside this range (plan Sec 3.1) -- a
# complete-by-construction sweep that needs no data file, no county table, and
# no maintenance as USPS adds ZIPs.
MIN_ZIP = 40000
MAX_ZIP = 42799  # inclusive

# The sentinel ServiceTime string for a closed day in HoursOfOperationList --
# every provider carries exactly 7 rows (Mon-Sun), even a Mon-Fri center.
CLOSED_DAY = "No Information Available"

# The four Y/N/null age flags on a search summary record, in display order,
# with the label used to build `ages_served` (plan Sec 4.9).
AGE_FLAGS = (
    ("Infant", "infant", "Infant"),
    ("Toddler", "toddler", "Toddler"),
    ("PreSchool", "preschool", "Preschool"),
    ("SchoolAge", "school", "School Age"),
)

# An Aura ERROR state (plan Sec 5.2) is re-issued this many times before the
# ZIP is given up on and logged as failed.
MAX_ERROR_RETRIES = 2

# Baseline unique count (full live enumeration 2026-08-20: 2,010). Warn if a
# run falls far short.
EXPECTED_MIN_PROVIDERS = 1800


def _aura_body(message):
    """URL-encode the four Aura form fields for one action envelope."""
    return urlencode(
        {
            "message": json.dumps(message),
            "aura.context": json.dumps(AURA_CONTEXT),
            "aura.pageURI": AURA_PAGE_URI,
            "aura.token": "null",
        }
    )


def _num(value, cast=int):
    """Coerce a KICCS number to ``cast`` (int/float), or None.

    Numbers arrive as plain floats on the wire (``84.0``, ``290673.0``). The
    Aura *client* -- not this spider -- sometimes wraps them as
    ``{"source": "...", "parsedValue": ...}``; the epic's DevTools-captured
    sample payloads show only that wrapped shape, which does not occur on the
    real wire (plan Sec 5.4). Accept either.
    """
    if isinstance(value, dict):
        value = value.get("parsedValue", value.get("source"))
    if value is None:
        return None
    return cast(float(value))


_MC_RE = re.compile(r"\bMc([a-z])")


def title_county(name):
    """Title-case an ALL-CAPS county, keeping "McCracken" style intact.

    Python's plain ``str.title()`` lower-cases the letter after a "c", turning
    Kentucky's three Mc- counties into "Mccracken"/"Mccreary"/"Mclean" (plan
    Sec 5.9). Re-uppercase the letter following "Mc" after title-casing.
    """
    if not isinstance(name, str) or not name.strip():
        return None
    return _MC_RE.sub(lambda m: "Mc" + m.group(1).upper(), name.strip().title())


def format_phone(value):
    """Format 10 bare digits as "(270) 783-4484"; anything else passes through
    as bare digits (Hawaii's format_phone shape, hawaii.py:342). Kentucky's
    PhoneNumber is 10 bare digits on 100% of rows (plan Sec 2.6)."""
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return digits


def format_hours(hours_list):
    """Collapse the 7-day HoursOfOperationList into a compact string, or None.

    Drops days whose ServiceTime is the "No Information Available" sentinel
    (closed days; length is never a signal, plan Sec 5.10); collapses to
    "Monday-Friday 7:30 AM - 3:30 PM" when every open day shares the same
    window, else lists each day (plan Sec 4.11).
    """
    if not hours_list:
        return None
    rows = [
        (h.get("Day"), h.get("ServiceTime"))
        for h in hours_list
        if h.get("Day") and h.get("ServiceTime") and h.get("ServiceTime") != CLOSED_DAY
    ]
    if not rows:
        return None
    times = {time for _, time in rows}
    if len(times) == 1 and len(rows) > 1:
        return f"{rows[0][0]}-{rows[-1][0]} {rows[0][1]}"
    return "; ".join(f"{day} {time}" for day, time in rows)


def ages_from_flags(record):
    """Derive ``(ages_served, age_flags)`` from the four Y/N/null age flags.

    ``Y`` -> True, ``N`` -> False, ``null`` -> the field is omitted entirely
    (left unset on the item, per plan Sec 4.9). ``ages_served`` joins the
    labels of the flags that are "Y".
    """
    labels = []
    flags = {}
    for source_key, field, label in AGE_FLAGS:
        raw = record.get(source_key)
        if raw == "Y":
            flags[field] = True
            labels.append(label)
        elif raw == "N":
            flags[field] = False
    return ", ".join(labels) or None, flags


class KentuckySpider(scrapy.Spider):
    name = "kentucky"
    allowed_domains = ["kynect.ky.gov"]

    custom_settings = {
        # The endpoint resets any request whose Apex call takes >~7s, and
        # concurrency -- not delay -- is what pushes healthy ~5s calls over
        # that line: a cold burst of 8 lost 7/8, while sustained 4-way ran at
        # 92% success (plan Sec 2.7). Four is the measured ceiling; the
        # default sits at 2 (Ryan, 2026-08-20) -- this is a state benefits
        # portal, and the extra time buys a comfortable margin under the
        # deadline. Exposed as `-a concurrency=N` (see from_crawler).
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0,
        "DOWNLOAD_TIMEOUT": 60,
        # Resets are transient (Sec 2.7 #3: no cooldown, no ban, instant
        # retry succeeds) -- the default RetryMiddleware already retries
        # ResponseFailed/ConnectionLost, so RETRY_TIMES is the whole fix.
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, zips=None, details=1, concurrency=2, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zip_list = self._parse_zips(zips)
        self.do_details = str(details).strip().lower() not in ("0", "false")
        self.concurrency = int(concurrency)

        self.seen = set()  # ProviderIds already emitted/scheduled
        self.zips_done = 0
        self.zips_with_hits = 0
        self.zips_failed = set()  # ZIPs that never resolved (Aura ERROR loop)
        self.detail_failures = 0
        self.error_state_count = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Apply `-a concurrency=N` to the *crawler's* settings.

        Mutating ``self.custom_settings`` from ``__init__`` does NOT work:
        ``Crawler.__init__`` calls ``spidercls.update_settings()`` on the
        **class**, long before any instance exists, so an instance-level dict
        is never read and the argument silently does nothing.

        ``from_crawler`` is early enough, though -- ``Crawler.crawl()`` runs
        ``_create_spider()`` (this) *before* ``_apply_settings()`` freezes the
        settings and before ``_create_engine()`` reads the concurrency. Setting
        it here at ``spider`` priority is what actually changes parallelism.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        for key in ("CONCURRENT_REQUESTS", "CONCURRENT_REQUESTS_PER_DOMAIN"):
            crawler.settings.set(key, spider.concurrency, priority="spider")
        return spider

    @staticmethod
    def _parse_zips(zips):
        """Parse the `-a zips=` argument: comma-separated ZIPs and/or
        hyphenated ranges (`42101,40216`, `40200-40299`). Defaults to the full
        40000-42799 statewide sweep (plan Sec 3.1)."""
        if not zips:
            return list(range(MIN_ZIP, MAX_ZIP + 1))
        out = []
        for token in str(zips).split(","):
            token = token.strip()
            if not token:
                continue
            if "-" in token:
                lo, hi = token.split("-", 1)
                out.extend(range(int(lo), int(hi) + 1))
            else:
                out.append(int(token))
        return out

    # --- enumeration (search) ------------------------------------------- #

    def _search_request(self, zip5, attempt=1):
        query_data = {**QUERY_DATA_BASE, "zipCode5": f"{zip5:05d}"}
        message = {
            "actions": [
                {
                    "id": "84;a",
                    "descriptor": "aura://ApexActionController/ACTION$execute",
                    "callingDescriptor": "UNKNOWN",
                    "params": {
                        "namespace": "",
                        "classname": "SSP_ChildCareProviderSearchController",
                        "method": "getChildCareProviderDetails",
                        "params": {"queryData": query_data},
                        "cacheable": False,
                        "isContinuation": False,
                    },
                }
            ]
        }
        return scrapy.Request(
            AURA_URL,
            method="POST",
            body=_aura_body(message),
            headers=HEADERS,
            callback=self.parse_search,
            errback=self.search_errback,
            meta={"zip": zip5, "attempt": attempt},
            dont_filter=True,
        )

    def start_requests(self):
        self.logger.info(
            "Kentucky: sweeping %d ZIP(s) (details=%s, concurrency=%d)",
            len(self.zip_list),
            self.do_details,
            self.concurrency,
        )
        for zip5 in self.zip_list:
            yield self._search_request(zip5)

    def search_errback(self, failure):
        """A ZIP that exhausted RETRY_TIMES without a usable response -- the
        default RetryMiddleware already covers the Sec 2.7 connection-reset
        exceptions, so reaching here means every retry failed."""
        zip5 = failure.request.meta.get("zip")
        self.zips_done += 1
        self.zips_failed.add(zip5)
        self.logger.warning(
            "Kentucky: ZIP %05d search failed after all retries (%s) -- a "
            "pocket of providers may be missing (plan Sec 2.7)",
            zip5,
            failure.value,
        )
        self._maybe_log_progress()

    def parse_search(self, response):
        zip5 = response.meta["zip"]
        data = response.json()
        # HTTP 200 does not mean the action succeeded -- Aura's real status
        # is actions[0].state (plan Sec 5.2). HttpErrorMiddleware cannot help
        # here since the transport-level response is always a 200.
        action = (data.get("actions") or [{}])[0]
        if action.get("state") != "SUCCESS":
            self.error_state_count += 1
            attempt = response.meta.get("attempt", 1)
            if attempt <= MAX_ERROR_RETRIES:
                self.logger.warning(
                    "Kentucky: ZIP %05d search returned Aura state=%r (attempt %d/%d) -- re-issuing",
                    zip5,
                    action.get("state"),
                    attempt,
                    MAX_ERROR_RETRIES,
                )
                yield self._search_request(zip5, attempt=attempt + 1)
                return
            self.zips_failed.add(zip5)
            self.zips_done += 1
            self.logger.warning(
                "Kentucky: ZIP %05d search gave up after %d Aura ERROR "
                "responses in a row -- if this count is high across the "
                "run, `fwuid` may have gone stale (plan Sec 5.6)",
                zip5,
                attempt,
            )
            self._maybe_log_progress()
            return

        self.zips_done += 1
        payload = action.get("returnValue") or {}
        # A ZIP with no providers returns SUCCESS with no returnValue.
        # returnValue key at all -- not null, not "" (plan Sec 2.4/5.3). This
        # is the normal answer for ~75% of the 2,800 ZIPs, so it is the quiet
        # path: no warning, no items, no requests.
        raw = payload.get("returnValue")
        if not raw:
            self._maybe_log_progress()
            return

        # Unlike the detail call, the search's returnValue.returnValue is a
        # JSON *string* that needs a second json.loads (plan Sec 5.3).
        decoded = json.loads(raw)
        records = decoded.get("sspChildCareProviderDetails") or []
        if records:
            self.zips_with_hits += 1

        for record in records:
            provider_id = _num(record.get("ProviderId"))
            if provider_id is None or provider_id in self.seen:
                continue
            self.seen.add(provider_id)
            item = self._item_from_summary(record, provider_id)
            license_number = record.get("ProviderCLRNumber")
            if self.do_details and license_number:
                yield self._detail_request(provider_id, license_number, item)
            else:
                yield item

        self._maybe_log_progress()

    def _maybe_log_progress(self):
        if self.zips_done % 100 == 0:
            self.logger.info(
                "Kentucky: %d/%d ZIPs done, %d with hits, %d unique providers so far",
                self.zips_done,
                len(self.zip_list),
                self.zips_with_hits,
                len(self.seen),
            )

    def _item_from_summary(self, record, provider_id):
        """Build a ProviderItem from one search summary record. Everything
        except capacity/costs/inspections comes from here (plan Sec 3.2)."""
        item = ProviderItem()
        item["source_state"] = "Kentucky"
        item["provider_url"] = SEARCH_PAGE_URL
        item["ky_provider_id"] = provider_id

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        put("provider_name", record.get("ProviderName"))
        put("license_number", record.get("ProviderCLRNumber"))
        put("provider_type", record.get("ProviderType"))
        put("status", record.get("ProviderStatus"))

        # Line 2 (suite/highway, ~3% of rows) appended after ", " (Sec 4.6).
        address = record.get("LocationAddressLine1")
        line2 = record.get("LocationAddressLine2")
        if address and line2:
            address = f"{address}, {line2}"
        put("address", address)
        put("city", record.get("LocationCity"))
        put("state", record.get("LocationStateDescription"))
        zip5 = record.get("LocationZipCode5")
        if zip5 is not None:
            # LocationZipCode5 is a float (42101.0); str() would give
            # "42101.0" (plan Sec 5.4).
            item["zip"] = f"{int(zip5):05d}"
        put("county", title_county(record.get("LocationCountyDescription")))

        # Coordinates are published full-precision strings on 99.7% of rows
        # (Sec 4.7) -- no geocoding step needed for this source.
        put("latitude", record.get("AddressLatitude"))
        put("longitude", record.get("AddressLongitude"))
        put("phone", format_phone(record.get("PhoneNumber")))

        if record.get("NumberOfStars") is not None:
            item["ky_stars_rating"] = record["NumberOfStars"]

        ages_served, age_flags = ages_from_flags(record)
        put("ages_served", ages_served)
        for field, value in age_flags.items():
            item[field] = value

        # Tri-state Y/N/null flags -> booleans; null leaves the field unset
        # (plan Sec 4.12).
        subsidy = record.get("IsSubsidyAccepted")
        if subsidy in ("Y", "N"):
            item["scholarships_accepted"] = subsidy == "Y"
        transportation = record.get("Transportation")
        if transportation in ("Y", "N"):
            item["transportation"] = transportation == "Y"
        prek = record.get("PreKPartnershipFlag")
        if prek in ("Y", "N"):
            item["ky_prek_partnership"] = prek == "Y"
        ongoing = record.get("IsOngoingProcess")
        if ongoing in ("Y", "N"):
            item["ky_ongoing_process"] = ongoing == "Y"

        put("hours", format_hours(record.get("HoursOfOperationList")))

        return item

    # --- detail (per provider) ------------------------------------------ #

    def _detail_request(self, provider_id, license_number, item):
        message = {
            "actions": [
                {
                    "id": "84;a",
                    "descriptor": "aura://ApexActionController/ACTION$execute",
                    "callingDescriptor": "UNKNOWN",
                    "params": {
                        "namespace": "",
                        "classname": "SSP_ChildCareProviderSearchController",
                        "method": "fetchBrightwheelDetailsForProvider",
                        # Both keys are required -- a null licenseNumber resets the
                        # connection exactly like the Sec 2.7 timeout (plan Sec 5.5).
                        "params": {"providerId": provider_id, "licenseNumber": license_number},
                        "cacheable": False,
                        "isContinuation": False,
                    },
                }
            ]
        }
        return scrapy.Request(
            AURA_URL,
            method="POST",
            body=_aura_body(message),
            headers=HEADERS,
            callback=self.parse_detail,
            errback=self.detail_errback,
            meta={"item": item, "provider_id": provider_id},
            dont_filter=True,
        )

    def detail_errback(self, failure):
        """If a detail call fails after all retries, still emit the item from
        the summary alone (capacity/inspections unset) -- losing a whole
        provider over one optional call is the worst available outcome
        (plan Sec 3.2)."""
        item = failure.request.meta.get("item")
        if item is not None:
            self.detail_failures += 1
            self.logger.warning(
                "Kentucky: detail request failed for provider %s (%s); emitting summary-only item",
                failure.request.meta.get("provider_id"),
                failure.value,
            )
            yield item

    def parse_detail(self, response):
        item = response.meta["item"]
        data = response.json()
        action = (data.get("actions") or [{}])[0]
        if action.get("state") != "SUCCESS":
            self.error_state_count += 1
            self.detail_failures += 1
            self.logger.warning(
                "Kentucky: detail for provider %s returned Aura state=%r; emitting summary-only item",
                response.meta.get("provider_id"),
                action.get("state"),
            )
            yield item
            return

        # Unlike search, returnValue.returnValue is already a dict here, not
        # a JSON string (plan Sec 5.3).
        result = (action.get("returnValue") or {}).get("returnValue") or {}
        if not result.get("bIsSuccess"):
            self.detail_failures += 1
            yield item
            return

        kiccs = (result.get("mapResponse") or {}).get("KICCSDataDetails") or {}
        if kiccs.get("Capacity") is not None:
            item["capacity"] = kiccs["Capacity"]

        costs = self._parse_costs(kiccs.get("ServiceCostList") or [])
        if costs:
            item["ky_service_costs"] = costs

        # Both flags are tri-state; null is the majority value for
        # accreditation and means "unknown", not False (plan Sec 2.5).
        accreditation = kiccs.get("IsAcceditationsAvailable")
        if accreditation in ("Y", "N"):
            item["ky_accreditation_available"] = accreditation == "Y"
        food_permit = kiccs.get("IsFoodPermitAvailable")
        if food_permit in ("Y", "N"):
            item["ky_food_permit"] = food_permit == "Y"

        ongoing_processes = kiccs.get("OngoingProcessListUpdated") or []
        if ongoing_processes:
            item["ky_ongoing_processes"] = [
                {"process_type": p.get("ProcessType"), "status": p.get("Status")} for p in ongoing_processes
            ]

        inspections = self._parse_inspections(kiccs)
        if inspections:
            item["inspections"] = inspections

        yield item

    @staticmethod
    def _parse_costs(rows):
        costs = []
        for row in rows:
            cost = {"age_group": row.get("AgeGroup")}
            full_time = _num(row.get("FullTimeCost"), cast=float)
            part_time = _num(row.get("PartTimeCost"), cast=float)
            if full_time is not None:
                cost["full_time_cost"] = full_time
            if part_time is not None:
                cost["part_time_cost"] = part_time
            costs.append(cost)
        return costs

    @staticmethod
    def _parse_inspections(kiccs):
        """Join InspectionHistoryList (the clean list, a JSON *string*) with
        InspectionHistoryListUpdated (a dict list, same InspectionIds) on
        InspectionId to pick up ApprovalDate/POC_ID where present (plan
        Sec 6.2). Sorted newest-first."""
        raw_history = kiccs.get("InspectionHistoryList")
        if not raw_history:
            return []
        history = (json.loads(raw_history) or {}).get("inspections") or []

        updated_by_id = {}
        for row in kiccs.get("InspectionHistoryListUpdated") or []:
            insp_id = _num(row.get("InspectionId"))
            if insp_id is not None:
                updated_by_id[insp_id] = row

        out = []
        for row in history:
            entry = InspectionItem()
            if row.get("InspectionEndDate"):
                entry["date"] = row["InspectionEndDate"]
            if row.get("InspectionType"):
                entry["type"] = row["InspectionType"]
            if row.get("ReportName"):
                entry["ky_report_name"] = row["ReportName"]
            insp_id = _num(row.get("InspectionId"))
            if insp_id is not None:
                entry["ky_inspection_id"] = insp_id
            updated = updated_by_id.get(insp_id) or {}
            if updated.get("ApprovalDate"):
                entry["status_updated"] = updated["ApprovalDate"]
            if updated.get("POC_ID"):
                entry["ky_poc_id"] = updated["POC_ID"]
            out.append(entry)

        out.sort(key=lambda e: e.get("date") or "", reverse=True)
        return out

    def closed(self, reason):
        self.logger.info(
            "Kentucky: finished (%s) -- %d/%d ZIPs done (%d failed), %d "
            "with hits, %d unique providers, %d detail failures, %d Aura "
            "ERROR responses",
            reason,
            self.zips_done,
            len(self.zip_list),
            len(self.zips_failed),
            self.zips_with_hits,
            len(self.seen),
            self.detail_failures,
            self.error_state_count,
        )
        if len(self.zip_list) >= (MAX_ZIP - MIN_ZIP + 1) and len(self.seen) < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Kentucky: only %d providers found (< %d baseline) -- possible incomplete crawl",
                len(self.seen),
                EXPECTED_MIN_PROVIDERS,
            )
        if self.zips_failed:
            self.logger.warning(
                "Kentucky: %d ZIP(s) never resolved: %s -- these pockets of providers are missing from this run",
                len(self.zips_failed),
                sorted(self.zips_failed),
            )
        # Diagnostic per plan Sec 3.3: lots of ResponseFailed/ConnectionLost
        # and zero HTTP error codes is the Sec 2.7 deadline -- lower
        # concurrency, don't add delay or proxies. A high ERROR-state count
        # instead points at Sec 5.6 (fwuid gone stale).
        if self.error_state_count > 20:
            self.logger.warning(
                "Kentucky: %d requests returned an Aura ERROR state -- if "
                "unexpectedly high, check whether `fwuid` has gone stale "
                "(plan Sec 5.6); it is a framework build id that Salesforce "
                "rotates on org upgrades.",
                self.error_state_count,
            )
