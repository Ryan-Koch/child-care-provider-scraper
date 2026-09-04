"""Massachusetts child care provider spider.

Source: the Massachusetts EEC "Find Child Care" search at
https://childcare.mass.gov/findchildcare -- a Salesforce Aura community
backed by a clean JSON-over-Aura Apex endpoint that works cookieless,
tokenless, and without a browser. This is a direct Aura-API spider in the
mold of kentucky.py (contrast montana.py, which drives its Aura community
with Playwright because its Apex methods are not directly reachable --
Massachusetts' are, so no browser is used here).

One host, two Apex methods, both POSTed to
``https://childcare.mass.gov/aura?...&aura.ApexAction.execute=1``:

  * ``EEC_ChildCareSearchController.callApex`` -- the search. Returns *all*
    providers for one exact ZIP code (no radius, no pagination, no result
    cap). There is no statewide/county/city/name search, so the spider
    sweeps the complete MA ZIP space, 01001-02799 (1,799 requests), and
    dedupes on the Salesforce ``accountId``.
  * ``EEC_ProviderDetailsController.getProviderDetails`` -- the detail.
    Keyed by the provider's ``Encrypted_Id__c``. Returns capacity by age
    group, schedules, a rate table, environment/meals/financial-assistance
    facets, the CCR&R record, and the full monitoring-visit and
    investigation/complaint history.

The two Apex methods live behind *different* Aura apps (the search page and
the detail page are separate Aura apps), so they carry different
``aura.context`` payloads (different ``app``/``loaded`` values) even though
they share one Salesforce framework build id (``fwuid``).

See tasks/massachusettes_story/massachusettes_plan.md for the full recon
writeup (live-verified cookieless 2026-09-04).
"""

import json
import re
from urllib.parse import urlencode

import scrapy

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://childcare.mass.gov"
SEARCH_URL = f"{BASE_URL}/aura?r=1&aura.ApexAction.execute=1"
DETAIL_URL = f"{BASE_URL}/aura?r=0&aura.ApexAction.execute=1"
SEARCH_PAGE_URI = "/findchildcare"
DETAIL_PAGE_URI_PREFIX = "/eec_childcareproviderdetail?memberId="

SEARCH_HEADER = "ApexActionController.execute:EEC_ChildCareSearchController.callApex"
DETAIL_HEADER = "ApexActionController.execute:EEC_ProviderDetailsController.getProviderDetails"

# Salesforce framework build id baked into every request's aura.context.
# Captured from a browser DevTools session against the search page on
# 2026-09-04; MA's org will bump this (and likely the per-app `loaded`
# hashes below) a few times a year on platform upgrades. When it goes stale
# the endpoint stops returning clean Apex SUCCESS -- see closed().
FWUID = "MzNzN1lSdDZQRXpUcEpsWHBlZGd5UWtVMjdnTGFERUU2S3FfSVdrcU92bkExNC4xOTIuODM4ODYwOA"

# The search page and the detail page are two different Aura apps -- each
# carries its own `app` + `loaded` value (plan Sec 2.1).
SEARCH_CONTEXT = {
    "mode": "PROD",
    "fwuid": FWUID,
    "app": "c:EEC_ChildCareSearchApp",
    "loaded": {"APPLICATION@markup://c:EEC_ChildCareSearchApp": "1067_gHRsnf0jupz4UbjO_bfOpg"},
    "dn": [],
    "globals": {},
    "uad": True,
}
DETAIL_CONTEXT = {
    "mode": "PROD",
    "fwuid": FWUID,
    "app": "c:EEC_ChildCareProviderDetailApp",
    "loaded": {"APPLICATION@markup://c:EEC_ChildCareProviderDetailApp": "1067_YCBURgewcp0ozxQGJaEklA"},
    "dn": [],
    "globals": {},
    "uad": True,
}

BASE_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "Origin": BASE_URL,
}

# Massachusetts' USPS ZIPs live entirely inside this range (plan Sec 3.1) --
# a complete-by-construction sweep that needs no data file. 028xx+ is Rhode
# Island, so 02799 is the clean upper bound.
MIN_ZIP = 1001
MAX_ZIP = 2799  # inclusive

# An Aura ERROR state (plan Sec 5.1) is re-issued this many times before a
# ZIP is given up on and logged as failed.
MAX_ERROR_RETRIES = 2

# Baseline unique count -- set conservatively; reset after run 1 (plan Sec 10).
EXPECTED_MIN_PROVIDERS = 5000

# The nine per-age capacity-count keys on a detail response (plan Sec 6.2),
# in display order. `multiAge` has no search-summary equivalent.
AGE_GROUP_KEYS = (
    "infantBirth",
    "infantToddler",
    "toddler",
    "toddlerPreschool",
    "preschool",
    "preschoolSA",
    "kindergarten",
    "schoolAge",
    "multiAge",
)

# Same nine groups as they appear on a search summary's accRecord (present on
# only ~20% of rows, plan Sec 6.2); no multiAge equivalent exists there.
SEARCH_AGE_FIELD_MAP = {
    "infantBirth": "Infant_birth_15_mo__c",
    "infantToddler": "Infant_Toddler_birth_33_mo__c",
    "toddler": "Toddler_15_mo_33_mo__c",
    "toddlerPreschool": "Toddler_Preschool_15_mo_K__c",
    "preschool": "Preschool_33_mo_K__c",
    "preschoolSA": "Preschool_SA_33_mo_8_yr__c",
    "kindergarten": "Kindergarten__c",
    "schoolAge": "School_Age_5_yr_14_yr__c",
}

# The four common age-flag labels, in ages_served display order (plan Sec 6.2).
_AGE_FLAG_LABELS = (("infant", "Infant"), ("toddler", "Toddler"), ("preschool", "Preschool"), ("school", "School Age"))


def _aura_body(message, context, page_uri):
    """URL-encode the four Aura form fields for one action envelope."""
    return urlencode(
        {
            "message": json.dumps(message),
            "aura.context": json.dumps(context),
            "aura.pageURI": page_uri,
            "aura.token": "null",
        }
    )


def _headers(endpoint_header):
    return {**BASE_HEADERS, "X-SFDC-LDS-Endpoints": endpoint_header}


def _put(item, key, value):
    """Set ``item[key] = value``, skipping blank strings/empty lists/None.

    Mirrors kentucky.py's per-item ``put`` closure, but as a free function so
    it can be shared between summary-building and detail-enrichment.
    """
    if isinstance(value, str):
        value = value.strip() or None
    elif isinstance(value, list):
        value = value or None
    if value is not None:
        item[key] = value


def _put_if_absent(item, key, value):
    """Like ``_put``, but only when ``item`` doesn't already carry a value
    for ``key`` -- used for the "search-preferred, detail-fallback" fields
    (plan Sec 6.1), so a detail enrichment never clobbers a better search
    value with a blanker one."""
    if not item.get(key):
        _put(item, key, value)


def split_semicolon(value):
    """Split a MA ``";"``-joined field (meals, transportation, environment,
    ...) into a cleaned list, or None. Empty/whitespace-only tokens are
    dropped; an all-empty result is None rather than ``[]``."""
    if not value:
        return None
    parts = [p.strip() for p in value.split(";") if p.strip()]
    return parts or None


def _int_or_zero(value):
    """Coerce a per-age count (int or numeric string) to int; anything else
    (None, "", non-numeric) is treated as 0 -- absence and zero are
    indistinguishable per-field, which is why the caller checks the whole
    group for "any count present" rather than trusting a single field."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return 0


def age_flags_from_counts(counts):
    """Derive ``(ages_served, flags, capacity_by_age)`` from the nine
    per-age capacity counts (plan Sec 6.2).

    ``counts`` is a dict keyed by ``AGE_GROUP_KEYS`` (some values may be
    missing/None -- treated as 0). When every count is 0 (no per-age data
    published at all, e.g. an informal-care record), all three return values
    are empty/None -- the caller must leave every age field unset rather than
    emitting a wall of Falses for data that was never published.

    Otherwise the four common booleans are set True/False based on which
    groups contributed a positive count (the group boundaries overlap
    deliberately, e.g. ``preschoolSA`` counts toward both preschool and
    school), ``ages_served`` joins the True labels, and ``capacity_by_age``
    keeps every non-zero raw group for full traceability.
    """
    values = {key: _int_or_zero(counts.get(key)) for key in AGE_GROUP_KEYS}
    if not any(values.values()):
        return None, {}, None

    flags = {
        "infant": bool(values["infantBirth"] or values["infantToddler"]),
        "toddler": bool(values["infantToddler"] or values["toddler"] or values["toddlerPreschool"]),
        "preschool": bool(values["toddlerPreschool"] or values["preschool"] or values["preschoolSA"]),
        "school": bool(values["preschoolSA"] or values["schoolAge"] or values["kindergarten"]),
    }
    labels = [label for field, label in _AGE_FLAG_LABELS if flags[field]]
    ages_served = ", ".join(labels) or None
    capacity_by_age = {key: count for key, count in values.items() if count > 0} or None
    return ages_served, flags, capacity_by_age


def _search_age_counts(acc):
    """Map a search accRecord's ~20%-populated age fields onto the
    ``AGE_GROUP_KEYS`` shape (plan Sec 6.2). No search-side ``multiAge``."""
    return {key: acc.get(field) for key, field in SEARCH_AGE_FIELD_MAP.items()}


def format_hours(schedule_list):
    """Collapse the preferred schedule's ``daysOfServiceList`` into a
    compact string, or None (plan Sec 6.5).

    Prefers the "Full Year Schedule" entry (falling back to the first
    schedule that has any days); collapses a uniform week to
    "Monday-Friday 06:00 AM - 11:30 PM", else lists each day. Every schedule
    (all types, with drop-in/extended-day flags) is separately preserved in
    full on ``ma_schedules``.
    """
    if not schedule_list:
        return None
    preferred = None
    for sched in schedule_list:
        if sched.get("scheduleType") == "Full Year Schedule" and sched.get("daysOfServiceList"):
            preferred = sched
            break
    if preferred is None:
        for sched in schedule_list:
            if sched.get("daysOfServiceList"):
                preferred = sched
                break
    if preferred is None:
        return None

    rows = [
        (day.get("dayOfTheWeek"), day.get("startTime"), day.get("endTime"))
        for day in preferred["daysOfServiceList"]
        if day.get("dayOfTheWeek") and day.get("startTime") and day.get("endTime")
    ]
    if not rows:
        return None
    windows = {(start, end) for _, start, end in rows}
    if len(windows) == 1 and len(rows) > 1:
        return f"{rows[0][0]}-{rows[-1][0]} {rows[0][1]} - {rows[0][2]}"
    return "; ".join(f"{day} {start} - {end}" for day, start, end in rows)


_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_MDY_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")


def _normalize_last_issue_date(value):
    """Normalize ``ma_last_issue_date`` to ISO ``YYYY-MM-DD``.

    Unlike the common ``license_begin_date``, this state-specific field is
    not routed through the shared normalization pipeline's ``DATE_FIELDS``
    (that list is common-fields-only), so the spider normalizes it itself --
    the Maine ``_normalize_date`` precedent. The search summary's
    ``Last_Issue_Date__c`` is already ISO; the detail call's ``lastIssueDate``
    is ``M/D/YYYY`` (not zero-padded, plan Sec 6.4). An unparseable value
    passes through unchanged.
    """
    if not value:
        return value
    if _ISO_DATE_RE.fullmatch(value):
        return value
    match = _MDY_DATE_RE.fullmatch(value)
    if match:
        month, day, year = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return value


def _date_sort_key(value):
    """Parse a visit/investigation date (ISO ``YYYY-MM-DD`` or ``M/D/YYYY``)
    into a sortable ``(year, month, day)`` tuple; unparseable/blank values
    sort first (oldest), via ``(0, 0, 0)``.

    Visits (``dateOfVisit``) and investigations (``createdDate``) arrive in
    two different raw date formats on the wire, so a plain string sort would
    interleave the two lists incorrectly -- this normalizes both to a
    comparable tuple without needing the normalization pipeline's full date
    parser (which only runs after the spider yields the item).
    """
    if not value:
        return (0, 0, 0)
    try:
        if "-" in value:
            year, month, day = value.split("-")
        elif "/" in value:
            month, day, year = value.split("/")
        else:
            return (0, 0, 0)
        return (int(year), int(month), int(day))
    except ValueError:
        return (0, 0, 0)


class MassachusettsSpider(scrapy.Spider):
    name = "massachusetts"
    allowed_domains = ["childcare.mass.gov"]

    custom_settings = {
        # Recon (plan Sec 2.8/3.3) drew no throttling across ~80 requests at
        # this concurrency; this is a state public-records portal, so stay
        # polite via a small delay rather than pushing harder just because
        # nothing broke yet. Exposed as `-a concurrency=N` (see from_crawler)
        # -- if a run starts drawing errors, lower this before anything else.
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 60,
        "RETRY_TIMES": 3,
        # robots.txt disallows /aura; this is a public records dataset behind
        # a state search UI (Kentucky/Indiana precedent, plan Sec 5.5).
        "ROBOTSTXT_OBEY": False,
        "DEFAULT_REQUEST_HEADERS": {},
    }

    def __init__(self, zips=None, details=1, concurrency=4, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zip_list = self._parse_zips(zips)
        self.do_details = str(details).strip().lower() not in ("0", "false")
        self.concurrency = int(concurrency)

        self.seen = set()  # accountIds already emitted/scheduled
        self.zips_done = 0
        self.zips_with_hits = 0
        self.zips_failed = set()  # ZIPs that never resolved (Aura ERROR loop)
        self.detail_failures = 0
        self.error_state_count = 0
        # Raw vocab observed live, checked against the normalization tables
        # in closed() so a new value gets flagged instead of silently falling
        # through to "unknown"/"other" (plan Sec 10 acceptance criteria).
        self.status_values_seen = set()
        self.provider_type_values_seen = set()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Apply `-a concurrency=N` to the *crawler's* settings.

        Mutating ``self.custom_settings`` from ``__init__`` does NOT work:
        ``Crawler.__init__`` calls ``spidercls.update_settings()`` on the
        class, long before any instance exists, so an instance-level dict is
        never read and the argument silently does nothing (see the
        ``project_custom_settings_instance_noop`` memory / kentucky.py's
        identical ``from_crawler``).
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        for key in ("CONCURRENT_REQUESTS", "CONCURRENT_REQUESTS_PER_DOMAIN"):
            crawler.settings.set(key, spider.concurrency, priority="spider")
        return spider

    @staticmethod
    def _parse_zips(zips):
        """Parse the `-a zips=` argument: comma-separated ZIPs and/or
        hyphenated ranges (`02301,01844`, `02100-02199`). Defaults to the
        full 01001-02799 statewide sweep (plan Sec 3.1)."""
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
        wrapper_data = json.dumps({"selectedZipCode": f"{zip5:05d}"})
        message = {
            "actions": [
                {
                    "id": "5;a",
                    "descriptor": "aura://ApexActionController/ACTION$execute",
                    "callingDescriptor": "UNKNOWN",
                    "params": {
                        "namespace": "",
                        "classname": "EEC_ChildCareSearchController",
                        "method": "callApex",
                        "params": {"wrapperData": wrapper_data, "tabName": "providerCityZip"},
                        "cacheable": False,
                        "isContinuation": False,
                    },
                }
            ]
        }
        return scrapy.Request(
            SEARCH_URL,
            method="POST",
            body=_aura_body(message, SEARCH_CONTEXT, SEARCH_PAGE_URI),
            headers=_headers(SEARCH_HEADER),
            callback=self.parse_search,
            errback=self.search_errback,
            meta={"zip": zip5, "attempt": attempt},
            dont_filter=True,
        )

    def start_requests(self):
        self.logger.info(
            "Massachusetts: sweeping %d ZIP(s) (details=%s, concurrency=%d)",
            len(self.zip_list),
            self.do_details,
            self.concurrency,
        )
        for zip5 in self.zip_list:
            yield self._search_request(zip5)

    def search_errback(self, failure):
        """A ZIP that exhausted RETRY_TIMES without a usable response."""
        zip5 = failure.request.meta.get("zip")
        self.zips_done += 1
        self.zips_failed.add(zip5)
        self.logger.warning(
            "Massachusetts: ZIP %05d search failed after all retries (%s) -- a pocket of providers may be missing",
            zip5,
            failure.value,
        )
        self._maybe_log_progress()

    def parse_search(self, response):
        zip5 = response.meta["zip"]
        data = response.json()
        # HTTP 200 does not mean the action succeeded -- Aura's real status
        # is actions[0].state (plan Sec 5.1).
        action = (data.get("actions") or [{}])[0]
        if action.get("state") != "SUCCESS":
            self.error_state_count += 1
            attempt = response.meta.get("attempt", 1)
            if attempt <= MAX_ERROR_RETRIES:
                self.logger.warning(
                    "Massachusetts: ZIP %05d search returned Aura state=%r (attempt %d/%d) -- re-issuing",
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
                "Massachusetts: ZIP %05d search gave up after %d Aura ERROR "
                "responses in a row -- if this count is high across the run, "
                "`fwuid`/`loaded` may have gone stale (plan Sec 5.6)",
                zip5,
                attempt,
            )
            self._maybe_log_progress()
            return

        self.zips_done += 1
        # actions[0].returnValue.returnValue is already a dict here (unlike
        # Kentucky's search, no second json.loads -- plan Sec 2.3). A ZIP
        # with no providers returns SUCCESS with an EMPTY inner returnValue
        # ({}) -- accountData is absent, not [] (plan Sec 2.4/5.3). This is
        # the normal answer for most of the 1,799 swept ZIPs, so it is the
        # quiet path: no warning, no items.
        payload = action.get("returnValue") or {}
        rv = payload.get("returnValue") or {}
        records = rv.get("accountData") or []
        if records:
            self.zips_with_hits += 1

        for record in records:
            account_id = record.get("accountId")
            if not account_id or account_id in self.seen:
                continue
            self.seen.add(account_id)
            item = self._item_from_summary(record)
            encrypted_id = (record.get("accRecord") or {}).get("Encrypted_Id__c")
            if self.do_details and encrypted_id:
                yield self._detail_request(encrypted_id, item)
            else:
                yield item

        self._maybe_log_progress()

    def _maybe_log_progress(self):
        if self.zips_done % 100 == 0:
            self.logger.info(
                "Massachusetts: %d/%d ZIPs done, %d with hits, %d unique providers so far",
                self.zips_done,
                len(self.zip_list),
                self.zips_with_hits,
                len(self.seen),
            )

    def _item_from_summary(self, record):
        """Build a ProviderItem from one search accountData record. Every
        common field except status/hours/inspections comes from here (plan
        Sec 6.1); the detail fan-out enriches/overrides in parse_detail."""
        acc = record.get("accRecord") or {}
        item = ProviderItem()
        item["source_state"] = "Massachusetts"
        item["provider_url"] = BASE_URL + (record.get("redirectLink") or "")

        account_id = record.get("accountId")
        if account_id:
            item["ma_account_id"] = account_id
        encrypted_id = acc.get("Encrypted_Id__c")
        if encrypted_id:
            item["ma_encrypted_id"] = encrypted_id

        _put(item, "provider_name", record.get("accountName"))
        _put(item, "license_number", acc.get("Current_License_Number__c"))
        provider_type = record.get("recordTypeName")
        _put(item, "provider_type", provider_type)
        if provider_type:
            self.provider_type_values_seen.add(provider_type)

        # Line 2 (rare) appended after ", " -- Kentucky/most-states precedent.
        address1 = acc.get("Provider_Address_1__c")
        address2 = acc.get("Provider_Address_2__c")
        address = f"{address1}, {address2}" if address1 and address2 else address1
        _put(item, "address", address)
        _put(item, "city", (acc.get("Provider_City__r") or {}).get("Name"))
        _put(item, "state", acc.get("State__c"))
        zip_code = acc.get("Provider_Zip_Code__c")
        if zip_code:
            # Provider_Zip_Code__c is ZIP+4 ("02301-5035"); keep the 5-digit
            # prefix (plan Sec 6.1).
            item["zip"] = zip_code[:5]

        # Coordinates are published full-precision strings on 100% of
        # records (plan Sec 1) -- no geocoding step needed for this source.
        _put(item, "latitude", acc.get("BillingLatitude"))
        _put(item, "longitude", acc.get("BillingLongitude"))
        _put(item, "phone", record.get("phoneNumber") or acc.get("Phone"))
        _put(item, "email", acc.get("Provider_Email__c"))
        _put(item, "capacity", acc.get("Capacity__c"))
        # D-3: license_begin_date is the FIRST issuance; the most-recent
        # issuance is kept separately on ma_last_issue_date.
        _put(item, "license_begin_date", acc.get("First_Issue_Date__c"))

        _put(item, "ma_last_issue_date", _normalize_last_issue_date(acc.get("Last_Issue_Date__c")))
        _put(item, "ma_temporary_status", acc.get("Temporary_Status__c"))
        _put(item, "ma_regional_office_address", acc.get("Region_Address__c"))
        _put(item, "ma_ccrr_name", acc.get("Associated_CCRR__c"))
        city_record = acc.get("Provider_City__r") or {}
        ccrr_record = city_record.get("CCRR__r") or {}
        _put(item, "ma_ccrr_phone", ccrr_record.get("CCRR_Phone__c"))

        redact = acc.get("Redact_Contact_Info_On_Public_Portal__c")
        if isinstance(redact, bool):
            item["ma_contact_redacted"] = redact

        is_informal = record.get("isInformalChildCare")
        if isinstance(is_informal, bool):
            item["ma_is_informal"] = is_informal

        ages_served, flags, capacity_by_age = age_flags_from_counts(_search_age_counts(acc))
        self._apply_age_data(item, ages_served, flags, capacity_by_age)

        return item

    @staticmethod
    def _apply_age_data(item, ages_served, flags, capacity_by_age):
        """Set ages_served/age-flags/ma_capacity_by_age on ``item`` when
        ``flags`` carries real data (i.e. at least one per-age count was
        published and non-zero) -- a no-op otherwise, so a caller can freely
        try the search-derived counts and then the detail-derived counts
        without ever stamping Falses/None over real data."""
        if not flags:
            return
        if ages_served is not None:
            item["ages_served"] = ages_served
        for field, value in flags.items():
            item[field] = value
        if capacity_by_age is not None:
            item["ma_capacity_by_age"] = capacity_by_age

    # --- detail (per provider) ------------------------------------------ #

    def _detail_request(self, encrypted_id, item):
        message = {
            "actions": [
                {
                    "id": "5;a",
                    "descriptor": "aura://ApexActionController/ACTION$execute",
                    "callingDescriptor": "UNKNOWN",
                    "params": {
                        "namespace": "",
                        "classname": "EEC_ProviderDetailsController",
                        "method": "getProviderDetails",
                        # One key -- no licence number needed (contrast
                        # Kentucky's detail call).
                        "params": {"providerId": encrypted_id},
                        "cacheable": False,
                        "isContinuation": False,
                    },
                }
            ]
        }
        page_uri = f"{DETAIL_PAGE_URI_PREFIX}{encrypted_id}"
        return scrapy.Request(
            DETAIL_URL,
            method="POST",
            body=_aura_body(message, DETAIL_CONTEXT, page_uri),
            headers=_headers(DETAIL_HEADER),
            callback=self.parse_detail,
            errback=self.detail_errback,
            meta={"item": item, "encrypted_id": encrypted_id},
            dont_filter=True,
        )

    def detail_errback(self, failure):
        """If a detail call fails after all retries, still emit the item from
        the summary alone -- losing a whole provider over one optional call
        is the worst available outcome (plan Sec 3.2)."""
        item = failure.request.meta.get("item")
        if item is not None:
            self.detail_failures += 1
            self.logger.warning(
                "Massachusetts: detail request failed for encrypted id %s (%s); emitting summary-only item",
                failure.request.meta.get("encrypted_id"),
                failure.value,
            )
            yield item

    def parse_detail(self, response):
        item = response.meta["item"]
        encrypted_id = response.meta.get("encrypted_id")
        data = response.json()
        action = (data.get("actions") or [{}])[0]
        if action.get("state") != "SUCCESS":
            # An unknown/blank providerId returns state: ERROR (plan Sec 5.2)
            # -- fall back to the summary-only item, never drop the provider.
            self.error_state_count += 1
            self.detail_failures += 1
            self.logger.warning(
                "Massachusetts: detail for %s returned Aura state=%r; emitting summary-only item",
                encrypted_id,
                action.get("state"),
            )
            yield item
            return

        det = (action.get("returnValue") or {}).get("returnValue") or {}
        if not det:
            self.detail_failures += 1
            self.logger.warning(
                "Massachusetts: detail for %s returned an empty payload; emitting summary-only item",
                encrypted_id,
            )
            yield item
            return

        # provider_name: detail's `name` takes priority over the search
        # summary's `accountName` (plan Sec 6.1).
        _put(item, "provider_name", det.get("name"))

        status_raw = det.get("providerStatus")
        _put(item, "status", status_raw)
        if status_raw:
            self.status_values_seen.add(status_raw)

        # Search-preferred fields: only fill from the detail call when the
        # search summary didn't already carry a value (mainly informal
        # records, which omit these on the search side).
        _put_if_absent(item, "address", det.get("providerAddress"))
        _put_if_absent(item, "phone", det.get("providerPhone"))
        _put_if_absent(item, "email", det.get("providerEmail"))
        _put_if_absent(item, "capacity", det.get("capacity"))
        _put_if_absent(item, "license_begin_date", det.get("firstIssueDate"))

        # Detail-preferred fields: overwrite the search-summary placeholder
        # (set in _item_from_summary) whenever the detail call carries a
        # value.
        _put(item, "ma_last_issue_date", _normalize_last_issue_date(det.get("lastIssueDate")))
        _put(item, "ma_regional_office_address", det.get("eecRegionalOfficeAddress"))
        ccrr = det.get("ccrrRecord") or {}
        _put(item, "ma_ccrr_name", ccrr.get("name"))
        _put(item, "ma_ccrr_phone", ccrr.get("ccrrPhone"))
        _put(item, "ma_ccrr_website", ccrr.get("ccrrWebsite"))
        _put(item, "ma_ccrr_city", ccrr.get("ccrrCity"))

        # D-4: informal/exempt records have no public licence number -- fall
        # back to the P-###### program number so license_number is never
        # empty. The program number is always kept on ma_program_number too.
        program_number = det.get("providerNumber")
        _put(item, "ma_program_number", program_number)
        if not item.get("license_number"):
            _put(item, "license_number", program_number)

        # Detail-only fields (D-5 provider-reported extras).
        _put(item, "accreditation", det.get("providerAccreditation"))
        _put(item, "meals", split_semicolon(det.get("meals")))
        _put(item, "transportation", split_semicolon(det.get("transportation")))
        _put(item, "languages", split_semicolon(det.get("languageSpoken")))
        _put(item, "administrator", det.get("contactName"))
        _put(item, "ma_licensor", det.get("eecLicensor"))
        _put(item, "ma_regional_website", det.get("regionalWebsite"))
        _put(item, "ma_umbrella_name", det.get("umbrellaName"))
        _put(item, "ma_availability", det.get("availability"))
        _put(item, "ma_schedule_options", split_semicolon(det.get("availableScheduleOptions")))
        _put(item, "ma_environment", split_semicolon(det.get("environment")))
        _put(item, "ma_financial_assistance", split_semicolon(det.get("typesOfFinancialAssistance")))
        _put(item, "ma_special_needs", det.get("specialNeeds"))
        _put(item, "ma_special_skills", det.get("specialSkills"))

        if "isGSA" in det:
            item["ma_is_gsa"] = bool(det["isGSA"])
        if "isInformalChildCare" in det:
            item["ma_is_informal"] = bool(det["isInformalChildCare"])
        summer_camp = det.get("isUnderDphSummerCamp")
        if summer_camp in ("Yes", "No"):
            item["ma_dph_summer_camp"] = summer_camp == "Yes"

        # Presence-gated booleans: only set when the source field carries
        # data at all, so "no info published" doesn't collapse into a false
        # "no" (Delaware precedent).
        financial_raw = det.get("typesOfFinancialAssistance")
        if financial_raw:
            item["scholarships_accepted"] = "EEC Subsidies" in financial_raw
        availability = det.get("availability")
        if availability:
            item["accepting_new_children"] = availability.strip().startswith("Slots available")

        schedule_list = det.get("scheduleWrapperList")
        hours = format_hours(schedule_list)
        if hours:
            item["hours"] = hours
        schedules = self._parse_schedules(schedule_list)
        if schedules:
            item["ma_schedules"] = schedules
        cost_table = self._parse_cost_table(schedule_list)
        if cost_table:
            item["ma_cost_table"] = cost_table

        detail_counts = {key: det.get(key) for key in AGE_GROUP_KEYS}
        ages_served, flags, capacity_by_age = age_flags_from_counts(detail_counts)
        self._apply_age_data(item, ages_served, flags, capacity_by_age)

        inspections = self._parse_inspections(det)
        if inspections:
            item["inspections"] = inspections

        yield item

    @staticmethod
    def _parse_schedules(schedule_list):
        """Every schedule (all types, with drop-in/extended-day flags and the
        full day-by-day times), independent of the collapsed `hours` string
        (plan Sec 6.4)."""
        out = []
        for sched in schedule_list or []:
            days = [
                {"day": day.get("dayOfTheWeek"), "start": day.get("startTime"), "end": day.get("endTime")}
                for day in sched.get("daysOfServiceList") or []
            ]
            out.append(
                {
                    "schedule_type": sched.get("scheduleType"),
                    "drop_in": sched.get("dropInCareAvailable"),
                    "extended_day": sched.get("extendedDayOptionAvailable"),
                    "days": days,
                }
            )
        return out

    @staticmethod
    def _parse_cost_table(schedule_list):
        """Only the populated (non-blank amount) per-age fee rows across
        every schedule's ageGroupList (plan Sec 6.4) -- most rateType rows
        are blank placeholders (10 rate types x 5 age groups per schedule),
        so this is a small, high-signal subset of a much larger sparse grid.
        """
        costs = []
        for sched in schedule_list or []:
            schedule_type = sched.get("scheduleType")
            age_group_list = sched.get("ageGroupList") or {}
            for key, fees in age_group_list.items():
                if not key.endswith("FeeList") or not isinstance(fees, list):
                    continue
                for fee in fees:
                    amount = fee.get("amount")
                    if amount:
                        costs.append(
                            {
                                "schedule_type": schedule_type,
                                "age_group": fee.get("ageGroup"),
                                "rate_type": fee.get("rateType"),
                                "amount": amount,
                            }
                        )
        return costs

    @staticmethod
    def _parse_visit(visit):
        """One monitoring visit -> one InspectionItem, keeping EVERY
        visitDomainList row (D-1), not just the non-compliant subset."""
        entry = InspectionItem()
        if visit.get("dateOfVisit"):
            entry["date"] = visit["dateOfVisit"]
        if visit.get("typeOfVisit"):
            entry["type"] = visit["typeOfVisit"]
        if visit.get("closedDate"):
            entry["status_updated"] = visit["closedDate"]
        if visit.get("id"):
            entry["ma_visit_id"] = visit["id"]
        if visit.get("announcementType"):
            entry["ma_announcement_type"] = visit["announcementType"]
        if visit.get("isPreLicensing"):
            # Misnamed by the source -- actually the visit reason.
            entry["ma_visit_reason"] = visit["isPreLicensing"]
        if visit.get("levelOfCompliance"):
            entry["ma_level_of_compliance"] = visit["levelOfCompliance"]
        if visit.get("licensorAssigned"):
            entry["ma_licensor"] = visit["licensorAssigned"]

        domains = []
        for domain in visit.get("visitDomainList") or []:
            regulations = [
                {"name": reg.get("Name"), "article_text": reg.get("ArticleText")}
                for reg in domain.get("regulations") or []
            ]
            domains.append(
                {
                    "domain": domain.get("domainName"),
                    "indicator": domain.get("indicator"),
                    "description": domain.get("description"),
                    "level_of_compliance": domain.get("levelOfCompliance"),
                    "is_key_indicator": domain.get("isKeyIndicator"),
                    "regulation_name": domain.get("regulationName"),
                    "regulations": regulations,
                }
            )
        if domains:
            entry["ma_domains"] = domains
        return entry

    @staticmethod
    def _parse_investigation(investigation):
        """One investigation/complaint -> one InspectionItem, discriminated
        by the literal `type` "Investigation" (D-2 keeps the redacted
        violation narratives)."""
        entry = InspectionItem()
        if investigation.get("createdDate"):
            entry["date"] = investigation["createdDate"]
        entry["type"] = "Investigation"
        if investigation.get("investigationOutcome"):
            entry["original_status"] = investigation["investigationOutcome"]
        if investigation.get("id"):
            entry["ma_investigation_id"] = investigation["id"]
        if investigation.get("investigatorAssigned"):
            entry["ma_investigator"] = investigation["investigatorAssigned"]
        noncompliance = investigation.get("nonComplianceIdentified")
        if noncompliance in ("Yes", "No"):
            entry["ma_noncompliance_identified"] = noncompliance == "Yes"

        violations = [
            {
                "regulation": row.get("regulations"),
                "result": row.get("result"),
                "statement": row.get("statementOfNonCompliant"),
                "corrective_action_plan": row.get("correctiveActionPlan"),
            }
            for row in investigation.get("nonCompliants") or []
        ]
        if violations:
            entry["ma_violations"] = violations
        return entry

    def _parse_inspections(self, det):
        """Join monitoring visits and investigations into one newest-first
        list (plan Sec 6.3)."""
        out = [self._parse_visit(visit) for visit in det.get("visitList") or []]
        out.extend(self._parse_investigation(inv) for inv in det.get("investigationList") or [])
        out.sort(key=lambda entry: _date_sort_key(entry.get("date")), reverse=True)
        return out

    def closed(self, reason):
        self.logger.info(
            "Massachusetts: finished (%s) -- %d/%d ZIPs done (%d failed), %d "
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
                "Massachusetts: only %d providers found (< %d baseline) -- possible incomplete crawl",
                len(self.seen),
                EXPECTED_MIN_PROVIDERS,
            )
        if self.zips_failed:
            self.logger.warning(
                "Massachusetts: %d ZIP(s) never resolved: %s -- these pockets of providers are missing from this run",
                len(self.zips_failed),
                sorted(self.zips_failed),
            )
        # Diagnostic per plan Sec 5.6: a high ERROR-state count points at
        # `fwuid`/`loaded` having gone stale on a Salesforce org upgrade.
        if self.error_state_count > 20:
            self.logger.warning(
                "Massachusetts: %d requests returned an Aura ERROR state -- "
                "if unexpectedly high, re-capture `fwuid` and both `loaded` "
                "values from a live browser DevTools session (plan Sec 5.6).",
                self.error_state_count,
            )
        unmapped_status = sorted(
            value for value in self.status_values_seen if value.strip().lower() not in norm.STATUS_MAP
        )
        if unmapped_status:
            self.logger.warning(
                "Massachusetts: unmapped providerStatus value(s) seen -- add to STATUS_BUCKETS: %s",
                unmapped_status,
            )
        unmapped_types = sorted(
            value for value in self.provider_type_values_seen if value.strip().lower() not in norm.FACILITY_CATEGORY_MAP
        )
        if unmapped_types:
            self.logger.warning(
                "Massachusetts: unmapped provider_type value(s) seen -- add to FACILITY_CATEGORY_BUCKETS: %s",
                unmapped_types,
            )
