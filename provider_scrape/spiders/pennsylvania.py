"""Pennsylvania child care provider spider.

Source: the PA COMPASS provider search at
https://www.compass.dhs.pa.gov/providersearch (an Angular SPA) -- but this
spider talks directly to the SPA's public JSON API instead of driving the
browser.

API flow, per county, for the Child Care Provider (CCP) program:

  1. POST ``childcaresummary/getchildcaresummary`` with the full advanced-search
     criteria (county + program + every care level + every star rating). This
     registers the search server-side against a ``browser-session-id`` header
     (a per-county UUID -- NOT a cookie). Returns ``{"isSuccessful": true}``.
  2. GET ``childcareresults/getchildcareresults`` and POLL: the search runs
     asynchronously, returning ``searchStatusServedByCallBack: false`` until it
     is ready, then a ``provider`` list.
  3. PAGINATE: each provider row carries a server-assigned ``searchIdentifier``.
     Re-POST the criteria with that id and an incrementing ``groupIdentifier``
     (2, 3, 4, ...); the results GET returns a *cumulative* set. Stop when no new
     (providerIdentifier, providerLocationIdentifier) rows appear.
  4. For each unique provider location, POST
     ``childcareinformation/getchildcareinfo`` for the fully-structured detail
     record and build the item from it (no HTML).

The three "No results" counties (27/57/66) simply return an empty served list and
finish; nothing hangs.
"""
import json
import uuid

import scrapy

from provider_scrape.items import ProviderItem

API_BASE = "https://www.compass.dhs.pa.gov/api/providersearch/v1"
SUMMARY_URL = f"{API_BASE}/childcaresummary/getchildcaresummary"
RESULTS_URL = f"{API_BASE}/childcareresults/getchildcareresults"
INFO_URL = f"{API_BASE}/childcareinformation/getchildcareinfo"

# Child Care Provider only -- yields the Center/Family/Group license types that
# make up the existing web-app dataset. The site also exposes Head Start (HDS),
# Pre-K Counts (PKC), etc.; those are intentionally excluded.
PROGRAMS = ["CCP"]

# The advanced-search form's full care-level and star-rating vocabularies. We
# select them all so the search is unfiltered on those facets (every provider).
ALL_CARE_LEVELS = [
    "UTO", "TOT", "ONE", "TWO", "THREE", "FOU", "FIV", "SIX", "SEV", "EIG",
    "NIN", "TEN", "ELE", "TWE", "THI", "FRT", "FTN",
]
ALL_STAR_RATINGS = ["0", "1", "2", "3", "4"]

# careLevelOpeningStatus code -> label (reference table R00132 / getEnrollingStatus).
OPENINGS_MAP = {
    "C": "Call for Availability",
    "E": "Enrolling",
    "N": "Not Enrolling",
    "X": "Not Operating due to a State of Emergency",
}

# PA has 67 counties, keyed "01".."67" in the search form.
COUNTIES = [f"{i:02d}" for i in range(1, 68)]

MAX_POLLS = 15       # results GET polls before giving up on a search
MAX_GROUPS = 100     # pagination safety cap (Philadelphia needs ~12)


def format_phone(value):
    """Format a 10-digit phone as ``(XXX) XXX-XXXX``; pass anything else through."""
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return str(value).strip() or None


def join_labels(value):
    """Join a list of label strings with ``, ``; return "" for an empty/None list.

    Empty string (not None) matches the previous spider, which joined UI list
    items and produced "" when a section was absent.
    """
    if isinstance(value, list):
        return ", ".join(str(v).strip() for v in value if v is not None and str(v).strip())
    return ""


class PennsylvaniaSpider(scrapy.Spider):
    name = "pennsylvania"
    allowed_domains = ["compass.dhs.pa.gov"]

    custom_settings = {
        # Plain JSON requests -- no Playwright meta, so scrapy-playwright's
        # download handler transparently delegates to the default handler.
        "DOWNLOAD_DELAY": 0.1,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 60,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, *args, counties=None, **kwargs):
        super().__init__(*args, **kwargs)
        # Global dedupe of provider locations scheduled for detail fetch, keyed
        # by (providerIdentifier, providerLocationIdentifier).
        self.seen = set()
        # Optional ``-a counties=01,60`` to restrict the run (targeted re-runs
        # and smoke tests); defaults to all 67 counties.
        if counties:
            self.counties = [c.strip().zfill(2) for c in counties.split(",") if c.strip()]
        else:
            self.counties = COUNTIES

    # --- request builders ------------------------------------------------- #

    def _headers(self, session_id, origin_page):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "browser-session-id": session_id,
            "api-origin-page": origin_page,
            "Referer": "https://www.compass.dhs.pa.gov/providersearch/",
        }

    def _summary_body(self, county, group_id, search_id):
        # sortOrder is "" for the first page and "ASC" when paginating (mirrors
        # the SPA); the server ties pages together via searchIdentifier.
        sort_order = "ASC" if group_id > 1 else ""
        return {
            "starRating": ALL_STAR_RATINGS,
            "street": "", "city": "", "zipCode": [],
            "careLevel": ALL_CARE_LEVELS,
            "program": PROGRAMS,
            "county": [county],
            "municipality": [], "schoolDistrictSearch": [], "serviceSchedule": [],
            "mealOptions": [], "providerType": [], "environment": [],
            "additionalActivities": [], "unitsOfCare": [], "language": [],
            "languageUsage": [], "searchDistance": 0, "additionalCharges": [],
            "accreditation": [], "days": [], "publicTransportation": [],
            "providerName": "", "publicSchool": [], "openTime": "", "closeTime": "",
            "schooldistrictchildcare": "", "transportationDistrict": "",
            "transportationCounty": "", "FinancialProgramParticipation": [],
            "absoluteAddress": "", "fromHours": "", "fromMinutes": "",
            "toHours": "", "toMinutes": "", "callerApplication": "",
            "callerLanguage": "English", "callerSearchIdentifier": "",
            "otherEarlyLearningPrograms": [], "emergencyOperationStatus": [],
            "groupIdentifier": group_id, "paymentOption": [],
            "searchIdentifier": search_id, "searchType": "AdvancedSearch",
            "specialAccommodations": [], "homeVisitingPrograms": [],
            "geographicCriteriaType": "CountySearch", "sortBy": "",
            "sortOrder": sort_order, "searchResultsDeleteType": 0,
            "enrollmentStatus": [],
        }

    def _summary_request(self, meta):
        """POST the search criteria for ``meta['county']`` / ``meta['group']``."""
        body = self._summary_body(meta["county"], meta["group"], meta["search_id"])
        origin = (
            "providersearch/advancedsearch"
            if meta["group"] == 1
            else "providersearch/searchresults"
        )
        return scrapy.Request(
            SUMMARY_URL,
            method="POST",
            body=json.dumps(body),
            headers=self._headers(meta["sid"], origin),
            callback=self.after_summary,
            errback=self.on_error,
            dont_filter=True,
            meta=meta,
        )

    def _results_request(self, meta):
        """GET (poll) the results for the search registered under this session."""
        return scrapy.Request(
            RESULTS_URL,
            method="GET",
            headers=self._headers(meta["sid"], "providersearch/searchresults"),
            callback=self.parse_results,
            errback=self.on_error,
            dont_filter=True,
            meta=meta,
        )

    def _info_request(self, provider):
        body = {
            "callerApplication": "",
            "program": provider.get("program") or "CCP",
            "providerIdentifier": provider.get("providerIdentifier"),
            "providerLocationIdentifier": provider.get("providerLocationIdentifier"),
            "schoolDistrictServed": "",
            "callerLanguage": "English",
        }
        # The detail endpoint is stateless (keyed by provider id, not session),
        # so a throwaway session id is fine and lets these fan out concurrently.
        return scrapy.Request(
            INFO_URL,
            method="POST",
            body=json.dumps(body),
            headers=self._headers(str(uuid.uuid4()), "providersearch/childcaresummary"),
            callback=self.parse_detail,
            errback=self.on_error,
            dont_filter=True,
            meta={"provider": provider},
        )

    # --- crawl flow ------------------------------------------------------- #

    def start_requests(self):
        for county in self.counties:
            meta = {
                "county": county,
                "sid": str(uuid.uuid4()),
                "group": 1,
                "search_id": 0,
                "accum": {},       # (providerId, locationId) -> provider row
                "prev_count": -1,  # deduped size before this batch
                "poll": 0,
            }
            yield self._summary_request(meta)

    def after_summary(self, response):
        # getchildcaresummary just registers the search; kick off result polling.
        meta = dict(response.meta)
        meta["poll"] = 0
        yield self._results_request(meta)

    def parse_results(self, response):
        meta = response.meta
        try:
            data = json.loads(response.text)
        except ValueError:
            data = {}

        if not data.get("searchStatusServedByCallBack"):
            if meta["poll"] < MAX_POLLS:
                nxt = dict(meta)
                nxt["poll"] = meta["poll"] + 1
                yield self._results_request(nxt)
            else:
                self.logger.warning(
                    "County %s group %s: results not served after %d polls; "
                    "finishing with %d collected",
                    meta["county"], meta["group"], MAX_POLLS, len(meta["accum"]),
                )
                yield from self._finish_or_paginate(meta, grew=False)
            return

        providers = data.get("provider") or []
        accum = meta["accum"]
        for p in providers:
            # The results payload can include a null-id sentinel row (program
            # None, providerIdentifier None); it has no detail record (its
            # getchildcareinfo 400s), so skip anything without a real id.
            if not p.get("providerIdentifier"):
                continue
            key = (p.get("providerIdentifier"), p.get("providerLocationIdentifier"))
            accum[key] = p

        search_id = meta["search_id"]
        if not search_id:
            for p in providers:
                if p.get("searchIdentifier"):
                    search_id = p["searchIdentifier"]
                    break

        new_count = len(accum)
        # Grew only if this batch added rows. max(prev, 0) makes a genuinely
        # empty county (new_count == 0) finish immediately instead of fetching
        # one pointless extra page.
        grew = new_count > max(meta["prev_count"], 0)
        nxt = dict(meta)
        nxt["search_id"] = search_id
        nxt["prev_count"] = new_count
        yield from self._finish_or_paginate(nxt, grew=grew)

    def _finish_or_paginate(self, meta, grew):
        if grew and meta["group"] < MAX_GROUPS:
            # More rows are still coming: fetch the next cumulative page.
            nxt = dict(meta)
            nxt["group"] = meta["group"] + 1
            nxt["poll"] = 0
            yield self._summary_request(nxt)
            return

        # Pagination complete for this county -> fan out detail requests.
        accum = meta["accum"]
        self.logger.info(
            "County %s: %d provider location(s) collected across %d group(s)",
            meta["county"], len(accum), meta["group"],
        )
        for provider in accum.values():
            key = (
                provider.get("providerIdentifier"),
                provider.get("providerLocationIdentifier"),
            )
            if key in self.seen:
                continue
            self.seen.add(key)
            yield self._info_request(provider)

    def parse_detail(self, response):
        try:
            provider = (json.loads(response.text) or {}).get("provider") or {}
        except ValueError:
            provider = {}
        if not provider:
            self.logger.warning(
                "Empty detail for %s", response.meta.get("provider", {})
                .get("providerName")
            )
            return
        yield self.build_item(provider)

    # --- item construction ------------------------------------------------ #

    @staticmethod
    def build_item(provider):
        """Map a ``getchildcareinfo`` provider record to a ``ProviderItem``."""
        item = ProviderItem()
        item["source_state"] = "Pennsylvania"
        item["provider_name"] = provider.get("providerName")

        # Address: "STREET, CITY, ST ZIP" (the format the normalization pipeline
        # parses into city/state/zip).
        street = ", ".join(
            str(provider.get(f)).strip()
            for f in ("addressLine1", "addressLine2", "addressLine3")
            if provider.get(f) and str(provider.get(f)).strip()
        )
        city = (provider.get("city") or "").strip()
        state = (provider.get("state") or "").strip()
        zip_code = str(provider.get("zipCode") or "").strip()
        st_zip = " ".join(x for x in (state, zip_code) if x)
        item["address"] = ", ".join(x for x in (street, city, st_zip) if x)

        item["phone"] = format_phone(provider.get("phoneNumber"))
        item["provider_type"] = provider.get("providerType")
        item["capacity"] = provider.get("providerMaxCapacity")

        stars = provider.get("keystoneStars")
        item["pa_stars_rating"] = str(stars) if stars not in (None, "") else None

        referral = provider.get("referralStatus")
        if referral == "ACT":
            item["pa_certificate_status"] = "Active"
        elif referral:
            item["pa_certificate_status"] = "Inactive"
        else:
            item["pa_certificate_status"] = None

        item["pa_school_district"] = join_labels(provider.get("schoolDistrict")) or None
        item["pa_meal_options"] = join_labels(provider.get("mealOptions"))
        item["pa_schedule"] = join_labels(provider.get("generalSchedule"))

        cost_table = []
        for cl in provider.get("careLevel") or []:
            cost_table.append({
                "age_group": (cl.get("careLevel") or "").strip() or None,
                "full_time_rate": cl.get("ftRate"),
                "part_time_rate": cl.get("ptRate"),
                "openings": OPENINGS_MAP.get(cl.get("careLevelOpeningStatus"), "-"),
            })
        item["pa_cost_table"] = cost_table

        return item

    def on_error(self, failure):
        self.logger.error("Request failed: %s", repr(failure))
