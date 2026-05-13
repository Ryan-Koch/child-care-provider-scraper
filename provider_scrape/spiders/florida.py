import asyncio
import json
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
    "Alachua", "Baker", "Bay", "Bradford", "Brevard", "Broward", "Calhoun",
    "Charlotte", "Citrus", "Clay", "Collier", "Columbia", "DeSoto", "Dixie",
    "Duval", "Escambia", "Flagler", "Franklin", "Gadsden", "Gilchrist",
    "Glades", "Gulf", "Hamilton", "Hardee", "Hendry", "Hernando", "Highlands",
    "Hillsborough", "Holmes", "Indian River", "Jackson", "Jefferson",
    "Lafayette", "Lake", "Lee", "Leon", "Levy", "Liberty", "Madison",
    "Manatee", "Marion", "Martin", "Miami-Dade", "Monroe", "Nassau",
    "Okaloosa", "Okeechobee", "Orange", "Osceola", "Palm Beach", "Pasco",
    "Pinellas", "Polk", "Putnam", "Santa Rosa", "Sarasota", "Seminole",
    "St. Johns", "St. Lucie", "Sumter", "Suwannee", "Taylor", "Union",
    "Volusia", "Wakulla", "Walton", "Washington",
]


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
        never reissue the call ourselves."""
        page = response.meta["playwright_page"]
        try:
            total = len(FL_COUNTIES)
            successes = 0
            for index, county in enumerate(FL_COUNTIES, start=1):
                self.logger.info(
                    f"[{index}/{total}] navigating SPA for {county}"
                )

                api_resp = await self._fetch_county_via_spa(page, county)
                if api_resp is None:
                    continue
                status, body = api_resp

                if status != 200:
                    self.logger.error(
                        f"[{index}/{total}] {county}: HTTP {status} "
                        f"— first 200 chars: {body[:200]}"
                    )
                    continue

                try:
                    data = json.loads(body)
                except json.JSONDecodeError as exc:
                    self.logger.error(
                        f"[{index}/{total}] {county}: bad JSON: {exc}"
                    )
                    continue

                if not isinstance(data, list) or not data:
                    self.logger.warning(
                        f"[{index}/{total}] {county}: empty/unexpected payload"
                    )
                    continue

                records = data[0].get("publicSearches") or []
                self.logger.info(
                    f"[{index}/{total}] {county}: {len(records)} providers"
                )
                successes += 1

                for record in records:
                    yield self.parse_provider(record, county)

                await asyncio.sleep(self.BETWEEN_COUNTY_SLEEP_S)

            self.logger.info(
                f"Finished: {successes}/{total} counties returned data"
            )
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
            async with page.expect_response(
                matcher, timeout=self.RESPONSE_TIMEOUT_MS
            ) as resp_info:
                await page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=self.NAV_TIMEOUT_MS,
                )
            api_resp = await resp_info.value
        except Exception as exc:
            self.logger.error(
                f"{county}: failed waiting for API response: {exc}"
            )
            return None

        try:
            status = api_resp.status
            body = await api_resp.text()
        except Exception as exc:
            self.logger.error(
                f"{county}: failed reading API response body: {exc}"
            )
            return None

        return status, body

    def parse_provider(self, data, county):
        item = ProviderItem()
        item["source_state"] = "Florida"
        item["county"] = county

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
                "monday", "tuesday", "wednesday",
                "thursday", "friday", "saturday", "sunday",
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

        item["sutq_rating"] = _num(data.get("compositesScore"))
        item["fl_wels_rating_date"] = data.get("welsRatingDate")
        item["fl_vpk_school_year_composite_score"] = _num(
            data.get("vpkSchoolYearCompositeScore")
        )
        item["fl_vpk_school_year_wels_rating_date"] = data.get(
            "vpkSchoolYearWelsRatingDate"
        )
        item["fl_vpk_summer_composite_score"] = _num(
            data.get("vpkSummerCompositeScore")
        )
        item["fl_vpk_summer_wels_rating_date"] = data.get(
            "vpkSummerWelsRatingDate"
        )

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
        inspections = []
        for bucket in year_buckets or []:
            reports = bucket.get("inspectionReport") or []
            for report in reports:
                ins = InspectionItem()
                ins["date"] = report.get("inspectionDate")
                ins["fl_has_violation"] = report.get("hasViolation")
                ins["fl_inspection_id"] = report.get("id")
                inspections.append(ins)
        return inspections
