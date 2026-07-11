import json
import re
from datetime import datetime

import scrapy

from provider_scrape.items import ProviderItem, InspectionItem


# Age-range unit abbreviations used by the API (e.g. "0W - 12 Y") expanded to
# the readable form the previous detail-page scrape produced ("0 Weeks - 12 Years").
_AGE_UNITS = {"D": "Days", "W": "Weeks", "M": "Months", "Y": "Years"}


def _clean(value):
    """Trim to a non-empty string, or None. Ints (license #, etc.) are stringified."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_date(value):
    """Convert the API's ``YYYYMMDD`` dates to ISO ``YYYY-MM-DD``.

    The API dates are all-digit ``YYYYMMDD``. It uses several placeholders/corrupt
    values that are *not* real dates — ``"0"``, ``"99999999"`` (its "no date"
    sentinel), or malformed lengths (``"70506"``) — all of which become None so
    they never reach the output or the pipeline's date parser. A genuine calendar
    date is required (``"20250230"`` is rejected). Any non-digit value is passed
    through untouched for the pipeline to handle (defensive; not seen in practice).
    """
    text = _clean(value)
    if not text:
        return None
    if text.isdigit():
        if len(text) == 8:
            try:
                return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                return None  # sentinel (99999999) or invalid calendar date
        return None  # wrong-length placeholder/corrupt value (0, 70506, ...)
    return text


def _parse_capacity(value):
    """Extract the integer from ``"60 Children"`` -> ``60`` (raw kept if no digits)."""
    text = _clean(value)
    if not text:
        return None
    match = re.match(r"\d+", text)
    return int(match.group()) if match else text


def _expand_age_range(value):
    """Expand ``"0W - 12 Y"`` -> ``"0 Weeks - 12 Years"`` (unknown units kept)."""
    text = _clean(value)
    if not text:
        return None
    return re.sub(
        r"(\d+)\s*([A-Za-z])",
        lambda m: f"{m.group(1)} {_AGE_UNITS.get(m.group(2).upper(), m.group(2))}",
        text,
    )


def _build_address(street, city, state, zip_code):
    """Assemble the state's ``"STREET CITY, ST ZIP"`` shape, tolerating gaps."""
    left = _clean(street) or ""
    city = _clean(city) or ""
    state = _clean(state) or ""
    if city and state:
        mid = f"{city}, {state}"
    else:
        mid = city or state
    right = _clean(zip_code) or ""
    combined = " ".join(part for part in (left, mid, right) if part).strip()
    return combined or None


class AlaskaSpider(scrapy.Spider):
    """Alaska child care providers, read directly from the site's JSON API.

    The public site (findccprovider.health.alaska.gov) is a Blazor WebAssembly
    SPA. Its results grid intermittently fails to render even though the
    underlying ``GET /api/Provider`` call returns 200 with the full dataset — a
    client-side binding race we cannot control from the outside. Driving that UI
    with Playwright therefore produced sporadic 0-item runs. We bypass the UI and
    read the same API the app consumes:

      * ``GET /api/Provider``            -> roster of every provider (no history)
      * ``GET /api/Provider/{facilityId}`` -> that provider **with** complianceEvents

    This is deterministic, needs no browser, and finishes in seconds.
    """

    name = "alaska"
    allowed_domains = ["findccprovider.health.alaska.gov"]

    SITE = "https://findccprovider.health.alaska.gov"
    API_BASE = "https://findccprovider.health.alaska.gov/api/Provider"

    custom_settings = {
        # Plain JSON over HTTP — no browser. Override the project-wide Playwright
        # download handler with the standard one so no chromium is launched.
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
        # Be polite across the ~400 per-provider detail calls.
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.25,
        "RETRY_TIMES": 3,
    }

    def start_requests(self):
        yield scrapy.Request(
            self.API_BASE,
            callback=self.parse_list,
            headers={"Accept": "application/json"},
        )

    def parse_list(self, response):
        providers = json.loads(response.text)
        self.logger.info(f"Provider roster returned {len(providers)} records.")
        for roster in providers:
            facility_id = roster.get("facilityId")
            if facility_id is None:
                continue
            # The roster omits complianceEvents; the per-facility endpoint
            # returns the same record with them populated. Carry the roster row
            # through so an errback can still emit the provider if the detail
            # call fails after retries.
            yield scrapy.Request(
                f"{self.API_BASE}/{facility_id}",
                callback=self.parse_detail,
                errback=self.errback_detail,
                headers={"Accept": "application/json"},
                cb_kwargs={"roster": roster},
            )

    def parse_detail(self, response, roster):
        data = json.loads(response.text)
        yield self.build_item(data)

    def errback_detail(self, failure):
        # Detail call failed after retries: fall back to the roster row so the
        # provider is still emitted (just without compliance history).
        roster = failure.request.cb_kwargs.get("roster", {})
        self.logger.warning(
            f"Detail fetch failed for facilityId={roster.get('facilityId')}: "
            f"{failure.value!r}. Emitting roster record without inspections."
        )
        return [self.build_item(roster)]

    def build_item(self, data):
        item = ProviderItem()
        item["source_state"] = "Alaska"

        facility_id = data.get("facilityId")
        if facility_id is not None:
            item["provider_url"] = f"{self.SITE}/ProviderInfo/{facility_id}"

        item["provider_name"] = _clean(data.get("facilityName"))
        item["license_number"] = _clean(data.get("licenseNumber"))
        item["phone"] = _clean(data.get("phoneNumber"))
        item["capacity"] = _parse_capacity(data.get("capacity"))
        item["ages_served"] = _expand_age_range(data.get("ageRange"))

        first = _clean(data.get("firstName"))
        last = _clean(data.get("lastName"))
        item["administrator"] = " ".join(p for p in (first, last) if p) or None

        # CCAP = Child Care Assistance Program (state subsidy) -> scholarships.
        item["scholarships_accepted"] = _clean(data.get("acceptsCCAP"))

        effective = _format_date(data.get("effectiveDate"))
        item["status_date"] = effective
        item["license_begin_date"] = effective
        item["license_expiration"] = _format_date(data.get("expirationDate"))

        city = _clean(data.get("city"))
        state = _clean(data.get("state"))
        zip_code = _clean(data.get("zip"))
        item["address"] = _build_address(
            data.get("address"), city, state, zip_code
        )
        # Structured components straight from the API (the pipeline skips its
        # best-effort address parse when all three are already present).
        if city:
            item["city"] = city
        if state:
            item["state"] = state
        if zip_code:
            item["zip"] = zip_code

        item["inspections"] = self.build_inspections(data.get("complianceEvents"))
        return item

    @staticmethod
    def build_inspections(events):
        # The API returns one row per violated regulation, so a single
        # inspection often appears as many rows that are identical once reduced
        # to these generic fields (the per-violation detail — section, statute,
        # comments — is not captured). Deduplicate on the reduced fingerprint,
        # mirroring the previous detail-page scrape.
        inspections = []
        seen = set()
        for event in events or []:
            # The inspection/investigation date is the primary event date; fall
            # back through the other timestamps if it is a "0" placeholder.
            date = (
                _format_date(event.get("insInvDate"))
                or _format_date(event.get("violationDate"))
                or _format_date(event.get("intakeDate"))
                or _format_date(event.get("complianceDate"))
            )
            type_ = _clean(event.get("complianceType"))
            findings = _clean(event.get("findings"))
            action = _clean(event.get("actionTaken"))
            status_updated = _format_date(event.get("complianceDate"))

            fingerprint = (date, type_, findings, action, status_updated)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)

            insp = InspectionItem()
            insp["date"] = date
            insp["type"] = type_
            insp["original_status"] = findings
            insp["corrective_status"] = action
            insp["status_updated"] = status_updated
            inspections.append(insp)
        return inspections
