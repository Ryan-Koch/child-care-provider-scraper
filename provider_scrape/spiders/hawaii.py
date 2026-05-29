"""Hawaii child care provider spider.

Scrapes https://childcareprovidersearch.dhs.hawaii.gov/, a Microsoft Power Apps
portal backed by Azure Logic App endpoints that return SAP HANA data wrapped in a
`hanaResponse` envelope. The crawl has three stages (see hawaii_plan.md):

1. Areas:  POST the area-table endpoint -> island/area tree -> `Areas` filter strings.
2. Search: POST the search endpoint once per island, subdividing geographically
           whenever a response trips the hard 100-provider cap.
3. Detail: GET /details/?serviceId=<id> and parse the provider data embedded
           directly in the server-rendered HTML (no JS execution / Playwright).
"""

import json
import re

import scrapy

from ..items import InspectionItem, ProviderItem


# --- Azure Logic App endpoints (hardcoded fallbacks; we prefer the live URLs
# scraped off the landing page so a rotated SAS `sig` token doesn't break us). ---
AREAS_URL = (
    "https://prod-28.usgovtexas.logic.azure.us:443/workflows/"
    "5a3c6892c14442138e4b600e03411aa2/triggers/manual/paths/invoke"
    "?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0"
    "&sig=sPbgvsl2Ese18mRBq1xAB51oZVzIoUG4TQuFTuzERpg"
)
SEARCH_URL = (
    "https://prod-06.usgovtexas.logic.azure.us:443/workflows/"
    "179f51f14f6a4837b49e82a3099bc3c3/triggers/manual/paths/invoke"
    "?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0"
    "&sig=V2SJOS2DthZkevCZtKQR-6GAHNVv1p57XZKIYJKewYo"
)
# Workflow ids identify each endpoint regardless of the rotating sig token.
AREAS_WORKFLOW_ID = "5a3c6892c14442138e4b600e03411aa2"
SEARCH_WORKFLOW_ID = "179f51f14f6a4837b49e82a3099bc3c3"

LANDING_URL = "https://childcareprovidersearch.dhs.hawaii.gov/"
DETAIL_URL = "https://childcareprovidersearch.dhs.hawaii.gov/details/?serviceId={}"
INSPECTIONS_URL = (
    "https://childcareprovidersearch.dhs.hawaii.gov/inspections/?serviceId={}"
)

# visitType codes on the inspections page (ported from the site's getInspectionType).
VISIT_TYPE_MAP = {
    "LD": "Drop-In",
    "LI": "Initial",
    "LM": "Monitoring",
    "LO": "Off-year",
    "LR": "Annual/Biennial",
}

# The state root code; islands are its direct children.
ROOT_AREA = "AA"
# The backend silently truncates a search at this many providers, so a response
# of exactly this length means we've lost data and must subdivide geographically.
PROVIDER_CAP = 100

AZURE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://childcareprovidersearch.dhs.hawaii.gov",
    "Referer": "https://childcareprovidersearch.dhs.hawaii.gov/",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
    ),
}

# 1 = Sunday ... 7 = Saturday (verified against a sample provider's Mon-Fri shift).
WEEKDAY_ABBR = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}

# histories[].status codes have no published table in the captured HTML; map the
# known set and fall back to the raw code for anything unseen.
STATUS_MAP = {"AC": "Active", "PE": "Pending"}

# contactModes mode codes.
PHONE_MODES = {"PH", "P2", "MO"}


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested directly)
# ---------------------------------------------------------------------------


def extract_endpoint_urls(html):
    """Pull the area-table and search Logic App URLs off the landing page.

    Returns (areas_url, search_url), each None when the regex finds no URL with
    the matching workflow id (the caller falls back to the hardcoded constants).
    """
    urls = re.findall(
        r"https://prod-\d+\.usgovtexas\.logic\.azure\.us[^'\"\s]+", html
    )
    areas_url = search_url = None
    for url in urls:
        if AREAS_WORKFLOW_ID in url and areas_url is None:
            areas_url = url
        elif SEARCH_WORKFLOW_ID in url and search_url is None:
            search_url = url
    return areas_url, search_url


def extract_embedded_json(html, var_name):
    """Parse a server-rendered ``const <var_name> = `{...}`;`` JSON literal.

    Handles both the bare backtick form and the ``JSON.parse(`{...}`)`` form the
    detail page uses for its code tables. Returns the decoded object, or None if
    the literal is absent or unparseable.
    """
    match = re.search(
        r"const\s+%s\s*=\s*(?:JSON\.parse\()?`(.*?)`" % re.escape(var_name),
        html,
        re.DOTALL,
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _match_braces(text, start):
    """Return the substring from the `{` at `start` through its matching `}`.

    String-aware so braces inside JSON string values don't throw off the count.
    Returns None if no balanced close is found.
    """
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(text)):
        char = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def extract_braced_json(html, prefix):
    """Parse a ``<prefix>{...}`` JS object literal (e.g. ``const cachedList = {``).

    Unlike the backtick-literal code tables, the inspections page assigns plain
    object literals, so we brace-match from the first `{` after the prefix.
    """
    idx = html.find(prefix)
    if idx == -1:
        return None
    brace = html.find("{", idx + len(prefix))
    if brace == -1:
        return None
    blob = _match_braces(html, brace)
    if blob is None:
        return None
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None


def extract_inspection_details(html):
    """Map {visitId: response} for each ``allInspections.push({...})`` block.

    Only visits whose detail the server pre-rendered appear here (the inspections
    page lazily fetches the rest on click), so callers must tolerate misses.
    """
    details = {}
    for match in re.finditer(
        r"allInspections\.push\(\s*\{\s*inspectionId:\s*(\d+)\s*,\s*response:\s*",
        html,
    ):
        visit_id = int(match.group(1))
        brace = html.find("{", match.end())
        if brace == -1:
            continue
        blob = _match_braces(html, brace)
        if blob is None:
            continue
        try:
            details[visit_id] = json.loads(blob)
        except json.JSONDecodeError:
            continue
    return details


def count_requirements_not_met(visit_response):
    """Count visitDetails items flagged "not met" (itemReqMet == 'N')."""
    if not visit_response:
        return None
    items = visit_response.get("hanaResponse", {}).get("visitDetails", [])
    return sum(1 for item in items if item.get("itemReqMet") == "N")


def code_table_map(code_table_json, value_field="description"):
    """Turn a `{hanaResponse:{codeTableRows:[...]}}` table into a {code: value} map.

    For value_field="description" an absent/None value falls back to the code so a
    lookup never raises; pass value_field=None to get the whole row keyed by code.
    """
    result = {}
    if not code_table_json:
        return result
    rows = code_table_json.get("hanaResponse", {}).get("codeTableRows", [])
    for row in rows:
        code = row.get("code")
        if code is None:
            continue
        if value_field is None:
            result[code] = row
        else:
            result[code] = row.get(value_field) or code
    return result


def service_type_name(code, type_map):
    """Map a serviceType code to its public name, falling back to the raw code."""
    if code is None:
        return None
    return type_map.get(code, code)


def build_area_index(code_table_rows):
    """Build (parent_of, children) maps from the area code table rows.

    parent_of: {code: parent_code}; children: {code: [child_code, ...]}.
    The synthetic root (parent is None) is kept in parent_of with a None parent.
    """
    parent_of = {}
    children = {}
    for row in code_table_rows:
        code = row.get("code")
        if code is None:
            continue
        parent = row.get("parent")
        parent_of[code] = parent
        children.setdefault(code, [])
        if parent:
            children.setdefault(parent, []).append(code)
    return parent_of, children


def fully_qualified(code, parent_of):
    """Concatenate ancestor codes (excluding the root 'AA') ending with `code`.

    Mirrors the site's JS: town JO under AH under AB becomes "ABAHJO"; a top-level
    island like AB stays "AB"; the root is never emitted.
    """
    chain = [code]
    cur = code
    while parent_of.get(cur) and parent_of[cur] != ROOT_AREA:
        cur = parent_of[cur]
        chain.append(cur)
    return "".join(reversed(chain))


def subtree_codes(root, children):
    """Return `root` plus every descendant code, breadth-first."""
    collected = []
    queue = [root]
    while queue:
        node = queue.pop(0)
        collected.append(node)
        queue.extend(children.get(node, []))
    return collected


def build_search_body(areas_csv):
    """Search filter JSON with every Center and Home type flag set true."""
    return {
        "ProviderName": "",
        "ZipCode": "",
        "Areas": areas_csv,
        "Type": {
            "Center": "true", "Center1": "true", "Center2": "true",
            "Center3": "true", "Home": "true", "Home1": "true", "Home2": "true",
        },
        "Ages": {
            "InfantandToddler": "false", "Preschool": "false", "School-aged": "false",
        },
        "Others": {
            "Accredited": "false", "WeekendCare": "false",
            "MealsProvided": "false", "SnacksProvided": "false",
        },
    }


def format_phone(value):
    """Normalize a contactModes phone value to "(808) 935-4304".

    Values carry a leading country marker ("NA"=domestic, "IN"=international);
    strip it and any non-digits, then format a 10-digit number, else return the
    bare digits.
    """
    if not value:
        return None
    raw = value
    if raw[:2] in ("NA", "IN"):
        raw = raw[2:]
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return digits


def convert_military_time(time_int):
    """Convert an HHMM integer (730 -> "7:30 AM", 1530 -> "3:30 PM")."""
    if time_int is None:
        return None
    hour, minute = divmod(int(time_int), 100)
    period = "AM" if hour < 12 else "PM"
    display_hour = hour % 12 or 12
    return f"{display_hour}:{minute:02d} {period}"


def format_hours(shifts):
    """Render the shifts list into a single-line "Mon 7:30 AM - 3:30 PM; ..." string.

    Days are emitted in week order (Sun..Sat); a day with multiple ranges joins
    them with commas. Returns None when there are no shifts/hours.
    """
    if not shifts:
        return None
    by_day = {}
    for shift in shifts:
        for hours in shift.get("hours", []):
            day = hours.get("weekdayNumber")
            if day is None:
                continue
            start = convert_military_time(hours.get("startTime"))
            end = convert_military_time(hours.get("endTime"))
            by_day.setdefault(day, []).append(f"{start} - {end}")
    parts = []
    for day in sorted(by_day):
        ranges = ", ".join(by_day[day])
        parts.append(f"{WEEKDAY_ABBR.get(day, day)} {ranges}")
    return "; ".join(parts) if parts else None


def translate_age(unit, value):
    """Render an age value/unit pair the way the site does (weeks or months)."""
    if value is None or unit is None:
        return None
    value = int(value)
    if unit == "W":
        if value == 0:
            return "1 day"
        if value > 52:
            years = value // 52
            rem = value % 52
            label = f"{years} year{'s' if years > 1 else ''}"
            if rem:
                label += f" {rem} week{'s' if rem > 1 else ''}"
            return label
        return f"{value} week{'s' if value > 1 else ''}"
    if unit == "M":
        if value >= 12:
            years = value // 12
            rem = value % 12
            label = f"{years} year{'s' if years > 1 else ''}"
            if rem:
                label += f" {rem} month{'s' if rem > 1 else ''}"
            return label
        return f"{value} month{'s' if value > 1 else ''}"
    return None


def format_age_range(min_unit, min_value, max_unit, max_value):
    """Build an "over X - under Y" age range, tolerating either side being null."""
    low = translate_age(min_unit, min_value)
    high = translate_age(max_unit, max_value)
    if low and high:
        return f"over {low} - under {high}"
    if low:
        return f"over {low}"
    if high:
        return f"under {high}"
    return None


def _address_parts(addr):
    """Ordered non-empty street/building components of an address dict."""
    return [
        part
        for part in (addr.get("street1"), addr.get("street2"), addr.get("building"))
        if part
    ]


def format_address(addr, provider_kind):
    """Render a location/mailing address dict to a single line.

    For home-based providers ("CG") the state suppresses the street for privacy,
    showing only "City, State zip" — replicate that so we don't publish addresses
    the source itself hides. Center-based ("OR") providers get the full address.
    """
    if not addr:
        return None
    city = addr.get("city")
    state = addr.get("state")
    zip_code = addr.get("zipCode")
    # "City, State zip" (e.g. "HILO, HI 96720").
    tail = " ".join(filter(None, [state, str(zip_code) if zip_code else None]))
    locality = ", ".join(filter(None, [city, tail])) or None

    if provider_kind == "CG":
        return locality
    streets = ", ".join(_address_parts(addr))
    if streets and locality:
        return f"{streets}, {locality}"
    return streets or locality


class HawaiiSpider(scrapy.Spider):
    name = "hawaii"
    allowed_domains = [
        "childcareprovidersearch.dhs.hawaii.gov",
        "usgovtexas.logic.azure.us",
    ]

    custom_settings = {
        # This spider is plain HTTP only; force the default handlers so a stray
        # default can't pull in the Playwright browser registered globally.
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 1.0,  # the backend 502s on rapid resends
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_url = SEARCH_URL
        self.parent_of = {}
        self.children = {}
        # serviceIds already dispatched, so overlapping area queries don't
        # produce duplicate items.
        self.seen_service_ids = set()
        # Code tables parsed lazily off the first detail page and reused after.
        self._code_tables_loaded = False
        self.service_type_map = {}
        self.meals_map = {}
        self.languages_map = {}
        self.accreditations_map = {}

    def start_requests(self):
        yield scrapy.Request(
            LANDING_URL, callback=self.parse_landing, meta={"playwright": False}
        )

    def parse_landing(self, response):
        """Scrape the live endpoint URLs, then kick off the area-table fetch."""
        areas_url, search_url = extract_endpoint_urls(response.text)
        if areas_url:
            self.logger.info("Using scraped area-table endpoint URL")
        else:
            self.logger.warning("Area-table URL not found on landing page; using fallback")
            areas_url = AREAS_URL
        if search_url:
            self.search_url = search_url
            self.logger.info("Using scraped search endpoint URL")
        else:
            self.logger.warning("Search URL not found on landing page; using fallback")
            self.search_url = SEARCH_URL

        yield scrapy.Request(
            areas_url,
            method="POST",
            body="",
            headers=AZURE_HEADERS,
            callback=self.parse_areas,
            meta={"playwright": False},
            dont_filter=True,
        )

    def parse_areas(self, response):
        """Build the island/area tree and launch the per-island search."""
        data = json.loads(response.text)
        rows = data.get("hanaResponse", {}).get("codeTableRows", [])
        self.parent_of, self.children = build_area_index(rows)
        descriptions = {r.get("code"): r.get("description") for r in rows}

        islands = self.children.get(ROOT_AREA, [])
        self.logger.info(
            "Area tree built: %s areas, %s islands (%s)",
            len(self.parent_of),
            len(islands),
            ", ".join(descriptions.get(c, c) for c in islands),
        )
        for island in islands:
            island_name = descriptions.get(island, island)
            yield self._search_request(island, island_name, single=False)

    def _search_request(self, area_code, island_name, single):
        """Build a search POST for an area subtree (single=False) or just the
        bare area code (single=True, used to catch providers coded directly at a
        capped node rather than at a leaf)."""
        if single:
            areas_csv = fully_qualified(area_code, self.parent_of)
        else:
            areas_csv = ",".join(
                fully_qualified(c, self.parent_of)
                for c in subtree_codes(area_code, self.children)
            )
        return scrapy.Request(
            self.search_url,
            method="POST",
            body=json.dumps(build_search_body(areas_csv)),
            headers=AZURE_HEADERS,
            callback=self.parse_search,
            meta={
                "playwright": False,
                "area_code": area_code,
                "island_name": island_name,
                "single": single,
            },
            dont_filter=True,
        )

    def parse_search(self, response):
        """Emit detail requests for each service; subdivide on the 100-cap."""
        area_code = response.meta["area_code"]
        island_name = response.meta["island_name"]
        single = response.meta["single"]

        data = json.loads(response.text)
        results = data.get("hanaResponse", {}).get("results", []) or []
        capped = len(results) >= PROVIDER_CAP and not single
        child_codes = self.children.get(area_code, [])

        self.logger.info(
            "Search %s%s (%s): %s providers%s",
            area_code,
            " [single]" if single else "",
            island_name,
            len(results),
            " -- CAP HIT, subdividing" if capped else "",
        )

        if capped and child_codes:
            # Re-query each child subtree finer, plus the node alone to catch any
            # provider coded directly at this level rather than at a leaf.
            for child in child_codes:
                yield self._search_request(child, island_name, single=False)
            yield self._search_request(area_code, island_name, single=True)
            return

        for provider in results:
            for service in provider.get("services", []):
                request = self._detail_request(provider, service, island_name)
                if request is not None:
                    yield request

    def _detail_request(self, provider, service, island_name):
        """Build a detail GET for one service, deduped by serviceId."""
        service_id = service.get("serviceId")
        if service_id is None or service_id in self.seen_service_ids:
            return None
        self.seen_service_ids.add(service_id)

        item = ProviderItem()
        item["source_state"] = "HI"
        item["provider_name"] = service.get("serviceName")
        item["license_holder"] = provider.get("name")
        item["county"] = island_name
        item["hi_island"] = island_name
        item["hi_service_id"] = service_id
        item["hi_provider_id"] = provider.get("providerId")
        item["hi_service_type_code"] = service.get("serviceType")
        item["hi_area_code"] = service.get("area")
        item["hi_provider_kind"] = provider.get("providerType")
        item["inspections"] = []
        detail_url = DETAIL_URL.format(service_id)
        item["provider_url"] = detail_url

        return scrapy.Request(
            detail_url,
            callback=self.parse_detail,
            errback=self.detail_errback,
            meta={"partial_item": item, "playwright": False},
            dont_filter=True,
        )

    def _load_code_tables(self, html):
        """Parse and cache the embedded code tables off the first detail page."""
        if self._code_tables_loaded:
            return
        self.service_type_map = code_table_map(
            extract_embedded_json(html, "hanaJSON"), value_field="publicName"
        )
        self.meals_map = code_table_map(extract_embedded_json(html, "serviceMealsResponse"))
        self.languages_map = code_table_map(
            extract_embedded_json(html, "serviceLanguagesResponse")
        )
        self.accreditations_map = code_table_map(
            extract_embedded_json(html, "serviceAccreditationsResponse")
        )
        self._code_tables_loaded = True
        self.logger.info(
            "Loaded code tables: %s service types, %s meals, %s languages, %s accreditations",
            len(self.service_type_map),
            len(self.meals_map),
            len(self.languages_map),
            len(self.accreditations_map),
        )

    def parse_detail(self, response):
        item = response.meta["partial_item"]
        self._load_code_tables(response.text)

        bundle = extract_embedded_json(response.text, "response")
        if bundle is None:
            self.logger.warning(
                "No embedded data on detail page for serviceId %s; keeping partial item",
                item.get("hi_service_id"),
            )
            yield item
            return

        summary = bundle.get("summary", {})
        if summary.get("hanaResponseStatus", {}).get("responseCode") == 400:
            self.logger.info(
                "Detail page for serviceId %s reports invalid service id; dropping",
                item.get("hi_service_id"),
            )
            return

        self.fill_summary(item, summary.get("hanaResponse", {}))
        self.fill_details(item, bundle.get("details", {}).get("hanaResponse", {}))
        self.fill_history(item, bundle.get("history", {}).get("hanaResponse", {}))

        self.logger.info(
            "Parsed detail for %s (serviceId %s): type=%s, capacity=%s",
            item.get("provider_name"),
            item.get("hi_service_id"),
            item.get("provider_type"),
            item.get("capacity"),
        )
        # Inspections live on a sibling page keyed by the same serviceId; fetch
        # it to populate the item, then emit. The item is emitted regardless of
        # whether that fetch succeeds (see inspections_errback).
        yield scrapy.Request(
            INSPECTIONS_URL.format(item["hi_service_id"]),
            callback=self.parse_inspections,
            errback=self.inspections_errback,
            meta={"partial_item": item, "playwright": False},
            dont_filter=True,
        )

    def parse_inspections(self, response):
        """Populate item['inspections'] from the server-rendered visit list."""
        item = response.meta["partial_item"]
        cached = extract_braced_json(response.text, "const cachedList = ")
        summaries = (
            (cached or {}).get("hanaResponse", {}).get("visitSummaries", []) or []
        )
        details_by_visit = extract_inspection_details(response.text)

        inspections = []
        for visit in summaries:
            visit_id = visit.get("visitId")
            visit_type = visit.get("visitType")
            inspection = InspectionItem()
            inspection["date"] = visit.get("visitDate")
            inspection["type"] = VISIT_TYPE_MAP.get(visit_type, visit_type)
            inspection["hi_visit_id"] = visit_id
            inspection["hi_licensing_period_start"] = visit.get("licensingPeriodStart")
            inspection["hi_licensing_period_end"] = visit.get("licensingPeriodEnd")
            not_met = count_requirements_not_met(details_by_visit.get(visit_id))
            if not_met is not None:
                inspection["hi_requirements_not_met"] = not_met
            inspections.append(inspection)

        item["inspections"] = inspections
        self.logger.info(
            "Parsed %s inspection(s) for serviceId %s",
            len(inspections),
            item.get("hi_service_id"),
        )
        yield item

    def inspections_errback(self, failure):
        """Inspections fetch failed; emit the provider with no inspection list."""
        item = failure.request.meta.get("partial_item")
        if item is not None:
            self.logger.warning(
                "Inspections fetch failed for serviceId %s; emitting without inspections",
                item.get("hi_service_id"),
            )
            yield item

    def detail_errback(self, failure):
        item = failure.request.meta.get("partial_item")
        if item is not None:
            self.logger.warning(
                "Detail fetch failed for serviceId %s; emitting partial item",
                item.get("hi_service_id"),
            )
            yield item

    def fill_summary(self, item, summary):
        if not summary:
            return
        item["provider_name"] = summary.get("serviceName") or item.get("provider_name")
        item["license_holder"] = summary.get("providerName") or item.get("license_holder")
        item["provider_type"] = service_type_name(
            summary.get("serviceType"), self.service_type_map
        )
        item["hi_provider_kind"] = summary.get("providerType") or item.get("hi_provider_kind")
        item["license_number"] = summary.get("licenseNumber")
        item["license_begin_date"] = summary.get("effectiveDate")
        item["license_expiration"] = summary.get("expirationDate")
        item["capacity"] = summary.get("capacity")
        item["ages_served"] = format_age_range(
            summary.get("minAgeUnit"), summary.get("minAgeValue"),
            summary.get("maxAgeUnit"), summary.get("maxAgeValue"),
        )
        license_type = summary.get("licenseType")
        item["hi_license_type"] = {"P": "Provisional", "R": "Regular"}.get(
            license_type, license_type
        )

    def fill_details(self, item, details):
        if not details:
            return
        provider_kind = item.get("hi_provider_kind")
        item["address"] = format_address(details.get("locationAddress"), provider_kind)
        item["hi_mailing_address"] = format_address(
            details.get("mailingAddress"), provider_kind
        )
        item["hi_usda_food_program"] = details.get("usdaFoodProgram")
        item["hi_diapered_children_accepted"] = details.get("diaperedChildrenAccepted")
        item["hi_demonstration_project"] = details.get("demonstrationProject")
        item["hours"] = format_hours(details.get("shifts"))

        item["hi_meals"] = [
            self.meals_map.get(c, c) for c in (details.get("meals") or [])
        ]
        item["hi_accreditations"] = [
            self._accreditation_entry(a) for a in (details.get("accreditations") or [])
        ]
        languages = [
            self.languages_map.get(c, c) for c in (details.get("caregiverLanguages") or [])
        ]
        if languages:
            item["languages"] = languages

        self._fill_contacts(item, details.get("contactModes"))

    def _accreditation_entry(self, accreditation):
        """Map one accreditation dict to {name, effective_date, expiration_date}.

        The `accreditationType` code is resolved against the accreditations table
        (e.g. "02" -> "NECPA"), falling back to the raw code when unmapped.
        """
        code = accreditation.get("accreditationType")
        return {
            "name": self.accreditations_map.get(code, code),
            "effective_date": accreditation.get("effectiveDate"),
            "expiration_date": accreditation.get("expirationDate"),
        }

    def _fill_contacts(self, item, contact_modes):
        for contact in contact_modes or []:
            mode = contact.get("mode")
            value = contact.get("value")
            if mode in PHONE_MODES and not item.get("phone"):
                item["phone"] = format_phone(value)
            elif mode == "EM":
                item["email"] = value
            elif mode == "WW":
                item["provider_website"] = value

    def fill_history(self, item, history):
        if not history:
            return
        histories = history.get("histories") or []
        item["hi_status_history"] = histories
        if histories:
            current = histories[-1]
            raw_status = current.get("status")
            item["status"] = STATUS_MAP.get(raw_status, raw_status)
            item["status_date"] = current.get("statusDate")
        item["hi_license_history"] = history.get("licenses") or []
