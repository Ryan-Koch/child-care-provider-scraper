import json
import re
import urllib.parse

import scrapy
from scrapy.http import TextResponse

from provider_scrape.items import InspectionItem, ProviderItem

# Aura API endpoint for Salesforce Experience Cloud
AURA_ENDPOINT = "https://cclb.michigan.gov/s/sfsites/aura"

# Initial page URL to extract the dynamic fwuid token
INITIAL_PAGE_URL = "https://cclb.michigan.gov/s/statewide-facility-search"

# Search pagination settings
PAGE_SIZE = 200

# Michigan has 83 counties. The Salesforce SOQL OFFSET limit is 2000, which
# caps unfiltered searches at ~2200 records. We search per-county to stay
# under this limit (the largest county, Wayne, has ~1900 providers).
MICHIGAN_COUNTIES = [
    "Alcona", "Alger", "Allegan", "Alpena", "Antrim", "Arenac", "Baraga",
    "Barry", "Bay", "Benzie", "Berrien", "Branch", "Calhoun", "Cass",
    "Charlevoix", "Cheboygan", "Chippewa", "Clare", "Clinton", "Crawford",
    "Delta", "Dickinson", "Eaton", "Emmet", "Grand Traverse", "Genesee",
    "Gladwin", "Gogebic", "Gratiot", "Hillsdale", "Houghton", "Huron",
    "Ingham", "Ionia", "Iosco", "Iron", "Isabella", "Jackson", "Kalamazoo",
    "Kalkaska", "Kent", "Keweenaw", "Lake", "Lapeer", "Leelanau", "Lenawee",
    "Livingston", "Luce", "Mackinac", "Macomb", "Manistee", "Marquette",
    "Mason", "Mecosta", "Menominee", "Midland", "Missaukee", "Monroe",
    "Montcalm", "Montmorency", "Muskegon", "Newaygo", "Oakland", "Oceana",
    "Ogemaw", "Ontonagon", "Osceola", "Oscoda", "Otsego", "Ottawa",
    "Presque Isle", "Roscommon", "Saginaw", "Sanilac", "Schoolcraft",
    "Shiawassee", "St Clair", "St Joseph", "Tuscola", "Van Buren",
    "Washtenaw", "Wayne", "Wexford",
]

# Aura descriptor for OmniStudio Apex action execution
AURA_DESCRIPTOR = "aura://ApexActionController/ACTION$execute"

# Apex class that contains all the facility search/detail methods
APEX_CLASSNAME = "cchirp_getFacilityDetails"

# Inspection document type codes and their human-readable labels
DOC_TYPE_LABELS = {
    "INSP": "Inspection",
    "EXTINSP": "Exit Inspection",
    "EXTRNWL": "External Renewal",
    "EXTSIR": "External Special Investigation Report",
    "RNWL": "Renewal",
    "CAP": "Corrective Action Plan",
    "ADD": "Addendum",
    "SIR": "Special Investigation Report",
    "ORIG": "Original",
}


def clean_address(address):
    """Remove trailing 'null' strings from addresses returned by the API."""
    if not address:
        return None
    # The API sometimes appends literal "null" to addresses
    cleaned = re.sub(r"\s*null\s*$", "", address, flags=re.IGNORECASE).strip()
    return cleaned if cleaned else None


def format_hours(operational_details):
    """Format an array of {Day, OpenTime, CloseTime} into a readable string.

    Example output: "Mon: 7:00 AM-6:00 PM; Tue: 7:00 AM-6:00 PM"
    """
    if not operational_details:
        return None

    parts = []
    for entry in operational_details:
        day = entry.get("Day", "")
        open_time = entry.get("OpenTime", "")
        close_time = entry.get("CloseTime", "")
        if day and open_time and close_time:
            parts.append(f"{day}: {open_time}-{close_time}")
    return "; ".join(parts) if parts else None


def parse_inspection_doc(doc):
    """Parse an inspection document record into an InspectionItem.

    Document Title format: LicenseNumber_TYPE_YYYYMMDD (with optional suffixes)
    Examples:
      "DC730018297_INSP_20240512"
      "DC730018297_INSP_20240512.pdf"
      "DC460016557_EXTRNWL_20250121 (2)"
      "DC630023376_EXTINSP_20250310_RM"
    """
    insp = InspectionItem()
    insp["report_url"] = doc.get("docurl")

    title = doc.get("Title", "")
    # Strip .pdf extension(s) if present
    title_clean = re.sub(r"(\.pdf)+$", "", title, flags=re.IGNORECASE)

    # Try to parse the structured title format:
    # LicenseNumber_TYPE_YYYYMMDD with optional trailing suffixes (_RM, (1), etc.)
    match = re.match(r"^[^_]+_([A-Z]+)_(\d{8})", title_clean)
    if match:
        type_code = match.group(1)
        date_str = match.group(2)
        insp["type"] = DOC_TYPE_LABELS.get(type_code, type_code)

        # Convert YYYYMMDD to MM/DD/YYYY
        insp["date"] = f"{date_str[4:6]}/{date_str[6:]}/{date_str[:4]}"
    else:
        # Fallback: use the raw title as the type
        insp["type"] = title if title else None

    insp["date"] = insp.get("date") or doc.get("CreatedDate")
    return insp


class MichiganSpider(scrapy.Spider):
    """Spider for Michigan child care provider data from cclb.michigan.gov.

    Uses the Salesforce Aura framework API (pure HTTP POST requests).
    The spider flow is:
        1. Fetch initial HTML page to extract the dynamic fwuid token
        2. Call search API with pagination (pageSize=200)
        3. For each provider, batch 4 detail API calls into 1 HTTP request
        4. Combine responses into ProviderItem
    """

    name = "michigan"
    allowed_domains = ["cclb.michigan.gov"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
    }

    def start_requests(self):
        """Fetch the initial HTML page to extract the fwuid token."""
        yield scrapy.Request(
            INITIAL_PAGE_URL,
            callback=self.parse_initial_page,
        )

    def parse_initial_page(self, response):
        """Extract fwuid from the initial page and search each county."""
        fwuid = self._extract_fwuid(response)
        if not fwuid:
            self.logger.error("Could not extract fwuid from initial page")
            return

        self.logger.info(
            f"Extracted fwuid: {fwuid} — launching searches for "
            f"{len(MICHIGAN_COUNTIES)} counties"
        )
        for county in MICHIGAN_COUNTIES:
            yield self._build_search_request(
                fwuid, page_number=1, county=county
            )

    def parse_search(self, response):
        """Parse search results, yield detail requests, and handle pagination."""
        fwuid = response.meta["fwuid"]
        county = response.meta.get("county")

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"[{county}] Failed to parse search response JSON")
            return

        # Navigate the Aura response structure to get the search results
        search_result = self._extract_action_result(data, action_index=0)
        if not search_result:
            self.logger.warning(f"[{county}] No search result found in response")
            return

        # The returnValue is double-encoded: the outer returnValue contains
        # a "returnValue" key whose value is a JSON string
        return_value = self._decode_return_value(
            search_result.get("returnValue", {})
        )
        results = return_value.get("results", [])
        total_records = return_value.get("totalRecords", 0)
        record_end = return_value.get("recordEnd", 0)
        current_page = response.meta["page_number"]

        self.logger.info(
            f"[{county}] page {current_page}: got {len(results)} results, "
            f"{record_end}/{total_records} total"
        )

        # Yield batched detail requests for each provider
        for result in results:
            provider_id = result.get("id")
            if provider_id:
                yield self._build_detail_request(fwuid, provider_id, result)

        # Paginate if there are more records
        if record_end < total_records:
            next_page = current_page + 1
            yield self._build_search_request(
                fwuid, page_number=next_page, county=county
            )

    def parse_detail(self, response):
        """Parse the batched detail response (4 API methods) into a ProviderItem."""
        search_data = response.meta["search_data"]
        provider_id = response.meta["provider_id"]

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse detail JSON for {provider_id}")
            return

        # Extract and decode results from each of the 4 batched actions.
        # Each returnValue is double-encoded JSON (string inside JSON).
        detail_raw = self._extract_decoded_return_value(data, action_index=0)
        operational_raw = self._extract_decoded_return_value(data, action_index=1)
        service_raw = self._extract_decoded_return_value(data, action_index=2)
        docs_info = (
            self._extract_decoded_return_value(data, action_index=3) or {}
        )

        # getDetailInfo returns an array — take the first element
        if isinstance(detail_raw, list):
            detail_info = detail_raw[0] if detail_raw else {}
        else:
            detail_info = detail_raw or {}

        # getOperationalDetailInfo returns an array of day/time entries directly
        if isinstance(operational_raw, list):
            op_details = operational_raw
        else:
            op_details = (operational_raw or {}).get("operationalDetails", [])

        # getServiceDetailInfo returns an array — take the first element
        if isinstance(service_raw, list):
            service_info = service_raw[0] if service_raw else {}
        else:
            service_info = service_raw or {}

        item = ProviderItem()
        item["source_state"] = "Michigan"
        item["provider_url"] = (
            f"https://cclb.michigan.gov/s/licensed-child-care-facility"
            f"?id={provider_id}"
        )

        # Basic fields from search data, overridden by detail when available.
        # Note: search data uses lowercase "name" while detail uses "Name".
        item["provider_name"] = (
            detail_info.get("Name")
            or search_data.get("name")
            or search_data.get("Name")
        )
        item["license_number"] = (
            detail_info.get("LicenseNumber") or search_data.get("LicenseNumber")
        )
        item["provider_type"] = (
            detail_info.get("Type") or search_data.get("LicenseType")
        )
        item["status"] = (
            detail_info.get("Status") or search_data.get("Status")
        )
        item["mi_license_status"] = (
            detail_info.get("LicenseStatus") or search_data.get("LicenseStatus")
        )

        # Address - strip trailing "null"
        raw_address = detail_info.get("Address") or search_data.get("Address")
        item["address"] = clean_address(raw_address)

        item["phone"] = detail_info.get("Phone")
        # The API field is named "Country" but actually contains the county
        item["county"] = (
            detail_info.get("Country") or search_data.get("BillingCountry")
        )
        item["capacity"] = detail_info.get("Capacity")
        item["license_holder"] = (
            detail_info.get("LicenseName") or search_data.get("LicenseName")
        )
        item["license_begin_date"] = detail_info.get("EffectiveDate")
        item["license_expiration"] = detail_info.get("ExpirationDate")

        # Michigan-specific fields
        item["mi_licensee_address"] = clean_address(
            detail_info.get("LicenseeAddress")
        )

        # Operational hours
        item["hours"] = format_hours(op_details)

        # Services
        item["mi_services_provided"] = service_info.get("ServicesProvided")
        item["mi_full_day"] = service_info.get("FullDay")

        # Inspections - merge documents with and without violations
        documents = docs_info.get("documents", [])
        documents_no_violation = docs_info.get("documentsWithoutViolation", [])
        all_docs = documents + documents_no_violation

        inspections = []
        for doc in all_docs:
            insp = parse_inspection_doc(doc)
            if insp.get("date") or insp.get("type"):
                inspections.append(insp)
        item["inspections"] = inspections

        yield item

    # ---- Request builders ----

    def _build_search_request(self, fwuid, page_number, county=None):
        """Build an Aura API request for the facility search.

        When county is provided, filters results to that county using the
        "country" param (Salesforce field name for county in this API).
        """
        search_params = {
            "pageSize": PAGE_SIZE,
            "pageNumber": page_number,
        }
        if county:
            search_params["country"] = county

        message = {
            "actions": [
                {
                    "id": "1;a",
                    "descriptor": AURA_DESCRIPTOR,
                    "callingDescriptor": "UNKNOWN",
                    "params": {
                        "namespace": "",
                        "classname": APEX_CLASSNAME,
                        "method": "getFacility",
                        "params": search_params,
                        "cacheable": False,
                        "isContinuation": False,
                    },
                }
            ]
        }

        params = {
            "r": "1",
            "aura.ApexAction.execute": "1",
        }
        url = f"{AURA_ENDPOINT}?{urllib.parse.urlencode(params)}"

        body = urllib.parse.urlencode(
            {
                "message": json.dumps(message),
                "aura.context": json.dumps(
                    {
                        "mode": "PROD",
                        "fwuid": fwuid,
                        "app": "siteforce:communityApp",
                    }
                ),
                "aura.token": "null",
            }
        )

        return scrapy.Request(
            url,
            method="POST",
            body=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            callback=self.parse_search,
            meta={
                "fwuid": fwuid,
                "page_number": page_number,
                "county": county,
            },
            dont_filter=True,
        )

    def _build_detail_request(self, fwuid, provider_id, search_data):
        """Build a batched Aura API request with all 4 detail methods."""
        detail_methods = [
            "getDetailInfo",
            "getOperationalDetailInfo",
            "getServiceDetailInfo",
            # Note: the API has a typo - "Genertated" not "Generated"
            "getGenertatedDocs",
        ]
        actions = [
            {
                "id": f"{i + 1};a",
                "descriptor": AURA_DESCRIPTOR,
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": APEX_CLASSNAME,
                    "method": method,
                    "params": {"accountId": provider_id},
                    "cacheable": False,
                    "isContinuation": False,
                },
            }
            for i, method in enumerate(detail_methods)
        ]

        message = {"actions": actions}
        params = {
            "r": "4",
            "aura.ApexAction.execute": "1",
        }
        url = f"{AURA_ENDPOINT}?{urllib.parse.urlencode(params)}"

        body = urllib.parse.urlencode(
            {
                "message": json.dumps(message),
                "aura.context": json.dumps(
                    {
                        "mode": "PROD",
                        "fwuid": fwuid,
                        "app": "siteforce:communityApp",
                    }
                ),
                "aura.token": "null",
            }
        )

        return scrapy.Request(
            url,
            method="POST",
            body=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            callback=self.parse_detail,
            meta={
                "fwuid": fwuid,
                "provider_id": provider_id,
                "search_data": search_data,
            },
            dont_filter=True,
        )

    # ---- Response parsing helpers ----

    def _extract_fwuid(self, response):
        """Extract the fwuid token from the initial HTML page.

        The fwuid is embedded in the page's script tag URLs as URL-encoded JSON
        and changes with each Salesforce deployment.
        """
        body = response.text

        # Pattern 1: "fwuid":"<value>" in JSON embedded in the page
        match = re.search(r'"fwuid"\s*:\s*"([^"]+)"', body)
        if match:
            return match.group(1)

        # Pattern 2: URL-encoded fwuid in script src/data-src attributes
        # e.g., %22fwuid%22%3A%22TOKEN_VALUE%22
        match = re.search(r'%22fwuid%22%3A%22([^%]+)%22', body)
        if match:
            return match.group(1)

        # Pattern 3: fwuid in the auraFW JS URL path segment
        # e.g., /s/sfsites/auraFW/javascript/TOKEN_VALUE/aura_prod.js
        match = re.search(r'/auraFW/javascript/([A-Za-z0-9_-]{20,})/', body)
        if match:
            return match.group(1)

        return None

    def _extract_action_result(self, data, action_index=0):
        """Extract the result for a specific action from the Aura response.

        Aura responses have the structure:
        {"actions": [{"id": "1;a", "state": "SUCCESS", "returnValue": {...}}]}
        """
        actions = data.get("actions", [])
        if action_index < len(actions):
            action = actions[action_index]
            if action.get("state") == "SUCCESS":
                return action
        return None

    def _extract_action_return_value(self, data, action_index=0):
        """Extract the returnValue for a specific action from the Aura response."""
        action = self._extract_action_result(data, action_index)
        if action:
            return action.get("returnValue", {})
        return None

    def _decode_return_value(self, outer_return_value):
        """Decode the double-encoded returnValue from the Aura response.

        The OmniStudio Apex action wraps the actual return value in an outer
        object: {"returnValue": "<JSON string>"}. This method extracts and
        parses the inner JSON string.
        """
        if not outer_return_value:
            return {}
        inner = outer_return_value.get("returnValue")
        if inner is None:
            return outer_return_value
        if isinstance(inner, str):
            try:
                return json.loads(inner)
            except json.JSONDecodeError:
                self.logger.warning(f"Failed to decode inner returnValue JSON")
                return {}
        # Already decoded (e.g. in tests)
        return inner

    def _extract_decoded_return_value(self, data, action_index=0):
        """Extract and decode the returnValue for a specific action."""
        raw = self._extract_action_return_value(data, action_index)
        if raw is None:
            return None
        return self._decode_return_value(raw)
