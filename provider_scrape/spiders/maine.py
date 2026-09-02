"""Maine Child Care Provider Search spider.

Source: Two separate websites:

1. **Search (primary)**: https://search.childcarechoices.me/ -- ASP.NET WebForms
   search by ZIP code, returns providers with coordinates, star ratings, and
   opening information.

2. **Licensing detail**: https://gateway.maine.gov/dhhs/childcare-licensing/ --
   ASP.NET session-redirect pattern for licensing history, capacity, status, and
   specialist information.

For license-exempt providers, the detail URL uses `/childcare-service-providers/`
instead of `/childcare-licensing/`.

**Critical session behavior**: The gateway.maine.gov detail pages use a server-side
session redirect: `Default.aspx?id={id}` → 302 → `details.aspx`. Provider data is
loaded into the `ASP.NET_SessionId` session during the redirect. Each detail
request MUST use its own unique `cookiejar` to avoid session collisions.

"""

import re

from scrapy import FormRequest, Spider

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_BASE = "https://search.childcarechoices.me"
LICENSED_DETAIL = "https://gateway.maine.gov/dhhs/childcare-licensing/Default.aspx?id={id}"
EXEMPT_DETAIL = "https://gateway.maine.gov/dhhs/childcare-service-providers/Default.aspx?id={id}"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)


def _parse_plot_addresses(text):
    """Parse the plotAddresses JavaScript call in the search results.

    Returns a list of dicts, one per provider, with fields:
    - address, provider_name, phone, latitude, longitude, star_rating
    - provider_type, infant_slots, toddler_slots, preschool_slots,
      school_age_slots, openings_updated, accepts_ccap, license_number

    The function handles:
    - Star rating digit appended to address (e.g., '044014' → strip last char)
    - [br] markers in provider names (replaced with space)
    - Trailing whitespace on license numbers
    - "Never" openings date → None
    """
    match = re.search(r"plotAddresses\('", text)
    if not match:
        return []

    # Find the closing of plotAddresses call
    # Match plotAddresses('arg1', 'arg2', 'arg3', 'arg4', 'arg5')
    start = match.start() + len("plotAddresses('")
    paren_depth = 1
    end = start
    in_quote = True  # First char after opening quote is data
    while end < len(text) and paren_depth > 0:
        if text[end] == "(":
            paren_depth += 1
        elif text[end] == ")":
            paren_depth -= 1
            if paren_depth == 0:
                # Found the closing paren, now check if there's a trailing quote
                # The format is plotAddresses('arg1', ..., 'arg5')
                # After closing paren there might be ');' or similar
                break
        end += 1

    # Find the closing quote after the closing paren
    if paren_depth == 0 and end < len(text):
        end += 1
        while end < len(text) and text[end] not in (";", "'"):
            end += 1
        if end < len(text) and text[end] == "'":
            end += 1

    if paren_depth != 0:
        return []

    inner = text[start:end]
    # Parse the 5 arguments manually to handle single quotes within arguments
    # Strategy: split on commas that are outside of quotes
    args = []
    current = ""
    in_quote = True  # First char after plotAddresses(' is a quote
    i = 0
    while i < len(inner):
        c = inner[i]
        if c == "'" and (i == 0 or inner[i-1] != "\\"):
            in_quote = not in_quote
            current += c
        elif c == "," and not in_quote:
            # This is a separator comma
            args.append(current)
            current = ""
        else:
            current += c
        i += 1
    if current:
        args.append(current)

    if len(args) < 4:
        return []

    # Strip outer quotes and whitespace from each argument
    bigstring = args[0].strip().strip("'")
    providertype = args[1].strip().strip("'")
    agesserved = args[2].strip().strip("'")
    licensenumber = args[3].strip().strip("'")

    # Parse bigstring: providers separated by *, each provider:
    # {address}{star_digit}%{name}<{phone}~{lat}^{lng}
    # Address ends with a digit (0-5) or 'R' for restricted
    providers = []
    provider_entries = bigstring.split("*")

    # Parse provider types
    type_entries = providertype.split("~")
    # Parse ages
    age_entries = agesserved.split("@")
    # Parse license numbers
    lic_entries = licensenumber.split("!")

    # Use min length to avoid index errors (gotcha #3: agesserved may have extra entry)
    count = min(len(provider_entries), len(type_entries), len(lic_entries))

    for i in range(count):
        provider = {}

        # Parse bigstring entry
        entry = provider_entries[i].strip()
        if not entry:
            continue

        # Extract star rating digit from end of address
        # Last char is star rating: 0-5 or R (for "2 Restricted")
        star_match = re.search(r"^(.*?)([0-5R])%(.*?)$", entry)
        if star_match:
            address = star_match.group(1)
            star_rating = star_match.group(2)
            name_phone = star_match.group(3)
        else:
            # Fallback: try to extract just the last char before %
            match = re.search(r"(.*)%(.*)$", entry)
            if match:
                address = match.group(1)
                name_phone = match.group(2)
                star_rating = None
            else:
                address = entry
                name_phone = ""
                star_rating = None

        # Parse name and phone
        if "<" in name_phone:
            parts = name_phone.split("<", 1)
            name = parts[0]
            phone_rest = parts[1]
        else:
            name = name_phone
            phone_rest = ""

        # Parse phone, lat, lng
        if "~" in phone_rest:
            phone_rest_parts = phone_rest.split("~", 1)
            phone = phone_rest_parts[0]
            lat_lng = phone_rest_parts[1]
        else:
            phone = phone_rest
            lat_lng = ""

        if "^" in lat_lng:
            lat_lng_parts = lat_lng.split("^", 1)
            latitude = lat_lng_parts[0]
            longitude = lat_lng_parts[1].split(">")[0] if len(lat_lng_parts) > 1 else ""
        else:
            latitude = lat_lng
            longitude = ""

        # Handle [br] in name
        name = name.replace("[br]", " ")

        provider["address"] = address
        provider["provider_name"] = name if name.strip() else None
        provider["phone"] = phone if phone.strip() else None
        provider["latitude"] = latitude if latitude.strip() else None
        provider["longitude"] = longitude if longitude.strip() else None
        provider["star_rating"] = star_rating

        # Parse provider type (strip stray <br /> tags left over from the
        # source markup, then whitespace)
        type_raw = type_entries[i].strip() if i < len(type_entries) else ""
        provider["provider_type"] = re.sub(r"<br\s*/?>", "", type_raw).strip() or None

        # Parse ages and slots
        age_raw = age_entries[i] if i < len(age_entries) else ""
        provider["ages_raw"] = age_raw

        # Parse license number
        lic_raw = lic_entries[i].strip() if i < len(lic_entries) else ""
        provider["license_number"] = lic_raw.strip() or None

        providers.append(provider)

    return providers


def _normalize_date(value):
    """Normalize MM/DD/YYYY or MM/DD/YY to YYYY-MM-DD.

    The search page (me_openings_updated) uses 4-digit years; the detail
    page (status_date) uses 2-digit years. Both are routed through this
    function, so it must handle each format explicitly -- a bare regex
    without an end anchor would otherwise match the first two digits of a
    4-digit year and silently mangle the date (e.g. "09/11/2025" ->
    "2020-09-11").
    """
    if not value:
        return None
    stripped = value.strip()

    # 4-digit year, e.g. "09/11/2025"
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})$", stripped)
    if match:
        month, day, year = match.groups()
        return f"{year}-{month}-{day}"

    # 2-digit year, e.g. "01/15/25"
    match = re.match(r"(\d{2})/(\d{2})/(\d{2})$", stripped)
    if match:
        month, day, year = match.groups()
        # 2-digit year: >50 → 1900s, ≤50 → 2000s
        year_int = int(year)
        year_full = 1900 + year_int if year_int > 50 else 2000 + year_int
        return f"{year_full}-{month}-{day}"

    return value


class MaineSpider(Spider):
    """Maine child care provider search spider."""

    name = "maine"
    allowed_domains = ["search.childcarechoices.me", "gateway.maine.gov"]

    custom_settings = {
        "USER_AGENT": USER_AGENT,
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 6,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.4,
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408],
    }

    def __init__(self, start=None, end=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_zip = int(start) if start else 3901
        self.end_zip = int(end) if end else 4992
        self.seen_ids = set()

    def start_requests(self):
        """Iterate all Maine ZIP codes (03901-04992)."""
        for zipcode in range(self.start_zip, self.end_zip + 1):
            zip_str = f"{zipcode:05d}"
            url = f"{SEARCH_BASE}/?search={zip_str}&dist="
            yield FormRequest(
                url,
                callback=self.parse_search_page,
                meta={"zipcode": zip_str, "cookiejar": zip_str},
                dont_filter=True,
            )

    def parse_search_page(self, response):
        """Extract form tokens and POST with zip code."""
        zipcode = response.meta["zipcode"]
        viewstate = response.css('input#__VIEWSTATE::attr(value)').get('')
        event_validation = response.css('input#__EVENTVALIDATION::attr(value)').get('')

        if not viewstate or not event_validation:
            self.logger.warning("ZIP %s: missing form tokens, skipping", zipcode)
            return

        yield FormRequest(
            url=f"{SEARCH_BASE}/?search={zipcode}&dist=",
            callback=self.parse_results,
            method='POST',
            formdata={
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': 'CA0B0334',
                '__SCROLLPOSITIONX': '0',
                '__SCROLLPOSITIONY': '0',
                '__EVENTVALIDATION': event_validation,
                'ctl00$MainContent$txtAddress': zipcode,
                'ctl00$MainContent$ddwnDistance': '',
                'ctl00$MainContent$ddnlststar': 'ALL PROGRAMS',
                'ctl00$MainContent$Button1': 'Find Programs',
            },
            meta={"zipcode": zipcode, "cookiejar": zipcode},
            dont_filter=True,
        )

    def parse_results(self, response):
        """Parse plotAddresses call and fan out to detail pages."""
        zipcode = response.meta["zipcode"]

        # Check for passValidation (no results)
        if "passValidation" in response.text:
            self.logger.debug("ZIP %s: no results (passValidation)", zipcode)
            return

        # Find plotAddresses call
        plot_match = re.search(r"plotAddresses\('", response.text)
        if not plot_match:
            self.logger.debug("ZIP %s: no plotAddresses call found", zipcode)
            return

        providers = _parse_plot_addresses(response.text)
        if not providers:
            self.logger.debug("ZIP %s: parsed 0 providers from plotAddresses", zipcode)
            return

        new_providers = 0
        skipped_duplicates = 0

        for provider in providers:
            license_number = provider.get("license_number")
            if not license_number:
                continue

            # Deduplication
            if license_number in self.seen_ids:
                skipped_duplicates += 1
                continue

            self.seen_ids.add(license_number)
            new_providers += 1

            # Build partial ProviderItem from search data
            item = ProviderItem()
            item["source_state"] = "Maine"
            item["provider_url"] = f"{SEARCH_BASE}/?search={zipcode}&dist="

            # Address (already stripped of star rating digit)
            item["address"] = provider.get("address")

            # Parse coordinates
            lat = provider.get("latitude")
            lng = provider.get("longitude")
            if lat:
                item["latitude"] = lat
            if lng:
                item["longitude"] = lng

            item["provider_name"] = provider.get("provider_name")
            item["phone"] = provider.get("phone")
            item["license_number"] = license_number

            # Provider type from search
            item["provider_type"] = provider.get("provider_type")

            # Star rating
            star = provider.get("star_rating")
            if star:
                if star == "R":
                    item["me_star_rating"] = "2 Restricted"
                else:
                    item["me_star_rating"] = star

            # Parse ages/slots from agesserved field
            age_raw = provider.get("ages_raw", "")
            item = self._parse_ages(item, age_raw)

            # Determine detail URL (licensed vs exempt)
            if provider.get("provider_type"):
                prov_type = provider["provider_type"].upper()
                if "LICENSE EXEMPT" in prov_type:
                    detail_url = EXEMPT_DETAIL.format(id=license_number)
                else:
                    detail_url = LICENSED_DETAIL.format(id=license_number)
            else:
                detail_url = LICENSED_DETAIL.format(id=license_number)

            # Fan out to detail page
            yield FormRequest(
                detail_url,
                callback=self.parse_detail,
                meta={
                    "item": item,
                    "cookiejar": f"detail_{license_number}",
                },
                dont_filter=True,
            )

        self.logger.info(
            "ZIP %s: %d new providers found (%d duplicates skipped)",
            zipcode,
            new_providers,
            skipped_duplicates,
        )

    def _parse_ages(self, item, age_html):
        """Parse age slots and CCAP from ages_served HTML fragment."""
        if not age_html:
            return item

        # Slot counts
        infant_match = re.search(r'Open Infant Slots[^:]+:\s*(\d+)', age_html)
        toddler_match = re.search(r'Open Toddler Slots[^:]+:\s*(\d+)', age_html)
        preschool_match = re.search(r'Open Preschool Slots[^:]+:\s*(\d+)', age_html)
        school_match = re.search(r'Open School Age Slots[^:]+:\s*(\d+)', age_html)

        if infant_match:
            item["me_infant_slots"] = int(infant_match.group(1))
        if toddler_match:
            item["me_toddler_slots"] = int(toddler_match.group(1))
        if preschool_match:
            item["me_preschool_slots"] = int(preschool_match.group(1))
        if school_match:
            item["me_school_age_slots"] = int(school_match.group(1))

        # Openings updated date
        openings_match = re.search(r'Openings last updated:\s*([^<]+)', age_html)
        if openings_match:
            date_val = openings_match.group(1).strip()
            if date_val == "Never":
                item["me_openings_updated"] = None
            else:
                item["me_openings_updated"] = _normalize_date(date_val)

        # CCAP acceptance
        ccap_match = re.search(r'Accepts CCAP:\s*(\w+)', age_html)
        if ccap_match:
            ccap_val = ccap_match.group(1).strip()
            item["scholarships_accepted"] = ccap_val.upper() == "YES"

        return item

    def parse_detail(self, response):
        """Parse licensing detail page and merge with search data."""
        item = response.meta["item"]

        def _span(field_id):
            """Extract span text by ID, or None if blank."""
            val = response.css(f'span#{field_id}::text').get()
            return val.strip() if val and val.strip() else None

        # Status and status date
        item["status"] = _span("MainContent_ProgramStatusLabel")
        status_date = _span("MainContent_StatusDateLabel")
        if status_date:
            item["status_date"] = _normalize_date(status_date)

        # Capacity: pass a clean int when possible; otherwise pass the raw
        # string through so the shared pipeline normalizer can decide how to
        # handle non-numeric values (e.g. ranges like "6-12").
        capacity = _span("MainContent_CapacityLabel")
        if capacity:
            try:
                item["capacity"] = int(capacity)
            except (ValueError, TypeError):
                item["capacity"] = capacity

        # License holder (owner)
        item["license_holder"] = _span("MainContent_ProgramOwnerLabel")

        # Licensing specialist
        item["me_licensing_specialist"] = _span("MainContent_LicensingSpecialistNameLabel")
        item["me_licensing_specialist_email"] = _span("MainContent_LicensingSpecialistEmailLabel")

        # Temporary closed
        temp_closed = _span("MainContent_TemporaryClosedLabel")
        if temp_closed:
            item["me_temporarily_closed"] = temp_closed

        # Times renewed
        times_renewed = _span("MainContent_TimesRenewedLabel")
        if times_renewed:
            item["me_times_renewed"] = int(times_renewed) if times_renewed.isdigit() else times_renewed

        # Provider type from detail page (more descriptive)
        detail_type = _span("MainContent_ProgramTypeLabel")
        if detail_type:
            item["provider_type"] = detail_type

        # Licensing history and DocuWare link
        item["inspections"] = self._parse_licensing_history(response)

        # Final provider URL
        item["provider_url"] = response.url

        yield item

    def _parse_licensing_history(self, response):
        """Parse licensing history table and DocuWare link."""
        inspections = []

        # Find table with "Approval Date" header. Use "." (string-value of
        # the node, including descendant text) rather than "text()" -- the
        # real markup wraps the header text in a <b> tag
        # (<th><b>Approval Date</b></th>), so a text()-only predicate never
        # matches and the whole table is silently dropped.
        table = response.xpath(
            '//table[.//th[contains(., "Approval Date")] '
            'or .//td[contains(., "Approval Date")]]'
        )

        if table:
            # Get all rows (skip header)
            rows = table.xpath('.//tr[count(*) > 1]')
            for row in rows:
                cells = row.xpath('.//td/text()').getall()
                if len(cells) >= 4:
                    cells = [c.strip() for c in cells]
                    insp = InspectionItem()
                    insp["date"] = cells[0] or None  # Approval Date
                    insp["type"] = cells[1] or None  # License Type (Full/Conditional)
                    insp["me_licensed_from"] = cells[2] or None  # Licensed From
                    insp["me_licensed_to"] = cells[3] or None  # Licensed To
                    inspections.append(insp)

        # DocuWare link
        dw_href = response.css('a#MainContent_DocuWareListLink::attr(href)').get()
        if dw_href:
            insp = InspectionItem()
            insp["type"] = "Licensing Documents"
            insp["report_url"] = dw_href
            inspections.append(insp)

        return inspections if inspections else None
