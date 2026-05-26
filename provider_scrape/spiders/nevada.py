import re
from urllib.parse import urlencode, urljoin

import scrapy
from scrapy.http import HtmlResponse

from ..items import InspectionItem, ProviderItem


DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
DAY_ABBR = {d: d[:3] for d in DAY_NAMES}

# Fields from each search row required to build a detail-page URL.
DETAIL_URL_FIELDS = (
    "hLicenseeId",
    "hfProgram",
    "hLicenseTypeCode",
    "hLicenseeType",
    "hdnentityType",
    "hfLicenseNumber",
    "HfAddressTypeCode",
    "hLicenseId",
)


def short_name(name):
    """Return the suffix of an ASP.NET control name after the last `$`."""
    return name.rsplit("$", 1)[-1] if name else name


def collect_row_fields(row):
    """Build a {short_field_name: value} map for one search-result row."""
    fields = {}
    for hidden in row.css('input[type="hidden"]'):
        name = hidden.attrib.get("name", "")
        value = hidden.attrib.get("value", "")
        if name:
            fields[short_name(name)] = value
    return fields


def build_detail_url(base_url, row_fields):
    """Construct the SODPublicView detail-page URL from row hidden fields."""
    params = {
        "LicenseeId": row_fields.get("hLicenseeId", ""),
        "Program": row_fields.get("hfProgram", ""),
        "CredentialType": row_fields.get("hLicenseTypeCode", ""),
        "LicenseeType": row_fields.get("hLicenseeType", ""),
        "mode": "",
        "EntityType": row_fields.get("hdnentityType", ""),
        "LicenseNumber": row_fields.get("hfLicenseNumber", ""),
        "AddressTypeCode": row_fields.get("HfAddressTypeCode", ""),
        "LicenseId": row_fields.get("hLicenseId", ""),
        "LikePopUp": "",
        "Mode": "V",
        "IsPopUp": "Y",
    }
    return urljoin(base_url, "/Protected/INS/SODPublicView.aspx?" + urlencode(params))


def base_facility_type(credential_type):
    """Strip parenthetical modifiers from a credential type ("CENTER (PROVISIONAL)" -> "CENTER")."""
    if not credential_type:
        return None
    return re.sub(r"\s*\(.*?\)\s*", "", credential_type).strip() or None


def clean_text(text):
    if text is None:
        return None
    cleaned = text.replace("\xa0", " ").strip()
    return cleaned or None


def format_hours(day_rows):
    """Format a list of (day, [span_texts]) into a single-line hours string.

    Each day's span_texts are the values displayed in the Hours-of-Operation
    grid (status, from-hour, from-min, from-ampm, to-hour, to-min, to-ampm).
    Closed days are omitted from the result.
    """
    parts = []
    for day, spans in day_rows:
        if not spans:
            continue
        status = spans[0] if spans else ""
        if not status or status.lower().startswith("closed"):
            continue
        if len(spans) < 7:
            parts.append(f"{DAY_ABBR.get(day, day)} {status}")
            continue
        from_hh, from_mm, from_ampm = spans[1], spans[2] or "00", spans[3]
        to_hh, to_mm, to_ampm = spans[4], spans[5] or "00", spans[6]
        if from_hh and to_hh:
            parts.append(
                f"{DAY_ABBR.get(day, day)} {from_hh}:{from_mm} {from_ampm}"
                f" - {to_hh}:{to_mm} {to_ampm}"
            )
    return "; ".join(parts) if parts else None


def format_age_range(from_age, to_age):
    """Render the primary age range, handling weeks/years units."""
    from_age = clean_text(from_age)
    to_age = clean_text(to_age)
    if not from_age and not to_age:
        return None
    # If the "to" is a bare number, assume years (matches the page's "years" suffix).
    if to_age and re.fullmatch(r"\d+(\.\d+)?", to_age):
        to_age = f"{to_age} years"
    if not from_age:
        return f"up to {to_age}"
    if not to_age:
        return f"from {from_age}"
    return f"{from_age} - {to_age}"


class NevadaSpider(scrapy.Spider):
    name = "nevada"
    allowed_domains = ["nvdpbh.aithent.com"]
    start_urls = [
        "https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx?Program=HF&PubliSearch=Y"
    ]

    custom_settings = {
        # Per-row detail fetches are cheap GETs but be polite to the state site.
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 0.5,
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"playwright": True, "playwright_include_page": True},
            )

    async def parse(self, response):
        self.logger.info("Loading Nevada search page")
        page = response.meta.get("playwright_page")
        if not page:
            self.logger.error("Playwright page not available; cannot submit search")
            return

        try:
            bu_selector = (
                "#ctl00_ContentPlaceHolder1_ucLicenseeSearchPublic_ddlBusinessUnit"
            )
            await page.wait_for_selector(bu_selector, timeout=60000)
            await page.select_option(bu_selector, "CCP")
            self.logger.info("Selected Business Unit: CCP")

            content = await page.content()
            rendered = HtmlResponse(url=response.url, body=content, encoding="utf-8")

            formdata = {}
            for hidden in rendered.css('input[type="hidden"]'):
                name = hidden.attrib.get("name")
                if name:
                    formdata[name] = hidden.attrib.get("value", "")
            formdata[
                "ctl00$ContentPlaceHolder1$ucLicenseeSearchPublic$ddlBusinessUnit"
            ] = "CCP"
            formdata["ctl00$ContentPlaceHolder1$CommonLinkButton1"] = "Search"

            yield scrapy.FormRequest(
                url=response.url,
                formdata=formdata,
                callback=self.parse_search_results,
                meta={"playwright": False, "visited_pages": {1}, "page_num": 1},
            )
        finally:
            try:
                await page.close()
            except Exception:
                pass

    def parse_search_results(self, response):
        page_num = response.meta.get("page_num", 1)
        total_records_value = response.css(
            'input[id$="hdnTotalRecords"]::attr(value)'
        ).get()
        total_records = (
            int(total_records_value) if total_records_value and total_records_value.isdigit() else None
        )

        rows = response.css('table[id*="ResultsGrid"] tr')
        provider_rows = [r for r in rows if r.css('input[type="hidden"]')]
        self.logger.info(
            "Page %s: parsing %s provider rows (total reported: %s)",
            page_num,
            len(provider_rows),
            total_records,
        )

        for row in provider_rows:
            row_fields = collect_row_fields(row)
            if not row_fields.get("hfName") or not row_fields.get(
                "hfLicenseNumberToDisplay"
            ):
                continue
            yield from self.dispatch_provider(response, row_fields)

        yield from self.follow_pagination(response, page_num)

    def dispatch_provider(self, response, row_fields):
        partial_item = self.build_provider_from_row(row_fields)
        detail_url = build_detail_url(response.url, row_fields)
        partial_item["provider_url"] = detail_url

        yield scrapy.Request(
            url=detail_url,
            callback=self.parse_detail,
            errback=self.detail_errback,
            meta={"partial_item": partial_item, "playwright": False},
            dont_filter=True,
        )

    def build_provider_from_row(self, row_fields):
        item = ProviderItem()
        item["source_state"] = "NV"

        item["provider_name"] = clean_text(row_fields.get("hfName"))
        item["license_number"] = clean_text(row_fields.get("hfLicenseNumberToDisplay"))
        # hdnStatusCode is the human-readable label ("Active"); hdnStatus is the code ("ACT").
        item["status"] = clean_text(row_fields.get("hdnStatusCode")) or clean_text(
            row_fields.get("hdnStatus")
        )
        item["address"] = clean_text(row_fields.get("hPrimaryAddress"))
        item["phone"] = clean_text(row_fields.get("hPhoneNumber"))
        item["email"] = clean_text(row_fields.get("hEmail"))
        item["county"] = clean_text(row_fields.get("hdCounty"))
        item["license_expiration"] = clean_text(row_fields.get("hExpiryDate"))
        item["administrator"] = clean_text(row_fields.get("hContactName"))

        credential_type = clean_text(row_fields.get("hCredentialType"))
        item["nv_credential_type"] = credential_type
        facility_type = base_facility_type(credential_type)
        item["nv_facility_type"] = facility_type
        item["provider_type"] = facility_type
        item["nv_operation_id"] = clean_text(row_fields.get("hLicenseId"))

        item["inspections"] = []
        return item

    def follow_pagination(self, response, page_num):
        page_links = response.xpath(
            '//a[contains(@href, "Page$") and not(contains(@href, "..."))]/@href'
        ).getall()
        visited = response.meta.get("visited_pages", {page_num})

        next_page_num = None
        for href in page_links:
            match = re.search(r"Page\$(\d+)", href)
            if not match:
                continue
            candidate = int(match.group(1))
            if candidate in visited:
                continue
            if next_page_num is None or candidate < next_page_num:
                next_page_num = candidate

        if next_page_num is None:
            self.logger.info(
                "Pagination complete - last page: %s, pages visited: %s",
                page_num,
                len(visited),
            )
            return

        self.logger.info("Pagination: page %s -> page %s", page_num, next_page_num)

        formdata = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ucLicenseeSearchResult$ResultsGrid",
            "__EVENTARGUMENT": f"Page${next_page_num}",
        }
        for hidden in response.css('input[type="hidden"]'):
            name = hidden.attrib.get("name")
            if name:
                formdata[name] = hidden.attrib.get("value", "")

        yield scrapy.FormRequest(
            url=response.url,
            formdata=formdata,
            callback=self.parse_search_results,
            meta={
                "playwright": False,
                "visited_pages": visited | {next_page_num},
                "page_num": next_page_num,
            },
            dont_filter=True,
        )

    def parse_detail(self, response):
        item = response.meta["partial_item"]
        license_number = item.get("license_number")
        self.logger.info(
            "Parsing detail page for provider: %s (%s)",
            item.get("provider_name"),
            license_number,
        )

        detail = self.extract_detail_fields(response)
        for key, value in detail.items():
            if value is not None:
                item[key] = value

        inspections = self.extract_inspections(response)
        item["inspections"] = inspections
        self.logger.info(
            "Detail for %s: capacity=%s, ages=%s, inspections=%s",
            license_number,
            item.get("capacity"),
            item.get("ages_served"),
            len(inspections),
        )
        yield item

    def detail_errback(self, failure):
        item = failure.request.meta.get("partial_item")
        if item is not None:
            self.logger.warning(
                "Detail fetch failed for %s (%s); yielding partial item",
                item.get("provider_name"),
                item.get("license_number"),
            )
            yield item

    def extract_detail_fields(self, response):
        """Extract capacity, ages_served, hours from the detail page."""
        # Capacity: total spaces requested
        total = clean_text(
            response.css("#ctl00_ContentPlaceHolder1_ucChildrenAge_lblTotal::text").get()
        )
        capacity = int(total) if total and total.isdigit() else None

        # Ages served: primary age range (Row 1)
        from_age = response.css(
            "#ctl00_ContentPlaceHolder1_ucChildrenAge_txtRow1Age1::attr(value)"
        ).get()
        to_age = response.css(
            "#ctl00_ContentPlaceHolder1_ucChildrenAge_txtRow1Age2::attr(value)"
        ).get()
        ages_served = format_age_range(from_age, to_age)

        hours = format_hours(self.extract_hours_rows(response))

        return {
            "capacity": capacity,
            "ages_served": ages_served,
            "hours": hours,
        }

    def extract_hours_rows(self, response):
        """Yield (day_name, [span_texts]) for each day row of the hours grid.

        Iterates per-<td> so positional alignment is preserved even when a
        cell renders as an empty span (live HTML uses &nbsp; but empty is
        also possible).
        """
        rows = []
        hours_rows = response.css(
            'table[id$="ucHoursOfOperation_ucGridUserControl_ResultsGrid"] tr'
        )
        for row in hours_rows:
            day = clean_text(row.css('span[id$="_lblDay"]::text').get())
            if not day:
                continue
            cells = row.css("td")
            # The first cell holds the day label; remaining cells are the
            # disabled dropdown spans (status, fromHH, fromMM, fromAMPM,
            # toHH, toMM, toAMPM).
            spans = []
            for cell in cells[1:]:
                text = cell.css("span.dropDownDisableSection::text").get()
                spans.append(clean_text(text) or "")
            rows.append((day, spans))
        return rows

    def extract_inspections(self, response):
        """Parse the Statement of Deficiency grid into InspectionItem list."""
        results = []
        sod_rows = response.css(
            'table[id$="ucSODgrid_ResultsGrid"] tr'
        )
        for row in sod_rows:
            inspection_number = clean_text(
                row.css('span[id$="_lblInspectionNumber"]::text').get()
            )
            inspection_date = clean_text(
                row.css('span[id$="_lblInspectionEndDate"]::text').get()
            )
            if not inspection_number and not inspection_date:
                continue
            inspection_reason = clean_text(
                row.css('span[id$="_InspectionReason"]::text').get()
            )
            # lblCount can be wrapped in a <font> tag in the live HTML, so pull
            # text from any descendant rather than the span's direct text node.
            count_text = clean_text(
                " ".join(
                    row.css('span[id$="_lblCount"] *::text, span[id$="_lblCount"]::text').getall()
                )
            )
            count_match = re.search(r"\d+", count_text or "")
            deficiency_count = int(count_match.group(0)) if count_match else None

            status_code = clean_text(
                row.css('input[id$="_hdSODStatusCode"]::attr(value)').get()
            )
            status_reason = clean_text(
                row.css('input[id$="_hdSODStatusReasonCode"]::attr(value)').get()
            )

            inspection = InspectionItem()
            inspection["date"] = inspection_date
            inspection["type"] = inspection_reason
            inspection["original_status"] = status_code
            inspection["corrective_status"] = status_reason
            inspection["nv_inspection_number"] = inspection_number
            inspection["nv_deficiency_count"] = deficiency_count
            results.append(inspection)
        return results
