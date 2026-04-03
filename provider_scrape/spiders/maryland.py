import asyncio
import logging
import os
import re

import pypdfium2 as pdfium
import scrapy
import tesserocr

from provider_scrape.items import InspectionItem, ProviderItem

# tessdata path for tesserocr — bundled fast model
TESSDATA_DIR = os.environ.get("TESSDATA_PREFIX", "/tmp/tessdata")


def extract_address_from_pdf(pdf_bytes):
    """Extract the precise address from an inspection report PDF via OCR.

    The PDFs are image-based with a consistent form layout. The ADDRESS field
    is on page 1, containing street, city, state, and zip in a row.
    """
    try:
        pdf = pdfium.PdfDocument(pdf_bytes)
        try:
            page = pdf[0]
            bitmap = page.render(scale=2)
            img = bitmap.to_pil()
        finally:
            pdf.close()

        # Crop to the ADDRESS region (consistent position across all reports)
        # At 2x scale on landscape letter (3300x2550), the ADDRESS row is ~y=1750-1950
        crop = img.crop((0, 1750, 3300, 1950))

        text = tesserocr.image_to_text(crop, path=TESSDATA_DIR)
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Find "ADDRESS:" and extract the address from the same or next line
        for i, line in enumerate(lines):
            if "ADDRESS" in line.upper():
                # Address may be after the colon on the same line
                after_colon = line.split(":", 1)[-1].strip()
                if after_colon:
                    return _format_address(after_colon)
                # Or on the next line
                if i + 1 < len(lines):
                    return _format_address(lines[i + 1])
                break

        return None
    except Exception as e:
        logging.getLogger("maryland").warning(f"OCR address extraction failed: {e}")
        return None


def _format_address(raw):
    """Format a raw OCR address string into 'street, city, state zip'."""
    # tesserocr returns: "325 N Howard Street Baltimore MD 21201"
    # Try to parse into components using the state code as anchor
    match = re.match(r"^(.+?)\s+(MD)\s+(\d{5}(?:-\d{4})?)$", raw.strip())
    if match:
        street_city = match.group(1).strip()
        state = match.group(2)
        zipcode = match.group(3)
        # The last word(s) before MD is the city — but city can be multi-word
        # so just return as "street city, MD zipcode"
        return f"{street_city}, {state} {zipcode}"
    return raw.strip()


class MarylandSpider(scrapy.Spider):
    name = "maryland"
    allowed_domains = ["checkccmd.org"]
    start_urls = ["https://www.checkccmd.org/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 10,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_fi = set()
        # Track requested pages per county to keep pagination chains independent
        self.requested_pages_by_county = {}

    def parse(self, response):
        """Extract form options, then launch a fresh session per county."""
        self.logger.info("Loaded search page, extracting form options...")

        self._fac_types = response.css(
            "#MainContent_ddlFacType option::attr(value)"
        ).getall()
        self._license_statuses = response.css(
            "#MainContent_ddlLicenseStatus option::attr(value)"
        ).getall()
        counties = response.css(
            "#MainContent_ddlCountyList option::attr(value)"
        ).getall()
        self._cities = response.css(
            "#MainContent_ddlCityList option::attr(value)"
        ).getall()

        self.logger.info(
            f"Form options: {len(self._fac_types)} types, "
            f"{len(self._license_statuses)} statuses, "
            f"{len(counties)} counties, {len(self._cities)} cities "
            f"— launching one search per county"
        )

        # Each county gets its own cookie jar (session) so concurrent searches
        # don't overwrite each other's server-side state. We load the search
        # page fresh for each county to get a clean ViewState + session.
        for county in counties:
            self.requested_pages_by_county[county] = set()
            yield scrapy.Request(
                "https://www.checkccmd.org/",
                callback=self.parse_county_search,
                cb_kwargs={"county_key": county},
                meta={"cookiejar": county},
                dont_filter=True,
            )

    def parse_county_search(self, response, county_key):
        """Submit the search form for a single county."""
        self.logger.info(f"[{county_key}] Submitting search...")
        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                "ctl00$MainContent$ddlFacType": self._fac_types,
                "ctl00$MainContent$ddlLicenseStatus": self._license_statuses,
                "ctl00$MainContent$ddlCountyList": [county_key],
                "ctl00$MainContent$ddlCityList": self._cities,
                "ctl00$MainContent$SearchButton": "SEARCH",
            },
            callback=self.parse_results,
            cb_kwargs={"county_key": county_key},
            meta={"cookiejar": county_key},
            dont_click=True,
            dont_filter=True,
        )

    def parse_results(self, response, county_key=None):
        """Parse the search results page with pagination."""
        pager_row = response.css("tr.dataPager")
        current_page = 1
        if pager_row:
            current_page_text = pager_row.css("span::text").get()
            if current_page_text and current_page_text.strip().isdigit():
                current_page = int(current_page_text.strip())

        total_text = response.css("#MainContent_lblTotalRows::text").get()
        self.logger.info(f"[{county_key}] page {current_page} — Total: {total_text}")

        # Extract provider detail links, deduplicating by facility ID
        rows = response.css("#grdResults tr.rowStyle")
        self.logger.info(
            f"[{county_key}] Found {len(rows)} provider rows on page {current_page}."
        )

        for row in rows:
            cols = row.css("td")
            link = cols[0].css("a::attr(href)").get() if cols else None
            if link:
                # Deduplicate by facility ID to avoid re-scraping detail pages
                # when duplicate pagination chains cause the same page to be
                # processed more than once.
                fi_match = re.search(r"fi=(\d+)", link)
                if fi_match:
                    fi = fi_match.group(1)
                    if fi in self.seen_fi:
                        continue
                    self.seen_fi.add(fi)

                # Extract fields only available on the results page
                address = (
                    cols[2].css("::text").get("").strip() if len(cols) > 2 else None
                )
                school_name = (
                    cols[4].css("::text").get("").strip() if len(cols) > 4 else None
                )
                program_type = (
                    cols[5].css("::text").get("").strip() if len(cols) > 5 else None
                )

                yield response.follow(
                    link,
                    callback=self.parse_detail,
                    cb_kwargs={
                        "address": address or None,
                        "school_name": school_name or None,
                        "program_type": program_type or None,
                    },
                )

        # Pagination - find next page via __doPostBack.
        # Track requested pages per county to keep chains independent.
        requested_pages = self.requested_pages_by_county.get(county_key, set())
        if pager_row:
            next_page = current_page + 1
            target_page = None

            next_link = pager_row.css(f'a[href*="Page${next_page}"]')
            if next_link:
                target_page = next_page
            else:
                # Check for "..." link which jumps to the next set of pages
                ellipsis_links = pager_row.css("a")
                for el_link in ellipsis_links:
                    text = el_link.css("::text").get("").strip()
                    href = el_link.attrib.get("href", "")
                    if text == "..." and f"Page${next_page}" not in href:
                        match = re.search(r"Page\$(\d+)", href)
                        if match:
                            jump_page = int(match.group(1))
                            if jump_page > current_page:
                                target_page = jump_page
                                break

            if target_page and target_page not in requested_pages:
                requested_pages.add(target_page)
                self.logger.info(f"[{county_key}] Navigating to page {target_page}...")
                yield scrapy.FormRequest.from_response(
                    response,
                    formdata={
                        "__EVENTTARGET": "ctl00$MainContent$grdResults",
                        "__EVENTARGUMENT": f"Page${target_page}",
                    },
                    callback=self.parse_results,
                    cb_kwargs={"county_key": county_key},
                    meta={"cookiejar": county_key},
                    dont_click=True,
                    dont_filter=True,
                )

    def parse_detail(self, response, address=None, school_name=None, program_type=None):
        """Parse a provider detail page."""
        # Check if we actually got the detail page (not redirected to search)
        if "FacilityDetail" not in response.url:
            self.logger.warning(
                f"Expected detail page but got: {response.url}. Skipping."
            )
            return

        item = ProviderItem()
        item["source_state"] = "Maryland"
        item["provider_url"] = response.url

        # Fields from the results page (not available on detail page)
        item["address"] = address
        item["md_school_name"] = school_name
        item["provider_type"] = program_type

        # Closed/suspended providers use a different panel with *Op suffixed IDs
        is_non_operating = (
            response.css("#MainContent_PnlNonOperating").get() is not None
        )

        if is_non_operating:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderNameOp"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicenseOp"
            )
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatusOp"
            )
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgramOp"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditationOp"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevelOp"
            )
        else:
            item["provider_name"] = self._get_span_text(
                response, "MainContent_txtProviderName"
            )
            item["license_number"] = self._get_span_text(
                response, "MainContent_txtLicense"
            )
            item["county"] = self._get_span_text(response, "MainContent_txtCounty")
            item["status"] = self._get_span_text(
                response, "MainContent_txtProviderStatus"
            )
            item["phone"] = self._get_span_text(response, "MainContent_txtPhone")
            item["email"] = self._get_span_text(response, "MainContent_txtEmail")

            # Capacity - may contain total + age breakdowns separated by <br> tags
            capacity_raw = self._get_span_html(response, "MainContent_txtCapacity")
            if capacity_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", capacity_raw) if p.strip()
                ]
                if parts:
                    item["capacity"] = parts[0]
                    if len(parts) > 1:
                        item["ages_served"] = "; ".join(parts[1:])

            # Hours
            hours_raw = self._get_span_html(response, "MainContent_txtHours")
            if hours_raw:
                parts = [
                    p.strip() for p in re.split(r"<br\s*/?>", hours_raw) if p.strip()
                ]
                item["hours"] = "; ".join(parts)

            # Maryland-specific fields
            item["md_approved_education"] = self._get_span_text(
                response, "MainContent_txtApprovedEducationProgram"
            )
            item["md_accreditation"] = self._get_span_text(
                response, "MainContent_txtAccreditation"
            )
            item["md_fatalities"] = self._get_span_text(
                response, "MainContent_txtFatalities"
            )
            item["md_serious_injuries"] = self._get_span_text(
                response, "MainContent_txtInjuries"
            )
            item["md_excels_level"] = self._get_span_text(
                response, "MainContent_txtEXCELSLevel"
            )

        # Inspections
        item["inspections"] = self._extract_inspections(response)

        # Try to get precise address from the first inspection report PDF
        first_report_url = self._get_first_report_url(response)
        if first_report_url:
            yield scrapy.Request(
                first_report_url,
                callback=self.parse_inspection_pdf,
                cb_kwargs={"item": item},
                dont_filter=True,
            )
        else:
            yield item

    async def parse_inspection_pdf(self, response, item):
        """Extract precise address from an inspection report PDF via OCR."""
        # Run CPU-bound OCR in a thread pool to avoid blocking the reactor
        pdf_bytes = response.body
        precise_address = await asyncio.to_thread(extract_address_from_pdf, pdf_bytes)
        if precise_address:
            self.logger.debug(
                f"OCR address for {item.get('provider_name')}: {precise_address}"
            )
            item["address"] = precise_address
        yield item

    def _get_first_report_url(self, response):
        """Get the URL of the first inspection report PDF from the detail page."""
        rows = response.css("#MainContent_grdInspection tr")
        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) >= 1:
                link = cols[0].css("a::attr(href)").get()
                if link:
                    return response.urljoin(link)
        return None

    def _extract_inspections(self, response):
        """Extract inspection records from the detail page."""
        inspections = []
        rows = response.css("#MainContent_grdInspection tr")

        for row in rows[1:]:
            cols = row.css("td")
            if len(cols) < 7:
                continue

            insp = InspectionItem()

            report_link = cols[0].css("a::attr(href)").get()
            if report_link:
                insp["report_url"] = response.urljoin(report_link)

            insp["date"] = cols[2].css("::text").get("").strip()
            insp["type"] = cols[3].css("::text").get("").strip()

            md_regulation = cols[4].css("::text").get("").strip()
            md_finding = cols[5].css("::text").get("").strip()
            md_status = cols[6].css("::text").get("").strip()

            insp["md_regulation"] = (
                md_regulation if md_regulation and md_regulation != "\xa0" else None
            )
            insp["md_finding"] = (
                md_finding if md_finding and md_finding != "\xa0" else None
            )
            insp["md_inspection_status"] = (
                md_status if md_status and md_status != "\xa0" else None
            )

            if insp.get("date") or insp.get("type"):
                inspections.append(insp)

        return inspections

    def _get_span_text(self, response, span_id):
        """Extract text from a span element by its ID."""
        texts = response.css(f"#{span_id}::text").getall()
        if texts:
            return " ".join(t.strip() for t in texts if t.strip())
        return None

    def _get_span_html(self, response, span_id):
        """Extract inner HTML from a span element by its ID."""
        span = response.css(f"#{span_id}")
        if span:
            outer = span.get()
            if outer:
                inner = re.sub(r"^<span[^>]*>", "", outer)
                inner = re.sub(r"</span>\s*$", "", inner)
                return inner
        return None
