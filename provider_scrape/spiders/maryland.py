import asyncio
import json
import logging
import os
import re

import pypdfium2 as pdfium
import scrapy
import tesserocr

from provider_scrape.items import InspectionItem, ProviderItem

# tessdata path for tesserocr — bundled fast model
TESSDATA_DIR = os.environ.get("TESSDATA_PREFIX", "/tmp/tessdata")

# Maryland EXCELS "Find a Program" public API. Keyed by license number, it
# returns the precise street address (with the house number the licensing site
# omits from HTML), lat/long coordinates, and the EXCELS rating breakdown — in
# one fast (~1-2s) JSON call on a separate, non-tarpitting domain. This replaces
# the slow inspection-report PDF download + OCR for the ~91% of inspected
# providers that participate in EXCELS. See maryland_performance_epic.
EXCELS_SEARCH_URL = (
    "https://findaprogram.marylandexcels.org/api/fap/search?license={license}"
)
EXCELS_REFERER = "https://findaprogram.marylandexcels.org/"

# Realistic browser UA applied to every request ("stealth-lite") to reduce the
# chance of tripping checkccmd.org's anti-bot protection under concurrency.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Results/pagination requests are cheap but chain-critical: each results page is
# what enqueues both the next page and that page's detail requests. Give them a
# priority well above the default (0) so a pagination postback jumps ahead of
# the large, server-throttled backlog of detail requests. Otherwise a postback
# can sit queued for hours behind the details, by which point its ASP.NET
# ViewState has gone stale and the retry silently returns the *previous* page —
# truncating that county's pagination chain (see maryland_performance_epic).
RESULTS_PRIORITY = 100

# A stale postback returns the wrong page; we re-issue the navigation toward the
# page we actually wanted. Bound those re-issues so repeated stale returns for
# one page are surfaced (logged at ERROR) and give up rather than looping.
MAX_NAV_ATTEMPTS = 5


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
    allowed_domains = ["checkccmd.org", "findaprogram.marylandexcels.org"]
    start_urls = ["https://www.checkccmd.org/"]

    custom_settings = {
        # Detail/results pages on checkccmd are cheap and parallelize cleanly
        # (measured: 8 concurrent → 4.5s wall, no penalty). The slow part —
        # inspection-report PDFs — is now only a small EXCELS-miss fallback,
        # routed to its own low-concurrency "checkccmd-pdf" slot so it can't
        # stall the detail scrape.
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 10,
        "USER_AGENT": USER_AGENT,
        # The slow, server-rendered inspection PDF is only fetched for the small
        # set of providers absent from EXCELS (when ocr_fallback is on). Pin
        # those to a dedicated low-concurrency slot so they can't stall the fast
        # detail scrape and stay below the ~6-concurrent point where the PDF
        # endpoint starts to tarpit.
        "DOWNLOAD_SLOTS": {
            "checkccmd-pdf": {"concurrency": 3, "delay": 0.4},
        },
    }

    def __init__(self, ocr_fallback=True, counties=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_fi = set()
        # Pages whose rows we've successfully parsed, per county. Used to avoid
        # re-extracting a page that's delivered twice (e.g. a late retry) and to
        # decide whether forward navigation still has somewhere to go.
        self.parsed_pages_by_county = {}
        # How many times we've issued a navigation postback for a given page,
        # per county. Caps stale-postback self-healing at MAX_NAV_ATTEMPTS.
        self.nav_attempts_by_county = {}
        # When EXCELS has no address for a provider, fall back to the slow
        # inspection-report PDF + OCR. Disable (``-a ocr_fallback=false``) for a
        # pure-fast run that downloads zero PDFs.
        self.ocr_fallback = str(ocr_fallback).lower() not in ("false", "0", "no")
        # Optional debug filter: ``-a counties="Howard,Carroll"`` restricts the
        # crawl to counties whose dropdown label contains one of these terms
        # (case-insensitive). Used for limited verification runs.
        if counties:
            self._county_filter = [c.strip().lower() for c in str(counties).split(",")]
        else:
            self._county_filter = None

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

        if self._county_filter:
            counties = [
                c
                for c in counties
                if any(term in c.lower() for term in self._county_filter)
            ]
            self.logger.info(
                f"County debug filter active — restricting to: {counties}"
            )

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
            self.parsed_pages_by_county[county] = set()
            self.nav_attempts_by_county[county] = {}
            yield scrapy.Request(
                "https://www.checkccmd.org/",
                callback=self.parse_county_search,
                cb_kwargs={"county_key": county},
                meta={"cookiejar": county},
                dont_filter=True,
                priority=RESULTS_PRIORITY,
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
            cb_kwargs={"county_key": county_key, "expected_page": 1},
            meta={"cookiejar": county_key},
            dont_click=True,
            dont_filter=True,
            priority=RESULTS_PRIORITY,
        )

    def parse_results(self, response, county_key=None, expected_page=1):
        """Parse the search results page, with self-healing pagination.

        Pagination is a sequential chain: each results page enqueues the next.
        Under load the licensing site throttles, and a paginated postback that
        times out and is retried can come back rendering the *previous* page —
        its ASP.NET ViewState went stale. We detect that mismatch
        (``current_page != expected_page``) and re-issue the navigation toward
        the page we actually wanted, using this fresh response, bounded by
        ``MAX_NAV_ATTEMPTS``. Previously such a stale postback silently
        truncated the county's chain (the old dedup guard refused to re-request
        the page), which dropped ~60% of providers in a full run.
        """
        pager_row = response.css("tr.dataPager")
        current_page = 1
        if pager_row:
            current_page_text = pager_row.css("span::text").get()
            if current_page_text and current_page_text.strip().isdigit():
                current_page = int(current_page_text.strip())

        # Self-heal a stale postback: the server rendered a different page than
        # we navigated to. Re-issue the navigation toward the page we wanted
        # (this fresh response carries a usable ViewState) without extracting
        # the wrong page's rows.
        if current_page != expected_page:
            self.logger.warning(
                f"[{county_key}] expected page {expected_page} but server "
                f"returned page {current_page} (stale postback) — re-navigating."
            )
            yield from self._navigate_to(response, county_key, expected_page)
            return

        total_text = response.css("#MainContent_lblTotalRows::text").get()
        self.logger.info(f"[{county_key}] page {current_page} — Total: {total_text}")

        # Skip a page that's already been parsed (e.g. a duplicate/late retry
        # delivery): don't re-extract its rows or re-drive pagination from it.
        parsed_pages = self.parsed_pages_by_county.setdefault(county_key, set())
        if current_page in parsed_pages:
            self.logger.debug(
                f"[{county_key}] page {current_page} already parsed — skipping."
            )
            return
        parsed_pages.add(current_page)

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

                # Pin the detail request to this county's cookie session. The
                # licensing site is ASP.NET, which holds a per-session request
                # lock — requests sharing one session are processed serially by
                # the server (~16s each). Without this, every detail request
                # across all counties shares the one default session and the
                # whole crawl serializes. Per-county jars let the ~24 counties'
                # detail requests run concurrently.
                yield response.follow(
                    link,
                    callback=self.parse_detail,
                    cb_kwargs={
                        "address": address or None,
                        "school_name": school_name or None,
                        "program_type": program_type or None,
                    },
                    meta={"cookiejar": county_key},
                )

        # Advance the chain to the next page (sequential, with windowed-pager
        # "..." jumps). The end of pagination is when no next link is offered.
        next_target = self._resolve_next_page(pager_row, current_page)
        if next_target and next_target not in parsed_pages:
            yield from self._navigate_to(response, county_key, next_target)

    @staticmethod
    def _resolve_next_page(pager_row, current_page):
        """Return the next page number to navigate to, or None at the last page.

        The pager renders only a window of page links; when the immediate next
        page isn't linked, a "..." link jumps to the start of the next window.
        """
        if not pager_row:
            return None
        next_page = current_page + 1
        if pager_row.css(f'a[href*="Page${next_page}"]'):
            return next_page
        # Fall back to the "..." link that jumps to the next set of pages.
        for el_link in pager_row.css("a"):
            text = el_link.css("::text").get("").strip()
            href = el_link.attrib.get("href", "")
            if text == "..." and f"Page${next_page}" not in href:
                match = re.search(r"Page\$(\d+)", href)
                if match:
                    jump_page = int(match.group(1))
                    if jump_page > current_page:
                        return jump_page
        return None

    def _navigate_to(self, response, county_key, target_page):
        """Issue a paginated postback to ``target_page``, bounded by retries.

        Shared by normal forward navigation and stale-postback self-healing, so
        repeated stale returns for the same page are capped at
        ``MAX_NAV_ATTEMPTS`` (and surfaced at ERROR) instead of looping or
        silently dying. The postback is high-priority so it resolves before its
        ViewState can go stale behind the detail-request backlog.
        """
        attempts = self.nav_attempts_by_county.setdefault(county_key, {})
        count = attempts.get(target_page, 0)
        if count >= MAX_NAV_ATTEMPTS:
            self.logger.error(
                f"[{county_key}] gave up navigating to page {target_page} after "
                f"{count} attempts (stale postbacks) — chain truncated here."
            )
            return
        attempts[target_page] = count + 1
        self.logger.info(
            f"[{county_key}] Navigating to page {target_page} (attempt {count + 1})..."
        )
        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                "__EVENTTARGET": "ctl00$MainContent$grdResults",
                "__EVENTARGUMENT": f"Page${target_page}",
            },
            callback=self.parse_results,
            cb_kwargs={"county_key": county_key, "expected_page": target_page},
            meta={"cookiejar": county_key},
            dont_click=True,
            dont_filter=True,
            priority=RESULTS_PRIORITY,
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

        first_report_url = self._get_first_report_url(response)

        # Enrich the precise address (house number for centers) + rooftop
        # coordinates from the Maryland EXCELS public API, keyed by license
        # number. Falls back to the inspection-report PDF + OCR only when EXCELS
        # has no record at all (and ocr_fallback is enabled).
        license_number = item.get("license_number")
        if license_number and str(license_number).strip().isdigit():
            yield scrapy.Request(
                EXCELS_SEARCH_URL.format(license=str(license_number).strip()),
                callback=self.parse_excels,
                cb_kwargs={"item": item, "first_report_url": first_report_url},
                headers={"Referer": EXCELS_REFERER, "Accept": "application/json"},
                dont_filter=True,
            )
        else:
            yield from self._address_fallback(item, first_report_url)

    def parse_excels(self, response, item, first_report_url=None):
        """Enrich the item with a precise address + coordinates from EXCELS.

        The EXCELS ``search?license=`` endpoint returns ``{"data": [ {...} ]}``.
        A record carries rooftop-accurate ``lat``/``long`` for both centers and
        family homes (verified: a family-home coordinate reverse-geocodes to the
        exact house, and a center coordinate matches an independent geocode of
        its house-numbered address to 0 m). So whenever EXCELS has a record we
        capture the precise location and are done — no PDF:

        * **Centers** also expose a house-numbered street address, so we adopt
          it as ``address``.
        * **Family homes** have the house number withheld from the address
          string, but the rooftop coordinate already pins the exact location
          (and the number is recoverable from it by reverse-geocoding), so the
          PDF offers no accuracy upside.

        Only a *true* EXCELS miss (no record at all) optionally falls back to the
        inspection-report PDF + OCR.
        """
        record = None
        try:
            data = json.loads(response.text).get("data")
            if isinstance(data, list) and data:
                record = data[0]
        except (ValueError, AttributeError) as e:
            self.logger.debug(
                f"EXCELS parse failed for license "
                f"{item.get('license_number')}: {e}"
            )

        if record:
            self._apply_excels_location(item, record)
            street = (record.get("streetAddress") or "").strip()
            if street and street[0].isdigit():
                # House-numbered address (centers): adopt the full address.
                item["address"] = self._compose_excels_address(record)
            # Family homes keep the results-page street-name address plus the
            # rooftop coordinates captured above.
            yield item
            return

        # True EXCELS miss: optionally OCR the inspection-report PDF.
        yield from self._address_fallback(item, first_report_url)

    @staticmethod
    def _apply_excels_location(item, record):
        """Set coordinates + structured city/state/ZIP from an EXCELS record.

        The normalization pipeline fills city/state/zip only when absent, so
        these authoritative values are not clobbered downstream.
        """
        lat = record.get("lat")
        lon = record.get("long")
        if lat is not None:
            item["latitude"] = str(lat)
        if lon is not None:
            item["longitude"] = str(lon)
        city = (record.get("city") or "").strip()
        zip_code = (record.get("zipcode") or "").strip()
        if city:
            item["city"] = city
        if (record.get("state") or "").strip().lower().startswith("maryland"):
            item["state"] = "MD"
        if zip_code:
            item["zip"] = zip_code

    def _address_fallback(self, item, first_report_url):
        """Yield the item, using PDF/OCR for a precise address when enabled.

        The PDF request is pinned to the low-concurrency ``checkccmd-pdf``
        download slot so the slow, server-rendered reports can't stall the
        detail/results crawl.
        """
        if self.ocr_fallback and first_report_url:
            yield scrapy.Request(
                first_report_url,
                callback=self.parse_inspection_pdf,
                cb_kwargs={"item": item},
                meta={"download_slot": "checkccmd-pdf"},
                dont_filter=True,
            )
        else:
            yield item

    @staticmethod
    def _compose_excels_address(record):
        """Join EXCELS address parts into 'street, city, MD zip'."""
        street = (record.get("streetAddress") or "").strip()
        city = (record.get("city") or "").strip()
        state = (record.get("state") or "").strip()
        if state.lower().startswith("maryland"):
            state = "MD"
        zip_code = (record.get("zipcode") or "").strip()
        state_zip = " ".join(p for p in [state, zip_code] if p)
        city_state_zip = ", ".join(p for p in [city, state_zip] if p)
        return ", ".join(p for p in [street, city_state_zip] if p)

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
