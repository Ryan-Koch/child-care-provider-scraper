"""Kansas child care provider spider.

Source: KDHE/KOEC's "Online Facility Compliance Search" (OIDS) at
https://khap.kdhe.ks.gov/OIDS/OIDS_Search.aspx -- a classic ASP.NET WebForms
app (Telerik RadControls). No Playwright: plain ``FormRequest``/``Request``
throughout. See tasks/kansas_epic/kansas_plan.md for the full recon writeup;
the three traps that will silently corrupt the dataset if missed are:

  1. **Pagination state lives in the ASP.NET Session, not in the posted
     ``__VIEWSTATE``.** Every county gets its own ``cookiejar``, and within a
     county the ``Next`` postbacks must be strictly sequential -- always
     yielded from the previous page's callback, never fanned out.
  2. **The ``nextBtn`` never reliably signals "no more pages."** Termination
     is on raw row count (``len(rows) < PAGE_SIZE``) only, never the button's
     state. ``MAX_PAGES`` is a loud safety valve, not the real stop rule.
  3. **The visible county text input is ignored by the server.** The Telerik
     ``_ClientState`` JSON blob is what the search handler actually reads,
     and ``searchBtn=Search`` must be posted explicitly -- lxml does not
     collect ``<button>`` elements, so plain ``FormRequest(formdata=...)`` is
     used throughout rather than ``FormRequest.from_response``.

Three phases, only two callbacks do real work:

  ``start_requests`` -> GET the search page -> ``parse_counties`` scrapes the
  106 county names (105 Kansas counties + the pseudo-county "Out of state")
  and fires one search POST per county, each in its own cookiejar.
  ``parse_results`` parses one results page (10 rows/page): every row mints a
  detail request (fully concurrent, ``dont_merge_cookies`` -- minting is
  stateless, see Sec 2.4) and, if the page was full, chains the next page
  postback sequentially. ``parse_detail`` renders one ``ProviderItem`` from
  the stable detail spans plus the listing row carried in ``meta`` (the
  listing is the only source for city/zip/county on the ~38% of facilities
  that suppress their address -- Sec 5.4).
"""

import json
import re

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://khap.kdhe.ks.gov/OIDS/OIDS_Search.aspx"

PAGE_SIZE = 10
# Safety valve only -- never the real stop rule (Sec 5.2). The true maximum is
# 72 (Johnson). Set too low, this silently truncates the biggest counties with
# no error of any kind -- that is exactly the failure mode this guards.
MAX_PAGES = 300

# 106 counties expected (105 Kansas counties + "Out of state"); warn (not
# error -- KDHE could plausibly add/rename one) if a scrape finds far fewer.
EXPECTED_MIN_COUNTIES = 100

# Owner-suppression marker. Match on the prefix only: the real string uses a
# CURLY apostrophe (U+2019) -- "Not Displayed by Owner’s Request" -- not
# a straight one.
SUPPRESSED_PREFIX = "Not Displayed"

COUNTY_DROPDOWN_SEL = "#ctl00_ContentPlaceHolder1_countyComboList_DropDown li.rcbItem::text"
COUNTY_CLIENTSTATE_FIELD = "ctl00_ContentPlaceHolder1_countyComboList_ClientState"
PROGRAM_TYPE_CLIENTSTATE_FIELD = "ctl00_ContentPlaceHolder1_programTypeListDetails_ClientState"

# Program Type list-box indices: 0 is the "Please Select" placeholder (must
# NOT be included -- that alone returns 0 rows), 1-7 are the seven real types.
PROGRAM_TYPE_CLIENT_STATE = json.dumps(
    {
        "isEnabled": True,
        "logEntries": [],
        "selectedIndices": [1, 2, 3, 4, 5, 6, 7],
        "checkedIndices": [],
        "scrollPosition": 0,
    }
)

# One row is a name-link table immediately followed by a details table; the
# two selectors below are 1:1 and same-order (verified against the live
# captures), so results are paired positionally with zip().
ROW_LINK_SEL = 'a[id^="ContentPlaceHolder1_SearchLink."]'
ROW_DETAIL_TABLE_SEL = 'table[runat="server"]'

# Detail page span ids. Two are misspelled at source -- copied exactly.
DETAIL_FIELD_IDS = {
    "name": "ctl00_ContentPlaceHolder1_faciltyNameValue",
    "license_number": "ctl00_ContentPlaceHolder1_licnseNumberValue",
    "owner": "ctl00_ContentPlaceHolder1_OwnerValue",
    "program_type": "ctl00_ContentPlaceHolder1_ProgramTypeValue",
    "address": "ctl00_ContentPlaceHolder1_AddressValue",
    "eff_date": "ctl00_ContentPlaceHolder1_LicenseEffDateVal",
    "exp_date": "ctl00_ContentPlaceHolder1_LicenseExpDateVal",
    "status": "ctl00_ContentPlaceHolder1_FacilityStatusValue",
    "phone": "ctl00_ContentPlaceHolder1_TelephoneNumberValue",
    "capacity": "ctl00_ContentPlaceHolder1_MaximumCapacityValue",
}

LICENSE_GRID_ID = "ctl00_ContentPlaceHolder1_licenseSurveyGrid_ctl00"
COMPLAINT_GRID_ID = "ctl00_ContentPlaceHolder1_complSurveyGrid_ctl00"
ORDER_GRID_ID = "ctl00_ContentPlaceHolder1_adminOrderGrid_ctl00"

FINDINGS_RE = re.compile(r"\((\d+)\s*/\s*(\d+)\)")
FINDINGS_SPAN_ID = "ctl00_ContentPlaceHolder1_DataLabel"

# Whitespace collapse before regex matching (the detail Address span has a
# literal <br/> between street and city -- see split_address docstring).
_WHITESPACE_RE = re.compile(r"\s+")

_ADDRESS_TAIL = re.compile(r"\s*,\s*([A-Z]{2})\s+(\d{5})(?:\s*-\s*(\d{4}))?\s*$")


def split_address(raw, city):
    """Re-insert the comma Kansas omits between street and city.

    ``raw`` is the detail page's Address value (already whitespace-collapsed
    -- Kansas separates street/city with a bare ``<br/>``, not a comma),
    ``city`` the City from the search listing. Returns ``(address, street)``.
    Conservative: if the tail doesn't match or the head doesn't actually end
    with ``city``, the raw string is returned unchanged and ``street`` is
    ``None`` -- never guess where the city starts.

    Validated live on 248/248 + 68/68 real (non-suppressed) Kansas addresses
    with zero fallbacks (kansas_plan.md Sec 6.5).
    """
    if not raw or not city:
        return raw, None
    m = _ADDRESS_TAIL.search(raw)
    if not m:
        return raw, None
    state, zip5, plus4 = m.group(1), m.group(2), m.group(3)
    head = raw[: m.start()].rstrip()
    if not head.lower().endswith(city.lower()):
        return raw, None
    street = head[: len(head) - len(city)].rstrip()
    if not street:
        return raw, None
    tail = f"{zip5}-{plus4}" if plus4 and plus4 != "0000" else zip5
    return f"{street}, {city}, {state} {tail}", street


def _hidden_fields(response):
    """Every ``<input type="hidden">`` name/value pair on the current page.

    Response-agnostic by design: the bare search page, a results page, and a
    detail page each carry a different set of hidden fields, and this always
    returns exactly what THIS response has -- which is also exactly what a
    real browser would echo back on its next postback.
    """
    fields = {}
    for inp in response.css('input[type="hidden"]'):
        name = inp.attrib.get("name")
        if name:
            fields[name] = inp.attrib.get("value", "")
    return fields


def _county_client_state(county):
    return json.dumps(
        {
            "logEntries": [],
            "value": county,
            "text": county,
            "enabled": True,
            "checkedIndices": [],
            "checkedItemsTextOverflows": False,
        }
    )


def _search_formdata(response, county):
    """Build the search POST body for one county.

    The county text input is cosmetic -- the server only reads the
    ClientState JSON blob (Sec 5.3) -- but it's set too for realism, since it
    costs nothing. ``searchBtn`` must be posted explicitly: lxml/
    ``FormRequest.from_response`` never collects ``<button>`` elements.
    """
    formdata = _hidden_fields(response)
    formdata["__EVENTTARGET"] = ""
    formdata["__EVENTARGUMENT"] = ""
    formdata["ctl00$ContentPlaceHolder1$countyComboList"] = county
    formdata[COUNTY_CLIENTSTATE_FIELD] = _county_client_state(county)
    formdata[PROGRAM_TYPE_CLIENTSTATE_FIELD] = PROGRAM_TYPE_CLIENT_STATE
    formdata["ctl00$ContentPlaceHolder1$gre_captcha_resp_value"] = ""
    formdata["ctl00$ContentPlaceHolder1$searchBtn"] = "Search"
    return formdata


def _next_formdata(response):
    """Build the ``Next`` postback body from the CURRENT results page."""
    formdata = _hidden_fields(response)
    formdata["__EVENTTARGET"] = ""
    formdata["__EVENTARGUMENT"] = ""
    formdata["ctl00$ContentPlaceHolder1$nextBtn"] = "Next"
    return formdata


def _zip5(raw):
    """First 5 digits of a listing zip (``"66743 - 0000"`` -> ``"66743"``)."""
    if not raw:
        return None
    head = raw.split("-", 1)[0].strip()
    return head or None


def _cell_text(td):
    """Join a grid ``<td>``'s text nodes, dropping blanks and bare ``&nbsp;``.

    Multi-line cells (e.g. an admin order Reason with several ``<br/>``
    -separated notes) are joined with "; " rather than glued together.
    """
    texts = [t.strip() for t in td.css("::text").getall()]
    texts = [t for t in texts if t and t != "\xa0"]
    return "; ".join(texts) if texts else None


class KansasSpider(scrapy.Spider):
    """Spider for Kansas KDHE/KOEC child care licensing data (OIDS)."""

    name = "kansas"
    allowed_domains = ["khap.kdhe.ks.gov"]
    source_state = "Kansas"

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 120,  # Next postbacks slow under load (up to ~15s)
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,  # no robots.txt exists (404)
        "USER_AGENT": ("Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0"),
    }

    def __init__(self, counties=None, findings=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Debug/smoke-test hook: `-a counties=Chase,Crawford,Barton` restricts
        # the crawl to a handful of counties instead of all 106. Off by
        # default (full statewide crawl).
        self.county_filter = None
        if counties:
            self.county_filter = {c.strip().lower() for c in str(counties).split(",") if c.strip()}
        # `-a findings=1` opts into fetching OIDS_ViewFacilityFindings.aspx
        # for every survey with a non-zero findings count (Sec 6.3). Off by
        # default -- it roughly doubles the crawl.
        self.fetch_findings = bool(int(findings))

        self.seen = set()  # SearchLink tokens already scheduled for a detail
        self.duplicate_rows = 0  # in-source duplicate rows skipped (Sec 5.6)
        self.county_running_total = {}
        self.county_final_pages = {}

    # ------------------------------------------------------------------ #
    # Phase 1 -- county enumeration + search
    # ------------------------------------------------------------------ #

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_counties, dont_filter=True)

    def parse_counties(self, response):
        """Scrape the county dropdown and fire one search POST per county.

        A single bootstrap GET's ``__VIEWSTATE`` seeds all 106 search POSTs --
        viewstate is not session-bound (verified live: a viewstate minted in
        one session drove a successful search in a brand-new session), so one
        GET is comfortably enough for the whole fan-out.
        """
        counties = [c.strip() for c in response.css(COUNTY_DROPDOWN_SEL).getall() if c.strip()]
        if len(counties) < EXPECTED_MIN_COUNTIES:
            self.logger.warning(
                "Kansas: only %d counties found on the search page (expected "
                "~106) -- the site's markup may have changed",
                len(counties),
            )
        if self.county_filter:
            counties = [c for c in counties if c.lower() in self.county_filter]
        self.logger.info("Kansas: %d counties queued for search", len(counties))

        for county in counties:
            self.county_running_total[county] = 0
            yield scrapy.FormRequest(
                SEARCH_URL,
                formdata=_search_formdata(response, county),
                callback=self.parse_results,
                # Mandatory, not a nicety: two counties sharing a cookiejar
                # corrupt each other's paging offset (Sec 5.1).
                meta={"cookiejar": county, "county": county, "page": 1},
                dont_filter=True,
            )

    # ------------------------------------------------------------------ #
    # Phase 2 -- results + sequential pagination
    # ------------------------------------------------------------------ #

    def _parse_rows(self, response):
        """Parse one results page into a list of listing-row dicts.

        Each result is a name-link table immediately followed by a details
        table (Owner Name / License Number / City / Zip / County / Program
        Type, in that fixed order); the two node lists line up positionally.
        """
        links = response.css(ROW_LINK_SEL)
        tables = response.css(ROW_DETAIL_TABLE_SEL)
        if len(links) != len(tables):
            self.logger.error(
                "Kansas: row/detail-table count mismatch at %s (%d links, %d tables) -- page markup may have changed",
                response.url,
                len(links),
                len(tables),
            )
        rows = []
        for link, table in zip(links, tables):
            token = link.attrib["id"][len("ContentPlaceHolder1_") :]
            values = [v.strip() for v in table.css("label::text").getall()][1::2]
            if len(values) < 6:
                self.logger.error(
                    "Kansas: unexpected row shape for token=%s: %r",
                    token,
                    values,
                )
                continue
            _owner, _license_number, city, zip_raw, county, program_type = values[:6]
            rows.append(
                {
                    "token": token,
                    "city": city,
                    "zip": zip_raw,
                    "county": county,
                    "program_type": program_type,
                }
            )
        return rows

    def parse_results(self, response):
        county = response.meta["county"]
        page = response.meta["page"]
        rows = self._parse_rows(response)
        self.county_running_total[county] = self.county_running_total.get(county, 0) + len(rows)
        self.logger.info(
            "Kansas: %s page %d -> %d rows (running total %d)",
            county,
            page,
            len(rows),
            self.county_running_total[county],
        )

        for row in rows:
            if row["token"] in self.seen:
                # The source itself repeats ~68 rows verbatim (Sec 5.6). Skip
                # the duplicate but keep it in the raw row count used below --
                # the page-size stop rule must never see the deduped count.
                self.duplicate_rows += 1
                continue
            self.seen.add(row["token"])
            yield self._mint_request(response, row)

        if len(rows) >= PAGE_SIZE:
            if page >= MAX_PAGES:
                self.logger.error(
                    "Kansas: %s reached MAX_PAGES=%d without a short page -- "
                    "pagination forcibly stopped and this county is likely "
                    "TRUNCATED",
                    county,
                    MAX_PAGES,
                )
                self.county_final_pages[county] = page
                return
            yield scrapy.FormRequest(
                response.url,
                formdata=_next_formdata(response),
                callback=self.parse_results,
                dont_filter=True,
                meta={"cookiejar": county, "county": county, "page": page + 1},
            )
        else:
            self.county_final_pages[county] = page

    def _mint_request(self, response, row):
        """POST the SearchLink token; the 302 -> GET is one round trip.

        Minting is stateless and cross-page (Sec 2.4), so these are isolated
        from every county's cookiejar via ``dont_merge_cookies`` and run at
        full concurrency without any chance of racing a county's ``Next``.
        """
        formdata = _hidden_fields(response)
        formdata["__EVENTTARGET"] = f"ctl00$ContentPlaceHolder1${row['token']}"
        formdata["__EVENTARGUMENT"] = ""
        return scrapy.FormRequest(
            response.url,
            formdata=formdata,
            callback=self.parse_detail,
            dont_filter=True,
            meta={"row": row, "dont_merge_cookies": True},
        )

    # ------------------------------------------------------------------ #
    # Phase 3 -- detail
    # ------------------------------------------------------------------ #

    @staticmethod
    def _span_text(response, span_id):
        text = "".join(response.css(f"#{span_id}::text").getall())
        text = _WHITESPACE_RE.sub(" ", text).strip()
        return text or None

    def parse_detail(self, response):
        row = response.meta.get("row", {})
        name = self._span_text(response, DETAIL_FIELD_IDS["name"])
        if not name:
            # Malformed/expired facilitysearch tokens 500 (retried by
            # default) or, rarer, 200 with no faciltyNameValue at all --
            # guard so a silently empty page never becomes a blank row
            # (Sec 5.8).
            self.logger.error(
                "Kansas: parse_detail missing faciltyNameValue at %s (token=%s) -- no item emitted",
                response.url,
                row.get("token"),
            )
            return

        item = ProviderItem()
        item["source_state"] = self.source_state
        item["provider_url"] = SEARCH_URL
        item["provider_name"] = name

        license_number = self._span_text(response, DETAIL_FIELD_IDS["license_number"])
        if license_number:
            item["license_number"] = license_number
        owner = self._span_text(response, DETAIL_FIELD_IDS["owner"])
        if owner:
            item["license_holder"] = owner

        provider_type = self._span_text(response, DETAIL_FIELD_IDS["program_type"])
        if provider_type:
            item["provider_type"] = provider_type
        listing_type = row.get("program_type")
        if listing_type and provider_type and listing_type != provider_type:
            item["ks_listing_program_type"] = listing_type

        status = self._span_text(response, DETAIL_FIELD_IDS["status"])
        if status:
            item["status"] = status

        # city/state/zip/county ALWAYS come from the listing row, never the
        # detail page: the detail address mashes street+city with no
        # delimiter (can't recover city from it), and CountyValue reads "--"
        # for every suppressed facility (~38% of rows) while the listing
        # County is always present and correctly spelled (Sec 4.3/4.4).
        if row.get("city"):
            item["city"] = row["city"]
        item["state"] = "KS"
        zip5 = _zip5(row.get("zip"))
        if zip5:
            item["zip"] = zip5
        if row.get("county"):
            item["county"] = row["county"]

        raw_address = self._span_text(response, DETAIL_FIELD_IDS["address"])
        suppressed = bool(raw_address) and raw_address.startswith(SUPPRESSED_PREFIX)
        item["ks_address_suppressed"] = suppressed
        if raw_address and not suppressed:
            address, street = split_address(raw_address, row.get("city"))
            item["address"] = address
            if street is None:
                self.logger.warning(
                    "Kansas: split_address fell back to the raw form for token=%s: %r",
                    row.get("token"),
                    raw_address,
                )

        phone = self._span_text(response, DETAIL_FIELD_IDS["phone"])
        if phone and not phone.startswith(SUPPRESSED_PREFIX):
            item["phone"] = phone

        cap_raw = self._span_text(response, DETAIL_FIELD_IDS["capacity"])
        if cap_raw:
            try:
                item["capacity"] = int(cap_raw)
            except ValueError:
                self.logger.warning(
                    "Kansas: non-integer capacity %r for token=%s",
                    cap_raw,
                    row.get("token"),
                )

        eff_date = self._span_text(response, DETAIL_FIELD_IDS["eff_date"])
        if eff_date:
            item["license_begin_date"] = eff_date
        exp_date = self._span_text(response, DETAIL_FIELD_IDS["exp_date"])
        if exp_date:
            item["license_expiration"] = exp_date

        if row.get("token"):
            item["ks_facility_token"] = row["token"]

        inspections = (
            self._parse_survey_rows(response, LICENSE_GRID_ID, "Licensing Survey")
            + self._parse_survey_rows(response, COMPLAINT_GRID_ID, "Complaint Survey")
            + self._parse_order_rows(response)
        )
        if inspections:
            item["inspections"] = inspections
        survey_counts = [
            insp["ks_findings_count"]
            for insp in inspections
            if insp.get("type") in ("Licensing Survey", "Complaint Survey")
            and insp.get("ks_findings_count") is not None
        ]
        if survey_counts:
            item["deficiencies"] = sum(survey_counts)

        if self.fetch_findings:
            pending = [
                insp
                for insp in inspections
                if insp.get("type") in ("Licensing Survey", "Complaint Survey")
                and (insp.get("ks_findings_count") or 0) > 0
                and insp.get("report_url")
            ]
            if pending:
                yield from self._fetch_findings_chain(item, pending)
                return
        yield item

    def _parse_survey_rows(self, response, grid_id, insp_type):
        """Parse a licensing/complaint survey grid into InspectionItems.

        Both grids share the same 10-column layout: Survey ID, NOSF ID,
        Survey Number, Date of Survey, Survey Reason, two hidden template
        oids, Survey Link, Findings, Facility Response. ``rgNoRecords`` (the
        "No ... Records Found." placeholder) is excluded by only selecting
        ``rgRow``/``rgAltRow`` -- an empty grid yields no items.
        """
        table = response.css(f"table#{grid_id}")
        out = []
        for tr in table.css("tr.rgRow, tr.rgAltRow"):
            tds = tr.css("td")
            if len(tds) < 10:
                self.logger.warning(
                    "Kansas: %s survey row with %d cells (expected 10)",
                    insp_type,
                    len(tds),
                )
                continue
            insp = InspectionItem()
            insp["type"] = insp_type
            survey_id = _cell_text(tds[0])
            if survey_id:
                insp["ks_survey_id"] = survey_id
            nosf_id = _cell_text(tds[1])
            if nosf_id:
                insp["ks_nosf_id"] = nosf_id
            survey_number = _cell_text(tds[2])
            if survey_number:
                insp["ks_survey_number"] = survey_number
            date = _cell_text(tds[3])
            if date:
                insp["date"] = date
            reason = _cell_text(tds[4])
            if reason:
                insp["ks_survey_reason"] = reason
            survey_link = tds[7].css("a::attr(href)").get()
            if survey_link:
                insp["ks_survey_template_url"] = survey_link
            findings_href = tds[8].css("a::attr(href)").get()
            if findings_href:
                insp["report_url"] = findings_href
            findings_text = tds[8].css("a::text").get()
            if findings_text:
                m = FINDINGS_RE.search(findings_text)
                if m:
                    insp["ks_findings_count"] = int(m.group(1))
                    insp["ks_regulations_reviewed"] = int(m.group(2))
                else:
                    self.logger.warning(
                        "Kansas: unparsed findings text %r",
                        findings_text,
                    )
            response_text = tds[9].css("a::text").get()
            response_text = response_text.strip() if response_text else None
            if response_text:
                insp["ks_facility_response"] = response_text
            out.append(insp)
        return out

    def _parse_order_rows(self, response):
        """Parse the administrative-order grid (5 plain-text columns)."""
        table = response.css(f"table#{ORDER_GRID_ID}")
        out = []
        for tr in table.css("tr.rgRow, tr.rgAltRow"):
            tds = tr.css("td")
            if len(tds) < 5:
                self.logger.warning(
                    "Kansas: administrative order row with %d cells (expected 5)",
                    len(tds),
                )
                continue
            insp = InspectionItem()
            insp["type"] = "Administrative Order"
            number = _cell_text(tds[0])
            if number:
                insp["ks_order_number"] = number
            date = _cell_text(tds[1])
            if date:
                insp["date"] = date
            order_type = _cell_text(tds[2])
            if order_type:
                insp["ks_order_type"] = order_type
            reason = _cell_text(tds[3])
            if reason:
                insp["ks_order_reason"] = reason
            final_status = _cell_text(tds[4])
            if final_status:
                insp["ks_order_final_status"] = final_status
            out.append(insp)
        return out

    # ------------------------------------------------------------------ #
    # Findings tier (opt-in, `-a findings=1`)
    # ------------------------------------------------------------------ #

    def _fetch_findings_chain(self, item, pending):
        """Fetch one findings page, then recurse for the rest, then yield.

        Sequential (one request in flight per item) rather than fanned out,
        so the item is only ever yielded once, after every pending findings
        page for it has been merged in.
        """
        first, rest = pending[0], pending[1:]
        yield scrapy.Request(
            first["report_url"],
            callback=self._parse_findings_step,
            meta={"item": item, "inspection": first, "pending": rest},
            dont_filter=True,
        )

    def _parse_findings_step(self, response):
        item = response.meta["item"]
        inspection = response.meta["inspection"]
        pending = response.meta["pending"]
        inspection["ks_findings"] = self._parse_findings_page(response)
        if pending:
            yield from self._fetch_findings_chain(item, pending)
        else:
            yield item

    @staticmethod
    def _parse_findings_page(response):
        """Parse OIDS_ViewFacilityFindings.aspx's citation/narrative pairs.

        The whole page is one span: alternating K.A.R. citation text and a
        ``<b> Description : ...</b>`` narrative, each pair terminated by
        ``<br/><br/>``. Returns ``[{"regulation": ..., "description": ...}]``.
        """
        raw = response.css(f"#{FINDINGS_SPAN_ID}").get() or ""
        inner = re.sub(r"^<span[^>]*>|</span>\s*$", "", raw, flags=re.S)
        # lxml re-serializes `<br/>` as `<br>` when the fragment is rendered
        # back out via `.get()` -- match either form.
        findings = []
        for block in re.split(r"(?:<br\s*/?>\s*){2}", inner):
            if not block.strip():
                continue
            m = re.match(
                r"(.*?)<br\s*/?>\s*<b>\s*Description\s*:\s*(.*?)</b>\s*$",
                block,
                re.S,
            )
            if not m:
                continue
            regulation = _WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", m.group(1))).strip()
            description = _WHITESPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", m.group(2))).strip()
            if regulation or description:
                findings.append({"regulation": regulation, "description": description})
        return findings

    # ------------------------------------------------------------------ #

    def closed(self, reason):
        total_unique = len(self.seen)
        self.logger.info(
            "Kansas: finished (%s) -- %d counties, %d unique facilities, %d in-source duplicate rows skipped",
            reason,
            len(self.county_running_total),
            total_unique,
            self.duplicate_rows,
        )
        for county in sorted(self.county_running_total):
            self.logger.info(
                "Kansas: county summary -- %s: %d rows across %s page(s)",
                county,
                self.county_running_total[county],
                self.county_final_pages.get(county, "?"),
            )
