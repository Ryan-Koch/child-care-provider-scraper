"""Missouri child care provider spider.

Source: Missouri DESE Office of Childhood "Child Care Search", an ASP.NET
WebForms app at https://healthapps.dhss.mo.gov/childcaresearch/ (fronted by
Imperva). See tasks/missouri/missouri_plan.md for the recon writeup this spider
implements.

Three phases:

  Phase 1  GET  SearchEngine.aspx                    harvest the form state
           POST SearchEngine.aspx (from_response)    an empty search returns
                                                     every provider in one page
                                                     (~3,300 rows, no
                                                     pagination); rows are
                                                     de-duplicated by DVN
  Phase 2  GET  Facility.aspx?LID=<DVN>              cookieless detail page --
                                                     enrich the item and harvest
                                                     the inspection/investigation
                                                     table (one stub per row)
  Phase 3  POST back to the detail page, one report at a time per provider
                                                     (default ON, `-a reports=0`
                                                     to skip). Each "View"
                                                     postback answers 302 ->
                                                     ViewInspection.aspx (routine
                                                     inspection) or
                                                     ViewInvestigation.aspx
                                                     (complaint investigation),
                                                     both plain cookieless GETs.

Quirks worth knowing:

  * The search POST must be built with FormRequest.from_response -- a
    hand-built payload gets HTTP 500, the server wants every hidden input.
  * Phase 2/3 requests set ``dont_merge_cookies``: ASP.NET serializes requests
    that share a session, and none of these pages need one.
  * The postback ViewState is stateless (one detail page's ViewState works for
    every row), so the per-provider chain is only there to join a provider's
    reports onto one item, not for correctness.
  * On routine reports ``lblComplience`` is boilerplate that always says "in
    compliance"; the real flag is the checked/unchecked image.
  * In the investigation tables the span ids are misleading or duplicated, so
    cells are read by ``td`` position.
"""

import re
from urllib.parse import parse_qs, urlparse

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://healthapps.dhss.mo.gov/childcaresearch"
SEARCH_URL = BASE_URL + "/SearchEngine.aspx"
DETAIL_URL = BASE_URL + "/Facility.aspx?LID={}"

SEARCH_SUBMIT_BUTTON = "ctl00$ContentPlaceHolder1$btnSubmit"
SEARCH_TABLE_ID = "ctl00_ContentPlaceHolder1_dgSearchEngine"
FORM_ID = "aspnetForm"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Report types shown in the detail page's table whose report lives on
# ViewInvestigation.aspx rather than ViewInspection.aspx.
INSPECTION_PATH = "viewinspection.aspx"
INVESTIGATION_PATH = "viewinvestigation.aspx"
# "Subsidy Renewal" rows redirect to a PDF viewer page (ViewChildCarePDF.aspx?
# LID=..&cid=<guid>) -- not a report, so the stub is kept as-is.
PDF_PATH = "viewchildcarepdf.aspx"

# Live baseline (2026-10-02): 3,307 rows / 3,216 unique DVNs. Used only for a
# completeness warning at the end of a run, never as a gate.
EXPECTED_PROVIDER_COUNT = 3216

PROGRESS_EVERY = 250

POSTBACK_TARGET_RE = re.compile(r"__doPostBack\('([^']+)'")
FULL_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
TIME_SUFFIX_RE = re.compile(r"\s+\d{1,2}:\d{2}:\d{2}\s*[AP]M$", re.IGNORECASE)


def clean(value):
    """Collapse whitespace; return None for empty/missing values."""
    if value is None:
        return None
    text = " ".join(str(value).replace("\xa0", " ").split())
    return text or None


def clean_name(value):
    """Clean a provider name; sparse rows carry literal backslash-escaped quotes."""
    text = clean(value)
    return text.replace('\\"', '"') if text else None


def span_text(selector, xpath):
    """Cleaned, joined text of the first node matched by ``xpath``."""
    return clean(" ".join(selector.xpath(xpath).xpath(".//text()").getall()))


def compose_address(street, city, state, zip_code):
    """ "STREET, CITY, ST ZIP" from whatever pieces exist; None if nothing."""
    tail = " ".join(part for part in (state, zip_code) if part)
    parts = [part for part in (street, city, tail) if part]
    return ", ".join(parts) or None


def compose_hours(start, end):
    """ "6:00 AM - 9:00 PM", only when both ends are present."""
    return f"{start} - {end}" if start and end else None


def _put(item, key, value):
    """Set ``item[key]`` only when ``value`` is non-empty."""
    if value not in (None, "", [], {}):
        item[key] = value


def _put_if_absent(item, key, value):
    """Set ``item[key]`` only when it is non-empty and the key is still empty."""
    if item.get(key) in (None, "", [], {}):
        _put(item, key, value)


# --------------------------------------------------------------------------- #
# Phase 1 -- search results
# --------------------------------------------------------------------------- #


def parse_search_row(row):
    """One search-result ``tr`` -> a ProviderItem (search-level fields only)."""

    def field(suffix):
        return span_text(row, f'.//span[substring(@id, string-length(@id) - {len(suffix) - 1}) = "{suffix}"]')

    dvn = field("_lblDVN")
    if not dvn:
        return None

    item = ProviderItem()
    item["source_state"] = "Missouri"
    item["license_number"] = dvn
    item["provider_url"] = DETAIL_URL.format(dvn)
    _put(item, "provider_name", clean_name(" ".join(row.xpath('.//a[contains(@id, "_LinkButton1")]//text()').getall())))
    _put(
        item, "address", compose_address(field("_lblAddress"), field("_lblCity"), field("_lblState"), field("_lblZip"))
    )
    _put(item, "phone", field("_lblPhone"))
    _put(item, "hours", compose_hours(field("_lblHoursFrom"), field("_lblHoursTo")))
    _put(item, "ages_served", field("_lblAgeRange"))
    _put(item, "capacity", field("_lblCapacity"))
    return item


def dedupe_rows(items):
    """Collapse rows sharing a DVN, keeping the row with a capacity.

    The search returns some DVNs twice: a full row plus a sparse row from a
    second backing system. Returns ``(unique_items, duplicates_collapsed)``,
    in first-seen order.
    """
    unique = {}
    duplicates = 0
    for item in items:
        dvn = item["license_number"]
        if dvn not in unique:
            unique[dvn] = item
            continue
        duplicates += 1
        if not unique[dvn].get("capacity") and item.get("capacity"):
            unique[dvn] = item
    return list(unique.values()), duplicates


# --------------------------------------------------------------------------- #
# Phase 2 -- detail page
# --------------------------------------------------------------------------- #


def _detail_span(response, name):
    return span_text(response, f'//span[@id="ctl00_ContentPlaceHolder1_{name}"]')


def apply_detail(response, item):
    """Overlay the detail page's fields on the search-row item.

    The detail page is authoritative, so non-empty values override; an empty
    detail value never blanks a search value.
    """
    _put(item, "provider_name", clean_name(_detail_span(response, "lblFacilityName")))
    _put(
        item,
        "address",
        compose_address(
            _detail_span(response, "lblFacilityAddress"),
            _detail_span(response, "lblCity"),
            _detail_span(response, "lblState"),
            _detail_span(response, "lblZipCode"),
        ),
    )
    _put(item, "county", _detail_span(response, "lblCounty"))
    _put(item, "phone", _detail_span(response, "lblFacilityPhone"))
    _put(item, "email", _detail_span(response, "lblFacilityEmail"))
    _put(item, "provider_type", _detail_span(response, "lblFacilityType"))
    _put(item, "license_begin_date", _detail_span(response, "lblLicenseeEffectiveDate"))
    _put(item, "capacity", _detail_span(response, "lblTotalCapacity"))
    _put(item, "ages_served", _detail_span(response, "lblAgeRange"))
    _put(
        item,
        "hours",
        compose_hours(_detail_span(response, "lblHoursFrom"), _detail_span(response, "lblHoursTo")),
    )

    anniversary = _detail_span(response, "lblLicenseeAnniversaryDate")
    _put(item, "mo_license_anniversary_date", anniversary)
    # MM/DD (licensed) is a renewal anniversary, not an expiry; only a full
    # MM/DD/YYYY (license-exempt) is effectively an expiration date.
    if anniversary and FULL_DATE_RE.match(anniversary):
        item["license_expiration"] = anniversary


def parse_inspection_rows(response):
    """Rows of the inspection/investigation table -> ``[(stub, event_target)]``."""
    rows = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_dgInspInv"]//tr[count(td) >= 3]')
    results = []
    for row in rows:
        target_match = POSTBACK_TARGET_RE.search(row.xpath(".//a/@href").get() or "")
        stub = InspectionItem()
        _put(stub, "date", span_text(row, "./td[2]"))
        _put(stub, "type", span_text(row, "./td[3]"))
        results.append((stub, target_match.group(1) if target_match else None))
    return results


# --------------------------------------------------------------------------- #
# Phase 3 -- report pages
# --------------------------------------------------------------------------- #


def _report_span(response, name):
    return span_text(response, f'//span[@id="ctl00_ContentPlaceHolder1_{name}"]')


def _table_rows(response, table_suffix):
    """Data rows (those with ``td`` cells) of the table whose id ends with the suffix."""
    return response.xpath(
        f'//table[substring(@id, string-length(@id) - {len(table_suffix) - 1}) = "{table_suffix}"]//tr[td]'
    )


def parse_open_violations(text):
    """ "13" -> 13, "NA" -> 0, anything else -> None."""
    value = clean(text)
    if value is None:
        return None
    if value.upper() == "NA":
        return 0
    return int(value) if value.isdigit() else None


def in_compliance_flag(response):
    """checked.jpg -> True, unchecked.jpg -> False, anything else -> None."""
    src = (
        response.xpath('//img[@id="ctl00_ContentPlaceHolder1_imgComplienceLicensingRules"]/@src').get() or ""
    ).lower()
    if src.endswith("/unchecked.jpg") or src == "unchecked.jpg":
        return False
    if src.endswith("/checked.jpg") or src == "checked.jpg":
        return True
    return None


def parse_inspection_report(response, stub, logger=None):
    """Fill ``stub`` from a ViewInspection.aspx page (routine inspection)."""
    _put(stub, "mo_specialist", _report_span(response, "lblChildCareSpecialist"))
    _put(stub, "mo_inspection_id", _report_span(response, "lblInspectionID"))
    _put(stub, "mo_notice", _report_span(response, "lblNotice"))
    _put(stub, "mo_arrival_time", _report_span(response, "lblArrivalTime"))
    _put(stub, "mo_departure_time", _report_span(response, "lblDepartureTime"))

    raw_open = _report_span(response, "lblOpenViolations")
    open_violations = parse_open_violations(raw_open)
    if open_violations is None and raw_open and logger is not None:
        logger.warning("Missouri: non-numeric open violation count %r at %s", raw_open, response.url)
    if open_violations is not None:
        stub["mo_open_violations"] = open_violations

    flag = in_compliance_flag(response)
    if flag is not None:
        stub["mo_in_compliance"] = flag

    compliance_list = []
    for row in _table_rows(response, "dgGroupName"):
        rule = span_text(row, './/span[contains(@id, "_lblGrpName")]')
        result = span_text(row, './/span[contains(@id, "_lblCompliance")]')
        if rule or result:
            compliance_list.append({"rule": rule, "result": result})
    if compliance_list:
        stub["mo_compliance_list"] = compliance_list


def parse_investigation_report(response, stub):
    """Fill ``stub`` from a ViewInvestigation.aspx page (complaint investigation)."""
    cid = parse_qs(urlparse(response.url).query)
    cid = {key.lower(): values for key, values in cid.items()}.get("cid")
    _put(stub, "mo_investigation_id", cid[0] if cid else None)
    _put(stub, "mo_specialist", _report_span(response, "lblAssignedSpecialist"))
    _put(stub, "original_status", _report_span(response, "lblDisposition"))
    _put(stub, "mo_disposition_date", _report_span(response, "lblDispositionDate"))
    _put(stub, "mo_approving_supervisor", _report_span(response, "lblApprovingSupervisor"))

    # Cells are read by position: the span ids in these tables are reused
    # (gvViolations' description is "lblDocumentDate"; a corrective-measure row
    # has two spans with the same id).
    violations = [
        {"rule": span_text(row, "./td[1]"), "description": span_text(row, "./td[2]")}
        for row in _table_rows(response, "gvViolations")
    ]
    _put(stub, "mo_violations", violations)

    summaries = [span_text(row, "./td[1]") for row in _table_rows(response, "gvConclusionSummary")]
    _put(stub, "mo_conclusion", "\n\n".join(s for s in summaries if s))

    measures = []
    for row in _table_rows(response, "gvCorrectiveMeasures"):
        completed_date = span_text(row, "./td[3]")
        measures.append(
            {
                "measure": span_text(row, "./td[1]"),
                "completed": span_text(row, "./td[2]"),
                "completed_date": TIME_SUFFIX_RE.sub("", completed_date) if completed_date else None,
            }
        )
    _put(stub, "mo_corrective_measures", measures)


def apply_provider_enrichment(response, item):
    """Provider-level fields from a routine report; only fills empty fields."""
    _put_if_absent(item, "license_holder", _report_span(response, "lblOwner"))
    _put_if_absent(item, "administrator", _report_span(response, "lblProviderorDirector"))
    _put_if_absent(item, "email", _report_span(response, "lblEmail"))
    _put_if_absent(item, "mo_limitations", _report_span(response, "lblLimitations"))

    state_zip = " ".join(
        part
        for part in (
            _report_span(response, "lblMailingState"),
            _report_span(response, "lblMailingZip"),
        )
        if part
    )
    _put_if_absent(
        item,
        "mo_mailing_address",
        compose_address(
            _report_span(response, "lblMailingAddress"),
            _report_span(response, "lblMailingCity"),
            None,
            state_zip,
        ),
    )


def total_deficiencies(inspections):
    """Open violations (routine) plus violations (investigation) over all
    fetched reports; None when no report was fetched."""
    fetched = False
    total = 0
    for stub in inspections or []:
        if "mo_inspection_id" in stub or "mo_open_violations" in stub:
            fetched = True
            total += stub.get("mo_open_violations") or 0
        elif "mo_investigation_id" in stub or "mo_violations" in stub:
            fetched = True
            total += len(stub.get("mo_violations") or [])
    return total if fetched else None


class MissouriSpider(scrapy.Spider):
    name = "missouri"
    allowed_domains = ["healthapps.dhss.mo.gov"]
    start_urls = [SEARCH_URL]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.1,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 120,
        "ROBOTSTXT_OBEY": False,
        "USER_AGENT": USER_AGENT,
    }

    def __init__(self, reports=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # `-a reports=0` skips Phase 3 and emits the inspection stubs only
        # (the New Hampshire `-a visits=0` precedent).
        self.fetch_reports = str(reports).strip().lower() not in ("0", "false")

        self.total_providers = 0
        self.details_done = 0
        self.providers_emitted = 0
        self.reports_fetched = 0
        self.detail_failures = 0
        self.report_failures = 0

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_search_form, dont_filter=True)

    # -- Phase 1 ----------------------------------------------------------- #

    def parse_search_form(self, response):
        """Submit the empty search the way the browser would (all hidden
        inputs included; a hand-built payload gets HTTP 500)."""
        self.logger.info("Missouri: Phase 1 -- submitting the empty search form")
        yield scrapy.FormRequest.from_response(
            response,
            formid=FORM_ID,
            clickdata={"name": SEARCH_SUBMIT_BUTTON},
            callback=self.parse_search_results,
            dont_filter=True,
        )

    def parse_search_results(self, response):
        rows = response.xpath(f'//table[@id="{SEARCH_TABLE_ID}"]//tr[td]')
        if not rows:
            self.logger.error(
                "Missouri: search results table missing or empty (HTTP %s, %d bytes) -- the form POST probably broke",
                response.status,
                len(response.body),
            )
            return

        parsed = [item for item in (parse_search_row(row) for row in rows) if item is not None]
        items, duplicates = dedupe_rows(parsed)
        sparse = sum(1 for item in items if not item.get("capacity"))
        self.total_providers = len(items)
        self.logger.info(
            "Missouri: Phase 1 done -- %d raw rows, %d unique DVNs, %d duplicates collapsed, %d without capacity "
            "(sparse); starting %d detail requests (reports=%s)",
            len(parsed),
            len(items),
            duplicates,
            sparse,
            len(items),
            self.fetch_reports,
        )

        for item in items:
            yield scrapy.Request(
                item["provider_url"],
                callback=self.parse_detail,
                errback=self.detail_errback,
                cb_kwargs={"item": item},
                meta={"dont_merge_cookies": True},
            )

    # -- Phase 2 ----------------------------------------------------------- #

    def _note_detail_done(self):
        self.details_done += 1
        if self.details_done % PROGRESS_EVERY == 0:
            self.logger.info("Missouri: Phase 2 -- detail %d / %d", self.details_done, self.total_providers)

    def detail_errback(self, failure):
        item = failure.request.cb_kwargs["item"]
        self.detail_failures += 1
        self.logger.warning(
            "Missouri: detail request failed for DVN %s (%s) -- emitting the search-row data only",
            item.get("license_number"),
            failure.value,
        )
        self._note_detail_done()
        yield from self._finish(item)

    def parse_detail(self, response, item):
        self._note_detail_done()
        apply_detail(response, item)

        rows = parse_inspection_rows(response)
        if rows:
            item["inspections"] = [stub for stub, _ in rows]

        targets = [target for _, target in rows]
        if not rows or not self.fetch_reports:
            yield from self._finish(item)
            return

        yield from self._request_report(item, targets, 0, response)

    # -- Phase 3 ----------------------------------------------------------- #

    def _request_report(self, item, targets, index, detail_response):
        """Postback for report ``index``, or finish the item past the last row."""
        while index < len(targets) and not targets[index]:
            self.logger.warning(
                "Missouri: DVN %s report %d/%d has no postback target -- skipping",
                item.get("license_number"),
                index + 1,
                len(targets),
            )
            index += 1

        if index >= len(targets):
            yield from self._finish(item)
            return

        self.logger.debug(
            "Missouri: DVN %s -- fetching report %d/%d", item.get("license_number"), index + 1, len(targets)
        )
        yield scrapy.FormRequest.from_response(
            detail_response,
            formid=FORM_ID,
            formdata={"__EVENTTARGET": targets[index], "__EVENTARGUMENT": ""},
            dont_click=True,
            callback=self.parse_report,
            errback=self.report_errback,
            cb_kwargs={"item": item, "targets": targets, "index": index, "detail_response": detail_response},
            meta={"dont_merge_cookies": True},
            # Every postback hits the same URL with only __EVENTTARGET changing.
            dont_filter=True,
        )

    def parse_report(self, response, item, targets, index, detail_response):
        stub = item["inspections"][index]
        path = urlparse(response.url).path.lower()
        dvn = item.get("license_number")

        if path.endswith(INSPECTION_PATH):
            # Provider-level fields come from the first routine report that
            # was actually parsed (most recent first; investigations skipped).
            first_routine = not any("mo_inspection_id" in earlier for earlier in item["inspections"][:index])
            parse_inspection_report(response, stub, logger=self.logger)
            stub["report_url"] = response.url
            if first_routine:
                apply_provider_enrichment(response, item)
            self.reports_fetched += 1
        elif path.endswith(INVESTIGATION_PATH):
            parse_investigation_report(response, stub)
            stub["report_url"] = response.url
            self.reports_fetched += 1
        elif path.endswith(PDF_PATH):
            self.logger.debug(
                "Missouri: DVN %s report %d/%d (%s) is a PDF viewer page -- keeping the stub",
                dvn,
                index + 1,
                len(targets),
                stub.get("type"),
            )
        else:
            self.report_failures += 1
            self.logger.warning(
                "Missouri: DVN %s report %d/%d redirected to unexpected path %s -- keeping the stub",
                dvn,
                index + 1,
                len(targets),
                path,
            )

        yield from self._request_report(item, targets, index + 1, detail_response)

    def report_errback(self, failure):
        kwargs = failure.request.cb_kwargs
        item, targets, index = kwargs["item"], kwargs["targets"], kwargs["index"]
        self.report_failures += 1
        self.logger.warning(
            "Missouri: report request failed for DVN %s, report %d/%d (%s) -- keeping the stub, continuing",
            item.get("license_number"),
            index + 1,
            len(targets),
            failure.value,
        )
        yield from self._request_report(item, targets, index + 1, kwargs["detail_response"])

    # -- Output ------------------------------------------------------------ #

    def _finish(self, item):
        deficiencies = total_deficiencies(item.get("inspections"))
        if deficiencies is not None:
            item["deficiencies"] = deficiencies

        self.providers_emitted += 1
        if self.providers_emitted % PROGRESS_EVERY == 0:
            self.logger.info(
                "Missouri: Phase 3 -- %d / %d providers finished, %d reports fetched, %d report failures",
                self.providers_emitted,
                self.total_providers,
                self.reports_fetched,
                self.report_failures,
            )
        yield item

    def closed(self, reason):
        self.logger.info(
            "Missouri: finished (%s) -- %d providers emitted, %d reports fetched, "
            "%d detail failures, %d report failures",
            reason,
            self.providers_emitted,
            self.reports_fetched,
            self.detail_failures,
            self.report_failures,
        )
        if self.providers_emitted and reason == "finished" and self.providers_emitted < EXPECTED_PROVIDER_COUNT * 0.9:
            self.logger.warning(
                "Missouri: only %d providers emitted (< 90%% of the %d baseline observed 2026-10-02)",
                self.providers_emitted,
                EXPECTED_PROVIDER_COUNT,
            )
