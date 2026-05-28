import json
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode, urljoin

import scrapy
from scrapy.http import HtmlResponse

from ..items import InspectionItem, ProviderItem


DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
DAY_ABBR = {d: d[:3] for d in DAY_NAMES}

# --- Silver State Stars QRIS quality dashboard (Power BI publish-to-web) ---
# Anonymous; the only credential is the resource key sent as a request header.
# IDs are stable for the life of the published report (see nevada_enrichment.md §1).
POWERBI_QUERY_URL = (
    "https://wabi-us-east-a-primary-api.analysis.windows.net"
    "/public/reports/querydata?synchronous=true"
)
POWERBI_RESOURCE_KEY = "545ba0e9-3934-4219-bbe9-3368f93e5a61"
POWERBI_DATASET_ID = "3f54d90a-38da-4039-ba07-9c33886e2832"
POWERBI_REPORT_ID = "b08aff48-6cf1-4cee-a756-facb59f0ea37"
POWERBI_VISUAL_ID = "f327a5ea18e0402d0b00"
POWERBI_MODEL_ID = 4563689
# Rows-per-window cap for the main query; the roster spans ~1-2 windows.
POWERBI_WINDOW = 500
# Snapshot period to fall back to if the runtime discovery query fails.
FALLBACK_PERIOD = (2026, "April")

# Columns the main quality query selects from QStarEnrollment, in projection order.
# Each tuple is (powerbi_property, provider_item_field, is_date). A None field means
# the column is used only for matching/diagnostics (or, for Address/City/Zip,
# stitched together into the address field for QRIS-only providers).
QUALITY_SELECT = [
    ("LicenseNumber", None, False),
    ("ProgramName", None, False),
    ("ProgramType", "nv_program_type", False),
    ("County", None, False),
    ("Region", "nv_region", False),
    ("StarRatingFriendlyName", "nv_star_rating", False),
    ("StatusFriendlyName", "nv_qris_status", False),
    ("RatingPeriodStartDate", "nv_rating_period_start", True),
    ("RatingPeriodEndDate", "nv_rating_period_end", True),
    ("DateEnrollmentFormSubmitted", "nv_qris_enrollment_date", True),
    ("RatingPeriodName", "nv_rating_period_name", False),
    ("SiteCharacteristic", "nv_site_characteristic", False),
    ("RatingPriority", "nv_rating_priority", False),
    ("Address", None, False),
    ("City", None, False),
    ("Zip", None, False),
]


def normalize_license(value):
    """Reduce a license number to its bare base for joining the two data sources.

    Strips any year/credential suffix ("831-26" -> "831"), surrounding whitespace,
    and leading zeros ("028" -> "28"), which the licensing and quality sources
    format inconsistently. Returns None for empty input.
    """
    if value is None:
        return None
    base = str(value).split("-", 1)[0].strip()
    if not base:
        return None
    return base.lstrip("0") or "0"


def epoch_ms_to_date(value):
    """Convert a Power BI epoch-millisecond timestamp to an MM/DD/YYYY string."""
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%m/%d/%Y")


def format_qris_address(street, city, zip_code):
    """Stitch the QRIS address parts into one "<street>, <city>, NV <zip>" string."""
    if not any([street, city, zip_code]):
        return None
    tail_parts = []
    if city:
        tail_parts.append(f"{city}, NV")
    else:
        tail_parts.append("NV")
    if zip_code:
        tail_parts.append(str(zip_code))
    tail = " ".join(tail_parts)
    return f"{street}, {tail}" if street else tail


def _col(prop, source="q"):
    """Build a Power BI query column reference for the given entity source alias."""
    return {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}}


def build_query_payload(command):
    """Wrap a SemanticQueryDataShapeCommand in the querydata request envelope."""
    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {"Commands": [command]},
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": POWERBI_DATASET_ID,
                    "Sources": [
                        {"ReportId": POWERBI_REPORT_ID, "VisualId": POWERBI_VISUAL_ID}
                    ],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": POWERBI_MODEL_ID,
    }


def build_period_discovery_command():
    """Query the data months that actually have rows (export columns on the fact
    table), used to pin the main query to the latest snapshot."""
    return {
        "SemanticQueryDataShapeCommand": {
            "Query": {
                "Version": 2,
                "From": [{"Name": "q", "Entity": "QStarEnrollment", "Type": 0}],
                "Select": [
                    {**_col("ExportYear"), "Name": "q.ExportYear"},
                    {**_col("ExportMonthName"), "Name": "q.ExportMonthName"},
                    {**_col("MonthNum"), "Name": "q.MonthNum"},
                ],
            },
            "Binding": {
                "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
                "DataReduction": {
                    "DataVolume": 3,
                    "Primary": {"Window": {"Count": POWERBI_WINDOW}},
                },
                "Version": 1,
            },
            "ExecutionMetricsKind": 1,
        }
    }


def build_quality_command(year, month_name, restart_token=None):
    """Build the main quality query pinned to one snapshot month.

    Literal syntax matters: Long literals carry an `L` suffix and no quotes
    ("2026L"); string literals are single-quoted inside the JSON ("'April'").
    A restart token (echoed verbatim from a prior response) pages the next window.
    """
    select = [{**_col(prop), "Name": f"q.{prop}"} for prop, _, _ in QUALITY_SELECT]
    window = {"Count": POWERBI_WINDOW}
    if restart_token is not None:
        window["RestartTokens"] = restart_token
    return {
        "SemanticQueryDataShapeCommand": {
            "Query": {
                "Version": 2,
                "From": [
                    {"Name": "q", "Entity": "QStarEnrollment", "Type": 0},
                    {"Name": "d", "Entity": "DateTable", "Type": 0},
                ],
                "Select": select,
                "Where": [
                    {
                        "Condition": {
                            "In": {
                                "Expressions": [_col("YearSlicer", "d")],
                                "Values": [[{"Literal": {"Value": f"{year}L"}}]],
                            }
                        }
                    },
                    {
                        "Condition": {
                            "In": {
                                "Expressions": [_col("MonthSlicer", "d")],
                                "Values": [[{"Literal": {"Value": f"'{month_name}'"}}]],
                            }
                        }
                    },
                    {
                        "Condition": {
                            "Not": {
                                "Expression": {
                                    "In": {
                                        "Expressions": [_col("Status")],
                                        "Values": [
                                            [{"Literal": {"Value": "'Dropped'"}}],
                                            [{"Literal": {"Value": "'Inactive'"}}],
                                            [{"Literal": {"Value": "'Closed'"}}],
                                        ],
                                    }
                                }
                            }
                        }
                    },
                ],
            },
            "Binding": {
                "Primary": {
                    "Groupings": [{"Projections": list(range(len(QUALITY_SELECT)))}]
                },
                "DataReduction": {"DataVolume": 3, "Primary": {"Window": window}},
                "Version": 1,
            },
            "ExecutionMetricsKind": 1,
        }
    }


def _decode_dm0(dm0, dict_names, value_dicts):
    """Decode a DataShapeResult DM0 row set into a list of value-lists.

    Each row uses delta/dictionary encoding (nevada_enrichment.md §5.5):
      C  - values for columns that are neither repeated nor null, in column order.
      R  - repeat bitmask: bit i set => column i is unchanged from the previous row.
      Ø  - null bitmask: bit i set => column i is null.
    Dictionary columns (those with a DN) carry an integer index into value_dicts,
    except when the value overflows the cache and arrives as an inline string.
    """
    count = len(dict_names)
    prev = [None] * count
    rows = []
    for rec in dm0:
        values = rec.get("C", [])
        repeat_mask = rec.get("R", 0)
        null_mask = rec.get("Ø", 0)
        ci = 0
        row = list(prev)
        for i in range(count):
            if repeat_mask & (1 << i):
                continue
            if null_mask & (1 << i):
                row[i] = None
                continue
            val = values[ci]
            ci += 1
            dname = dict_names[i]
            if dname and isinstance(val, int):
                val = value_dicts[dname][val]
            row[i] = val
        rows.append(row)
        prev = row
    return rows


def decode_data_shape(response_json):
    """Extract (rows, restart_token) from a querydata response.

    rows is a list of value-lists in projection order; restart_token is the DS-level
    `RT` value to thread into the next window, or None when the result is complete.
    Raises ValueError on an embedded query-definition error (HTTP 200 carrying
    `dsr.DataShapes` instead of `dsr.DS`).
    """
    data = response_json["results"][0]["result"]["data"]
    dsr = data["dsr"]
    if "DS" not in dsr:
        raise ValueError(
            "Power BI query-definition error: "
            + json.dumps(dsr.get("DataShapes"))[:300]
        )
    ds = dsr["DS"][0]
    primary = ds.get("PH", [{}])[0]
    dm0 = primary.get("DM0", [])
    if not dm0:
        return [], None
    dict_names = [col.get("DN") for col in dm0[0]["S"]]
    rows = _decode_dm0(dm0, dict_names, ds.get("ValueDicts", {}))
    return rows, ds.get("RT")

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
    # analysis.windows.net is the Power BI quality dashboard host (see §1).
    allowed_domains = ["nvdpbh.aithent.com", "analysis.windows.net"]
    start_urls = [
        "https://nvdpbh.aithent.com/Protected/LIC/LicenseeSearch.aspx?Program=HF&PubliSearch=Y"
    ]

    custom_settings = {
        # Per-row detail fetches are cheap GETs but be polite to the state site.
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_DELAY": 0.5,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Accumulate-then-enrich: hold every provider until the licensing scrape
        # finishes, then join Silver State Stars quality data before emitting.
        self.providers_by_license = {}
        self.pending_details = 0
        self.pagination_done = False
        self.quality_started = False
        self.quality_rows = []
        self.quality_window = 0

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

        self.pending_details += 1
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
        # Bare base number (no year suffix) — the quality-data join key.
        item["nv_license_base"] = clean_text(row_fields.get("hfLicenseNumber"))

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
            self.pagination_done = True
            # A page with no outstanding detail fetches (e.g. all already
            # returned) won't get another parse_detail to trip the gate.
            yield from self._maybe_start_quality()
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
        yield from self._accumulate_provider(item)

    def detail_errback(self, failure):
        item = failure.request.meta.get("partial_item")
        if item is not None:
            self.logger.warning(
                "Detail fetch failed for %s (%s); keeping partial item",
                item.get("provider_name"),
                item.get("license_number"),
            )
            yield from self._accumulate_provider(item)

    def _accumulate_provider(self, item):
        """Hold a finished provider for enrichment and trip the quality gate
        once the licensing scrape is fully drained."""
        key = normalize_license(item.get("nv_license_base") or item.get("license_number"))
        if key:
            self.providers_by_license[key] = item
        self.pending_details -= 1
        yield from self._maybe_start_quality()

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

    # ------------------------------------------------------------------
    # Quality enrichment (Silver State Stars QRIS via Power BI dashboard)
    # ------------------------------------------------------------------

    def _maybe_start_quality(self):
        """Kick off the quality fetch once, when the licensing scrape is drained.

        Gated on BOTH pagination completion and zero outstanding detail fetches:
        pending_details can momentarily hit 0 between pages, so the pagination
        flag is required to avoid a premature start.
        """
        if self.quality_started or not self.pagination_done or self.pending_details > 0:
            return
        self.quality_started = True
        self.logger.info(
            "Licensing scrape complete (%s providers); starting quality enrichment",
            len(self.providers_by_license),
        )
        yield self._quality_request(
            build_period_discovery_command(),
            callback=self.parse_period_discovery,
            errback=self.period_discovery_errback,
        )

    def _quality_request(self, command, callback, errback):
        """POST a crafted querydata command to the Power BI dashboard.

        Sent as a raw JSON body (not a FormRequest) and routed around the
        Playwright handler. Fresh ActivityId/RequestId per request help the WAF
        treat it as a browser call.
        """
        headers = {
            "X-PowerBI-ResourceKey": POWERBI_RESOURCE_KEY,
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://app.powerbi.com",
            "Referer": "https://app.powerbi.com/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "ActivityId": str(uuid.uuid4()),
            "RequestId": str(uuid.uuid4()),
        }
        return scrapy.Request(
            url=POWERBI_QUERY_URL,
            method="POST",
            body=json.dumps(build_query_payload(command), separators=(",", ":")),
            headers=headers,
            callback=callback,
            errback=errback,
            meta={"playwright": False},
            dont_filter=True,
        )

    def parse_period_discovery(self, response):
        """Pick the latest snapshot month with data, then launch the main query."""
        year, month_name = FALLBACK_PERIOD
        try:
            rows, _ = decode_data_shape(json.loads(response.text))
            latest = self._pick_latest_period(rows)
            if latest:
                year, month_name = latest
                self.logger.info(
                    "Quality snapshot period: %s %s", month_name, year
                )
            else:
                self.logger.warning(
                    "Period discovery returned no rows; using fallback %s %s",
                    month_name,
                    year,
                )
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self.logger.warning(
                "Period discovery failed (%s); using fallback %s %s",
                exc,
                month_name,
                year,
            )
        yield self._quality_request(
            build_quality_command(year, month_name),
            callback=self.parse_quality_window,
            errback=self.quality_errback,
        )

    def period_discovery_errback(self, failure):
        """Discovery request failed at the transport level — use the fallback."""
        year, month_name = FALLBACK_PERIOD
        self.logger.warning(
            "Period discovery request failed (%s); using fallback %s %s",
            failure.value,
            month_name,
            year,
        )
        yield self._quality_request(
            build_quality_command(year, month_name),
            callback=self.parse_quality_window,
            errback=self.quality_errback,
        )

    @staticmethod
    def _pick_latest_period(rows):
        """Choose the (ExportYear, ExportMonthName) with the highest year/month."""
        best = None
        for year, month_name, month_num in rows:
            if year is None or month_name is None:
                continue
            key = (year, month_num or 0)
            if best is None or key > best[0]:
                best = (key, year, month_name)
        return (best[1], best[2]) if best else None

    def parse_quality_window(self, response):
        """Accumulate one window of quality rows; page via restart token or finish."""
        rows, restart_token = decode_data_shape(json.loads(response.text))
        self.quality_window += 1
        for row in rows:
            self.quality_rows.append(
                {prop: row[i] for i, (prop, _, _) in enumerate(QUALITY_SELECT)}
            )
        self.logger.info(
            "Quality window %s: %s rows (cumulative %s)",
            self.quality_window,
            len(rows),
            len(self.quality_rows),
        )

        if restart_token is not None:
            # More rows remain: re-issue the same query with the next window's
            # restart token, reusing the period pinned on the request body.
            yield self._quality_request(
                self._next_window_command(response, restart_token),
                callback=self.parse_quality_window,
                errback=self.quality_errback,
            )
            return

        yield from self._enrich_and_finish()

    @staticmethod
    def _next_window_command(response, restart_token):
        """Rebuild the in-flight quality command with the next restart token by
        reading the pinned period back out of the request body."""
        body = json.loads(response.request.body)
        command = body["queries"][0]["Query"]["Commands"][0]
        where = command["SemanticQueryDataShapeCommand"]["Query"]["Where"]
        year = where[0]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"]
        month = where[1]["Condition"]["In"]["Values"][0][0]["Literal"]["Value"]
        year = int(year.rstrip("L"))
        month = month.strip("'")
        return build_quality_command(year, month, restart_token=restart_token)

    def quality_errback(self, failure):
        """A quality request failed; emit the license data we have without ratings."""
        self.logger.error(
            "Quality fetch failed (%s); emitting providers without enrichment",
            failure.value,
        )
        yield from self._yield_all_providers()

    def _enrich_and_finish(self):
        """Join the accumulated quality rows onto providers, then emit everything.

        QRIS rows whose license has no licensing-source match become standalone
        providers populated entirely from the QRIS row — the Silver State Stars
        roster includes programs the licensing search doesn't surface.
        """
        # Dedupe quality rows per license, keeping the latest rating period end.
        best_by_license = {}
        for row in self.quality_rows:
            key = normalize_license(row.get("LicenseNumber"))
            if not key:
                continue
            current = best_by_license.get(key)
            if current is None or (row.get("RatingPeriodEndDate") or 0) > (
                current.get("RatingPeriodEndDate") or 0
            ):
                best_by_license[key] = row

        license_provider_count = len(self.providers_by_license)
        matched = 0
        for key, provider in self.providers_by_license.items():
            row = best_by_license.get(key)
            if row is None:
                continue
            for prop, field, is_date in QUALITY_SELECT:
                if not field:
                    continue
                value = row.get(prop)
                if value is None:
                    continue
                provider[field] = epoch_ms_to_date(value) if is_date else value
            matched += 1

        qris_only = 0
        for key, row in best_by_license.items():
            if key in self.providers_by_license:
                continue
            self.providers_by_license[key] = self._build_provider_from_quality(row)
            qris_only += 1

        self.logger.info(
            "Quality enrichment complete: matched %s of %s license providers; "
            "added %s QRIS-only providers with no licensing match",
            matched,
            license_provider_count,
            qris_only,
        )
        yield from self._yield_all_providers()

    def _build_provider_from_quality(self, row):
        """Build a ProviderItem from a QRIS row with no licensing-source match.

        Pulls identity (name, county, license number) from the columns the
        quality query already selects for matching/diagnostics, then copies the
        same QRIS fields the enrichment path writes onto matched providers.
        """
        item = ProviderItem()
        item["source_state"] = "NV"
        license_number = row.get("LicenseNumber")
        item["license_number"] = license_number
        item["nv_license_base"] = license_number
        item["provider_name"] = row.get("ProgramName")
        item["county"] = row.get("County")
        address = format_qris_address(
            row.get("Address"), row.get("City"), row.get("Zip")
        )
        if address:
            item["address"] = address
        for prop, field, is_date in QUALITY_SELECT:
            if not field:
                continue
            value = row.get(prop)
            if value is None:
                continue
            item[field] = epoch_ms_to_date(value) if is_date else value
        item["inspections"] = []
        return item

    def _yield_all_providers(self):
        self.logger.info(
            "Yielding all providers for output. Count: %s",
            len(self.providers_by_license),
        )
        yield from self.providers_by_license.values()
