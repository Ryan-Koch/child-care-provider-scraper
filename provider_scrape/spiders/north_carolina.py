import re

import scrapy
from parsel import Selector
from scrapy_playwright.page import PageMethod

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://ncchildcare.ncdhhs.gov/childcaresearch"

# 100 NC counties, exactly as they appear in the search-page Telerik combobox.
NC_COUNTIES = [
    "ALAMANCE", "ALEXANDER", "ALLEGHANY", "ANSON", "ASHE", "AVERY",
    "BEAUFORT", "BERTIE", "BLADEN", "BRUNSWICK", "BUNCOMBE", "BURKE",
    "CABARRUS", "CALDWELL", "CAMDEN", "CARTERET", "CASWELL", "CATAWBA",
    "CHATHAM", "CHEROKEE", "CHOWAN", "CLAY", "CLEVELAND", "COLUMBUS",
    "CRAVEN", "CUMBERLAND", "CURRITUCK", "DARE", "DAVIDSON", "DAVIE",
    "DUPLIN", "DURHAM", "EDGECOMBE", "FORSYTH", "FRANKLIN", "GASTON",
    "GATES", "GRAHAM", "GRANVILLE", "GREENE", "GUILFORD", "HALIFAX",
    "HARNETT", "HAYWOOD", "HENDERSON", "HERTFORD", "HOKE", "HYDE",
    "IREDELL", "JACKSON", "JOHNSTON", "JONES", "LEE", "LENOIR",
    "LINCOLN", "MACON", "MADISON", "MARTIN", "MCDOWELL", "MECKLENBURG",
    "MITCHELL", "MONTGOMERY", "MOORE", "NASH", "NEW HANOVER",
    "NORTHAMPTON", "ONSLOW", "ORANGE", "PAMLICO", "PASQUOTANK",
    "PENDER", "PERQUIMANS", "PERSON", "PITT", "POLK", "RANDOLPH",
    "RICHMOND", "ROBESON", "ROCKINGHAM", "ROWAN", "RUTHERFORD",
    "SAMPSON", "SCOTLAND", "STANLY", "STOKES", "SURRY", "SWAIN",
    "TRANSYLVANIA", "TYRRELL", "UNION", "VANCE", "WAKE", "WARREN",
    "WASHINGTON", "WATAUGA", "WAYNE", "WILKES", "WILSON", "YADKIN",
    "YANCEY",
]

# DNN/Telerik control IDs.
COUNTY_COMBO_ID = "dnn_ctr1464_View_cboCounty"
SEARCH_BUTTON_SEL = "#dnn_ctr1464_View_btnSearch"
RETURN_TO_LIST_SEL = "#dnn_ctr1464_View_btnReturnToList"
DETAIL_PANEL_SEL = "#dnn_ctr1464_View_pnlDetail"
RESULTS_PANEL_SEL = "#dnn_ctr1464_View_pnlSearchResults"
RESULTS_TABLE_SEL = "#dnn_ctr1464_View_rgSearchResults_ctl00"
RECORD_COUNT_SEL = "#dnn_ctr1464_View_lblRecordCount"
NEXT_PAGE_SEL = "#dnn_ctr1464_View_rgSearchResults_ctl00 .rgPageNext"

# Wait condition signaling the Telerik client object for the county combobox
# is ready. We don't gate on __doPostBack because under scrapy-playwright the
# page-supplied function isn't reliably exposed on `window` even after
# `domcontentloaded` (works fine in vanilla playwright). We install our own
# postback shim instead.
_CLIENT_READY_FN = (
    "() => typeof window.$find === 'function' && !!window.$find('%s')"
) % COUNTY_COMBO_ID

# Shim that ensures `window.__doPostBack` exists. Mirrors ASP.NET's inline
# WebForms script: write to the hidden __EVENTTARGET/__EVENTARGUMENT inputs
# and call form.submit() directly. We bypass form.onsubmit because the DNN
# handler walks `arguments.caller`, which throws when invoked from inside
# page.evaluate's strict-mode realm. Idempotent.
_INSTALL_POSTBACK_SHIM = """
() => {
    if (typeof window.__doPostBack === 'function'
        && window.__doPostBack.__ncShim !== true) {
        return 'native';
    }
    const form = document.forms['Form'] || document.getElementById('Form');
    if (!form) return 'no-form';
    const fn = function(target, arg) {
        const et = form.querySelector('input[name="__EVENTTARGET"]');
        const ea = form.querySelector('input[name="__EVENTARGUMENT"]');
        if (et) et.value = target || '';
        if (ea) ea.value = arg || '';
        HTMLFormElement.prototype.submit.call(form);
    };
    fn.__ncShim = true;
    window.__doPostBack = fn;
    return 'installed';
}
"""

DETAIL_BASIC_PREFIX = "dnn_ctr1464_View_FacilityDetail_rptBasicFacilityInfo_"
DETAIL_LICENSE_PREFIX = "dnn_ctr1464_View_FacilityDetail_rptLicenseInfo_"
DETAIL_OWNER_PREFIX = "dnn_ctr1464_View_FacilityDetail_"

# Telerik selects the county client-side. Find the matching item by text and
# call .select(); this both updates the hidden ClientState input and fires the
# change handlers the postback expects.
_SELECT_COUNTY_SCRIPT = """
(countyName) => {
    const combo = window.$find && window.$find('%s');
    if (!combo) { return false; }
    const item = combo.findItemByText(countyName);
    if (!item) { return false; }
    item.select();
    return true;
}
""" % COUNTY_COMBO_ID


# ---- Parsing helpers ---------------------------------------------------------


def _clean(text):
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned or None


def _span_text(sel, span_id):
    """Return all text inside #span_id, whitespace-collapsed."""
    nodes = sel.css(f"#{span_id} ::text").getall()
    if not nodes:
        return None
    return _clean(" ".join(nodes))


def _read_obfuscated_email(sel, span_id):
    """NC renders emails as `local<i class="fa-at"></i>domain`, sometimes
    inside an <a href="mailto:..."> wrapper. Prefer the mailto href if there
    is one; otherwise rebuild the address by substituting the fa-at icon."""
    href = sel.css(f'#{span_id} a[href^="mailto:"]::attr(href)').get()
    if href:
        addr = href.split(":", 1)[1].strip()
        if addr:
            return addr
    raw = sel.css(f"#{span_id}").get()
    if not raw:
        return None
    with_at = re.sub(r'<i\b[^>]*\bfa-at\b[^>]*></i>', "@", raw)
    text = "".join(Selector(text=with_at).css("::text").getall())
    text = re.sub(r"\s+", "", text)
    return text if text and "@" in text else None


def _to_int(value):
    if value is None:
        return None
    digits = re.sub(r"[^\d-]", "", value)
    if not digits or digits == "-":
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _yes_no_to_bool(value):
    if value is None:
        return None
    v = value.strip().lower()
    if v in ("yes", "y", "true"):
        return True
    if v in ("no", "n", "false"):
        return False
    return None


def _join_address(street, city, state, zip_code):
    state_zip = " ".join(p for p in [state, zip_code] if p)
    csz = ", ".join(p for p in [city, state_zip] if p)
    full = ", ".join(p for p in [street, csz] if p)
    return full or None


def parse_basic(sel):
    """Parse the Basic Information accordion section."""
    pfx = DETAIL_BASIC_PREFIX
    street = _span_text(sel, pfx + "FacilityStreetLabel_0")
    city = _span_text(sel, pfx + "FacilityCityLabel_0")
    state = _span_text(sel, pfx + "FacilityStateLabel_0")
    zip_code = _span_text(sel, pfx + "FacilityZipLabel_0")
    county = _span_text(sel, pfx + "CountyNameLabel_0")

    return {
        "license_number": _span_text(sel, pfx + "LicenseNumberLabel_0"),
        "provider_name": _span_text(sel, pfx + "FacilityNameLabel_0"),
        "address": _join_address(street, city, state, zip_code),
        "county": county.title() if county else None,
        "email": _read_obfuscated_email(sel, pfx + "EmailLabel_0"),
        "provider_website": _span_text(sel, pfx + "WebsiteLabel_0"),
        "provider_type": _span_text(sel, pfx + "FacilityTypeLabel_0"),
        "phone": _span_text(sel, pfx + "PhoneLabel_0"),
        "scholarships_accepted": _yes_no_to_bool(
            _span_text(sel, pfx + "SubsidyLabel_0")
        ),
        "nc_sanitation_inspection_date": _span_text(
            sel, pfx + "InspectionDateLabel_0"
        ),
        "nc_sanitation_classification": _span_text(
            sel, pfx + "ClassDescriptionLabel_0"
        ),
        "nc_sanitation_score": _to_int(
            _span_text(sel, pfx + "SanitationScoreLabel_0")
        ),
    }


def _parse_one_license(sel, idx):
    """Pull one License Information block (current is idx=0; previous start at 1)."""
    pfx = DETAIL_LICENSE_PREFIX
    license_type = _span_text(sel, f"{pfx}lblLicenseType_{idx}")
    if license_type is None:
        return None

    restrictions = []
    for r_idx in range(0, 25):
        rule = _span_text(
            sel, f"{pfx}rptRestrictions_{idx}_lblRestriction_{r_idx}"
        )
        if rule is None:
            break
        restrictions.append(rule)

    program_pts = _to_int(
        _span_text(sel, f"{pfx}rptScores_{idx}_lblProgramStandardsPoints_0")
    )
    program_max = _to_int(
        _span_text(sel, f"{pfx}rptScores_{idx}_lblProgramStandardsMaxPoints_0")
    )
    educational_pts = _to_int(
        _span_text(sel, f"{pfx}rptScores_{idx}_lblEducationalStandardsPoints_0")
    )
    educational_max = _to_int(
        _span_text(sel, f"{pfx}rptScores_{idx}_lblEducationalStandardsMaxPoints_0")
    )
    total_pts = _to_int(_span_text(sel, f"{pfx}rptScores_{idx}_lblTotalScore_0"))

    # The "out of N" trailing the total-points span is plain text in the
    # parent col-md-6, so reach for the parent's full text content.
    total_max = None
    if total_pts is not None:
        parent = sel.xpath(
            f"//*[@id='{pfx}rptScores_{idx}_lblTotalScore_0']/.."
        )
        if parent:
            joined = " ".join(parent.css("::text").getall())
            m = re.search(r"out of\s+(\d+)", joined)
            if m:
                total_max = int(m.group(1))

    return {
        "license_type": license_type,
        "effective_date": _span_text(sel, f"{pfx}lblFromDate_{idx}"),
        "age_range": _span_text(sel, f"{pfx}lblAgeRange_{idx}"),
        "capacity_first_shift": _to_int(
            _span_text(sel, f"{pfx}lblFirstShiftCapacity_{idx}")
        ),
        "capacity_second_shift": _to_int(
            _span_text(sel, f"{pfx}lblSecondShiftCapacity_{idx}")
        ),
        "capacity_third_shift": _to_int(
            _span_text(sel, f"{pfx}lblThirdShiftCapacity_{idx}")
        ),
        "license_restrictions": restrictions or None,
        "program_standards_points": program_pts,
        "program_standards_max_points": program_max,
        "educational_standards_points": educational_pts,
        "educational_standards_max_points": educational_max,
        "star_rating_total_points": total_pts,
        "star_rating_max_points": total_max,
    }


def parse_licenses(sel):
    """Parse License Information section. Index 0 is current; others are
    previous licenses ordered most-recent-first."""
    licenses = []
    for idx in range(0, 25):
        entry = _parse_one_license(sel, idx)
        if entry is None:
            break
        licenses.append(entry)
    return licenses


def parse_special_features(sel):
    """Pull Facility Special Features.

    Returns (services_list, ratios_dict). services covers any list rendered
    inside a `subheading-grey` group whose label contains "Services" or
    "Amenities". ratios captures any "Staff/Child Ratio Policy" group.
    """
    section = sel.xpath(
        "//div[@class='accordionHeader' and "
        "normalize-space(text())='Facility Special Features']"
        "/following-sibling::div[1]"
    )
    if not section:
        return [], {}

    services = []
    services_block = section.xpath(
        ".//div[contains(@class,'subheading-grey') and "
        "(contains(., 'Services') or contains(., 'Amenities'))]"
        "/following-sibling::div[1]"
    )
    for row in services_block.css("div.row.border-bottom"):
        cols = row.css("div.col-md-12")
        if cols:
            txt = _clean(" ".join(cols.css("::text").getall()))
            if txt:
                services.append(txt)

    ratios = {}
    ratios_subheading = section.xpath(
        ".//div[contains(@class,'subheading-grey') and "
        "contains(., 'Staff/Child Ratio')]"
    )
    if ratios_subheading:
        # The ratio rows are siblings of the subheading row, sharing a parent
        # `content` div. Walk forward from the subheading until we exit it.
        parent = ratios_subheading.xpath("./parent::div/parent::div")
        for row in parent.css("div.row.border-bottom"):
            cols = row.css("div.col-md-6")
            if len(cols) >= 2:
                age_group = _clean(" ".join(cols[0].css("::text").getall()))
                ratio = _clean(" ".join(cols[1].css("::text").getall()))
                if age_group and ratio:
                    ratios[age_group] = ratio

    return services, ratios


def parse_owner(sel):
    """Pull Owner Information."""
    pfx = DETAIL_OWNER_PREFIX
    return {
        "name": _span_text(sel, pfx + "lblOwnerName"),
        "mailing_address": _span_text(sel, pfx + "lblOwnerMailingAddress"),
        "phone": _span_text(sel, pfx + "lblOwnerPhone"),
        "fax": _span_text(sel, pfx + "lblOwnerFax"),
        "email": _read_obfuscated_email(sel, pfx + "lblOwnerEmail"),
    }


def parse_visits(sel):
    """Pull DCDEE Visits as a list of InspectionItem."""
    section = sel.xpath(
        "//div[@class='accordionHeader' and "
        "normalize-space(text())='DCDEE Visits']/following-sibling::div[1]"
    )
    if not section:
        return []

    inspections = []
    visit_rows = section.css("div.row.border-left.border-right.border-bottom")
    for row in visit_rows:
        cols = row.css("div.col-md-4")
        if len(cols) < 3:
            continue
        date = _clean(" ".join(cols[0].css("::text").getall()))
        visit_type = _clean(" ".join(cols[1].css("::text").getall()))
        violations_cell = cols[2]
        violations_text = _clean(" ".join(violations_cell.css("::text").getall()))
        has_violations = bool(violations_text) and violations_text.lower() == "yes"

        violation_details = []
        if has_violations:
            collapse_target = violations_cell.css("a::attr(href)").get() or ""
            target_id = collapse_target.lstrip("#")
            if target_id:
                # The collapse div is a sibling immediately after the row.
                violation_block = section.css(f"#{target_id}")
                for alert in violation_block.css(".alert-secondary"):
                    text = _clean(" ".join(alert.css("::text").getall()))
                    if text:
                        violation_details.append(text)

        inspection = InspectionItem()
        inspection["date"] = date
        inspection["type"] = visit_type
        inspection["nc_violations"] = violation_details or None
        inspections.append(inspection)
    return inspections


def build_item(html, county_hint=None):
    """Map a fully-rendered NC detail-page HTML string to a ProviderItem."""
    sel = Selector(text=html)

    basic = parse_basic(sel)
    licenses = parse_licenses(sel)
    services, ratios = parse_special_features(sel)
    owner = parse_owner(sel)
    inspections = parse_visits(sel)

    item = ProviderItem()
    item["source_state"] = "North Carolina"
    item["provider_url"] = SEARCH_URL

    item["provider_name"] = basic["provider_name"]
    item["license_number"] = basic["license_number"]
    item["address"] = basic["address"]
    item["county"] = basic["county"] or (
        county_hint.title() if county_hint else None
    )
    item["email"] = basic["email"]
    item["provider_website"] = basic["provider_website"]
    item["provider_type"] = basic["provider_type"]
    item["phone"] = basic["phone"]
    item["scholarships_accepted"] = basic["scholarships_accepted"]
    item["nc_sanitation_inspection_date"] = basic["nc_sanitation_inspection_date"]
    item["nc_sanitation_classification"] = basic["nc_sanitation_classification"]
    item["nc_sanitation_score"] = basic["nc_sanitation_score"]

    current = licenses[0] if licenses else None
    history = [
        {k: entry[k] for k in entry if k != "license_type" or entry["license_type"]}
        for entry in licenses[1:]
    ] or None

    if current:
        item["nc_license_type"] = current["license_type"]
        item["nc_license_effective_date"] = current["effective_date"]
        item["ages_served"] = current["age_range"]
        item["nc_capacity_first_shift"] = current["capacity_first_shift"]
        item["nc_capacity_second_shift"] = current["capacity_second_shift"]
        item["nc_capacity_third_shift"] = current["capacity_third_shift"]
        item["nc_license_restrictions"] = current["license_restrictions"]

        shift_total = sum(
            s for s in [
                current["capacity_first_shift"],
                current["capacity_second_shift"],
                current["capacity_third_shift"],
            ] if s
        )
        item["capacity"] = shift_total or None

        # Star rating: prefer current; fall back to most-recent previous license
        # that has it. NC carries ratings forward across renewals.
        star_total = current["star_rating_total_points"]
        star_max = current["star_rating_max_points"]
        prog_pts = current["program_standards_points"]
        educ_pts = current["educational_standards_points"]
        if star_total is None:
            for prev in licenses[1:]:
                if prev["star_rating_total_points"] is not None:
                    star_total = prev["star_rating_total_points"]
                    star_max = prev["star_rating_max_points"]
                    prog_pts = prev["program_standards_points"]
                    educ_pts = prev["educational_standards_points"]
                    break
        item["nc_star_rating_total_points"] = star_total
        item["nc_star_rating_max_points"] = star_max
        item["nc_program_standards_points"] = prog_pts
        item["nc_educational_standards_points"] = educ_pts
    else:
        item["nc_license_type"] = None
        item["nc_license_effective_date"] = None
        item["nc_capacity_first_shift"] = None
        item["nc_capacity_second_shift"] = None
        item["nc_capacity_third_shift"] = None
        item["nc_license_restrictions"] = None
        item["capacity"] = None
        item["nc_star_rating_total_points"] = None
        item["nc_star_rating_max_points"] = None
        item["nc_program_standards_points"] = None
        item["nc_educational_standards_points"] = None

    item["nc_license_history"] = history
    item["nc_special_features"] = services or None
    item["nc_staff_child_ratios"] = ratios or None

    item["nc_owner_name"] = owner["name"]
    item["nc_owner_mailing_address"] = owner["mailing_address"]
    item["nc_owner_phone"] = owner["phone"]
    item["nc_owner_fax"] = owner["fax"]
    item["nc_owner_email"] = owner["email"]

    item["inspections"] = inspections or None
    return item


def parse_pagination_total(html):
    """Read the record count and total page count off the results page.

    The footer span renders as 'X items in N pages'. Returns
    (record_count, total_pages); either may be None if the page is malformed.
    """
    sel = Selector(text=html)
    record_count = _to_int(_span_text(sel, "dnn_ctr1464_View_lblRecordCount"))
    pager_text = " ".join(
        sel.css("#dnn_ctr1464_View_rgSearchResults_ctl00 .rgInfoPart ::text").getall()
    )
    pages_match = re.search(r"in\s+(\d+)\s+pages?", pager_text)
    items_match = re.search(r"(\d+)\s+items?", pager_text)
    total_pages = int(pages_match.group(1)) if pages_match else None
    if record_count is None and items_match:
        record_count = int(items_match.group(1))
    if record_count is not None and total_pages is None:
        # Single-page results don't render a pager; assume one page.
        total_pages = 1
    return record_count, total_pages


def count_rows_on_page(html):
    """Count current-page rgRow / rgAltRow rows in the results grid."""
    sel = Selector(text=html)
    return len(
        sel.css(
            "#dnn_ctr1464_View_rgSearchResults_ctl00 tbody tr.rgRow, "
            "#dnn_ctr1464_View_rgSearchResults_ctl00 tbody tr.rgAltRow"
        )
    )


# ---- Spider ------------------------------------------------------------------


class NorthCarolinaSpider(scrapy.Spider):
    """Scrapes https://ncchildcare.ncdhhs.gov/childcaresearch by county.

    Each county runs in its own playwright context, parallel up to
    CONCURRENT_REQUESTS (controlled by the `concurrency` spider arg).
    """

    name = "north_carolina"
    allowed_domains = ["ncchildcare.ncdhhs.gov"]

    custom_settings = {
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 90 * 1000,
        "RETRY_TIMES": 2,
        "DOWNLOAD_DELAY": 0.5,
        # The DNN backend serves a stripped page (missing __EVENTTARGET and
        # the WebForms scripts) when the User-Agent looks like a bot, so
        # present a normal browser UA. The same UA also propagates to the
        # playwright context via PLAYWRIGHT_CONTEXT_ARGS below.
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    def __init__(self, concurrency=4, counties=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.concurrency = int(concurrency)

        if counties:
            requested = [c.strip().upper() for c in counties.split(",") if c.strip()]
            self.counties = [c for c in requested if c in NC_COUNTIES]
            unknown = [c for c in requested if c not in NC_COUNTIES]
            if unknown:
                self.logger.warning(
                    "Ignoring unknown counties: %s", ", ".join(unknown)
                )
        else:
            self.counties = list(NC_COUNTIES)

        # Override CONCURRENT_REQUESTS at spider-instance level so the
        # `concurrency` arg actually changes parallelism.
        self.custom_settings = dict(self.custom_settings)
        self.custom_settings["CONCURRENT_REQUESTS"] = self.concurrency
        self.custom_settings["CONCURRENT_REQUESTS_PER_DOMAIN"] = self.concurrency

    def start_requests(self):
        self.logger.info(
            "NC: launching %d county crawl(s) with concurrency=%d",
            len(self.counties),
            self.concurrency,
        )
        for county in self.counties:
            yield scrapy.Request(
                SEARCH_URL,
                callback=self.parse_county,
                dont_filter=True,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": f"nc-{county}",
                    "playwright_context_kwargs": {
                        "viewport": {"width": 1366, "height": 900},
                        "user_agent": (
                            "Mozilla/5.0 (X11; Linux x86_64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    },
                    "playwright_page_methods": [
                        PageMethod(
                            "wait_for_load_state", "domcontentloaded", timeout=60000
                        ),
                        PageMethod(
                            "wait_for_selector", f"#{COUNTY_COMBO_ID}", timeout=60000
                        ),
                    ],
                    "county": county,
                },
            )

    async def parse_county(self, response):
        page = response.meta["playwright_page"]
        county = response.meta["county"]
        try:
            yield_count = 0
            async for item in self._crawl_county(page, county):
                yield_count += 1
                yield item
            self.logger.info(
                "NC[%s]: county complete, yielded %d providers", county, yield_count
            )
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await page.context.close()
            except Exception:
                pass

    async def _crawl_county(self, page, county):
        # Wait for Telerik to register the combobox client object.
        try:
            await page.wait_for_function(_CLIENT_READY_FN, timeout=60000)
        except Exception as e:
            self.logger.error(
                "NC[%s]: Telerik combobox never ready (%s); skipping", county, e
            )
            return

        # Install our own __doPostBack shim if the page didn't expose one.
        # Some scrapy-playwright loads of /childcaresearch (e.g. when the UA
        # is non-browser-like and DNN serves a stripped page) drop the inline
        # WebForms script — the facility-name and next-page links rely on
        # `javascript:__doPostBack(...)`, so they need a global to call.
        shim_state = await page.evaluate(_INSTALL_POSTBACK_SHIM)
        if shim_state == "no-form":
            self.logger.error(
                "NC[%s]: ASP.NET form not found on page; skipping", county
            )
            return

        # 1. Pick county. Try Telerik client API first; fall back to clicking
        #    the dropdown if the JS API isn't ready.
        selected = await page.evaluate(_SELECT_COUNTY_SCRIPT, county)
        if not selected:
            self.logger.warning(
                "NC[%s]: Telerik selectByText failed; falling back to dropdown click",
                county,
            )
            try:
                await page.click(f"#{COUNTY_COMBO_ID} .rcbActionButton", timeout=15000)
                await page.wait_for_selector(
                    f"#{COUNTY_COMBO_ID}_DropDown li.rcbItem", timeout=15000
                )
                await page.locator(
                    f"#{COUNTY_COMBO_ID}_DropDown li.rcbItem", has_text=county
                ).first.click(timeout=15000)
            except Exception as e:
                self.logger.error(
                    "NC[%s]: could not select county (%s); skipping", county, e
                )
                return

        # 2. Submit by clicking the visible Search button. Its inline onclick
        #    runs the page-native `__doPostBack`, which calls form.submit()
        #    via the DNN onsubmit hook. Triggering __doPostBack directly from
        #    page.evaluate hits a strict-mode `arguments.caller` access in
        #    that hook, but a synthetic click executes in the page's own JS
        #    realm where the same code path runs non-strict.
        try:
            await page.click(SEARCH_BUTTON_SEL, timeout=20000)
        except Exception as e:
            self.logger.error(
                "NC[%s]: search-button click failed (%s); skipping", county, e
            )
            return
        try:
            await page.wait_for_selector(RESULTS_PANEL_SEL, timeout=60000)
        except Exception as e:
            self.logger.warning(
                "NC[%s]: results panel never appeared (%s); skipping county",
                county, e,
            )
            return
        # Re-install the shim — postback navigated to a fresh document.
        await page.evaluate(_INSTALL_POSTBACK_SHIM)

        html = await page.content()
        record_count, total_pages = parse_pagination_total(html)
        if not record_count:
            self.logger.info("NC[%s]: 0 records; nothing to scrape", county)
            return
        total_pages = total_pages or 1
        self.logger.info(
            "NC[%s]: search returned %d records across %d page(s)",
            county, record_count, total_pages,
        )

        seen = 0
        page_idx = 1
        while True:
            html = await page.content()
            row_count = count_rows_on_page(html)
            if row_count == 0:
                self.logger.warning(
                    "NC[%s]: page %d/%d had 0 rows; stopping (seen=%d/%d)",
                    county, page_idx, total_pages, seen, record_count,
                )
                return
            # Telerik occasionally pads the final page with a phantom row.
            # Cap row iteration at the remaining expected count to avoid
            # wasting a 20s click timeout on a row that doesn't exist.
            remaining = record_count - seen
            rows_to_process = min(row_count, max(remaining, 0))
            self.logger.info(
                "NC[%s]: page %d/%d, %d rows (processing %d, seen=%d/%d)",
                county, page_idx, total_pages, row_count, rows_to_process,
                seen, record_count,
            )

            for row_idx in range(rows_to_process):
                row_link_sel = (
                    f'tr[id="dnn_ctr1464_View_rgSearchResults_ctl00__{row_idx}"]'
                    " td:nth-child(2) a"
                )
                try:
                    await page.click(row_link_sel, timeout=20000)
                    await page.wait_for_selector(DETAIL_PANEL_SEL, timeout=60000)
                except Exception as e:
                    self.logger.warning(
                        "NC[%s]: row %d on page %d failed to open (%s); skipping",
                        county, row_idx, page_idx, e,
                    )
                    seen += 1
                    continue
                # Detail-page navigation re-rendered the document; re-shim.
                await page.evaluate(_INSTALL_POSTBACK_SHIM)

                detail_html = await page.content()
                yield build_item(detail_html, county_hint=county)
                seen += 1

                try:
                    await page.click(RETURN_TO_LIST_SEL, timeout=20000)
                    await page.wait_for_selector(RESULTS_PANEL_SEL, timeout=60000)
                except Exception as e:
                    self.logger.error(
                        "NC[%s]: failed to return to results from row %d page %d (%s); aborting county",
                        county, row_idx, page_idx, e,
                    )
                    return
                await page.evaluate(_INSTALL_POSTBACK_SHIM)

            if page_idx >= total_pages:
                self.logger.info(
                    "NC[%s]: reached final page %d/%d (yielded %d/%d)",
                    county, page_idx, total_pages, seen, record_count,
                )
                return

            self.logger.info(
                "NC[%s]: advancing to page %d/%d",
                county, page_idx + 1, total_pages,
            )
            try:
                await page.click(NEXT_PAGE_SEL, timeout=20000)
                await page.wait_for_load_state("domcontentloaded", timeout=60000)
                await page.wait_for_selector(RESULTS_TABLE_SEL, timeout=60000)
            except Exception as e:
                self.logger.warning(
                    "NC[%s]: next-page click failed on page %d (%s); stopping",
                    county, page_idx, e,
                )
                return
            await page.evaluate(_INSTALL_POSTBACK_SHIM)
            page_idx += 1
