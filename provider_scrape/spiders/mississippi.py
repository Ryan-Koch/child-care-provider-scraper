"""Mississippi MDHS child care provider search (mdhs.provider.webapps.ms.gov).

Site: https://www.mdhs.provider.webapps.ms.gov/ccsearch.aspx

MDHS **redesigned this application** (Bootstrap 5 markup, new ASP.NET control
tree) some time between the 2026-09-05 build and 2026-09-27. Every selector
the v1 spider used is gone, so this is a rebuild of the parse layer against
the current site. What changed, and what it means here:

 1. **The results markup is new.** A provider is now
    ``<div id="div_<facility id>" tabindex="-1" class="row border border-1
    rounded-3 ...">`` -- the old ``div.col-md-12[tabindex="1"]`` (single-quoted
    id, v1 Sec 5.1) no longer exists, which is why every county reported "0
    providers". The div id gained a ``div_`` prefix; it is stripped so
    ``ms_facility_id`` stays stable against the v1 corpus. Fields inside the
    container are ``<dl><dt>label</dt><dd>value</dd></dl>`` pairs, 20 rows per
    page (was 25).

 2. **The four detail tabs moved off the results page.** v1 read License /
    Inspections / Investigations / Monetary Penalties out of hidden panes that
    were already in the results HTML. Each is now its own page, reached by a
    ``__doPostBack`` on that provider's row, so the detail data costs up to
    four extra POSTs per provider (Phase 3 below). ``-a details=`` controls
    that (Sec "Spider arguments").

 3. **Detail postbacks are resolved from the posted ``__VIEWSTATE``, not from
    server-side session position** -- live-verified 2026-09-27: after paging
    the same session to page 2, re-posting page 1's viewstate with
    ``lstProviders$ctrl0$...`` still returned page 1's first provider. So a
    page's detail postbacks are order-independent and safe to interleave with
    each other and with the pager (contrast Maryland, where detail access was
    referer/session gated). The full hidden-field set must be echoed back
    though: a body carrying only ``__VIEWSTATE`` bounces to the empty search
    page, and one missing ``hdnFocusControl`` 500s.

 4. **The postback is positional** (``ctrl0``..``ctrl19`` = row order on the
    page that minted the viewstate), so every detail response is checked
    against the provider name it was requested for (``_name_key``) and its
    data is dropped rather than merged into the wrong provider if they differ.

 5. **Pagination is now a plain "Next Page" link plus an authoritative
    counter**: ``#lblRecordCount`` reads "201 records found - Page 1 of 11".
    The last page has no pager markup at all (live-verified on Clarke, "6
    records found - Page 1 of 1"), so the v1 numeric-window/"Last"-disabled
    pager parsing is gone. The label gives a per-county expected total, which
    ``closed()`` now reconciles against what was actually captured.

 6. **The v1 "poison page 37" (Sec: statewide walk) is not reachable** -- v1
    already worked around it by searching per county, which this keeps. The
    county fan-out is also still the only source of the ``county`` field (the
    redesign does not print a county on the row). ``ddlCounty`` no longer has
    to be re-sent on a pager postback (the redesign keeps the filter in the
    viewstate; verified both ways), but it is re-sent anyway -- harmless, and
    it keeps the v1 guarantee if MDHS reverts that behaviour.

 7. **The address is delimited now.** ``<dd>`` holds "<street><br>CITY, MS
    ZIP+4", so the v1 ``ddlCity`` longest-suffix dictionary (``split_city``)
    is only a fallback for a row that somehow lacks the city/state/zip line.
    ``zip`` and the assembled ``address`` keep v1's 5-digit form (the site now
    prints ZIP+4).

Three fields the redesign stopped publishing, and one it added:

 * **Coordinates are gone.** There is no ``htJson`` GeoJSON blob any more and
   the row's map link is an address query, not a lat/lon pair -- so
   ``latitude``/``longitude``/``geocode_source`` are no longer set here and
   Mississippi now depends on ``geocode_enrich`` (v1 got 1,398/1,463 from the
   site). ``closed()`` says so explicitly.
 * **Exam type is gone.** The Site Visit page prints only start/end/result per
   visit, so ``ms_exam_type`` is no longer populated for a regular visit
   (v1 had Renewal/Mid-Year/Initial/...).
 * **Site visit history is shorter.** A PLACE TO GROW shows 9 visits where v1
   captured 15 (nothing between 2019 and 2023 survived), so the inspection
   total will drop. That is what the site publishes now.
 * **Follow-up inspections are new** -- nested under their parent visit, and
   emitted as their own inspection rows tagged ``ms_exam_type="Follow Up"``.

Flow: GET the search page (harvest ``ddlCounty``, plus ``ddlCity`` for the
address fallback) -> one search POST per county, each on its own cookiejar ->
walk that county's pages with the Next Page postback -> for every row, chain
its (up to four) detail postbacks and emit the provider once the chain ends.
"""

import re
import urllib.parse

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://www.mdhs.provider.webapps.ms.gov/ccsearch.aspx"

# Safety valve only -- never the real stop rule, which is the "Page X of Y"
# counter plus the presence of a Next Page link. Per-county result sets are
# shallow (Hinds, the largest, is 11 pages), so this is a generous ceiling.
MAX_PAGES = 200

# Sanity floors for the two dropdown harvests (82 counties / 488 cities as of
# 2026-09-27) -- warn, don't fail: MDHS could plausibly add either.
EXPECTED_MIN_COUNTIES = 70
EXPECTED_MIN_CITIES = 400

# A provider row. The ``div_``-prefixed id is the facility id, and it is the
# only ``div.row.border`` on the page that has one (the badge/modal rows carry
# no id), so the attribute-prefix match anchors precisely on the 20 (or fewer,
# on the last page) provider containers -- item 1.
PROVIDER_CONTAINER_SEL = 'div.row.border[id^="div_"]'
CONTAINER_ID_PREFIX = "div_"

# "201 records found - Page 1 of 11" (item 5). The count is the county's
# authoritative expected total; the page numbers drive the stop rule.
RECORD_COUNT_SEL = "#lblRecordCount::text"
_RECORD_COUNT_RE = re.compile(
    r"(?P<total>[\d,]+)\s+records?\s+found\s*-\s*Page\s+(?P<page>\d+)\s+of\s+(?P<pages>\d+)",
    re.IGNORECASE,
)

# The pager is a single link (no numeric window, no "Last"), absent entirely
# on the final page -- item 5.
NEXT_PAGE_SEL = 'a[id$="lnkNextPage"]::attr(href)'
_DOPOSTBACK_RE = re.compile(r"__doPostBack\('(?P<target>[^']+)'")

# The four per-row detail postbacks, in the order the chain walks them:
# (link-id suffix on the row, handler name, label for logs). Targets are read
# off each row's own link hrefs rather than rebuilt from "lstProviders$ctrlN$"
# so a control rename can't silently mint a dead postback -- item 4.
DETAIL_TABS = (
    ("lbkBtnViewLicense", "_apply_license", "License"),
    ("lnkBtnViewInspection", "_parse_site_visits", "Site Visits"),
    ("lnkBtnViewInvestigation", "_parse_investigations", "Investigations"),
    ("lnkBtnViewMonetaryPenalty", "_parse_monetary_penalties", "Monetary Penalties"),
)
DETAILS_MODES = ("all", "license", "off")

# Every detail page titles itself "<what> of <br/>PROVIDER NAME" in this span
# -- the guard that a positional postback came back with the provider we asked
# for (item 4), and the tell that a postback bounced to the bare search page
# (no such span at all).
DETAIL_PROVIDER_SEL = 'span[id$="lblProviderInfo"]::text'

# ``lblLicenseStatusLabel`` text, e.g. "ACTIVE (12/01/2025 - 11/30/2026)" or
# "PENDING-INSPECTION (10/01/2026 - 09/30/2027)". The word group allows an
# internal hyphen (PENDING-INSPECTION) without also matching into the date
# range's own " - " separator, which is anchored by the parens. Unchanged by
# the redesign.
_STATUS_RE = re.compile(r"^(?P<word>[A-Z][A-Z-]*)\s*\((?P<begin>[\d/]+)\s*-\s*(?P<end>[\d/]+)\)\s*$")

# "JACKSON, MS 39213-7271" -- the address line the redesign added (item 7).
_CITY_STATE_ZIP_RE = re.compile(r"^(?P<city>.+?),\s*MS\s+(?P<zip>\d{5})(?:-\d{4})?\s*$", re.IGNORECASE)
# v1's tail, kept for the fallback path: "<street><CITY>, MS" with no zip.
_MS_ADDRESS_TAIL_RE = re.compile(r"^(?P<head>.*),\s*MS$")

# onclick="... window.open('PublicViewInspectionDocument.aspx?pdf=<token>', ...
# The redesign stopped URL-encoding the token, so a raw "+" or "/" now appears
# in the markup and has to be re-encoded before the URL is usable.
_PDF_WINDOW_OPEN_RE = re.compile(r"window\.open\('(?P<page>[^'?]+\.aspx)\?(?P<key>[^'=]+)=(?P<token>[^']*)'")

# Ages Served list item ids -> the item's coarse infant/toddler/preschool/
# school booleans. Anchored at the end so "liAge5to9"/"liAge10to12" can't be
# swallowed by the "liAge5" alternative.
_AGE_LI_RE = re.compile(r"li(InfantCare|Age1|Age2|Age3|Age4|Age5|Age5to9|Age10to12)$", re.IGNORECASE)
_AGE_GROUP_BUCKET = {
    "infantcare": "infant",
    "age1": "toddler",
    "age2": "toddler",
    "age3": "preschool",
    "age4": "preschool",
    "age5": "preschool",
    "age5to9": "school",
    "age10to12": "school",
}

# The redesign spells the months out ("January"); v1 emitted the site's old
# three-letter form. Mapped back so ms_months_of_operation stays comparable
# with the v1 corpus.
_MONTH_ABBREV = {
    "january": "Jan",
    "february": "Feb",
    "march": "Mar",
    "april": "Apr",
    "may": "May",
    "june": "Jun",
    "july": "Jul",
    "august": "Aug",
    "september": "Sep",
    "october": "Oct",
    "november": "Nov",
    "december": "Dec",
}

# Days & Hours Of Operation label -> the abbreviation used when assembling
# `hours` (e.g. "Mon-Fri 06:30 AM-11:00 PM; Sat 08:00 AM-03:00 PM"). The
# redesign relabelled the weekday row ("Monday through Friday"); v1's
# "Monday-Friday" spelling is kept so either wording works.
_DAY_ABBREV = {
    "Monday through Friday": "Mon-Fri",
    "Monday-Friday": "Mon-Fri",
    "Saturday": "Sat",
    "Sunday": "Sun",
}
_WHITESPACE_RE = re.compile(r"\s+")
_HOURS_TO_RE = re.compile(r"\s+To\s+", re.IGNORECASE)

# The services list doubles as the subsidy flag in the redesign, so this entry
# is pulled out of ms_services and turned into the boolean v1 read from its own
# span.
_SUBSIDY_SERVICE = "accepts mdhs subsidy children"

_NAME_KEY_RE = re.compile(r"[^A-Z0-9]")


def _name_key(name):
    """Comparison key for the positional-postback guard (item 4).

    Case- and punctuation-insensitive: the results row and the detail page
    render the same name through different controls, and an apostrophe or a
    double space must not read as "this is a different provider".
    """
    return _NAME_KEY_RE.sub("", (name or "").upper())


def split_city(addr_head, known_cities):
    """Split "<street><CITY>" into ``(street, Title-Cased city)``.

    Only the fallback path now (item 7): the redesign prints the city on its
    own line. Kept because a row without that line is still parseable this
    way -- Mississippi's v1 address had no delimiter between street and city
    (multi-word cities like "YAZOO CITY" make a naive last-token split
    unsafe), so the ``known_cities`` set -- harvested from the site's own
    ``ddlCity`` dropdown -- is used as a longest-suffix dictionary: the
    longest known city that ``addr_head`` ends with, on a word boundary, wins.
    Returns ``(addr_head, None)`` when no known city matches -- never guess
    where the city starts.
    """
    up = addr_head.upper().rstrip()
    best = None
    for city in known_cities:
        if up.endswith(city) and (best is None or len(city) > len(best)):
            boundary = len(up) - len(city)
            if boundary == 0 or up[boundary - 1] == " ":
                best = city
    if not best:
        return addr_head, None
    street = addr_head[: len(addr_head) - len(best)].rstrip()
    return (street or addr_head), best.title()


def _hidden_fields(response):
    """Every ``<input type="hidden">`` name/value pair on the current page.

    Response-agnostic by design: the bare search page and a results page each
    carry a different set, and this always returns exactly what THIS response
    has -- which is also exactly what the next postback has to echo back. A
    partial echo does not work (item 3).
    """
    fields = {}
    for inp in response.css('input[type="hidden"]'):
        name = inp.attrib.get("name")
        if name:
            fields[name] = inp.attrib.get("value", "")
    return fields


def _next_formdata(response, target, county_value):
    """Build a pager postback body from the CURRENT results page.

    No ``btnFind`` on a pager turn -- that's the search button, only sent on
    the initial search. ``ddlCounty`` is re-sent: the redesign keeps the
    county filter in the viewstate without it (verified 2026-09-27), but v1's
    site silently reverted to the statewide set, and re-sending costs nothing
    (item 6). ``county_value`` is the ``ddlCounty`` option value (e.g. ``"25"``
    for Hinds), not the display name.
    """
    formdata = _hidden_fields(response)
    formdata["__EVENTTARGET"] = target
    formdata["__EVENTARGUMENT"] = ""
    formdata["ddlCounty"] = county_value
    return formdata


def _detail_formdata(hidden, target):
    """Build a detail postback body from a results page's hidden fields.

    ``hidden`` is the dict captured off the results page that minted the
    ctrl-index in ``target`` -- it must be that page's, not the current
    response's, because the index is positional against it (item 4). The dict
    is shared by all of a page's chains and never mutated here.
    """
    formdata = dict(hidden)
    formdata["__EVENTTARGET"] = target
    formdata["__EVENTARGUMENT"] = ""
    return formdata


def _parse_record_count(label):
    """Parse "201 records found - Page 1 of 11" -> ``(201, 1, 11)``.

    Returns ``None`` if the label is missing or unparseable -- the caller
    falls back to the presence of a Next Page link (item 5).
    """
    if not label:
        return None
    match = _RECORD_COUNT_RE.search(label)
    if not match:
        return None
    total = int(match.group("total").replace(",", ""))
    return total, int(match.group("page")), int(match.group("pages"))


def _next_page_target(response):
    """The ``__EVENTTARGET`` of the Next Page link, or ``None`` on the last page."""
    href = response.css(NEXT_PAGE_SEL).get()
    if not href:
        return None
    match = _DOPOSTBACK_RE.search(href)
    return match.group("target") if match else None


def _postback_target(link):
    """The ``__EVENTTARGET`` inside a row link's ``javascript:__doPostBack`` href."""
    href = link.attrib.get("href") or ""
    match = _DOPOSTBACK_RE.search(href)
    return match.group("target") if match else None


def _detail_targets(container):
    """Map each DETAIL_TABS link-id suffix to that row's postback target."""
    targets = {}
    for link in container.css("a[id]"):
        link_id = link.attrib["id"]
        for suffix, _handler, _label in DETAIL_TABS:
            if link_id.endswith(suffix) or f"{suffix}_" in link_id:
                target = _postback_target(link)
                if target:
                    targets[suffix] = target
    return targets


def _dl_fields(container):
    """The row's ``<dt>label</dt><dd>value</dd>`` pairs as ``{label: <dd sel>}``.

    Labels keep the site's wording minus the trailing colon ("Provider Name",
    "Address", "Phone Number", "Email", "Type"). The ``<dd>`` selector is
    returned rather than its text because Address needs its lines and
    Provider Name sits inside a ``<strong>`` (item 1).
    """
    fields = {}
    for dt in container.css("dl.row > dt"):
        label = " ".join(t.strip() for t in dt.css("::text").getall() if t.strip())
        if not label:
            continue
        dd = dt.xpath("following-sibling::dd[1]")
        if dd:
            fields[label.rstrip(":").strip()] = dd[0]
    return fields


def _dd_lines(dd):
    """A ``<dd>``'s own text, split into lines and stripped.

    Only direct text children: the Address cell also holds a "View on Google
    Maps" anchor and a visually-hidden span, neither of which is address text.
    """
    return [t.strip() for t in dd.xpath("./text()").getall() if t.strip()]


def _dd_text(dd):
    """A ``<dd>``'s visible text as one squashed line (Phone/Email/Type)."""
    text = " ".join(t.strip() for t in dd.css("::text").getall() if t.strip())
    return _WHITESPACE_RE.sub(" ", text).strip()


def _pdf_url(link, response):
    """Absolute ``PublicViewInspectionDocument.aspx`` URL from a document link.

    The redesign emits the token unencoded inside ``window.open(...)``, so the
    ``+`` / ``/`` / ``=`` it contains are percent-encoded here -- a raw ``+``
    in a query string would reach the server as a space and the document would
    not resolve.
    """
    onclick = link.attrib.get("onclick") or ""
    match = _PDF_WINDOW_OPEN_RE.search(onclick)
    if not match:
        return None
    token = urllib.parse.quote(match.group("token"), safe="")
    return response.urljoin(f"{match.group('page')}?{match.group('key')}={token}")


def _detail_provider_name(response):
    """The provider name a detail page says it is about, or ``None``.

    The span reads "License and Service Details of <br/>A PLACE TO GROW", so
    the name is its last text node. ``None`` means the span is absent, i.e.
    the postback bounced to the bare search page (item 3).
    """
    parts = [t.strip() for t in response.css(DETAIL_PROVIDER_SEL).getall() if t.strip()]
    return parts[-1] if parts else None


def _parse_hours(response):
    """Assemble ``hours`` from the Days and Hours of Operation list.

    e.g. ``"Mon-Fri 06:30 AM-11:00 PM; Sat 08:00 AM-03:00 PM"``. A day's
    ``<dt>``/``<dd>`` pair only exists when the provider operates that day, so
    this naturally omits closed days.
    """
    rows = response.xpath('//dl[contains(@id,"dlOperationDaysAndHrs")]/dt')
    parts = []
    for dt in rows:
        label = " ".join(t.strip() for t in dt.css("::text").getall() if t.strip()).rstrip(":").strip()
        dd = dt.xpath("following-sibling::dd[1]")
        if not label or not dd:
            continue
        text = _WHITESPACE_RE.sub(" ", " ".join(t.strip() for t in dd[0].css("::text").getall() if t.strip()))
        if not text:
            continue
        parts.append(f"{_DAY_ABBREV.get(label, label)} {_HOURS_TO_RE.sub('-', text)}")
    return "; ".join(parts) if parts else None


class MississippiSpider(scrapy.Spider):
    """Spider for Mississippi's MDHS child care provider search (ccsearch.aspx).

    Spider arguments:
      ``-a details=all`` (default) -- License, Site Visits, Investigations and
        Monetary Penalties for every provider: four extra postbacks each, and
        every postback body carries the page's ~600 KB viewstate, so this is
        the bulk of the run (~6k requests statewide).
      ``-a details=license`` -- License only (status/capacity/ages/hours but no
        inspection records): one extra postback per provider.
      ``-a details=off`` -- results pages only; a fast structural smoke test.
      ``-a counties=HINDS,CLARKE`` -- restrict the fan-out to these counties
        (display names, case-insensitive) instead of all 82.
    """

    name = "mississippi"
    allowed_domains = ["mdhs.provider.webapps.ms.gov"]
    source_state = "Mississippi"

    custom_settings = {
        # Within one county the pagination is a strict sequential chain, but
        # the 82 counties are otherwise independent (no shared viewstate) --
        # Kansas-style, run them concurrently. Each county still gets its OWN
        # cookiejar: concurrency without that corrupted the shared session's
        # server-side pagination state on the v1 site (live-verified
        # 2026-09-05). The delay caps the whole run at ~2 requests/second,
        # which matters more now that the detail postbacks make the statewide
        # run ~6k requests of ~600 KB each rather than v1's ~90.
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 120,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
        "DEFAULT_REQUEST_HEADERS": {"Referer": SEARCH_URL},
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0",
    }

    def __init__(self, details="all", counties=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        details = (details or "all").strip().lower()
        if details not in DETAILS_MODES:
            raise ValueError(f"mississippi: -a details must be one of {', '.join(DETAILS_MODES)} (got {details!r})")
        self.details_mode = details
        # Which of DETAIL_TABS this run walks, in order.
        if details == "off":
            self.detail_tabs = ()
        elif details == "license":
            self.detail_tabs = DETAIL_TABS[:1]
        else:
            self.detail_tabs = DETAIL_TABS
        # Optional -a counties=HINDS,CLARKE fan-out restriction (smoke tests).
        self.county_filter = {c.strip().upper() for c in counties.split(",") if c.strip()} if counties else None
        # Harvested from the search page's ddlCity dropdown; used by
        # split_city for the address fallback path (item 7).
        self.known_cities = set()
        # Harvested from ddlCounty: {value ("01".."82"): NAME}. Populated in
        # parse_search_page, one search POST fired per entry.
        self.counties = {}
        self.total_items = 0
        # Per-county bookkeeping for the closed() summary and the completeness
        # reconciliation: running item total, last page reached, and the
        # expected total the site itself printed on page 1 (item 5).
        self.county_running_total = {}
        self.county_final_pages = {}
        self.county_expected_total = {}
        self.failed_counties = {}
        self.seen_facility_ids = set()
        self.duplicate_facility_ids = 0
        # Detail-phase failures: {tab label: count}. A failure never drops the
        # provider -- the partial item is still emitted.
        self.detail_failures = {}

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_search_page)

    # ------------------------------------------------------------------ #
    # Phase 1 -- search page: harvest the dropdowns, fan out per county
    # ------------------------------------------------------------------ #

    def parse_search_page(self, response):
        """Harvest ddlCity/ddlCounty and fire one search POST per county.

        The site's own ``ddlCounty`` dropdown is the county dictionary (82
        entries). Sending only ``btnFind`` + ``ddlCounty`` keeps every other
        filter blank, which is what returns a county's full set.
        """
        self.known_cities = {
            (opt.css("::text").get() or "").strip().upper()
            for opt in response.css('select[name="ddlCity"] option')
            if opt.attrib.get("value", "").strip()
        }
        if len(self.known_cities) < EXPECTED_MIN_CITIES:
            self.logger.warning(
                "mississippi: only %d cities harvested from ddlCity (expected ~488) -- "
                "the address fallback split may degrade",
                len(self.known_cities),
            )

        self.counties = {
            opt.attrib["value"].strip(): (opt.css("::text").get() or "").strip()
            for opt in response.css('select[name="ddlCounty"] option')
            if opt.attrib.get("value", "").strip()
        }
        if len(self.counties) < EXPECTED_MIN_COUNTIES:
            self.logger.warning(
                "mississippi: only %d counties harvested from ddlCounty (expected ~82) -- "
                "the site's markup may have changed",
                len(self.counties),
            )

        selected = {
            value: name
            for value, name in self.counties.items()
            if self.county_filter is None or name.upper() in self.county_filter
        }
        if self.county_filter is not None:
            missing = self.county_filter - {name.upper() for name in selected.values()}
            if missing:
                self.logger.warning(
                    "mississippi: -a counties named %s, which ddlCounty does not offer",
                    ", ".join(sorted(missing)),
                )
        self.logger.info(
            "mississippi: searching %d of %d counties, details=%s",
            len(selected),
            len(self.counties),
            self.details_mode,
        )

        base_formdata = _hidden_fields(response)
        base_formdata["__EVENTTARGET"] = ""
        base_formdata["__EVENTARGUMENT"] = ""
        base_formdata["btnFind"] = "Search"

        for value, county_name in selected.items():
            self.county_running_total[county_name] = 0
            formdata = dict(base_formdata)
            formdata["ddlCounty"] = value
            yield scrapy.FormRequest(
                SEARCH_URL,
                formdata=formdata,
                callback=self.parse_results,
                dont_filter=True,
                # Mandatory, not a nicety (Kansas Sec 5.1 precedent): with
                # counties running CONCURRENTLY, two counties interleaving on
                # one shared cookiejar corrupt each other's server-side
                # pagination state -- live-verified on the v1 site.
                meta={
                    "page": 1,
                    "county": county_name,
                    "cookiejar": county_name,
                    # The ddlCounty option VALUE (e.g. "25" for Hinds) --
                    # carried forward so every pager postback can re-send it.
                    "county_value": value,
                },
            )

    # ------------------------------------------------------------------ #
    # Phase 2 -- results + sequential pagination (per county)
    # ------------------------------------------------------------------ #

    def parse_results(self, response):
        page = response.meta["page"]
        county = response.meta["county"]
        county_value = response.meta["county_value"]
        containers = response.css(PROVIDER_CONTAINER_SEL)
        # raw_count (the actual row count on THIS page) drives every
        # pagination/failure decision below; count (post-dedupe, scheduled)
        # only drives the reporting totals. A cross-county duplicate must
        # never be mistaken for an empty page -- the page still had a real
        # row, it just wasn't a NEW one (Kansas precedent: dedupe must never
        # affect the stop rule).
        raw_count = len(containers)

        counted = _parse_record_count(response.css(RECORD_COUNT_SEL).get())
        if counted:
            total, label_page, pages = counted
            self.county_expected_total.setdefault(county, total)
            if label_page != page:
                # The server served a different page than the one we asked
                # for: our place in this county's chain is lost, so stop it
                # rather than double-count or silently truncate.
                self.logger.error(
                    "mississippi: %s asked for page %d but the site returned page %d of %d -- "
                    "stopping this county; %d providers already captured",
                    county,
                    page,
                    label_page,
                    pages,
                    self.county_running_total.get(county, 0),
                )
                self.failed_counties[county] = (
                    f"page mismatch (asked {page}, got {label_page} of {pages}) -- "
                    f"{self.county_running_total.get(county, 0)} captured before failure"
                )
                return
        else:
            total = pages = None
            self.logger.warning(
                "mississippi: %s page %d has no parseable record-count label -- "
                "falling back to the Next Page link for the stop rule",
                county,
                page,
            )

        # Every detail postback for this page is resolved against THIS page's
        # viewstate (item 3/4), so it is captured once and shared by all of
        # the page's chains rather than rebuilt per provider.
        hidden = _hidden_fields(response) if self.detail_tabs else None

        count = 0
        for container in containers:
            item = self._build_item(container, response, county)
            if item is None:
                continue
            facility_id = item["ms_facility_id"]
            if facility_id in self.seen_facility_ids:
                self.duplicate_facility_ids += 1
                self.logger.warning(
                    "mississippi: %s: id=%s already seen in another county -- skipped as a defensive duplicate",
                    county,
                    facility_id,
                )
                continue
            self.seen_facility_ids.add(facility_id)
            count += 1
            yield from self._start_detail_chain(item, container, hidden, county)

        self.total_items += count
        self.county_running_total[county] = self.county_running_total.get(county, 0) + count
        self.county_final_pages[county] = page
        self.logger.info(
            "mississippi: %s page %d/%s -> %d providers (county total %d, running total %d)",
            county,
            page,
            pages if pages else "?",
            count,
            self.county_running_total[county],
            self.total_items,
        )

        next_target = _next_page_target(response)

        if raw_count == 0:
            if page == 1:
                # A genuinely tiny/empty county is plausible -- not
                # necessarily a failure. The label confirms it: 0 records
                # found is the site agreeing.
                self.logger.warning(
                    "mississippi: %s has 0 providers on its search (label total %s) -- may genuinely have none",
                    county,
                    total if total is not None else "?",
                )
            elif next_target is None:
                self.logger.info("mississippi: %s page %d -> 0 providers on the final page", county, page)
            else:
                self.logger.error(
                    "mississippi: %s hit an unexplained 0-provider page at page %d while the "
                    "pager still offers a next page -- stopping this county early; %d providers "
                    "already captured for it",
                    county,
                    page,
                    self.county_running_total[county],
                )
                self.failed_counties[county] = (
                    f"stopped at page {page} (0 providers with a live next-page link) -- "
                    f"{self.county_running_total[county]} captured before failure"
                )
            return

        if next_target is None:
            # No pager markup at all is exactly what the last page looks like
            # (item 5) -- clean stop, unless the label says pages remain.
            if pages is not None and page < pages:
                self.logger.error(
                    "mississippi: %s page %d has no next-page link but the site says there are "
                    "%d pages -- this county is TRUNCATED at %d providers",
                    county,
                    page,
                    pages,
                    self.county_running_total[county],
                )
                self.failed_counties[county] = f"no next-page link on page {page} of {pages}"
            return

        if pages is not None and page >= pages:
            # The counter says this was the last page; a still-present Next
            # Page link would just re-serve it, so stop on the counter.
            return

        if page >= MAX_PAGES:
            self.logger.error(
                "mississippi: %s reached MAX_PAGES=%d without exhausting the pager -- "
                "forcibly stopped and this county is likely TRUNCATED",
                county,
                MAX_PAGES,
            )
            self.failed_counties[county] = f"hit MAX_PAGES={MAX_PAGES}"
            return

        yield scrapy.FormRequest(
            response.url,
            formdata=_next_formdata(response, next_target, county_value),
            callback=self.parse_results,
            dont_filter=True,
            meta={
                "page": page + 1,
                "county": county,
                # Same cookiejar as this county's every other request (see
                # the search POST's meta comment in parse_search_page) --
                # keeps the whole chain pinned to one isolated session.
                "cookiejar": county,
                "county_value": county_value,
            },
        )

    # ------------------------------------------------------------------ #
    # Per-provider item construction (results row)
    # ------------------------------------------------------------------ #

    def _build_item(self, container, response, county):
        raw_id = container.attrib.get("id") or ""
        pid = raw_id[len(CONTAINER_ID_PREFIX) :] if raw_id.startswith(CONTAINER_ID_PREFIX) else raw_id
        fields = _dl_fields(container)
        name_dd = fields.get("Provider Name")
        name = _dd_text(name_dd) if name_dd is not None else None
        if not pid or not name:
            self.logger.error(
                "mississippi: a results container is missing its id/name at %s -- skipped",
                response.url,
            )
            return None

        item = ProviderItem()
        item["source_state"] = self.source_state
        item["provider_url"] = SEARCH_URL
        item["provider_name"] = name
        item["ms_facility_id"] = pid
        item["state"] = "MS"
        # Populated because we search per-county (the county filter value maps
        # 1:1 to the ddlCounty dropdown's display name); the redesign prints
        # no county on the row itself.
        item["county"] = county.title()

        if fields.get("Phone Number") is not None:
            phone = _dd_text(fields["Phone Number"])
            if phone:
                item["phone"] = phone
        if fields.get("Email") is not None:
            email = _dd_text(fields["Email"])
            if email:
                item["email"] = email
        if fields.get("Type") is not None:
            provider_type = _dd_text(fields["Type"])
            if provider_type:
                item["provider_type"] = provider_type
        if fields.get("Address") is not None:
            self._apply_address(item, _dd_lines(fields["Address"]), pid)

        # The row's own subsidy banner. The License page lists the same fact
        # among the services, so this is one of two sources.
        if container.css('span[id*="lblMDHSSubsidy"]'):
            item["scholarships_accepted"] = True
            item["ms_subsidy"] = True

        return item

    def _apply_address(self, item, lines, pid):
        """Assemble ``address``/``city``/``zip`` from the ``<dd>``'s lines.

        The redesign prints "<street>" then "CITY, MS ZIP+4" (item 7), so the
        city/state/zip line is matched from the end of the list; anything
        before it is street. ``zip`` and the assembled address keep v1's
        5-digit form. A row without that line falls back to v1's ddlCity
        longest-suffix split.
        """
        if not lines:
            self.logger.warning("mississippi: id=%s has an empty address cell", pid)
            return
        for index in range(len(lines) - 1, -1, -1):
            match = _CITY_STATE_ZIP_RE.match(lines[index])
            if not match:
                continue
            street = " ".join(lines[:index]).strip()
            city = match.group("city").strip().title()
            zip_code = match.group("zip")
            item["city"] = city
            item["zip"] = zip_code
            item["address"] = f"{street}, {city}, MS {zip_code}" if street else f"{city}, MS {zip_code}"
            return

        # Fallback: no "CITY, MS ZIP" line to key on.
        joined = " ".join(lines).strip()
        tail = _MS_ADDRESS_TAIL_RE.match(joined)
        if not tail:
            self.logger.warning("mississippi: id=%s address has no ', MS' line: %r", pid, lines)
            item["address"] = joined
            return
        street, city = split_city(tail.group("head").strip(), self.known_cities)
        if city:
            item["city"] = city
            item["address"] = f"{street}, {city}, MS"
        else:
            self.logger.warning(
                "mississippi: id=%s address city not found in the ddlCity dictionary: %r",
                pid,
                tail.group("head"),
            )
            item["address"] = f"{tail.group('head').strip()}, MS"

    # ------------------------------------------------------------------ #
    # Phase 3 -- per-provider detail postbacks (License / Site Visits /
    # Investigations / Monetary Penalties), walked as one chain per provider
    # ------------------------------------------------------------------ #

    def _start_detail_chain(self, item, container, hidden, county):
        """Either emit the row as-is (``-a details=off``) or start its chain."""
        if not self.detail_tabs or hidden is None:
            yield item
            return
        targets = _detail_targets(container)
        missing = [label for suffix, _h, label in self.detail_tabs if suffix not in targets]
        if missing:
            self.logger.warning(
                "mississippi: %s: id=%s row has no postback link for %s -- those details are skipped",
                county,
                item["ms_facility_id"],
                ", ".join(missing),
            )
        request = self._detail_request(item, targets, hidden, county, [], 0)
        yield request if request is not None else item

    def _detail_request(self, item, targets, hidden, county, inspections, tab_index):
        """The next detail postback in a provider's chain, or ``None`` if done."""
        for index in range(tab_index, len(self.detail_tabs)):
            suffix, handler, label = self.detail_tabs[index]
            target = targets.get(suffix)
            if not target:
                continue
            return scrapy.FormRequest(
                SEARCH_URL,
                formdata=_detail_formdata(hidden, target),
                callback=self.parse_detail,
                errback=self.detail_errback,
                dont_filter=True,
                # Details outrank page turns. Every one of these bodies carries
                # the page's ~600 KB viewstate, so draining a page's 20 chains
                # before advancing the pager keeps the scheduler holding ~160
                # of them (8 concurrent counties) instead of one per provider
                # in the state -- ~0.9 GB of queued request bodies otherwise.
                # It also keeps each viewstate in use close to when it was
                # issued. Throughput is unchanged: DOWNLOAD_DELAY, not
                # ordering, is what paces the run.
                priority=1,
                # `hidden` is the shared results-page dict (one per page, not
                # one per provider) -- see _detail_formdata.
                meta={
                    "item": item,
                    "targets": targets,
                    "hidden": hidden,
                    "county": county,
                    "cookiejar": county,
                    "inspections": inspections,
                    "tab_index": index,
                    "tab_label": label,
                    "tab_handler": handler,
                },
            )
        return None

    def parse_detail(self, response):
        """Merge one detail tab into the pending item, then advance the chain."""
        meta = response.meta
        item = meta["item"]
        label = meta["tab_label"]
        inspections = meta["inspections"]
        pid = item["ms_facility_id"]

        detail_name = _detail_provider_name(response)
        if detail_name is None:
            # No provider header at all: the postback bounced (usually back to
            # the bare search page). Skip this tab's data, keep the chain.
            self._note_detail_failure(label)
            self.logger.error(
                "mississippi: %s: id=%s %s postback did not return a detail page -- tab skipped",
                meta["county"],
                pid,
                label,
            )
        elif _name_key(detail_name) != _name_key(item["provider_name"]):
            # Positional postback resolved to a different provider (item 4).
            # Dropping the tab is right: merging it would corrupt this item.
            self._note_detail_failure(label)
            self.logger.error(
                "mississippi: %s: id=%s %s page is for %r, not %r -- tab dropped rather than "
                "merged into the wrong provider",
                meta["county"],
                pid,
                label,
                detail_name,
                item["provider_name"],
            )
        else:
            handler = getattr(self, meta["tab_handler"])
            if label == "License":
                handler(item, response, pid)
            else:
                inspections.extend(handler(response, pid))

        request = self._detail_request(
            item, meta["targets"], meta["hidden"], meta["county"], inspections, meta["tab_index"] + 1
        )
        if request is not None:
            yield request
            return
        yield self._finalize(item, inspections)

    def detail_errback(self, failure):
        """A dead detail postback must not cost us the provider.

        Emits the partial item (everything the row and any earlier tab gave)
        instead of dropping the chain, and counts the failure for ``closed()``.
        """
        meta = failure.request.meta
        item = meta.get("item")
        if item is None:
            return
        label = meta.get("tab_label", "detail")
        self._note_detail_failure(label)
        self.logger.error(
            "mississippi: %s: id=%s %s postback failed (%s) -- emitting the provider without it",
            meta.get("county"),
            item.get("ms_facility_id"),
            label,
            failure.value,
        )
        return [self._finalize(item, meta.get("inspections") or [])]

    def _note_detail_failure(self, label):
        self.detail_failures[label] = self.detail_failures.get(label, 0) + 1

    def _finalize(self, item, inspections):
        if inspections:
            item["inspections"] = inspections
        return item

    # ------------------------------------------------------------------ #
    # License and Service Details page
    # ------------------------------------------------------------------ #

    def _apply_license(self, item, response, pid):
        """License No / Capacity / Status / Services / ages / months / hours."""
        license_number = response.css('span[id$="lblLicenseNumberLabel"]::text').get()
        if license_number and license_number.strip():
            item["license_number"] = license_number.strip()

        capacity_raw = response.css('span[id$="lblCapacityLabel"]::text').get()
        if capacity_raw and capacity_raw.strip():
            try:
                item["capacity"] = int(capacity_raw.strip())
            except ValueError:
                self.logger.warning("mississippi: id=%s non-integer capacity %r", pid, capacity_raw)

        status_raw = response.css('span[id$="lblLicenseStatusLabel"]::text').get()
        if status_raw and status_raw.strip():
            self._apply_status(item, status_raw, pid)

        self._apply_services(item, response)
        self._apply_age_groups(item, response)
        self._apply_months(item, response, pid)

        hours = _parse_hours(response)
        if hours:
            item["hours"] = hours

    def _apply_status(self, item, status_raw, pid):
        """Extract the status word and the licence date range."""
        m = _STATUS_RE.match(status_raw.strip())
        if not m:
            self.logger.warning("mississippi: id=%s unparsed status %r", pid, status_raw)
            item["status"] = status_raw.strip()
            return
        item["status"] = m.group("word")
        item["license_begin_date"] = m.group("begin")
        item["license_expiration"] = m.group("end")

    def _apply_services(self, item, response):
        """Provider Services, now a ``<ul>`` rather than a comma-joined string.

        "Accepts MDHS Subsidy Children" is listed as a service in the
        redesign; it is lifted out into the boolean v1 read from its own span
        so ``ms_services`` stays a list of actual services.
        """
        services_span = response.css('span[id$="lblServicesLabel"]')
        services = [t.strip() for t in services_span.css("li::text").getall() if t.strip()]
        if not services:
            # Fallback for the v1 shape (comma-joined text, no list).
            raw = " ".join(t.strip() for t in services_span.css("::text").getall() if t.strip())
            services = [s.strip() for s in raw.split(",") if s.strip()]

        subsidy = any(s.lower() == _SUBSIDY_SERVICE for s in services)
        services = [s for s in services if s.lower() != _SUBSIDY_SERVICE]
        if services:
            item["ms_services"] = services
        services_lower = ", ".join(services).lower()
        item["head_start"] = "head start" in services_lower
        item["ms_early_head_start"] = "early head start" in services_lower
        if subsidy or response.css('span[id*="lblMDHSSubsidy"]'):
            item["scholarships_accepted"] = True
            item["ms_subsidy"] = True

    def _apply_age_groups(self, item, response):
        """Ages Served, read off the ``<li>`` ids (an absent age = not served)."""
        checked_buckets = {"infant": False, "toddler": False, "preschool": False, "school": False}
        labels = []
        for li in response.css('ul[id$="ulAgeGroupsServed"] > li'):
            match = _AGE_LI_RE.search(li.attrib.get("id", ""))
            if not match:
                continue
            bucket = _AGE_GROUP_BUCKET.get(match.group(1).lower())
            if bucket:
                checked_buckets[bucket] = True
            label = " ".join(t.strip() for t in li.css("::text").getall() if t.strip())
            if label:
                labels.append(label)
        item["infant"] = checked_buckets["infant"]
        item["toddler"] = checked_buckets["toddler"]
        item["preschool"] = checked_buckets["preschool"]
        item["school"] = checked_buckets["school"]
        if labels:
            item["ages_served"] = ", ".join(labels)

    def _apply_months(self, item, response, pid):
        """Months of Operation, mapped back to v1's three-letter form."""
        months = []
        for text in response.css('ul[id$="ulOperationMonths"] > li::text').getall():
            name = text.strip()
            if not name:
                continue
            abbrev = _MONTH_ABBREV.get(name.lower())
            if abbrev is None:
                self.logger.warning("mississippi: id=%s unrecognized month of operation %r", pid, name)
                months.append(name)
            else:
                months.append(abbrev)
        if months:
            item["ms_months_of_operation"] = months

    # ------------------------------------------------------------------ #
    # Site Visit / Investigation / Monetary Penalty pages
    # ------------------------------------------------------------------ #

    def _parse_site_visits(self, response, pid):
        """Site visits, plus the follow-up inspections nested under each one.

        Top-level visits carry no exam type any more (module docstring), so
        ``ms_exam_type`` is only set on follow-ups, where the markup names
        them. A visit can carry more than one document; the first is used, as
        in v1's one-``report_url``-per-row shape.
        """
        out = []
        for exam in response.css("#accordionSiteVisits > div.accordion-item"):
            record = self._exam_record(exam, "Exam", "lnkViewSiteVisitDoc", None, response, pid, "site visit")
            if record is not None:
                out.append(record)
            for followup in exam.css("div.child-accordion > div.accordion-item"):
                record = self._exam_record(
                    followup, "Followup", "lnkViewFollowupDoc", "Follow Up", response, pid, "follow up inspection"
                )
                if record is not None:
                    out.append(record)
        return out

    def _exam_record(self, sel, prefix, doc_id_part, exam_type, response, pid, kind):
        """One site-visit (or follow-up) accordion entry as an InspectionItem.

        The ``lblExam*`` / ``lblFollowup*`` id prefixes are what keep a parent
        visit's own dates apart from its nested follow-ups' -- the follow-up
        block is a descendant of the visit block, so a plain "first date span"
        read would mix them.
        """
        date = sel.css(f'span[aria-labelledby^="lbl{prefix}StartDt"]::text').get()
        if not date or not date.strip():
            self.logger.warning("mississippi: id=%s a %s entry has no start date -- skipped", pid, kind)
            return None
        insp = InspectionItem()
        insp["type"] = "Inspection"
        insp["date"] = date.strip()
        if exam_type:
            insp["ms_exam_type"] = exam_type
        end_date = sel.css(f'span[aria-labelledby^="lbl{prefix}EndDt"]::text').get()
        if end_date and end_date.strip():
            insp["ms_end_date"] = end_date.strip()
        status = sel.css(f'span[aria-labelledby^="lbl{prefix}Status"]::text').get()
        if status and status.strip():
            insp["original_status"] = status.strip()
        for link in sel.css(f'a[id*="{doc_id_part}"]'):
            url = _pdf_url(link, response)
            if url:
                insp["report_url"] = url
                break
        return insp

    def _parse_investigations(self, response, pid):
        return self._parse_grid(response, pid, "gvInvestigations", "lblInvestigationDate", "Investigation")

    def _parse_monetary_penalties(self, response, pid):
        return self._parse_grid(response, pid, "gvMonetaryPenalties", "lblDateIssued", "Monetary Penalty")

    def _parse_grid(self, response, pid, table_id, date_label, kind):
        """Investigations and Monetary Penalties share one 3-column GridView.

        The whole table is absent when a provider has none (the common case),
        which is an empty list, not a failure.
        """
        out = []
        for row in response.css(f'table[id$="{table_id}"] tr'):
            if row.css("th"):
                continue
            date = row.css(f'span[id*="{date_label}"]::text').get()
            if not date or not date.strip():
                self.logger.warning("mississippi: id=%s a %s row has no date -- skipped", pid, kind)
                continue
            insp = InspectionItem()
            insp["type"] = kind
            insp["date"] = date.strip()
            description = row.css('span[id*="lblDescription"]::text').get()
            if description and description.strip():
                insp["ms_description"] = description.strip()
            for link in row.css('a[id*="lnkBtnViewDocument"]'):
                url = _pdf_url(link, response)
                if url:
                    insp["report_url"] = url
                    break
            out.append(insp)
        return out

    # ------------------------------------------------------------------ #

    def closed(self, reason):
        self.logger.info(
            "mississippi: finished (%s) -- %d counties, %d providers, %d duplicate ids skipped, "
            "%d county failure(s), details=%s",
            reason,
            len(self.county_running_total),
            self.total_items,
            self.duplicate_facility_ids,
            len(self.failed_counties),
            self.details_mode,
        )
        self.logger.info(
            "mississippi: the site no longer publishes coordinates (the htJson blob went away in "
            "the 2026-09 redesign) -- latitude/longitude for this state now come from geocode_enrich"
        )
        # Completeness check the redesign made possible: every county's page 1
        # prints its own expected total (item 5).
        for county in sorted(self.county_running_total):
            captured = self.county_running_total[county]
            expected = self.county_expected_total.get(county)
            self.logger.info(
                "mississippi: county summary -- %s: %d providers across %s page(s) (site said %s)",
                county,
                captured,
                self.county_final_pages.get(county, "?"),
                expected if expected is not None else "?",
            )
            if expected is not None and captured != expected and county not in self.failed_counties:
                self.logger.error(
                    "mississippi: %s captured %d providers but the site's own counter said %d",
                    county,
                    captured,
                    expected,
                )
        if self.detail_failures:
            self.logger.error(
                "mississippi: %d detail postback(s) failed and were skipped: %s "
                "(the affected providers were still emitted, minus those tabs)",
                sum(self.detail_failures.values()),
                "; ".join(f"{label}: {count}" for label, count in sorted(self.detail_failures.items())),
            )
        if self.failed_counties:
            self.logger.error(
                "mississippi: %d county/counties did not finish cleanly: %s",
                len(self.failed_counties),
                "; ".join(f"{county} ({detail})" for county, detail in sorted(self.failed_counties.items())),
            )
