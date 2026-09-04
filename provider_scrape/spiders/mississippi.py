"""Mississippi child care provider spider.

Source: MDHS's public child-care search at
https://www.mdhs.provider.webapps.ms.gov/ccsearch.aspx -- a classic ASP.NET
WebForms app (IIS 10, ASP.NET 4.0, ``__VIEWSTATE``/``__EVENTVALIDATION``). See
tasks/mississippi_story/mississippi_plan.md for the full recon writeup; the
traps that will silently corrupt the dataset if missed are:

  1. **It's a full-page postback, not an AJAX partial** -- despite the source
     story calling it "AJAX style," there is no ``UpdatePanel``/
     ``__ASYNCPOST``. Every search and page turn returns a complete HTML
     document, parsed with plain ``FormRequest``/``Request`` throughout.
  2. **No detail phase at all.** Every field for every provider -- the
     always-visible panel *and* all four Bootstrap tabs (License & Services,
     Inspections, Investigations, Monetary Penalties) -- is embedded in the
     paginated results HTML. There is nothing to click into.
  3. **The provider container's ``id`` is SINGLE-quoted**
     (``<div id='20006183' class="col-md-12" tabindex="1">``) while every
     other attribute on the page is double-quoted. An attribute selector is
     quote-agnostic, so ``div.col-md-12[tabindex="1"]`` still matches; a naive
     double-quote-only regex would silently match zero providers.
  4. **Pagination is viewstate-driven and positional.** A fresh cookieless
     request replaying another session's ``__VIEWSTATE`` *sequentially*
     (one request in flight at a time) still turns the page correctly. But
     the pager's ``__EVENTTARGET`` values are positional (``ctl01$ctlNN``
     slides with the 5-page window), so a far page's target simply doesn't
     exist until you get there -- pages must be walked sequentially, never
     fanned out from a precomputed list. **This is NOT the same as being
     safe under concurrency** -- see item 8.
  5. **Coordinates are published** in a hidden ``htJson`` GeoJSON blob (one
     ``Feature`` per provider, ``coordinates: [lng, lat]``, keyed by
     ``properties.id``) -- so Mississippi skips the Census geocode pass
     entirely; ``geocode_source`` is stamped ``"state"`` directly here.
  6. **The address has no street/city delimiter** (``"875 E FIFTEENTH ST
     YAZOO CITY, MS"``) -- split via the site's own ``ddlCity`` dropdown
     (487 names) as a longest-suffix dictionary (see :func:`split_city`).
  7. **The statewide (unfiltered) result set has a "poison" page.** Live-run
     discovery, 2026-09-05: walking the full ~1,472-row statewide search
     sequentially deterministically 302-redirects to ``generalError.aspx``
     at page 37 (offset ~901-925) -- reproduced across independent runs at
     different speeds/elapsed times (ruling out a session/time timeout),
     with a stable ~400 KB ``__VIEWSTATE`` (ruling out viewstate bloat), and
     even when page 36 is reached via group-jump ellipses rather than a full
     walk (ruling out postback depth/count). The server appears to choke
     rendering that specific offset inside the full statewide row set --
     page 41 (the next group) loads fine, and the tail (page 59, via the
     ``Last`` button) is reachable, so the data exists; only that one
     offset, in a set that large, is unrenderable. **Fix: partition the
     search by county** (Sec below) so no single query's result set is ever
     deep enough to reach the poison offset.
  8. **Concurrent counties each get their own cookiejar.** When
     ``CONCURRENT_REQUESTS`` was raised so the 82 county chains run in
     parallel, a defensive per-county ``cookiejar`` was added (Kansas Sec
     5.1 precedent: "two counties sharing a cookiejar corrupt each other's
     paging offset"). Kept as good practice for the concurrent fan-out, but
     see item 9 -- it was NOT, by itself, what caused the failure observed
     immediately after enabling concurrency; that was a separate, plain
     formdata bug.
  9. **The pager postback silently drops the county filter.** Live-run
     discovery, 2026-09-05, root-caused via direct reproduction: the ORIGINAL
     ``_next_formdata`` built a page-turn's POST body from the current
     response's hidden fields plus the pager target only -- it never
     re-included ``ddlCounty``. The filter took effect on page 1 (sent
     explicitly by the search POST) but was silently DROPPED from page 2
     onward, so the server reverted every subsequent page to the FULL
     statewide result set. This produced two symptoms that looked like a
     repeat of item 7's poison page: (a) thousands of "cross-county
     duplicate" skips (large counties' page 2+ were actually re-scraping the
     statewide set), and (b) 14 large counties dying at the identical
     statewide "page 37" 302 once their own page count happened to reach it.
     **Fix, confirmed live:** ``ddlCounty`` (the option VALUE, e.g. ``"25"``
     for Hinds) is threaded through ``meta["county_value"]`` and re-sent on
     EVERY pager postback via ``_next_formdata``, not just the initial
     search. Confirmed live: Hinds alone (200 providers / 8 pages) completes
     cleanly with page 2 showing genuinely Hinds-filtered rows, no page-37
     302.

Two callbacks do all the work: ``start_requests`` -> GET the search page ->
``parse_search_page`` harvests the hidden fields, the ``ddlCity`` dictionary,
and the ``ddlCounty`` dictionary (82 counties), then fires one search POST
**per county** (``ddlCounty=<value>``, everything else blank, its own
``cookiejar``) rather than a single statewide search -- this dodges the
poison page (item 7), and as a bonus lets every item carry a real ``county``
value. ``parse_results`` parses one county's one results page -- coordinates
from ``htJson``, every field for every provider straight off the page markup
-- and, unless the pager shows this county is on its last page, chains that
county's next-page postback sequentially, within the SAME cookiejar and with
``ddlCounty`` re-sent every time (item 9). Each county's chain is otherwise
independent (no shared viewstate), so counties run concurrently; if one
county's chain hits an unexpected 0-provider page (a repeat of the
poison-page symptom, now scoped to a single ~25-row county page instead of
572 statewide rows) it is logged and abandoned WITHOUT aborting the other 81
counties.
"""

import json
import re

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

SEARCH_URL = "https://www.mdhs.provider.webapps.ms.gov/ccsearch.aspx"

# Safety valve only (Sec 5.2) -- never the real stop rule, which is driven
# entirely by the pager (Sec 2.3). Per-county result sets are shallow (a
# handful of pages at most), so this is a generous ceiling, not an expected
# depth.
MAX_PAGES = 200

# Sanity floor for the ddlCounty harvest (82 real Mississippi counties as of
# 2026-09-05) -- warn (not error; MDHS could plausibly add one) if a scrape
# finds far fewer.
EXPECTED_MIN_COUNTIES = 70

# The id of the top-level ListView pager. Each tab's nested table has its OWN
# DataPager with a different id (e.g. "...ucSiteVisit...lvDataPager") -- never
# confuse the two (Sec 5.3).
MAIN_PAGER_ID = "lvProvider_lvDataPager"

# The container div id is single-quoted at source
# (``<div id='20006183' class="col-md-12" tabindex="1">``) while every other
# attribute on the page is double-quoted (Sec 5.1). An attribute selector
# doesn't care about the source quote character, so this still matches. The
# other "col-md-12" divs on the page (the panel heading, the address
# sub-divs) lack ``tabindex="1"``, so the combination anchors precisely on
# the 25 (or fewer, on the last page) provider containers.
PROVIDER_CONTAINER_SEL = 'div.col-md-12[tabindex="1"]'

# ``lblLicenseStatusLabel`` text, e.g. " ACTIVE (09/01/2026 - 08/31/2027)" or
# " PENDING-INSPECTION (10/01/2026 - 09/30/2027)". The word group allows an
# internal hyphen (PENDING-INSPECTION) without also matching into the date
# range's own " - " separator, which is anchored by the parens (Sec 5.6).
_STATUS_RE = re.compile(r"^(?P<word>[A-Z][A-Z-]*)\s*\((?P<begin>[\d/]+)\s*-\s*(?P<end>[\d/]+)\)\s*$")

# The address text is "<street><CITY>, MS" with no delimiter between street
# and city (Sec 5.4) -- this only peels off the trailing ", MS".
_MS_ADDRESS_TAIL_RE = re.compile(r"^(?P<head>.*),\s*MS$")

# onclick="OpenPDFInPopUpWindow('PublicViewInspectionDocument.aspx', 'pdf','<url-encoded-token>');return false;"
_PDF_ONCLICK_RE = re.compile(r"OpenPDFInPopUpWindow\('([^']*)',\s*'([^']*)',\s*'([^']*)'\)")

# The pager is parsed from its serialized HTML string (not a scrapy Selector
# walk) because document order across mixed <input>/<a>/<span> siblings is
# what the Sec 2.3 algorithm depends on, and a plain regex over the
# already-parsed (entity-decoded) markup is the simplest way to get that
# reliably. A bare `<span>N</span>` (no href) is the current page; every
# other page is an `<a href="javascript:__doPostBack('TARGET','')">` link,
# numeric or "...".
_PAGER_CURRENT_RE = re.compile(r"<span>(\d+)</span>")
_PAGER_LINK_RE = re.compile(r"<a href=\"javascript:__doPostBack\('([^']+)',\s*''\)\">([^<]*)</a>")
_PAGER_LAST_RE = re.compile(r'<input[^>]*value="Last"[^>]*>')

# Age Group Served checkboxes -> the item's coarse infant/toddler/preschool/
# school booleans (Sec 6.1). These ids are stable (no casing drift, unlike
# the month checkboxes below), but matched via regex rather than a bare
# substring so "Age5PreSch" / "Age5to9" can't collide with a hypothetical
# "Age5" prefix.
_AGE_CHECKBOX_RE = re.compile(r"chkbx(Infant|Age1|Age2|Age3|Age4|Age5PreSch|Age5to9)(?:_\d+)?$", re.IGNORECASE)
_AGE_GROUP_BUCKET = {
    "infant": "infant",
    "age1": "toddler",
    "age2": "toddler",
    "age3": "preschool",
    "age4": "preschool",
    "age5presch": "preschool",
    "age5to9": "school",
}

# Days & Hours Of Operation label -> the abbreviation used when assembling
# `hours` (Sec 6.1 example: "Mon-Fri 06:00 AM-06:00 PM; Sat ...").
_DAY_ABBREV = {"Monday-Friday": "Mon-Fri", "Saturday": "Sat", "Sunday": "Sun"}
_WHITESPACE_RE = re.compile(r"\s+")
_HOURS_TO_RE = re.compile(r"\s+To\s+", re.IGNORECASE)


def split_city(addr_head, known_cities):
    """Split "<street><CITY>" into ``(street, Title-Cased city)`` (Sec 5.4).

    Mississippi's address has no delimiter between the street and the city
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

    Response-agnostic by design: the bare search page and a results page
    each carry a different set of hidden fields, and this always returns
    exactly what THIS response has -- which is also exactly what the next
    postback needs to echo back (the search response's own ``__VIEWSTATE``
    seeds the first pager postback, that page's response seeds the next, and
    so on -- Sec 5.2).
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
    the initial (empty) search (Sec 2.3). ``ddlCounty`` MUST be re-sent here
    too: live-verified 2026-09-05 that omitting it on a pager postback
    silently drops the county filter from page 2 onward, reverting the
    server to the full statewide result set (re-triggering the poison page
    on any county whose statewide-offset happens to land past ~row 900, and
    making every subsequent page a set of already-seen cross-county
    duplicates). ``county_value`` is the ``ddlCounty`` option value (e.g.
    ``"25"`` for Hinds), not the display name.
    """
    formdata = _hidden_fields(response)
    formdata["__EVENTTARGET"] = target
    formdata["__EVENTARGUMENT"] = ""
    formdata["ddlCounty"] = county_value
    return formdata


def _parse_pager(pager_html):
    """Parse the top-level results pager's serialized HTML (Sec 2.3).

    Returns ``(current_page, next_target, last_disabled)``, or ``None`` if no
    bare (link-less) ``<span>N</span>`` -- the current-page marker -- is
    found at all.

    ``next_target`` is the ``__EVENTTARGET`` for turning to page
    ``current + 1``: an in-window numeric link when one exists, otherwise the
    first "..." group-jump link that appears AFTER the current page's marker
    in the pager's HTML (the "next group" jump) -- exactly one of those two
    is always present unless ``current`` is the very last page, in which case
    both are absent and this returns ``None`` for ``next_target``.
    """
    current_match = _PAGER_CURRENT_RE.search(pager_html)
    if not current_match:
        return None
    current = int(current_match.group(1))
    current_pos = current_match.start()

    numeric = {}
    ellipsis_target = None
    for m in _PAGER_LINK_RE.finditer(pager_html):
        target, text = m.group(1), m.group(2).strip()
        if text.isdigit():
            numeric[int(text)] = target
        elif text == "..." and m.start() > current_pos and ellipsis_target is None:
            ellipsis_target = target

    last_tag = _PAGER_LAST_RE.search(pager_html)
    last_disabled = bool(last_tag and "disabled" in last_tag.group(0))

    next_target = numeric.get(current + 1, ellipsis_target)
    return current, next_target, last_disabled


def _parse_coordinates(response):
    """Parse the hidden ``htJson`` GeoJSON blob into ``{id: (lat, lon)}``.

    ``coordinates`` is GeoJSON order (``[lng, lat]`` -- Sec 5.7); swapping
    the axes lands every provider in the ocean off Somalia. Joined by
    ``properties.id`` (== the container div id), never by list position.
    scrapy/lxml already HTML-entity-decodes the attribute value, so no manual
    ``html.unescape`` is needed before ``json.loads``.
    """
    raw = response.css('input[name="htJson"]::attr(value)').get()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    coords = {}
    for feature in data.get("features") or []:
        geometry = feature.get("geometry") or {}
        points = geometry.get("coordinates")
        pid = (feature.get("properties") or {}).get("id")
        if pid and isinstance(points, list) and len(points) == 2:
            lon, lat = points
            coords[str(pid)] = (lat, lon)
    return coords


def _panel_body_fields(container):
    """Parse the always-visible panel body: Address/Phone/Email/Type + subsidy.

    Returns ``(fields, zip_code, subsidy)``. ``fields`` maps the label text
    (colon stripped) to its value text, e.g. ``{"Address": "1129 N NATCHEZ ST
    KOSCIUSKO, MS", "Phone Number": "662-792-4180", ...}``. ``zip_code`` is
    the ``Zip4`` span text (only present on the Address row). ``subsidy`` is
    True iff the "Accepts MDHS Subsidy Children" span is present here.

    Scoped to the OUTER wrapper div that is a direct child of ``panel-body``
    -- the ``tab-content`` (License/Inspections/Investigations/Monetary
    Penalties) is a *sibling* of that wrapper, not a descendant, so this
    never picks up the License tab's own label/span pairs even though a
    plain descendant ``label`` search would (both live under the same
    ``panel-body``).
    """
    fields = {}
    zip_code = None
    subsidy = False
    wrapper = container.xpath(
        './/div[@class="panel-body"]/div[contains(concat(" ", normalize-space(@class), " "), " col-md-8 ")]'
    )
    if not wrapper:
        return fields, zip_code, subsidy
    for row in wrapper[0].xpath("./div[contains(@class,'col-md-8') or contains(@class,'col-md-6')]"):
        label = row.css("label::text").get()
        if label:
            key = label.strip().rstrip(":")
            text = " ".join(t.strip() for t in row.xpath("./text()").getall() if t.strip())
            fields[key] = text
            if key == "Address":
                span_text = row.css("span::text").get()
                if span_text:
                    zip_code = span_text.strip()
        elif row.css('span[id*="lblMDHSSubsidy"]'):
            subsidy = True
    return fields, zip_code, subsidy


def _parse_hours(license_pane):
    """Assemble ``hours`` from the Days & Hours Of Operation fieldset.

    e.g. ``"Mon-Fri 06:00 AM-06:00 PM; Sat 08:00 AM-03:00 PM"``. A day's
    fieldset entry only exists in the markup when the provider operates that
    day, so this naturally omits closed days.
    """
    fieldset = license_pane.xpath('.//fieldset[.//h5[contains(text(),"Days")]]')
    if not fieldset:
        return None
    parts = []
    for span in fieldset[0].css('span[id*="Hours"]'):
        label = span.xpath("../label[1]//text()").get()
        text = " ".join(t.strip() for t in span.css("::text").getall() if t.strip())
        if not label or not text:
            continue
        text = _WHITESPACE_RE.sub(" ", text).strip()
        text = _HOURS_TO_RE.sub("-", text)
        day = _DAY_ABBREV.get(label.strip(), label.strip())
        parts.append(f"{day} {text}")
    return "; ".join(parts) if parts else None


class MississippiSpider(scrapy.Spider):
    """Spider for Mississippi's MDHS child care provider search (ccsearch.aspx)."""

    name = "mississippi"
    allowed_domains = ["mdhs.provider.webapps.ms.gov"]
    source_state = "Mississippi"

    custom_settings = {
        # Within one county the pagination is a strict sequential chain
        # (Sec 5.2), but the 82 counties are otherwise independent (no
        # shared viewstate) -- Kansas-style, run them concurrently. Each
        # county still gets its OWN cookiejar (module docstring item 8):
        # concurrency without that corrupts the shared session's server-side
        # pagination state. Pages are ~1.5-2 MB each, so a modest delay is
        # plenty polite.
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.5,
        "DOWNLOAD_TIMEOUT": 120,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
        "DEFAULT_REQUEST_HEADERS": {"Referer": SEARCH_URL},
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Harvested from the search page's ddlCity dropdown (Sec 5.4); used
        # by split_city to split the delimiter-less street/city address.
        self.known_cities = set()
        # Harvested from ddlCounty: {value ("01".."82"): NAME}. Populated in
        # parse_search_page, one search POST fired per entry.
        self.counties = {}
        self.total_items = 0
        # Per-county bookkeeping for the closed() summary and the
        # poison-page guard: running item total and last page reached.
        self.county_running_total = {}
        self.county_final_pages = {}
        # county -> a short description of why that county's chain stopped
        # early (a repeat of the statewide poison-page symptom, now scoped
        # to a single county instead of aborting the whole crawl).
        self.failed_counties = {}
        # Cross-county duplicate guard (Sec: dedupe defensively). Every
        # provider should belong to exactly one county, so a repeat id here
        # would itself be a signal worth investigating, not silently merged.
        self.seen_facility_ids = set()
        self.duplicate_facility_ids = 0

    # ------------------------------------------------------------------ #
    # Phase 1 -- bootstrap + statewide search
    # ------------------------------------------------------------------ #

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_search_page, dont_filter=True)

    def parse_search_page(self, response):
        """Harvest the city + county dictionaries and fire one search per county.

        The site's own ``ddlCounty`` dropdown is the county dictionary (82
        values, "01".."82"); every other filter is left blank -- a
        county-only search with ``btnFind=Search`` returns that county's full
        result set, page 1 (live-verified, 2026-09-05: a 1-provider county
        still renders a full (disabled) pager, so page-1 parsing is uniform
        regardless of county size). lxml/``FormRequest.from_response``
        collects ``<input type=submit>`` but building the formdata explicitly
        (rather than relying on that) keeps this robust to the page also
        carrying ``btnReset``, which must never be sent.

        A single statewide (unfiltered) search was the original approach, but
        its ~1,472-row result set has an unrenderable "poison" offset around
        row 901-925 that 302-redirects to ``generalError.aspx`` no matter how
        it's reached (module docstring item 7). Partitioning by county keeps
        every individual query well clear of that offset.
        """
        self.known_cities = {
            opt.attrib["value"].strip().upper()
            for opt in response.css('select[name="ddlCity"] option')
            if opt.attrib.get("value", "").strip()
        }
        if len(self.known_cities) < 400:
            self.logger.warning(
                "mississippi: only %d cities harvested from ddlCity (expected ~487) -- "
                "the address city-split (Sec 5.4) may fall back more often than usual",
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

        base_formdata = _hidden_fields(response)
        base_formdata["__EVENTTARGET"] = ""
        base_formdata["__EVENTARGUMENT"] = ""
        base_formdata["btnFind"] = "Search"

        for value, county_name in self.counties.items():
            self.county_running_total[county_name] = 0
            formdata = dict(base_formdata)
            formdata["ddlCounty"] = value
            yield scrapy.FormRequest(
                SEARCH_URL,
                formdata=formdata,
                callback=self.parse_results,
                dont_filter=True,
                # Mandatory, not a nicety (Kansas Sec 5.1 precedent): with
                # counties now running CONCURRENTLY (custom_settings), two
                # counties interleaving on one shared cookiejar corrupt each
                # other's server-side pagination state -- live-verified
                # 2026-09-05 (14/82 counties failed at an identical "page
                # 37" with nonsensical counts, e.g. DeSoto capturing only 1
                # provider, once concurrency was raised without this).
                meta={
                    "page": 1,
                    "county": county_name,
                    "cookiejar": county_name,
                    # The ddlCounty option VALUE (e.g. "25" for Hinds) --
                    # carried forward so every pager postback can re-send it
                    # (see _next_formdata; live-verified 2026-09-05 that
                    # omitting it drops the filter from page 2 on).
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
        coords = _parse_coordinates(response)
        containers = response.css(PROVIDER_CONTAINER_SEL)
        # raw_count (the actual row count on THIS page) drives every
        # pagination/failure decision below; count (post-dedupe, yielded)
        # only drives the reporting totals. A cross-county duplicate must
        # never be mistaken for an empty page -- the page still had a real
        # row, it just wasn't a NEW one (Kansas precedent: dedupe must never
        # affect the stop rule).
        raw_count = len(containers)

        count = 0
        for container in containers:
            item = self._build_item(container, coords, response, county)
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
            yield item

        self.total_items += count
        self.county_running_total[county] = self.county_running_total.get(county, 0) + count
        self.county_final_pages[county] = page
        self.logger.info(
            "mississippi: %s page %d -> %d providers (county total %d, running total %d)",
            county,
            page,
            count,
            self.county_running_total[county],
            self.total_items,
        )

        pager_html = response.css(f"#{MAIN_PAGER_ID}").get()
        parsed = _parse_pager(pager_html) if pager_html else None

        if raw_count == 0:
            if page == 1:
                # A genuinely tiny/empty county is plausible (live-verified
                # 2026-09-05: even a 1-provider county still renders a full,
                # disabled pager, so this isn't itself suspicious) -- not
                # necessarily a failure.
                self.logger.warning("mississippi: %s has 0 providers on its search -- may genuinely have none", county)
            elif parsed is not None and parsed[1] is None and parsed[2]:
                # The pager itself confirms this is the last page (Last
                # disabled, no next target) -- a clean stop even though this
                # particular page rendered 0 rows (shouldn't normally arise
                # since we'd have stopped a page earlier, but it's not the
                # poison symptom if the pager agrees we're done).
                self.logger.info("mississippi: %s page %d -> 0 providers, pager confirms last page", county, page)
            else:
                # No pager at all (matches the poison-page redirect, which
                # lands back on the bare search page -- Sec: module
                # docstring item 7), or a pager that doesn't cleanly say
                # "done". Log and abandon just this county -- never abort
                # the other 81.
                self.logger.error(
                    "mississippi: %s hit an unexplained 0-provider page at page %d "
                    "(likely the poison-page redirect, pager_present=%s) -- stopping "
                    "this county early; %d providers already captured for it",
                    county,
                    page,
                    bool(pager_html),
                    self.county_running_total[county],
                )
                self.failed_counties[county] = (
                    f"stopped at page {page} (0 providers, pager_present={bool(pager_html)}) -- "
                    f"{self.county_running_total[county]} captured before failure"
                )
            return

        if not pager_html:
            self.logger.error("mississippi: %s page %d has no results pager -- stopping this county", county, page)
            self.failed_counties[county] = f"stopped at page {page} (no pager found despite {count} providers)"
            return
        if parsed is None:
            self.logger.error(
                "mississippi: %s page %d pager has no current-page marker -- stopping this county",
                county,
                page,
            )
            self.failed_counties[county] = f"stopped at page {page} (unparseable pager)"
            return
        _current, next_target, last_disabled = parsed

        if next_target is None:
            if not last_disabled:
                self.logger.error(
                    "mississippi: %s page %d has no next-page target but Last is "
                    "still enabled -- this county's pagination may have stopped early",
                    county,
                    page,
                )
                self.failed_counties[county] = f"stopped at page {page} (no next target, Last still enabled)"
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
    # Per-provider item construction
    # ------------------------------------------------------------------ #

    def _build_item(self, container, coords, response, county):
        pid = container.attrib.get("id")
        name = container.css(".panel-heading strong::text").get()
        if not pid or not name:
            self.logger.error(
                "mississippi: a results container is missing its id/name at %s -- skipped",
                response.url,
            )
            return None

        item = ProviderItem()
        item["source_state"] = self.source_state
        item["provider_url"] = SEARCH_URL
        item["provider_name"] = name.strip()
        item["ms_facility_id"] = pid
        item["state"] = "MS"
        # Populated because we now search per-county (the county filter
        # value maps 1:1 to the ddlCounty dropdown's display name); the
        # statewide search this replaced had no per-record county at all.
        item["county"] = county.title()

        fields, zip_code, subsidy_body = _panel_body_fields(container)
        if zip_code:
            item["zip"] = zip_code
        if fields.get("Phone Number"):
            item["phone"] = fields["Phone Number"]
        if fields.get("Email"):
            item["email"] = fields["Email"]
        if fields.get("Type"):
            item["provider_type"] = fields["Type"]
        if fields.get("Address"):
            self._apply_address(item, fields["Address"], zip_code, pid)

        license_pane = container.css(f'div[id="pvLicense{pid}"]')
        self._apply_license_details(item, license_pane, pid, subsidy_body)

        if pid in coords:
            lat, lon = coords[pid]
            item["latitude"] = lat
            item["longitude"] = lon
            item["geocode_source"] = "state"
        else:
            self.logger.warning("mississippi: id=%s has no htJson coordinates", pid)

        inspections = (
            self._parse_inspections(container, pid, response)
            + self._parse_simple_table(container, pid, "Investigation", response)
            + self._parse_simple_table(container, pid, "Monetary Penalty", response)
        )
        if inspections:
            item["inspections"] = inspections

        return item

    def _apply_address(self, item, address_raw, zip_code, pid):
        """Split the delimiter-less address and assemble a display string (Sec 5.4)."""
        tail = f" {zip_code}" if zip_code else ""
        m = _MS_ADDRESS_TAIL_RE.match(address_raw.strip())
        if not m:
            self.logger.warning("mississippi: id=%s address has no ', MS' tail: %r", pid, address_raw)
            item["address"] = f"{address_raw.strip()}, MS{tail}"
            return
        head = m.group("head").strip()
        street, city = split_city(head, self.known_cities)
        if city:
            item["city"] = city
            item["address"] = f"{street}, {city}, MS{tail}"
        else:
            self.logger.warning(
                "mississippi: id=%s address city not found in the ddlCity dictionary: %r",
                pid,
                head,
            )
            item["address"] = f"{head}, MS{tail}"

    def _apply_license_details(self, item, license_pane, pid, subsidy_body):
        """License No / Capacity / Status / Services / age & month checkboxes / hours."""
        license_number = license_pane.css('span[id*="lblLicenseNumberLabel"]::text').get()
        if license_number:
            item["license_number"] = license_number.strip()

        capacity_raw = license_pane.css('span[id*="lblCapacityLabel"]::text').get()
        if capacity_raw and capacity_raw.strip():
            try:
                item["capacity"] = int(capacity_raw.strip())
            except ValueError:
                self.logger.warning("mississippi: id=%s non-integer capacity %r", pid, capacity_raw)

        status_raw = license_pane.css('span[id*="lblLicenseStatusLabel"]::text').get()
        if status_raw:
            self._apply_status(item, status_raw, pid)

        services_raw = license_pane.css('span[id*="lblServicesLabel"]::text').get()
        services_lower = ""
        if services_raw:
            services = [s.strip() for s in services_raw.split(",") if s.strip()]
            if services:
                item["ms_services"] = services
            services_lower = services_raw.lower()
        item["head_start"] = "head start" in services_lower
        item["ms_early_head_start"] = "early head start" in services_lower

        if subsidy_body or license_pane.css('span[id*="lblMDHSSubsidy"]'):
            item["scholarships_accepted"] = True
            item["ms_subsidy"] = True

        self._apply_age_groups(item, license_pane)
        self._apply_months(item, license_pane)

        hours = _parse_hours(license_pane)
        if hours:
            item["hours"] = hours

    def _apply_status(self, item, status_raw, pid):
        """Extract the status word and the licence date range (Sec 5.6)."""
        m = _STATUS_RE.match(status_raw.strip())
        if not m:
            self.logger.warning("mississippi: id=%s unparsed status %r", pid, status_raw)
            item["status"] = status_raw.strip()
            return
        item["status"] = m.group("word")
        item["license_begin_date"] = m.group("begin")
        item["license_expiration"] = m.group("end")

    def _apply_age_groups(self, item, license_pane):
        fieldset = license_pane.xpath('.//fieldset[.//h5[contains(text(),"Age Group Served")]]')
        checked_buckets = {"infant": False, "toddler": False, "preschool": False, "school": False}
        labels = []
        if fieldset:
            for checkbox in fieldset[0].css('input[type="checkbox"]'):
                match = _AGE_CHECKBOX_RE.search(checkbox.attrib.get("id", ""))
                if not match:
                    continue
                checked = "checked" in checkbox.attrib
                if not checked:
                    continue
                bucket = _AGE_GROUP_BUCKET.get(match.group(1).lower())
                if bucket:
                    checked_buckets[bucket] = True
                label = checkbox.xpath("following-sibling::label[1]//text()").get()
                if label:
                    labels.append(label.strip())
        item["infant"] = checked_buckets["infant"]
        item["toddler"] = checked_buckets["toddler"]
        item["preschool"] = checked_buckets["preschool"]
        item["school"] = checked_buckets["school"]
        if labels:
            item["ages_served"] = ", ".join(labels)

    def _apply_months(self, item, license_pane):
        """Months Of Operation, read off the adjacent <label> text (Sec 5.5).

        The checkbox ids drift in casing (``chkbxJan``/``chkbxFeb`` vs.
        ``ChkbxMar``..``ChkbxDec``, plus spelled-out ``June``/``July``/
        ``Sept``) -- matching the stable ``<label>Jan</label>``..``Dec``
        sibling text avoids depending on that id casing entirely.
        """
        fieldset = license_pane.xpath('.//fieldset[.//h5[contains(text(),"Months Of Operation")]]')
        if not fieldset:
            return
        months = []
        for checkbox in fieldset[0].css('input[type="checkbox"]'):
            if "checked" not in checkbox.attrib:
                continue
            label = checkbox.xpath("following-sibling::label[1]//text()").get()
            if label:
                months.append(label.strip())
        if months:
            item["ms_months_of_operation"] = months

    # ------------------------------------------------------------------ #
    # Inspections / Investigations / Monetary Penalties tabs
    # ------------------------------------------------------------------ #

    def _report_url(self, td, response):
        onclick = td.css('input[type="image"]::attr(onclick)').get()
        if not onclick:
            return None
        m = _PDF_ONCLICK_RE.search(onclick)
        if not m:
            return None
        page, key, token = m.groups()
        return response.urljoin(f"{page}?{key}={token}")

    def _warn_if_nested_pager(self, pane, kind, pid):
        """Log if a tab's own nested DataPager exposes a page-2 link (Sec 5.3).

        v1 only reads the first nested page of any tab's table; no sampled
        provider ever exceeded it (max 15 rows), but if MDHS grows one past
        the nested page size this makes the truncation visible instead of
        silent.
        """
        for pager in pane.css('span[id*="lvDataPager"]'):
            if pager.css("a"):
                self.logger.warning(
                    "mississippi: id=%s %s table has more than one page -- "
                    "later rows are NOT scraped (v1 limitation)",
                    pid,
                    kind,
                )

    def _parse_inspections(self, container, pid, response):
        pane = container.css(f'div[id="pvInspections{pid}"]')
        tables = pane.css("div.table-responsive > table")
        if not tables:
            return []
        self._warn_if_nested_pager(pane, "Inspections", pid)
        out = []
        for tr in tables[0].xpath("./tbody/tr"):
            tds = tr.xpath("./td")
            if len(tds) < 5:
                self.logger.warning(
                    "mississippi: id=%s inspection row with %d cells (expected 5)",
                    pid,
                    len(tds),
                )
                continue
            insp = InspectionItem()
            insp["type"] = "Inspection"
            exam_type = tds[0].css('span[id*="lblExamTypeName"]::text').get()
            if exam_type:
                insp["ms_exam_type"] = exam_type.strip()
            begin_date = tds[1].css('span[id*="lblBeginDate"]::text').get()
            if begin_date:
                insp["date"] = begin_date.strip()
            end_date = tds[2].css('span[id*="lblEndDate"]::text').get()
            if end_date:
                insp["ms_end_date"] = end_date.strip()
            status = tds[3].css('span[id*="lblExamStatusName"]::text').get()
            if status:
                insp["original_status"] = status.strip()
            url = self._report_url(tds[4], response)
            if url:
                insp["report_url"] = url
            out.append(insp)
        return out

    def _parse_simple_table(self, container, pid, kind, response):
        """Investigations and Monetary Penalties share one 3-column shape."""
        tag = "Investigations" if kind == "Investigation" else "MonetaryPenalties"
        pane = container.css(f'div[id="pv{tag}{pid}"]')
        tables = pane.css("div.table-responsive > table")
        if not tables:
            return []
        self._warn_if_nested_pager(pane, kind, pid)
        out = []
        for tr in tables[0].xpath("./tbody/tr"):
            tds = tr.xpath("./td")
            if len(tds) < 3:
                self.logger.warning(
                    "mississippi: id=%s %s row with %d cells (expected 3)",
                    pid,
                    kind,
                    len(tds),
                )
                continue
            insp = InspectionItem()
            insp["type"] = kind
            date = tds[0].css('span[id*="lblDateRecieved"]::text').get()
            if date:
                insp["date"] = date.strip()
            description = tds[1].css('span[id*="lblDocumentTypeDescription"]::text').get()
            if description:
                insp["ms_description"] = description.strip()
            url = self._report_url(tds[2], response)
            if url:
                insp["report_url"] = url
            out.append(insp)
        return out

    # ------------------------------------------------------------------ #

    def closed(self, reason):
        self.logger.info(
            "mississippi: finished (%s) -- %d counties, %d providers, %d duplicate ids skipped, %d county failure(s)",
            reason,
            len(self.county_running_total),
            self.total_items,
            self.duplicate_facility_ids,
            len(self.failed_counties),
        )
        for county in sorted(self.county_running_total):
            self.logger.info(
                "mississippi: county summary -- %s: %d providers across %s page(s)",
                county,
                self.county_running_total[county],
                self.county_final_pages.get(county, "?"),
            )
        if self.failed_counties:
            self.logger.error(
                "mississippi: %d county/counties did not finish cleanly: %s",
                len(self.failed_counties),
                "; ".join(f"{county} ({detail})" for county, detail in sorted(self.failed_counties.items())),
            )
