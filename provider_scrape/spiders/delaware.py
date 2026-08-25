"""Delaware child care provider spider.

Source: three open-data datasets on ``data.delaware.gov`` (Socrata), published
by DSCYF's Office of Child Care Licensing (OCCL) and consumed live by
``education.delaware.gov``'s own search portal. There is no HTML to parse, no
JavaScript to run, and no cookie/token/referer/CAPTCHA -- the entire crawl is
~6 plain JSON GETs that finish in seconds:

  * ``iuzd-3dbt`` -- 1,243 provider records (address, hours, ages, capacity,
    enforcement status, financial arrangements, injuries history).
  * ``wb83-pkcv`` -- compliance-review **violations**, one row per citation
    (8,135 rows / 2,372 distinct visits).
  * ``pnbd-85r6`` -- complaint investigations (2,010 rows), full narrative
    conclusion included.
  * ``education.delaware.gov/wp-json/occl/v1/facilities`` -- an advisory,
    12-hour-cached list of 890 ids the portal's own table happens to show.

Crawl shape (plan Sec 2): the two review datasets are paged to completion and
buffered in memory (small -- ~11 MB of JSON total), then grouped into an
in-memory ``license_number -> [InspectionItem, ...]`` index. Only once that
index (and the advisory portal list) is ready does the provider dataset get
paged; each ``ProviderItem`` is emitted with its inspections already
attached, so no provider is ever yielded twice and no hold-and-join
bookkeeping is needed.

Decisions already made by Ryan (2026-08-21, delaware_plan.md Sec 1) -- do not
re-litigate:

  D-1: enumerate all 1,243 Socrata rows, not the 890-row WP endpoint (Sec
       6.1 below); flag each with ``de_portal_listed``.
  D-2: emit all 2,010 complaint investigations, not just the "Substantiated"
       ones the portal displays.
  D-3: one InspectionItem per compliance *visit* (grouped by license + date),
       carrying every citation in ``de_violations`` -- mirrors
       ``ct_violations`` (connecticut.py).
  D-4: worklifesystems (Delaware's second public source, covering exempt
       providers and camps) is parked -- CAPTCHA-gated, out of scope here.
       See tasks/delaware_story/worklifesystems_findings.md. Do not build
       against it.

Gotchas that will silently corrupt or truncate the data if missed (full
detail: delaware_plan.md Sec 6):

  Sec 6.1 -- The WP endpoint above returns only 890 of 1,243 providers, and
             its filter isn't reproducible from any published column (it's
             lossy in *both* directions -- 22 of the 353 it omits are newer
             than anything it lists). It is used ONLY to set the advisory
             ``de_portal_listed`` boolean, and its failure must never fail
             the run (Sec 5.1).
  Sec 6.2 -- The facility-details HTML page is entirely JavaScript-rendered
             (four client-side Socrata calls on document.ready). A live GET
             returns two empty containers. NEVER fetch or parse it -- it is
             only ever emitted as ``provider_url`` for humans to click.
  Sec 6.3 -- ``wb83-pkcv`` contains ONLY non-compliance citations -- a clean
             visit produces no row at all. 613/1,243 providers have zero
             inspection history here; that is expected, not a scrape
             failure. Never compute a "clean inspection rate" from this
             data -- ``de_violation_count`` is always >= 1 by construction.
  Sec 6.4 -- Socrata dates carry a misleading time component
             (``T00:00:00.000`` / ``T04:00:00.000`` EDT baked in). Always
             slice to the first 10 characters; never parse-and-reformat --
             the portal's own JS does exactly that and renders dates a day
             off in a non-Eastern timezone.
  Sec 6.5 -- Socrata omits null keys entirely (always ``.get()``), and the
             default page size is 1000 with no truncation signal -- always
             page explicitly with an explicit ``$order``.
  Sec 6.6 -- ``geocoded_location.coordinates`` is GeoJSON: longitude first,
             latitude second -- the opposite of this project's field order.
"""

from collections import defaultdict
from urllib.parse import urlencode

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://data.delaware.gov/resource"
PROVIDERS_URL = f"{BASE_URL}/iuzd-3dbt.json"
COMPLIANCE_URL = f"{BASE_URL}/wb83-pkcv.json"
COMPLAINTS_URL = f"{BASE_URL}/pnbd-85r6.json"
PORTAL_LIST_URL = "https://education.delaware.gov/wp-json/occl/v1/facilities"
# The live JS-rendered detail page (Sec 6.2) -- never fetched, only emitted
# as `provider_url` for humans. Resolves for every id, including the 353 the
# portal's own table omits, because the page queries Socrata by resource_id.
DETAIL_URL_TMPL = (
    "https://education.delaware.gov/families/birth-age-5/child_care_search/facility-details/?license_number={}"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Socrata datasets to page through, keyed by an internal "phase" name.
# `:id` is Socrata's own row-order system column -- required because neither
# review dataset has a unique business key (plan Sec 2.1).
DATASETS = {
    "compliance": {"url": COMPLIANCE_URL, "order": ":id"},
    "complaints": {"url": COMPLAINTS_URL, "order": ":id"},
    "providers": {"url": PROVIDERS_URL, "order": "resource_id"},
}

# Socrata's default page size is 1000 with NO truncation signal (Sec 6.5) --
# always page explicitly. 5000 comfortably covers the largest dataset
# (8,135 rows) in 2 pages; MAX_PAGES is a hard stop so a paging bug can never
# turn into an infinite crawl (plan Sec 2.1).
PAGE_SIZE = 5000
MAX_PAGES = 50

# Live baselines (2026-08-21, delaware_plan.md Sec 1/5.4). Warn in closed()
# if a run falls far short -- a sign the API shape or coverage changed.
EXPECTED_MIN_PROVIDERS = 1000
EXPECTED_MIN_INSPECTIONS = 3000

# Phase 1 (portal list) sanity check (Sec 5.1): a response shaped like a real
# facilities list has hundreds of rows; the live baseline is 890. This is
# advisory only -- fewer than this many rows means "something's wrong",
# never "the state removed most providers overnight".
PORTAL_SANITY_MIN = 500
PORTAL_BASELINE = 890
# How far the portal id count can drift from the baseline before it's worth
# a warning. Not specified by the plan -- a judgment call: 200 is loose
# enough to absorb normal week-to-week churn but still catches a shape
# change (e.g. the WP route starting to return a different subset).
PORTAL_DRIFT_WARN = 200

# age_group -> which age-band flags it sets (delaware_plan.md Sec 7.2). Eight
# values, closed vocabulary, full coverage of all 1,243 providers. Kept as an
# explicit table rather than parsed from "X through Y" text -- a parser would
# silently mis-handle a new value; this logs instead.
AGE_GROUP_FLAGS = {
    "Infant through School-Age": ("infant", "toddler", "preschool", "school"),
    "Toddler through School-Age": ("toddler", "preschool", "school"),
    "School-Age": ("school",),
    "Infant through Pre-School": ("infant", "toddler", "preschool"),
    "Toddler through Pre-School": ("toddler", "preschool"),
    "Pre-School": ("preschool",),
    "Pre-School through School-Age": ("preschool", "school"),
    "Infant through Toddler": ("infant", "toddler"),
}

# The six `;`-joined financial_arrangements tokens (plan Sec 7.4), routed
# three ways: the subsidy program -> scholarships_accepted, the food program
# -> meals, and the four ownership tokens -> de_profit_status. The raw string
# is always kept too (de_financial_arrangements) -- it's the only lossless
# record and what the portal's own filter searches.
FINANCIAL_TOKEN_SCHOLARSHIP = "Purchase of Care"
FINANCIAL_TOKEN_MEALS = "Child Care Food Program"
FINANCIAL_PROFIT_TOKENS = {"Nonprofit", "Private", "Profit", "Publicly Operated"}


def _socrata_page_url(dataset_url, order_field, limit, offset):
    """One page of a Socrata SoQL query: explicit $limit/$offset/$order."""
    params = {"$limit": limit, "$offset": offset, "$order": order_field}
    return f"{dataset_url}?{urlencode(params)}"


def compose_address(street, city, zip_code):
    """Compose the full ``"street, city, DE zip"`` address string (Sec 7.1).

    Never guesses a missing piece; DE is always included since every row is
    a Delaware facility (``site_state`` is a constant ``"DE"`` in the
    source).
    """
    parts = [p for p in (street, city) if p]
    tail = f"DE {zip_code}" if zip_code else "DE"
    if parts:
        return ", ".join(parts) + ", " + tail
    return tail


def compose_hours(opens, closes):
    """``"7:00 AM - 6:00 PM"`` when both are present, else None (Sec 7.1)."""
    if opens and closes:
        return f"{opens} - {closes}"
    return None


def strip_trailing_period(age_range):
    """Strip the trailing ``.`` present on every ``age_range`` value."""
    if not isinstance(age_range, str):
        return age_range
    stripped = age_range.strip()
    if stripped.endswith("."):
        stripped = stripped[:-1].strip()
    return stripped or None


def age_flags_from_group(age_group, logger=None):
    """``age_group`` -> the age-band flags it implies (Sec 7.2), or ``{}``.

    Only the flags that are True are returned (mirrors connecticut.py /
    indiana.py) -- an unmapped value logs a warning and sets no flags so a
    future vocabulary change surfaces instead of silently mis-mapping.
    """
    if not age_group:
        return {}
    flags = AGE_GROUP_FLAGS.get(age_group)
    if flags is None:
        if logger:
            logger.warning(
                "Delaware: unmapped age_group %r -- no age flags set",
                age_group,
            )
        return {}
    return {flag: True for flag in flags}


def derive_status(enforcement_action, intent_to_revoke):
    """Derive ``status`` per Sec 7.3 -- Delaware publishes no status column.

    ``enforcement_action`` wins when both are present (8/1,243 providers):
    it is an action already taken, while ``intent_to_revoke`` is only a
    notice of one pending. In all 8 observed cases the two describe the same
    action at different stages, so preferring the taken action is the
    correct reading, not a coin flip.
    """
    if enforcement_action:
        return enforcement_action
    if intent_to_revoke:
        return intent_to_revoke
    return "Licensed"


def split_financial_arrangements(raw, logger=None):
    """Split the ``;``-joined ``financial_arrangements`` string three ways.

    Returns ``(scholarships_accepted, meals, profit_status)``. An
    unrecognized token is logged and otherwise ignored (Sec 7.4) so a new
    token surfaces instead of silently vanishing.
    """
    tokens = [t.strip() for t in (raw or "").split(";") if t.strip()]
    scholarships = False
    meals = False
    profit_status = None
    for token in tokens:
        if token == FINANCIAL_TOKEN_SCHOLARSHIP:
            scholarships = True
        elif token == FINANCIAL_TOKEN_MEALS:
            meals = True
        elif token in FINANCIAL_PROFIT_TOKENS:
            profit_status = token
        elif logger:
            logger.warning(
                "Delaware: unrecognized financial_arrangements token %r",
                token,
            )
    return scholarships, meals, profit_status


def violation_from_row(row):
    """One ``de_violations`` entry from a raw ``wb83-pkcv`` citation row
    (Sec 5.2). Dates are sliced to their first 10 characters (Sec 6.4);
    absent dates become ``None`` rather than an empty string.
    """
    due = row.get("regulation_correction_due") or ""
    corrected = row.get("regulation_corrected_date") or ""
    return {
        "regulation_code": row.get("regulation_code"),
        "description": row.get("regulation_short_desc"),
        "corrective_action": row.get("regulation_corrective_action"),
        "correction_status": row.get("regulation_correction_status"),
        "correction_due": due[:10] or None,
        "corrected_date": corrected[:10] or None,
        "how_corrected": row.get("regulation_how_corrected"),
    }


def build_visit_items(compliance_rows, logger=None):
    """Group ``wb83-pkcv`` rows into one InspectionItem per compliance visit
    (D-3). Grouping key is ``(license_number, visit_date[:10])`` -- verified
    no such pair ever carries two different ``facility_visit_type`` values
    (plan Sec 5.2), so the type is lifted onto the visit unambiguously.
    Violations within a visit are sorted by ``regulation_code`` for stable
    output; a provider's visits are sorted newest-first by
    ``build_inspection_index`` once complaints are merged in.

    Returns ``dict[license_number] -> [InspectionItem, ...]``. Rows missing
    a license number or visit date are skipped and logged (should not occur
    live -- every row in the dataset carries both).
    """
    grouped = defaultdict(list)
    for row in compliance_rows:
        license_number = row.get("license_number")
        visit_date_raw = row.get("provider_action_date_of_visit")
        if not license_number or not visit_date_raw:
            if logger:
                logger.warning(
                    "Delaware: compliance row missing license_number/visit date, skipped: %r",
                    row,
                )
            continue
        visit_date = visit_date_raw[:10]
        grouped[(license_number, visit_date)].append(row)

    by_license = defaultdict(list)
    for (license_number, visit_date), rows in grouped.items():
        item = InspectionItem()
        item["date"] = visit_date
        visit_type = rows[0].get("facility_visit_type")
        if visit_type:
            item["type"] = visit_type
        item["de_violation_count"] = len(rows)
        item["de_violations"] = sorted(
            (violation_from_row(r) for r in rows),
            key=lambda v: v.get("regulation_code") or "",
        )
        by_license[license_number].append(item)
    return dict(by_license)


def build_complaint_items(complaint_rows, logger=None):
    """One InspectionItem per ``pnbd-85r6`` row (D-2 -- every investigation
    is emitted, not just "Substantiated" ones). Missing fields (Sec 3.3:
    ``investigation_completed`` absent on 13, ``investigation_result`` on
    14, ``investigation_conclusion`` on 75) simply leave that item field
    unset.

    Returns ``dict[resource_id] -> [InspectionItem, ...]``. Rows missing a
    ``resource_id`` are skipped and logged (should not occur live).
    """
    by_license = defaultdict(list)
    for row in complaint_rows:
        resource_id = row.get("resource_id")
        if not resource_id:
            if logger:
                logger.warning(
                    "Delaware: complaint row missing resource_id, skipped: %r",
                    row,
                )
            continue
        item = InspectionItem()
        if row.get("investigation_type"):
            item["type"] = row["investigation_type"]
        completed = row.get("investigation_completed")
        if completed:
            item["date"] = completed[:10]
        if row.get("investigation_result"):
            item["original_status"] = row["investigation_result"]
        if row.get("investigation_conclusion"):
            item["de_investigation_conclusion"] = row["investigation_conclusion"]
        by_license[resource_id].append(item)
    return dict(by_license)


def build_inspection_index(compliance_rows, complaint_rows, logger=None):
    """Build the full ``license_number -> [InspectionItem, ...]`` index used
    to attach inspections to providers in Phase 2 (plan Sec 2/5.2).

    Compliance visits and complaint investigations share one list per
    provider, distinguishable by ``type``; the combined list is sorted
    newest-first by ``date`` (what the portal does for visits -- Sec 5.2),
    with items carrying no date sorted last.

    Returns ``(index, visit_item_count, complaint_item_count)``.
    """
    visits_by_license = build_visit_items(compliance_rows, logger)
    complaints_by_license = build_complaint_items(complaint_rows, logger)
    visit_count = sum(len(v) for v in visits_by_license.values())
    complaint_count = sum(len(v) for v in complaints_by_license.values())

    combined = defaultdict(list)
    for license_number, items in visits_by_license.items():
        combined[license_number].extend(items)
    for license_number, items in complaints_by_license.items():
        combined[license_number].extend(items)
    for items in combined.values():
        items.sort(key=lambda i: i.get("date") or "", reverse=True)

    return dict(combined), visit_count, complaint_count


def parse_portal_ids(data):
    """Extract the portal-listed ``resource_id`` set from a WP facilities
    response, or ``None`` on any shape/sanity failure (Sec 5.1). This
    request is purely advisory -- the caller must treat ``None`` as "skip
    de_portal_listed", never as a reason to fail the run.
    """
    if not isinstance(data, list) or len(data) <= PORTAL_SANITY_MIN:
        return None
    ids = set()
    for row in data:
        if isinstance(row, dict) and row.get("resource_id") is not None:
            ids.add(str(row["resource_id"]))
    return ids


class DelawareSpider(scrapy.Spider):
    name = "delaware"
    allowed_domains = ["data.delaware.gov", "education.delaware.gov"]

    # No app token needed (~6 requests total); Socrata's anonymous pool is
    # far above that. robots.txt permits /resource/... explicitly, but a 1s
    # delay + modest concurrency is good-citizen behavior anyway (plan Sec
    # 2.2) -- the whole run still finishes in well under a minute.
    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.compliance_rows = []
        self.complaint_rows = []
        self.inspections_by_license = {}
        self.portal_ids = None
        self.portal_failed = False

        self._compliance_done = False
        self._complaints_done = False
        self._portal_done = False
        self.page_cap_hit_phases = set()

        self.providers_emitted = 0
        self.visit_item_count = 0
        self.complaint_item_count = 0
        self.non_200_count = 0

    # --- Phases 3/4 + Phase 1: reviews (paged) + advisory portal list ---- #

    def start_requests(self):
        self.logger.info(
            "Delaware: starting -- fetching compliance + complaints review "
            "datasets and the advisory portal list before enumerating "
            "providers",
        )
        yield self._page_request("compliance", 0)
        yield self._page_request("complaints", 0)
        yield scrapy.Request(
            PORTAL_LIST_URL,
            callback=self.parse_portal_list,
            errback=self.portal_list_errback,
            headers=HEADERS,
            dont_filter=True,
        )

    def _page_request(self, phase, offset):
        cfg = DATASETS[phase]
        page_num = offset // PAGE_SIZE + 1
        url = _socrata_page_url(cfg["url"], cfg["order"], PAGE_SIZE, offset)
        return scrapy.Request(
            url,
            callback=self.parse_page,
            errback=self.page_errback,
            headers=HEADERS,
            meta={"phase": phase, "offset": offset, "page_num": page_num},
            dont_filter=True,
        )

    def _accumulator(self, phase):
        return self.compliance_rows if phase == "compliance" else self.complaint_rows

    def parse_page(self, response):
        phase = response.meta["phase"]
        offset = response.meta["offset"]
        page_num = response.meta["page_num"]
        try:
            rows = response.json()
        except Exception:
            rows = None
        if not isinstance(rows, list):
            self.logger.warning(
                "Delaware: %s page %d (offset=%d) returned a non-list body -- treating as empty",
                phase,
                page_num,
                offset,
            )
            rows = []

        self.logger.info(
            "Delaware: %s page %d (offset=%d) -- %d rows",
            phase,
            page_num,
            offset,
            len(rows),
        )

        if phase == "providers":
            for row in rows:
                item = self._build_provider_item(row)
                if item is not None:
                    self.providers_emitted += 1
                    yield item
        else:
            self._accumulator(phase).extend(rows)

        if len(rows) == PAGE_SIZE:
            next_page_num = page_num + 1
            if next_page_num > MAX_PAGES:
                self.page_cap_hit_phases.add(phase)
                self.logger.warning(
                    "Delaware: %s hit the MAX_PAGES=%d cap at page %d -- stopping early, data may be truncated",
                    phase,
                    MAX_PAGES,
                    page_num,
                )
                yield from self._finish_phase(phase)
                return
            yield self._page_request(phase, offset + PAGE_SIZE)
            return

        if phase == "providers":
            self.logger.info(
                "Delaware: provider enumeration complete -- %d emitted",
                self.providers_emitted,
            )
        else:
            yield from self._finish_phase(phase)

    def page_errback(self, failure):
        phase = failure.request.meta.get("phase")
        self.non_200_count += 1
        self.logger.warning(
            "Delaware: %s page request failed after retries (%s) -- phase may be incomplete",
            phase,
            failure.value,
        )
        if phase == "providers":
            return
        yield from self._finish_phase(phase)

    def _finish_phase(self, phase):
        if phase == "compliance":
            self._compliance_done = True
            self.logger.info(
                "Delaware: compliance dataset complete -- %d rows",
                len(self.compliance_rows),
            )
        elif phase == "complaints":
            self._complaints_done = True
            self.logger.info(
                "Delaware: complaints dataset complete -- %d rows",
                len(self.complaint_rows),
            )
        yield from self._maybe_start_providers()

    def parse_portal_list(self, response):
        try:
            data = response.json()
        except Exception:
            data = None
        self.portal_ids = parse_portal_ids(data)
        if self.portal_ids is None:
            self.portal_failed = True
            self.logger.warning(
                "Delaware: portal list response failed the shape/sanity "
                "check -- advisory only, de_portal_listed will be unset on "
                "every item, run continues",
            )
        else:
            self.logger.info(
                "Delaware: portal list loaded -- %d ids (baseline ~%d)",
                len(self.portal_ids),
                PORTAL_BASELINE,
            )
            if abs(len(self.portal_ids) - PORTAL_BASELINE) > PORTAL_DRIFT_WARN:
                self.logger.warning(
                    "Delaware: portal list count %d has drifted far from the %d baseline",
                    len(self.portal_ids),
                    PORTAL_BASELINE,
                )
        self._portal_done = True
        yield from self._maybe_start_providers()

    def portal_list_errback(self, failure):
        self.portal_failed = True
        self.portal_ids = None
        self.logger.warning(
            "Delaware: portal list request failed (%s) -- advisory only, "
            "de_portal_listed will be unset on every item, run continues",
            failure.value,
        )
        self._portal_done = True
        yield from self._maybe_start_providers()

    def _maybe_start_providers(self):
        if not (self._compliance_done and self._complaints_done and self._portal_done):
            return
        self.inspections_by_license, self.visit_item_count, self.complaint_item_count = build_inspection_index(
            self.compliance_rows,
            self.complaint_rows,
            self.logger,
        )
        self.logger.info(
            "Delaware: inspection index built -- %d visit items, %d "
            "complaint items, %d providers with history -- starting "
            "provider enumeration",
            self.visit_item_count,
            self.complaint_item_count,
            len(self.inspections_by_license),
        )
        yield self._page_request("providers", 0)

    # --- Phase 2: providers ------------------------------------------------ #

    def _build_provider_item(self, row):
        resource_id = row.get("resource_id")
        if not resource_id:
            self.logger.warning(
                "Delaware: provider row with no resource_id skipped: %r",
                row,
            )
            return None
        resource_id = str(resource_id)

        item = ProviderItem()
        item["source_state"] = "Delaware"
        item["provider_url"] = DETAIL_URL_TMPL.format(resource_id)

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            elif isinstance(value, list):
                value = value or None
            if value is not None:
                item[key] = value

        put("license_number", resource_id)
        put("provider_name", row.get("resource_name"))
        put("provider_type", row.get("resource_type"))
        put("county", row.get("site_county"))
        put(
            "address",
            compose_address(
                row.get("site_street_address"),
                row.get("site_city"),
                row.get("site_zip_code"),
            ),
        )
        put("phone", row.get("phone_number"))
        put("capacity", row.get("capacity"))

        # GeoJSON is longitude-first (Sec 6.6) -- the opposite of item
        # order. Absent on 51/1,243; left unset (not 0.0) for the post-run
        # geocoder to fill in.
        coords = (row.get("geocoded_location") or {}).get("coordinates") or []
        if len(coords) == 2:
            item["longitude"] = str(coords[0])
            item["latitude"] = str(coords[1])

        put("hours", compose_hours(row.get("site_opens_at"), row.get("site_closes_at")))
        put("ages_served", strip_trailing_period(row.get("age_range")))

        for field, value in age_flags_from_group(row.get("age_group"), self.logger).items():
            item[field] = value

        item["status"] = derive_status(row.get("enforcement_action"), row.get("intent_to_revoke"))

        put("de_enforcement_action", row.get("enforcement_action"))
        put("de_intent_to_revoke", row.get("intent_to_revoke"))
        put("de_special_conditions", row.get("special_conditions"))
        put("de_injuries_report", row.get("injuries_report"))

        # financial_arrangements is absent on 113/1,243 -- that means
        # "unpublished", not "no". Only split it (and set the derived
        # scholarships_accepted/meals/de_profit_status) when it is present,
        # so scholarships_accepted stays unset (unknown) rather than a false
        # "False" on those 113 records.
        financial_raw = row.get("financial_arrangements")
        if financial_raw:
            put("de_financial_arrangements", financial_raw)
            scholarships, meals, profit_status = split_financial_arrangements(financial_raw, self.logger)
            item["scholarships_accepted"] = scholarships
            if meals:
                item["meals"] = FINANCIAL_TOKEN_MEALS
            put("de_profit_status", profit_status)

        if self.portal_ids is not None:
            item["de_portal_listed"] = resource_id in self.portal_ids

        # Always set, even when empty (Sec 6.3: 613/1,243 providers
        # legitimately have zero inspection history -- that is not a scrape
        # failure and must not look like a missing field).
        item["inspections"] = self.inspections_by_license.get(resource_id, [])

        return item

    # --- shutdown ----------------------------------------------------------- #

    def closed(self, reason):
        total_inspection_items = self.visit_item_count + self.complaint_item_count
        self.logger.info(
            "Delaware: finished (%s) -- %d providers emitted; %d compliance "
            "rows -> %d visit items; %d complaint items; %d inspection "
            "items total; %d non-200 page responses; portal_failed=%s",
            reason,
            self.providers_emitted,
            len(self.compliance_rows),
            self.visit_item_count,
            self.complaint_item_count,
            total_inspection_items,
            self.non_200_count,
            self.portal_failed,
        )
        if self.providers_emitted < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Delaware: only %d providers emitted (< %d baseline) -- possible incomplete crawl",
                self.providers_emitted,
                EXPECTED_MIN_PROVIDERS,
            )
        if total_inspection_items < EXPECTED_MIN_INSPECTIONS:
            self.logger.warning(
                "Delaware: only %d inspection items built (< %d baseline) -- possible incomplete crawl",
                total_inspection_items,
                EXPECTED_MIN_INSPECTIONS,
            )
        if self.non_200_count:
            self.logger.warning(
                "Delaware: %d Socrata page request(s) failed during the crawl",
                self.non_200_count,
            )
        if self.portal_failed:
            self.logger.warning(
                "Delaware: the Phase 1 portal list was unavailable -- "
                "de_portal_listed is unset on every item (this does not "
                "affect provider completeness)",
            )
        if self.page_cap_hit_phases:
            self.logger.warning(
                "Delaware: MAX_PAGES cap was hit for phase(s) %s -- data may be truncated",
                sorted(self.page_cap_hit_phases),
            )
