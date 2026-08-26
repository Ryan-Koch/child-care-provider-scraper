"""Idaho child care provider spider.

Source: two independent sites:

  * ``idahostars.org`` -- the Idaho STARS child care search, a DNN
    (DotNetNuke) site with a DnnSharp ActionGrid Angular frontend and a
    DnnSharp ActionForm detail page. No CAPTCHA/Cloudflare/rate limiting.
  * ``www.idahochildcarecheck.org`` -- Idaho Child Care Check, a separate
    Drupal 10 site publishing health inspections and substantiated incident
    reports per provider.

Structurally closest to ``indiana.py`` (cookieless JSON API, search->detail,
``closed()`` guardrail), but with two extra wrinkles Indiana doesn't have:

  1. A one-time bootstrap step to grab a DNN ``RequestVerificationToken``
     (from a hidden input on the ``/Families`` page) that the detail API
     needs as a header -- no cookies required anywhere.
  2. The detail API's response isn't JSON: it's an HTML page containing a
     ``var loadResult = {...}`` JavaScript object literal (not strict JSON --
     see ``extract_static_text``). The actual provider detail lives inside
     that block as an HTML string, itself parsed with a second regex pass
     (see ``extract_detail_fields``).

Four-phase crawl per provider: (0) bootstrap the token once, (1) paginate the
ActionGrid listing API, (2) fetch the ActionForm detail page, (3) fetch the
Child Care Check page for inspections/incidents. See
``tasks/idaho_story/idaho_plan.md`` for the full field mapping and the raw
API/HTML captures the fixtures in this directory are trimmed from.
"""

import json
import re
from urllib.parse import urlencode

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BOOTSTRAP_URL = "https://idahostars.org/Families"
LISTING_URL = "https://idahostars.org/DesktopModules/DnnSharp/ActionGrid/Api.ashx"
DETAIL_URL = "https://idahostars.org/DesktopModules/MVC/DnnSharp/ActionForm/Load"
CHILD_CARE_CHECK_BASE = "https://www.idahochildcarecheck.org/provider"
PROVIDER_URL_BASE = "https://idahostars.org/Provider-Detail/ID"

# The listing API silently clamps any larger requested page size to 24
# (idaho_plan.md Sec 5.2).
PAGE_SIZE = 24
# Baseline unique count (calibrated live 2026-08-25: 461). Warn if a run
# falls far short -- a sign the API shape or coverage changed.
EXPECTED_MIN_PROVIDERS = 400

DETAIL_MODULE_ID = "16101"
DETAIL_TAB_ID = "5308"
DNNSF_TIME_OFFSET = "420"

# Fixed DNN module-state query params for the listing API. NOTE: `page13735`
# here (and the `page13736`/`size13735` seen in captured browser traffic) are
# DECOYS -- they do not control pagination. The real controls are `page` and
# `pagesize`, added per-request in `_listing_request` (idaho_plan.md Sec 5.1).
LISTING_PARAMS = {
    "method": "GetData",
    "TabId": "3675",
    "language": "en-US",
    "_aliasid": "140",
    "_mid": "13735",
    "_tabid": "3675",
    "_url": "https://idahostars.org/Families",
    "referrer": "",
    "timezone": "420",
    "ViewMode": "Legacy",
    "page13735": "1",
    "search": "",
    "sortAsc": "true",
    "View": "Legacy",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:154.0) Gecko/20100101 Firefox/154.0",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
CC_HEADERS = {
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "text/html",
}

# Detail-page `<strong>Label:</strong>` -> ProviderItem field. Director Name,
# Facility Type, and email are handled separately in `_enrich_from_detail`
# (they map to the common `administrator`/`provider_type`/`email` fields and
# email isn't a `<strong>` label at all). "ICCP Status" is deliberately
# omitted -- it's redundant with the listing API's ICCPStatus, which already
# populates `status`. "Comment on Accreditation", "Website", and "Facebook"
# are also skipped: idaho_plan.md Sec 5.11 notes Website/Facebook are often
# empty placeholders (the listing's WebsiteURL is more reliable), and
# Comment on Accreditation isn't part of the plan's field mapping.
DETAIL_FIELD_MAP = {
    "License Status": "id_license_status",
    "National Accreditation": "id_national_accreditation",
    "Quality Achiever Status": "id_quality_achiever_status",
    "Quality Achievement(s)": "id_quality_achievements",
    # Per idaho_plan.md Sec 5.6/items.py comment: these three are wrapped in
    # an HTML comment in every provider sampled live -- disabled site-wide,
    # not a per-provider gap -- so they stay unset today via comment
    # stripping in `extract_detail_fields`; mapped here in case the site
    # re-enables the feature.
    "Are there openings available": "id_openings_available",
    "Number of Openings": "id_number_of_openings",
    "Is there a waitlist": "id_waitlist",
    "Program Philosophy": "id_program_philosophy",
    "Philosophy Comment": "id_philosophy_comment",
    "Philosophy Description": "id_philosophy_description",
    "Program Description": "id_program_description",
    "Participating in USDA Food Program": "id_usda_food_program",
    "Family Style Dining": "id_family_style_dining",
    "Other Opportunities at this Center": "id_other_opportunities",
    "Opportunities Comment": "id_opportunities_comment",
    "Consistent Daily Schedule": "id_consistent_schedule",
    "Consistent Daily Schedule Comment": "id_consistent_schedule_comment",
    "Policy on Pets": "id_pet_policy",
    "Policy on Pets Comment": "id_pet_policy_comment",
}

_LOAD_RESULT_MARKER = "var loadResult"
_VALUE_RE = re.compile(r'"value"\s*:\s*"((?:[^"\\]|\\.)*)"')
_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
# `<strong>Label:</strong>` followed by its value text, up to the next
# structural tag. `</div>`/`<div>` are one alternative ("div") since a
# closing tag's `/` isn't matched by the bare "div" branch.
_STRONG_FIELD_RE = re.compile(
    r"<strong>(.*?)</strong>:?\s*(.*?)(?=<(?:br|strong|div|hr|/div|a[\s>]))",
    re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_MAILTO_RE = re.compile(r'mailto:([^"]+)"')


# --------------------------------------------------------------------------- #
# Pure helpers (listing API)
# --------------------------------------------------------------------------- #


def parse_listing_fields(result):
    """Flatten one listing-API ``result``'s ``fields`` array to a dict.

    Builds ``{Name: str(Value)}`` from the API's ``[{"Name": ..., "Value":
    ...}, ...]`` shape. ``ZipCode``/``AlternateRiseId``/``Id`` are sometimes
    ints, sometimes strings (idaho_plan.md Sec 5.3) -- always coerced to
    ``str`` here so downstream code never has to care.
    """
    fields = {}
    for entry in result.get("fields") or []:
        name = entry.get("Name")
        if not name:
            continue
        value = entry.get("Value")
        fields[name] = None if value is None else str(value)
    return fields


def build_address(fields):
    """``"{AddressLine}, {City}, ID {ZipCode}"``, or ``None`` if no street."""
    address_line = fields.get("AddressLine")
    if not address_line:
        return None
    city = fields.get("City") or ""
    zip_code = fields.get("ZipCode") or ""
    return f"{address_line}, {city}, ID {zip_code}".strip()


# --------------------------------------------------------------------------- #
# Pure helpers (detail Load API)
# --------------------------------------------------------------------------- #


def extract_static_text(response_text):
    """Pull the StaticText field's HTML out of a Load API response.

    The response embeds a ``var loadResult = {...}`` JavaScript object
    literal, NOT strict JSON -- it cannot be parsed with ``json.loads()``
    directly (idaho_plan.md Sec 5.4). The StaticText field, which carries
    every detail-page value, is reliably the LONGEST ``"value": "..."``
    string inside that block. Decoding just that string as a JSON string
    literal turns its ``\\u003c``-style escapes back into real HTML.

    Returns ``None`` if the marker or a usable ``"value"`` string isn't
    found (e.g. an unexpected response shape).
    """
    idx = response_text.find(_LOAD_RESULT_MARKER)
    if idx == -1:
        return None
    block = response_text[idx:]
    matches = list(_VALUE_RE.finditer(block))
    if not matches:
        return None
    longest = max(matches, key=lambda m: len(m.group(1)))
    try:
        return json.loads('"' + longest.group(1) + '"')
    except json.JSONDecodeError:
        return None


def extract_detail_fields(html_content):
    """Parse ``<strong>Label:</strong> value`` pairs out of the StaticText HTML.

    HTML comments are stripped FIRST. The template wraps a handful of fields
    (e.g. "Are there openings available") in ``<!-- ... -->`` -- confirmed
    disabled site-wide across every provider sampled live on 2026-08-25/26,
    not a per-provider gap -- and without stripping, a naive scan would treat
    that commented-out markup as live data (idaho_plan.md Sec 5.6 describes
    the empty-template case this also protects; the comment-wrapped fields
    are a related but distinct wrinkle the plan doesn't call out).
    """
    if not html_content:
        return {}
    stripped = _COMMENT_RE.sub("", html_content)
    fields = {}
    for match in _STRONG_FIELD_RE.finditer(stripped):
        label = match.group(1).strip().rstrip(":")
        value = _TAG_RE.sub("", match.group(2)).strip()
        if label:
            fields[label] = value
    return fields


def extract_email(html_content):
    """Pull the address out of a ``<a href="mailto:...">`` link, or ``None``.

    Some providers have an empty ``mailto:`` placeholder href
    (idaho_plan.md Sec 5.10) -- treated the same as "no email".
    """
    if not html_content:
        return None
    match = _MAILTO_RE.search(html_content)
    if not match or not match.group(1):
        return None
    return match.group(1)


# --------------------------------------------------------------------------- #
# Pure helpers (Child Care Check page)
# --------------------------------------------------------------------------- #


def _joined_text(selector_list, sub_selector):
    """Join a sub-selector's descendant text nodes into one cleaned string."""
    parts = [t.strip() for t in selector_list.css(sub_selector).getall()]
    joined = " ".join(t for t in parts if t)
    return joined or None


def parse_criteria(article):
    """Extract the numbered inspection-point criteria from one health-inspection article.

    Each criterion is a ``.inspection-point`` block with a pass/fail icon, a
    numbered label (e.g. "1. Provider Age/Supervision"), and an optional
    inspector's comment.
    """
    criteria = []
    for point in article.css(".inspection-point"):
        name = (point.css(".inspection-point__label::text").get() or "").strip()
        if not name:
            continue
        icon_class = point.css(".inspection-point__status i::attr(class)").get() or ""
        criteria.append(
            {
                "name": name,
                "passed": "inspection-passed" in icon_class,
                "comment": _joined_text(point, ".inspection-point__comments::text"),
            }
        )
    return criteria


def parse_inspections(response, report_url):
    """Build one InspectionItem per ``<article class="health-inspection-report">``."""
    inspections = []
    for article in response.css("article.health-inspection-report"):
        inspection = InspectionItem()
        inspection["report_url"] = report_url

        date = article.css(".field--name-field-inspection-date .date__item::text").get()
        if date:
            inspection["date"] = date.strip()
        activity = article.css(".field--name-field-inspection-activity .field--item::text").get()
        if activity:
            inspection["type"] = activity.strip()
        status = article.css(".field--name-field-inspection-status .field__value::text").get()
        if status:
            inspection["original_status"] = status.strip()
        resolved = article.css(".field--name-field-investigation-resolved::text").get()
        if resolved:
            inspection["id_investigation_resolved"] = resolved.strip()

        criteria = parse_criteria(article)
        if criteria:
            inspection["id_criteria"] = criteria
        inspections.append(inspection)
    return inspections


def parse_incidents(response, report_url):
    """Build one InspectionItem (``type`` = "Incident") per ``<div class="incident-report">``."""
    incidents = []
    for div in response.css("div.incident-report"):
        incident = InspectionItem()
        incident["type"] = "Incident"
        incident["report_url"] = report_url

        date = div.css(".field--name-field-incident-date::text").get()
        if date:
            incident["date"] = date.strip()
        category = div.css(".field--name-field-incident-category .field--items::text").get()
        if category:
            incident["original_status"] = category.strip()
        title = div.css("h2.node__title::text").get()
        if title:
            incident["id_incident_title"] = title.strip()

        description = _joined_text(div, ".field--name-field-incident-description ::text")
        if description:
            incident["id_incident_description"] = description
        resolution = _joined_text(div, ".field--name-field-incident-resolution .field--item ::text")
        if resolution:
            incident["id_incident_resolution"] = resolution

        incidents.append(incident)
    return incidents


class IdahoSpider(scrapy.Spider):
    name = "idaho"
    allowed_domains = ["idahostars.org", "www.idahochildcarecheck.org"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 1,
        "COOKIES_ENABLED": False,  # no cookies needed for any of the 3 endpoints
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verification_token = None
        self.provider_count = 0
        # The listing API has been observed (live, 2026-08-26) to repeat the
        # same result row within a single page -- e.g. "Angels Wings
        # Daycare/Preschool LLC" (Id 3950) appeared twice back-to-back on one
        # page, matching the same duplicate in the researcher's captured
        # api_return.json. Track seen ids so a source-side repeat never
        # yields the same provider twice.
        self.seen_ids = set()

    # --- phase 0: bootstrap ---------------------------------------------- #

    def start_requests(self):
        yield scrapy.Request(BOOTSTRAP_URL, callback=self.parse_bootstrap, dont_filter=True)

    def parse_bootstrap(self, response):
        token = response.css('input[name="__RequestVerificationToken"]::attr(value)').get()
        if not token:
            # Not fatal -- some deployments may not require it -- but flag it
            # loudly since every detail call depends on this.
            self.logger.warning("Idaho: no RequestVerificationToken found on bootstrap page; detail calls may fail")
        self.verification_token = token
        yield self._listing_request(page=1)

    # --- phase 1: listing API pagination ---------------------------------- #

    def _listing_request(self, page):
        params = {**LISTING_PARAMS, "page": str(page), "pagesize": str(PAGE_SIZE)}
        url = f"{LISTING_URL}?{urlencode(params)}"
        return scrapy.Request(
            url,
            headers=HEADERS,
            callback=self.parse_listing,
            meta={"page": page},
            dont_filter=True,
        )

    def parse_listing(self, response):
        data = response.json()
        page = response.meta["page"]

        # On page 1, learn the total and fan out the remaining pages -- the
        # server reports totalPages for OUR requested pagesize directly, no
        # local math needed.
        if page == 1:
            total_pages = data.get("totalPages") or 0
            self.logger.info(
                "Idaho: totalResults=%s -> %s pages of %s; fanning out pages 2..%s",
                data.get("totalResults"),
                total_pages,
                PAGE_SIZE,
                total_pages,
            )
            for p in range(2, total_pages + 1):
                yield self._listing_request(p)

        for result in data.get("results") or []:
            fields = parse_listing_fields(result)
            provider_id = fields.get("Id")
            if provider_id and provider_id in self.seen_ids:
                # Source-side repeat within/across pages -- see __init__.
                continue
            item = self._build_listing_item(fields)
            if item is None:
                continue
            self.seen_ids.add(provider_id)
            self.provider_count += 1
            yield self._detail_request(item)

    def _build_listing_item(self, fields):
        provider_id = fields.get("Id")
        if not provider_id:
            self.logger.warning("Idaho: listing result with no Id; skipping (%r)", fields.get("BusinessName"))
            return None

        item = ProviderItem()
        item["source_state"] = "Idaho"
        item["provider_url"] = f"{PROVIDER_URL_BASE}/{provider_id}"

        def put(key, value):
            if isinstance(value, str):
                value = value.strip() or None
            if value is not None:
                item[key] = value

        put("provider_name", fields.get("BusinessName"))
        put("license_number", provider_id)  # Idaho has no separate license number
        put("license_holder", fields.get("OperatorName"))
        put("status", fields.get("ICCPStatus"))
        put("address", build_address(fields))
        put("phone", fields.get("FacilityAddressPhone"))
        put("provider_website", fields.get("WebsiteURL"))
        put("id_alternate_rise_id", fields.get("AlternateRiseId"))
        return item

    # --- phase 2: detail (ActionForm Load API) ----------------------------- #

    def _detail_request(self, item):
        provider_id = item["license_number"]
        params = {
            "referrer": "",
            "openMode": "Always",
            "_url": f"{PROVIDER_URL_BASE}/{provider_id}",
            "ID": provider_id,
            "language": "en-US",
        }
        url = f"{DETAIL_URL}?{urlencode(params)}"
        headers = {
            **HEADERS,
            "ModuleId": DETAIL_MODULE_ID,
            "TabId": DETAIL_TAB_ID,
            "DNNSF-Time-Offset": DNNSF_TIME_OFFSET,
        }
        if self.verification_token:
            headers["RequestVerificationToken"] = self.verification_token
        return scrapy.Request(
            url,
            headers=headers,
            callback=self.parse_detail,
            errback=self.detail_errback,
            meta={"item": item},
            dont_filter=True,
        )

    def parse_detail(self, response):
        item = response.meta["item"]
        html_content = extract_static_text(response.text)
        if html_content:
            self._enrich_from_detail(item, html_content)
        else:
            self.logger.warning(
                "Idaho: could not extract StaticText for provider %s; keeping listing-only data",
                item.get("license_number"),
            )
        yield from self._fetch_child_care_check(item)

    def _enrich_from_detail(self, item, html_content):
        fields = extract_detail_fields(html_content)
        director = fields.get("Director Name")
        facility_type = fields.get("Facility Type")
        if not director and not facility_type:
            # Empty-value template: the requested ID returned the form's
            # blank shell rather than real data (idaho_plan.md Sec 5.6).
            # Leave the listing-sourced fields as the only data for this
            # provider rather than overwriting anything with blanks.
            self.logger.warning(
                "Idaho: empty detail template for provider %s; keeping listing-only data",
                item.get("license_number"),
            )
            return

        def put(key, value):
            if value:
                item[key] = value

        put("administrator", director)
        put("provider_type", facility_type)
        put("email", extract_email(html_content))
        for label, key in DETAIL_FIELD_MAP.items():
            put(key, fields.get(label))

    def detail_errback(self, failure):
        item = failure.request.meta.get("item")
        if item is None:
            return
        self.logger.warning(
            "Idaho: detail fetch failed for provider %s (%s); keeping listing-only data",
            item.get("license_number"),
            failure.value,
        )
        yield from self._fetch_child_care_check(item)

    # --- phase 3: Child Care Check (inspections + incidents) -------------- #

    def _fetch_child_care_check(self, item):
        alt_id = item.get("id_alternate_rise_id")
        if not alt_id:
            yield item
            return
        yield scrapy.Request(
            f"{CHILD_CARE_CHECK_BASE}/{alt_id}",
            headers=CC_HEADERS,
            callback=self.parse_child_care_check,
            errback=self.child_care_check_errback,
            meta={"item": item},
            dont_filter=True,
        )

    def parse_child_care_check(self, response):
        item = response.meta["item"]
        report_url = response.url
        combined = parse_inspections(response, report_url) + parse_incidents(response, report_url)
        if combined:
            item["inspections"] = combined
        yield item

    def child_care_check_errback(self, failure):
        # Most commonly an HTTP 404 (idaho_plan.md Sec 5.8) -- emit the
        # provider with no inspections rather than dropping it.
        item = failure.request.meta.get("item")
        if item is None:
            return
        self.logger.info(
            "Idaho: child care check page unavailable for provider %s (%s); emitting without inspections",
            item.get("license_number"),
            failure.value,
        )
        yield item

    def closed(self, reason):
        self.logger.info("Idaho: finished (%s) -- %d providers processed", reason, self.provider_count)
        if self.provider_count < EXPECTED_MIN_PROVIDERS:
            self.logger.warning(
                "Idaho: only %d providers found (< %d baseline) -- possible incomplete crawl",
                self.provider_count,
                EXPECTED_MIN_PROVIDERS,
            )
