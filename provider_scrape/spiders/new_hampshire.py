"""New Hampshire child care provider spider.

Source: NH Child Care Information System (NHCIS), a Salesforce Experience
Cloud (Visualforce) site at https://new-hampshire.my.site.com/nhccis/. See
tasks/new_hampshire/new_hampshire_plan.md for the full recon writeup this
spider implements.

Three phases:

  Phase 1  GET  /nhccis/NH_ChildCareSearch          harvest Visualforce
                                                     remoting tokens (same
                                                     pattern as washington.py)
           POST /nhccis/apexremote                  retrieveAccountRecords
                                                     with an all-empty 29-arg
                                                     vector -> the full
                                                     759-provider set, no cap
                                                     (plan Sec 1.1)
  Phase 2  GET  /nhccis/NH_childcaresearchaccountdetail?id=<Account Id>
                                                     plain server-rendered
                                                     HTML -- enrich the item
                                                     and harvest the
                                                     Licensing History table
                                                     (free, one InspectionItem
                                                     stub per visit row)
  Phase 3  POST back to the detail URL, one visit at a time, per provider
                                                     (default ON, `-a
                                                     visits=0` to skip) --
                                                     the per-visit compliance
                                                     detail modal.

Two Salesforce quirks drive most of the parsing complexity here:

  * The search response uses Salesforce's reference-sharing envelope
    (`"ref": true`): only 5 of 759 records carry an expanded `RecordType`
    (`{"s": N, "v": {...}}`); the rest carry a back-reference (`{"r": N}`).
    Rather than resolve the s/r chain generically, a RecordTypeId -> Name map
    is built once from the 5 expanded definitions and then used for every
    record's (always-present) RecordTypeId (plan Sec 1.2).
  * The Phase 3 postback's ViewState rotates on every response, so a
    provider's visits must be fetched as a serialized chain (detail -> visit
    1 -> visit 2 -> ...), each request carrying the *previous* response's
    ViewState. Providers stay concurrent with each other (plan Sec 1.4).

Per Ryan's decision (2026-09-29, plan Sec 4, option (a)): Phase 3 keeps a
full per-domain compliance roll-up (`nh_domains`) but retains item-level
detail (`nh_violations`) only for non-compliant items -- every item row is
still parsed (that's how a violation is found and how the domain -> item
join is validated), the compliant rows are just not kept in the output.
"""

import json
import re

import scrapy

from provider_scrape.items import InspectionItem, ProviderItem

BASE_URL = "https://new-hampshire.my.site.com/nhccis"
SEARCH_URL = BASE_URL + "/NH_ChildCareSearch"
APEXREMOTE_URL = BASE_URL + "/apexremote"
DETAIL_URL = BASE_URL + "/NH_childcaresearchaccountdetail?id={}"

# The retrieveAccountRecords argument vector, exactly as the site's own
# search form emits it (verified live, plan Sec 1.1) except index 1 -- the
# ZIP filter -- is blanked. An all-empty vector returns the complete
# 759-provider set with no result cap (verified by comparing 5 independent
# ZIP-filtered queries against this set -- every returned Id was already a
# member of it).
SEARCH_DATA_VECTOR = [
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    False,
    False,
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    False,
    False,
    False,
    False,
    False,
    False,
    False,
]

# Live baseline (2026-09-29): 759 providers. Logged as a caveat, not a hard
# gate -- there's no independent way to verify this is *every* NH provider
# vs. only those opted into the public search (plan Sec 9).
EXPECTED_PROVIDER_COUNT = 759

# Parsed once from the Phase 2 detail page's getNonComplianceItem() script
# block. Never hardcoded -- these are Visualforce-generated ids that will
# churn (plan Sec 1.4 / Sec 9).
FORM_ID_RE = re.compile(r'<form[^>]+id="([^"]+)"')
SIMILARITY_GROUPING_RE = re.compile(
    r"getNonComplianceItem=function\(selectedVisitId\)\{A4J\.AJAX\.Submit\('[^']*',null,"
    r"\{'similarityGroupingId':'([^']+)'"
)

VISIT_ID_RE = re.compile(r"getNonComplianceItem\('([^']+)'\)")
VISIT_DOCUMENT_ID_RE = re.compile(r"getVisitPublicationRecords\('([^']+)'\)")

# The "N / M" level-of-compliance roll-up's denominator, used only as a
# runtime sanity check on the domain -> item join (plan Sec 4.1).
DENOMINATOR_RE = re.compile(r"(\d+)\s*/\s*(\d+)")


def _clean(value):
    """Trim a string to None-or-non-empty; pass through anything else."""
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value


def coordinate(value):
    """Return a coordinate as a full-precision string, or None.

    Salesforce serializes this geolocation field two different ways and both
    have been observed on this endpoint: a plain float (``42.962232``) and the
    compound shape ``{"source": "42.962232000000000", "parsedValue":
    42.962232}``. ``source`` carries the unrounded value, so it wins when the
    compound shape shows up; a bare float is stringified as-is. Anything else
    (including an explicit null) yields None rather than a stringified dict.
    """
    if isinstance(value, dict):
        value = value.get("source", value.get("parsedValue"))
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _put_if_absent(item, key, value):
    """Set item[key] = value, but only when value is real and the key is
    not already populated (used for detail-page fields the search API never
    supplies, and for fields where the API value should win when present)."""
    value = _clean(value)
    if value in (None, "", [], {}):
        return
    existing = item.get(key)
    if existing not in (None, "", [], {}):
        return
    item[key] = value


# --------------------------------------------------------------------------- #
# Phase 1 -- search API
# --------------------------------------------------------------------------- #


def extract_search_records(search_json):
    """Unwrap the Salesforce `{"s": N, "v": {...}}` envelope down to a flat
    list of field dicts (plan Sec 1.2). Provider records are always
    individually unique, so they are always in expanded ("v") form -- only
    the shared `RecordType` sub-object is ever a back-reference."""
    payload = search_json[0]["result"]["v"]
    return [rec.get("v", rec) for rec in payload]


def build_record_type_map(records):
    """RecordTypeId -> Name, built from whichever records happen to carry
    the expanded RecordType definition (5 of 759 live, plan Sec 1.2). Every
    record's own RecordTypeId is always present, so this map is all that's
    needed -- no generic s/r reference resolution required."""
    mapping = {}
    for record in records:
        record_type = record.get("RecordType")
        if not isinstance(record_type, dict):
            continue
        value = record_type.get("v")
        if isinstance(value, dict) and value.get("Id") and value.get("Name"):
            mapping[value["Id"]] = value["Name"]
    return mapping


def normalize_shipping_state(raw):
    """ "New Hampshire" / "NH" / "NH\xa0" (dirty NBSP) -> "NH" (plan Sec 3.1).

    Anything else is returned whitespace-cleaned rather than guessed.
    """
    if not raw:
        return None
    cleaned = raw.replace("\xa0", " ").strip()
    if cleaned.lower() == "new hampshire":
        return "NH"
    return cleaned or None


def compose_address(street, city, state, postal_code):
    """ "STREET, CITY, NH ZIP" -- never guesses a missing piece."""
    line_parts = [p for p in (street, city) if p]
    tail = state or "NH"
    if postal_code:
        tail = f"{tail} {postal_code}"
    if line_parts:
        return ", ".join(line_parts) + ", " + tail
    return tail


def item_from_search_record(record, record_type_map):
    """Build the base ProviderItem from one search-response record (plan
    Sec 3.1)."""
    item = ProviderItem()
    item["source_state"] = "New Hampshire"

    account_id = record.get("Id")
    item["nh_account_id"] = account_id
    item["provider_url"] = DETAIL_URL.format(account_id)

    _put_if_absent(item, "provider_name", record.get("Name"))

    state = normalize_shipping_state(record.get("ShippingState"))
    address = compose_address(
        record.get("ShippingStreet"),
        record.get("ShippingCity"),
        state,
        record.get("ShippingPostalCode"),
    )
    _put_if_absent(item, "address", address)

    # Both the plain-float and the compound {"source", "parsedValue"} shapes
    # occur on this endpoint -- see coordinate(). Kept as full-precision
    # strings per the common-field canonical format.
    lat = coordinate(record.get("ShippingLatitude"))
    lng = coordinate(record.get("ShippingLongitude"))
    if lat is not None:
        item["latitude"] = lat
    if lng is not None:
        item["longitude"] = lng

    _put_if_absent(item, "phone", record.get("Phone"))
    _put_if_absent(item, "email", record.get("Email__c"))

    if record.get("Capacity__c") is not None:
        item["capacity"] = record["Capacity__c"]

    _put_if_absent(item, "status", record.get("License_Status__c"))

    provider_type = record_type_map.get(record.get("RecordTypeId"))
    _put_if_absent(item, "provider_type", provider_type)

    ages = record.get("Age_Group__c")
    if ages:
        item["ages_served"] = ", ".join(a.strip() for a in ages.split(";") if a.strip())

    # Openings, NOT capacity-by-age-group (plan Sec 3.4a) -- Phase 2's
    # Available Slots table supersedes these when present.
    if record.get("Infant__c") is not None:
        item["nh_infant_openings"] = record["Infant__c"]
    if record.get("Toddler__c") is not None:
        item["nh_toddler_openings"] = record["Toddler__c"]

    _put_if_absent(item, "nh_qris_rating", record.get("QRIS_Rating__c"))
    _put_if_absent(item, "nh_gsq_step", record.get("Qris_GSQ_Approved_Step__c"))
    _put_if_absent(item, "nh_licensed", record.get("Licensed__c"))
    _put_if_absent(item, "nh_licensed_plus", record.get("Licensed_Plus__c"))
    _put_if_absent(item, "nh_accreditation", record.get("Accreditation__c"))
    _put_if_absent(
        item,
        "nh_preventive_protective",
        record.get("Enrolled_as_a_Preventive_and_Protective__c"),
    )
    _put_if_absent(
        item,
        "nh_financial_assistance",
        record.get("Types_Of_Financial_Assistance_Accepted__c"),
    )
    if record.get("COVID_19_Closure__c") is not None:
        item["nh_covid_closure"] = record["COVID_19_Closure__c"]

    return item


# --------------------------------------------------------------------------- #
# Phase 2 -- detail page
# --------------------------------------------------------------------------- #


def extract_after_label(response, label):
    """The value following one of NH's "main-label" fields, in either of the
    detail page's two label/value shapes (plan Sec 3.2):

      1. Inline: ``<span class="main-label">Label: </span>VALUE`` -- VALUE is
         a sibling text node, or lives in a following sibling element (e.g.
         the Granite Step badge span).
      2. Two-column table: ``<td><span class="main-label">LABEL: </span>
         </td><td><div class="wordBreak">VALUE</div></td>`` -- VALUE lives in
         the label's parent ``<td>``'s next sibling ``<td>``.

    Matches ``label`` as a case-sensitive substring of the label element's
    normalized text (the site is inconsistent about leading spaces/trailing
    colons around the label itself, but consistent about case). Returns the
    first match in document order, or ``None`` when the label isn't found or
    carries no value.
    """
    matches = response.xpath(
        '(//span[contains(@class,"main-label")] | //div[contains(@class,"main-label")])'
        f'[contains(normalize-space(.), "{label}")]'
    )
    if not matches:
        return None
    label_el = matches[0]

    parts = label_el.xpath("following-sibling::text() | following-sibling::*//text()").getall()
    text = " ".join(p.strip() for p in parts if p.strip())
    if text:
        return text

    parts = label_el.xpath("parent::td/following-sibling::td[1]//text()").getall()
    text = " ".join(p.strip() for p in parts if p.strip())
    return text or None


def parse_header(response, item):
    """Availability -> accepting_new_children; website link (plan Sec 3.2).

    The header ``Status`` span is always empty on this site --
    ``License_Status__c`` (Phase 1) is the real status, so it's not parsed
    here.
    """
    availability = response.xpath(
        '(//span[@class="fontStyleSub" and contains(., "Availability")])[1]/following-sibling::p[1]/text()'
    ).get()
    _put_if_absent(item, "accepting_new_children", availability)

    website = response.xpath('//a[@data-social-share="website"]/@href').get()
    _put_if_absent(item, "provider_website", website)


def parse_program_info(response, item, logger=None):
    """Main Contact / Scholarship / Head Start (plan Sec 3.2).

    ``Type of Care:`` and ``Capacity:`` are cross-checked against the
    Phase 1 values and logged on a mismatch, but never overwritten --
    the API is the source of truth for both.
    """
    _put_if_absent(item, "administrator", extract_after_label(response, "Main Contact:"))
    _put_if_absent(
        item,
        "scholarships_accepted",
        extract_after_label(response, "Enrolled in the NH Child Care Scholarship Program:"),
    )
    # Routed through nh_head_start (not the common head_start field
    # directly) so the normalization pipeline's FIELD_COLLAPSE_MAP coerces
    # it to a boolean, matching every other state's head_start source field.
    _put_if_absent(item, "nh_head_start", extract_after_label(response, "This is a Head Start Program:"))
    _put_if_absent(
        item,
        "nh_early_head_start",
        extract_after_label(response, "This is an Early Head Start Program:"),
    )

    if logger is not None:
        _cross_check(logger, item, "provider_type", "Type of Care:", response)
        _cross_check(logger, item, "capacity", "Capacity:", response)


def _cross_check(logger, item, field, label, response):
    detail_value = extract_after_label(response, label)
    existing = item.get(field)
    if detail_value and existing and str(detail_value).strip() != str(existing).strip():
        logger.debug(
            "New Hampshire: %s cross-check mismatch for %r -- Phase 1=%r, detail page=%r",
            field,
            item.get("provider_name"),
            existing,
            detail_value,
        )


def parse_hours_and_rates(response, item):
    """Hours of Operation table -> hours + nh_schedule; Fees table ->
    nh_rates (plan Sec 3.2). Absent entirely (not just empty) when a
    provider has no published schedule."""
    panel = response.xpath('//div[contains(@class,"schedulePanel")]')
    if not panel:
        return
    # NOTE: NOT `.//table[1]` / `.//table[2]` -- an XPath positional
    # predicate filters among same-tag siblings under the SAME parent, and
    # each of these two tables is independently the *only* <table> under its
    # own parent, so both would satisfy `[1]`. Select all descendants and
    # index the (parsel) result list instead, in document order.
    tables = panel.xpath(".//table")
    if not tables:
        return

    schedule = []
    hours_parts = []
    for row in tables[0].xpath("./tbody/tr"):
        cells = [c.strip() for c in row.xpath("./td/text()").getall()]
        if len(cells) < 3 or not cells[0]:
            continue
        day, start, end = cells[0], cells[1], cells[2]
        schedule.append({"day": day, "start": start, "end": end})
        hours_parts.append(f"{day} {start}-{end}")
    if schedule:
        item["nh_schedule"] = schedule
        item["hours"] = "; ".join(hours_parts)

    if len(tables) > 1:
        rates_text = " ".join(t.strip() for t in tables[1].xpath(".//text()").getall() if t.strip())
        if rates_text:
            item["nh_rates"] = rates_text


# Available Slots labels -> item field. Detail-page values supersede the
# Phase 1 API's Infant__c/Toddler__c (plan Sec 3.2) -- but only when the
# detail page actually publishes a value: a blank cell means "unknown", not
# zero, so it must never clobber a real Phase 1 number.
AVAILABLE_SLOTS_FIELDS = (
    ("Infant:", "nh_infant_openings"),
    ("Toddler:", "nh_toddler_openings"),
    ("Preschool:", "nh_preschool_openings"),
    ("School Age:", "nh_school_age_openings"),
)


def parse_available_slots(response, item):
    for label, field in AVAILABLE_SLOTS_FIELDS:
        value = extract_after_label(response, label)
        if value is not None:
            item[field] = value


def parse_qris_and_endorsements(response, item, logger=None):
    """Granite Step for Quality (detail-page value wins, cross-checked
    against the Phase 1 QRIS step) and Endorsements (plan Sec 3.2).
    Endorsements is usually a badge image with no text -- that legitimately
    yields nothing to store."""
    gsq_step = extract_after_label(response, "Granite Step for Quality:")
    if gsq_step:
        if logger is not None:
            existing = item.get("nh_gsq_step")
            if existing and existing.strip() != gsq_step.strip():
                logger.debug(
                    "New Hampshire: nh_gsq_step cross-check mismatch for %r -- Phase 1=%r, detail page=%r",
                    item.get("provider_name"),
                    existing,
                    gsq_step,
                )
        item["nh_gsq_step"] = gsq_step

    _put_if_absent(item, "nh_endorsements", extract_after_label(response, "Endorsements:"))


def parse_other_information(response, item):
    """The "Other Information" two-column table (plan Sec 3.2).
    ``TYPE OF FINANCIAL ASSISTANCE:`` wins over the Phase 1 API's sparser
    field; ``TYPE OF CARE:`` is a cross-check only (handled by the caller),
    never stored."""
    financial_assistance = extract_after_label(response, "TYPE OF FINANCIAL ASSISTANCE:")
    if financial_assistance:
        item["nh_financial_assistance"] = financial_assistance

    _put_if_absent(item, "nh_environment", extract_after_label(response, "ENVIRONMENT:"))
    _put_if_absent(item, "transportation", extract_after_label(response, "TRANSPORTATION:"))
    _put_if_absent(
        item,
        "nh_schedule_options",
        extract_after_label(response, "AVAILABLE SCHEDULE OPTIONS:"),
    )
    _put_if_absent(item, "nh_special_needs", extract_after_label(response, "SPECIAL NEEDS:"))
    _put_if_absent(item, "languages", extract_after_label(response, "LANGUAGE SPOKEN BY STAFF:"))
    _put_if_absent(item, "meals", extract_after_label(response, "MEALS:"))
    _put_if_absent(item, "nh_special_skills", extract_after_label(response, "SPECIAL SKILLS:"))


def parse_license_history(response):
    """One stub per row of the Licensing History table (plan Sec 3.3) --
    free, no extra request. Returns a list of plain dicts in document order
    (most recent visit first, matching the page)."""
    rows = response.xpath('//div[@id="InspectionMonitoring"]//table[contains(@class,"ma__table")]/tbody/tr')
    visits = []
    for row in rows:
        date = _clean(row.xpath('./td[@data-label="Visit Date"]/text()').get())
        visit_type = (
            " ".join(t.strip() for t in row.xpath('./td[@data-label="Type of Visit"]//text()').getall() if t.strip())
            or None
        )
        level = (
            " ".join(
                t.strip() for t in row.xpath('./td[@data-label="Level of Compliance"]//text()').getall() if t.strip()
            )
            or None
        )

        onclick = row.xpath(".//a/@onclick").get() or ""
        visit_id_match = VISIT_ID_RE.search(onclick)
        visit_id = visit_id_match.group(1) if visit_id_match else None

        doc_onclick = row.xpath('./td[@data-label="Visit Documents"]//a/@onclick').get() or ""
        doc_match = VISIT_DOCUMENT_ID_RE.search(doc_onclick)
        visit_document_id = doc_match.group(1) if doc_match else None

        visits.append(
            {
                "date": date,
                "type": visit_type,
                "nh_level_of_compliance": level,
                "nh_visit_id": visit_id,
                "nh_visit_document_id": visit_document_id,
            }
        )
    return visits


def inspection_stub(visit):
    """The free InspectionItem fields from one Licensing History row (plan
    Sec 3.3) -- nh_domains/nh_violations/deficiencies are filled in later by
    Phase 3, if it runs."""
    insp = InspectionItem()
    for field in ("date", "type", "nh_level_of_compliance", "nh_visit_id", "nh_visit_document_id"):
        if visit.get(field):
            insp[field] = visit[field]
    return insp


# --------------------------------------------------------------------------- #
# Phase 3 -- per-visit compliance detail postback
# --------------------------------------------------------------------------- #


def extract_view_state(text):
    """The rotating ViewState triplet -- present on the Phase 2 detail page
    AND every Phase 3 postback response (plan Sec 1.4). Returns ``None`` if
    any of the 3 hidden inputs is missing."""
    sel = scrapy.Selector(text=text, type="html")
    view_state = sel.xpath('//input[@id="com.salesforce.visualforce.ViewState"]/@value').get()
    version = sel.xpath('//input[@id="com.salesforce.visualforce.ViewStateVersion"]/@value').get()
    mac = sel.xpath('//input[@id="com.salesforce.visualforce.ViewStateMAC"]/@value').get()
    if not (view_state and version and mac):
        return None
    return {"view_state": view_state, "view_state_version": version, "view_state_mac": mac}


def extract_postback_context(text):
    """Parsed ONCE from the Phase 2 detail page: the form id and
    similarityGroupingId (never re-emitted on the Phase 3 postback response
    itself, plan Sec 1.4) plus the initial ViewState triplet. Returns
    ``None`` if any piece is missing."""
    form_match = FORM_ID_RE.search(text)
    similarity_match = SIMILARITY_GROUPING_RE.search(text)
    view_state = extract_view_state(text)
    if not (form_match and similarity_match and view_state):
        return None
    context = dict(view_state)
    context["form_id"] = form_match.group(1)
    context["similarity_grouping_id"] = similarity_match.group(1)
    return context


def build_postback_formdata(context, visit_id):
    """The Phase 3 form POST body (plan Sec 1.4): the 4 hidden ViewState
    inputs, AJAXREQUEST, the similarityGroupingId parameter (name == value),
    and the target visit id."""
    form_id = context["form_id"]
    grouping = context["similarity_grouping_id"]
    return {
        "com.salesforce.visualforce.ViewState": context["view_state"],
        "com.salesforce.visualforce.ViewStateVersion": context["view_state_version"],
        "com.salesforce.visualforce.ViewStateMAC": context["view_state_mac"],
        form_id: form_id,
        "AJAXREQUEST": form_id,
        grouping: grouping,
        "selectedVisitId": visit_id,
    }


def _denominator(level_text):
    match = DENOMINATOR_RE.search(level_text or "")
    return int(match.group(2)) if match else None


def _modal_field(sel, label):
    """Value following a ``<div><b>Label</b></div>VALUE`` modal header
    field (plan Sec 4.3 header block), where VALUE is a sibling text node or
    lives in a following sibling element (e.g. the ``closedDate`` span)."""
    label_div = sel.xpath(f'//div[b[contains(normalize-space(.), "{label}")]]')
    if not label_div:
        return None
    parts = label_div.xpath("following-sibling::text() | following-sibling::*//text()").getall()
    text = " ".join(p.strip() for p in parts if p.strip())
    return text or None


def parse_visit_modal(text):
    """Parse one Phase 3 postback response into option (a)'s shape (plan
    Sec 4): a full per-domain roll-up plus non-compliant items only, tagged
    with their domain.

    Every item row is parsed regardless of result -- that's how a violation
    is identified and how the domain -> item join (data-target -> @id,
    NOT DOM position, plan Sec 4.1) is validated against the level-of-
    compliance denominator -- but only the non-compliant rows are kept.

    Pure (text in, data out) so it's unit-testable without a live spider or
    network. Returns ``(domains, violations, deficiencies, header_fields,
    warnings)``.
    """
    sel = scrapy.Selector(text=text, type="html")
    warnings = []

    header_fields = {}
    announcement = _clean(sel.xpath('//div[@class="boldLabel"]/following-sibling::text()').get())
    if announcement:
        header_fields["nh_announcement_type"] = announcement
    licensor = _modal_field(sel, "Licensor Assigned")
    if licensor:
        header_fields["nh_licensor"] = licensor
    corrective_accepted = _modal_field(sel, "Date Corrective Action Accepted")
    if corrective_accepted:
        header_fields["nh_corrective_action_accepted"] = corrective_accepted

    domains = []
    violations = []
    domain_rows = sel.xpath('//td[@data-label="Domain Category"]/parent::tr')
    if not domain_rows:
        warnings.append("no domain rows found in the Phase 3 response")

    for row in domain_rows:
        domain_name = _clean(row.xpath('.//span[@class="categoryName"]/text()').get())
        level = _clean(row.xpath('./td[@data-label="Level of Compliance"]/text()').get())
        domains.append({"domain": domain_name, "level_of_compliance": level})

        target = row.xpath('.//a[@data-toggle="collapse"]/@data-target').get()
        if not target:
            warnings.append(f"domain {domain_name!r} has no data-target -- skipping its items")
            continue
        container_id = target.lstrip("#")
        container = sel.xpath(f'//div[@id="{container_id}"]')
        if not container:
            warnings.append(
                f"domain {domain_name!r} data-target {container_id!r} has no matching container -- skipping its items"
            )
            continue

        item_rows = container.xpath('.//tr[td[@data-label="Visit Item Name"]]')
        expected = _denominator(level)
        if expected is not None and expected != len(item_rows):
            warnings.append(
                f"domain {domain_name!r} level-of-compliance denominator {expected} != "
                f"{len(item_rows)} joined item rows"
            )

        for item_row in item_rows:
            result = _clean(item_row.xpath('.//td[@data-label="Result"]/text()').get())
            if result == "Compliant":
                continue

            name_parts = item_row.xpath('.//td[@data-label="Visit Item Name"]//text()').getall()
            name = " ".join(t.strip() for t in name_parts if t.strip()) or None

            regulations = []
            for tip in item_row.xpath(
                './/td[@data-label="Associated Regulations"]//div[contains(@class,"ma__tooltip__inner")]'
            ):
                citation = _clean(tip.xpath('.//label[contains(@class,"ma__tooltip__open")]/text()').get())
                if citation:
                    citation = citation.rstrip(",").strip() or None
                reg_text = _clean(tip.xpath('.//div[contains(@class,"ma__tooltip__message")]//p/text()').get())
                regulations.append({"citation": citation, "text": reg_text})

            observations = None
            corrective_action_plan = None
            next_row = item_row.xpath("following-sibling::tr[1]")
            if next_row and "nonComplaintStatementClass" in (next_row.attrib.get("class") or ""):
                observations = _clean(next_row.xpath('.//div[@id="Non_Compliance"]/text()').get())
                corrective_action_plan = _clean(next_row.xpath('.//div[@id="CorrectiveAction"]/text()').get())

            violations.append(
                {
                    "domain": domain_name,
                    "item": name,
                    "result": result,
                    "regulations": regulations,
                    "observations": observations,
                    "corrective_action_plan": corrective_action_plan,
                }
            )

    return domains, violations, len(violations), header_fields, warnings


class NewHampshireSpider(scrapy.Spider):
    name = "new_hampshire"
    allowed_domains = ["new-hampshire.my.site.com"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.25,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "RETRY_TIMES": 5,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, visits=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # `-a visits=0` skips Phase 3 entirely -- mirrors Connecticut's
        # `-a violations=0` (plan Sec 4, "Note on crawl cost").
        self.do_visits = str(visits).strip().lower() not in ("0", "false")

        self.providers_emitted = 0
        self.visits_fetched = 0
        self.violations_found = 0
        self.detail_failures = 0
        self.postback_failures = 0

    def start_requests(self):
        yield scrapy.Request(SEARCH_URL, callback=self.parse_search_page, dont_filter=True)

    def parse_search_page(self, response):
        """Harvest Visualforce remoting tokens and request the full provider
        set (plan Sec 1.1) -- same pattern as washington.py."""
        match = re.search(r"RemotingProviderImpl\((\{.*?\})\)\);", response.text)
        if not match:
            self.logger.error("New Hampshire: could not find the Visualforce remoting config on the search page")
            return

        config = json.loads(match.group(1))
        vid = config["vf"]["vid"]
        methods = config["actions"]["NH_ChildCareSearchClass"]["ms"]
        method = next(m for m in methods if m["name"] == "retrieveAccountRecords")

        payload = json.dumps(
            {
                "action": "NH_ChildCareSearchClass",
                "method": "retrieveAccountRecords",
                "data": SEARCH_DATA_VECTOR,
                "type": "rpc",
                "tid": 2,
                "ctx": {
                    "csrf": method["csrf"],
                    "vid": vid,
                    "ns": method["ns"],
                    "ver": int(method["ver"]),
                    "authorization": method["authorization"],
                },
            }
        )

        yield scrapy.Request(
            APEXREMOTE_URL,
            method="POST",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "X-User-Agent": "Visualforce-Remoting",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": SEARCH_URL,
                "Origin": "https://new-hampshire.my.site.com",
            },
            callback=self.parse_search_results,
            dont_filter=True,
        )

    def parse_search_results(self, response):
        data = json.loads(response.text)
        records = extract_search_records(data)
        self.logger.info(
            "New Hampshire: search returned %d provider records (expected ~%d)",
            len(records),
            EXPECTED_PROVIDER_COUNT,
        )

        record_type_map = build_record_type_map(records)
        self.logger.info(
            "New Hampshire: resolved %d RecordType definitions from the search response: %s",
            len(record_type_map),
            record_type_map,
        )

        for record in records:
            account_id = record.get("Id")
            if not account_id:
                self.logger.warning("New Hampshire: search record missing Id, skipping: %r", record.get("Name"))
                continue
            item = item_from_search_record(record, record_type_map)
            yield scrapy.Request(
                DETAIL_URL.format(account_id),
                callback=self.parse_detail,
                cb_kwargs={"item": item},
                errback=self.detail_errback,
            )

    def detail_errback(self, failure):
        self.detail_failures += 1
        self.logger.warning("New Hampshire: detail page request failed (%s)", failure.value)

    def parse_detail(self, response, item):
        parse_header(response, item)
        parse_program_info(response, item, logger=self.logger)
        parse_hours_and_rates(response, item)
        parse_available_slots(response, item)
        parse_qris_and_endorsements(response, item, logger=self.logger)
        parse_other_information(response, item)

        visits = parse_license_history(response)
        item["inspections"] = [inspection_stub(v) for v in visits]

        if not visits or not self.do_visits:
            yield from self._emit(item)
            return

        context = extract_postback_context(response.text)
        if context is None:
            self.logger.warning(
                "New Hampshire: could not extract the Phase 3 postback context for "
                "provider %s -- emitting without visit compliance detail",
                item.get("nh_account_id"),
            )
            self.postback_failures += 1
            yield from self._emit(item)
            return

        self.logger.debug(
            "New Hampshire: starting Phase 3 visit chain for provider %s (%d visit(s))",
            item.get("nh_account_id"),
            len(visits),
        )
        yield from self._request_next_visit(item, visits, 0, context, response.url)

    def _request_next_visit(self, item, visits, index, context, detail_url):
        if index >= len(visits):
            yield from self._emit(item)
            return

        visit = visits[index]
        visit_id = visit.get("nh_visit_id")
        if not visit_id:
            self.logger.warning(
                "New Hampshire: visit %d/%d for provider %s has no visit id -- skipping",
                index + 1,
                len(visits),
                item.get("nh_account_id"),
            )
            yield from self._request_next_visit(item, visits, index + 1, context, detail_url)
            return

        self.logger.debug(
            "New Hampshire: provider %s -- fetching visit %d/%d (%s)",
            item.get("nh_account_id"),
            index + 1,
            len(visits),
            visit_id,
        )
        formdata = build_postback_formdata(context, visit_id)
        yield scrapy.FormRequest(
            detail_url,
            formdata=formdata,
            callback=self.parse_visit_detail,
            cb_kwargs={"item": item, "visits": visits, "index": index, "context": context, "detail_url": detail_url},
            errback=self.visit_errback,
            dont_filter=True,
        )

    def parse_visit_detail(self, response, item, visits, index, context, detail_url):
        visit = visits[index]
        domains, violations, deficiencies, header_fields, warnings = parse_visit_modal(response.text)
        for warning in warnings:
            self.logger.warning(
                "New Hampshire: %s (provider %s, visit %s)",
                warning,
                item.get("nh_account_id"),
                visit.get("nh_visit_id"),
            )

        insp = item["inspections"][index]
        insp["nh_domains"] = domains
        insp["nh_violations"] = violations
        for key, value in header_fields.items():
            insp[key] = value

        # `deficiencies` is a common ProviderItem-level field (a running
        # total across every inspection, matching the Connecticut/Kansas/
        # Wisconsin precedent) -- NOT a per-InspectionItem field, which
        # doesn't exist in the schema. Each visit's own non-compliant count
        # is still recoverable as len(nh_violations).
        item["deficiencies"] = (item.get("deficiencies") or 0) + deficiencies

        self.visits_fetched += 1
        self.violations_found += deficiencies

        next_view_state = extract_view_state(response.text)
        if next_view_state is None:
            self.logger.warning(
                "New Hampshire: lost the Phase 3 ViewState after visit %d/%d for provider "
                "%s -- stopping this provider's visit chain early",
                index + 1,
                len(visits),
                item.get("nh_account_id"),
            )
            self.postback_failures += 1
            yield from self._emit(item)
            return

        next_context = dict(context)
        next_context.update(next_view_state)
        yield from self._request_next_visit(item, visits, index + 1, next_context, detail_url)

    def visit_errback(self, failure):
        cb_kwargs = failure.request.cb_kwargs
        item = cb_kwargs.get("item")
        visits = cb_kwargs.get("visits")
        index = cb_kwargs.get("index", 0)
        context = cb_kwargs.get("context")
        detail_url = cb_kwargs.get("detail_url")
        self.postback_failures += 1
        self.logger.warning(
            "New Hampshire: visit detail request failed for provider %s, visit %d/%d (%s) "
            "-- continuing the chain without this visit's detail",
            item.get("nh_account_id") if item else None,
            index + 1,
            len(visits) if visits else 0,
            failure.value,
        )
        yield from self._request_next_visit(item, visits, index + 1, context, detail_url)

    def _emit(self, item):
        self.providers_emitted += 1
        if self.providers_emitted % 50 == 0:
            self.logger.info(
                "New Hampshire: progress -- %d/%d providers emitted",
                self.providers_emitted,
                EXPECTED_PROVIDER_COUNT,
            )
        yield item

    def closed(self, reason):
        self.logger.info(
            "New Hampshire: finished (%s) -- %d providers emitted, %d visits fetched, "
            "%d violations found, %d detail request failures, %d Phase 3 postback failures",
            reason,
            self.providers_emitted,
            self.visits_fetched,
            self.violations_found,
            self.detail_failures,
            self.postback_failures,
        )
        if self.providers_emitted and self.providers_emitted < EXPECTED_PROVIDER_COUNT * 0.9:
            self.logger.warning(
                "New Hampshire: only %d providers emitted (< 90%% of the %d baseline "
                "observed 2026-09-29) -- possible incomplete crawl",
                self.providers_emitted,
                EXPECTED_PROVIDER_COUNT,
            )
