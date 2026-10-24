"""Curated, human-authored explanations for common fields.

This is the **only** hand-maintained content behind
``state_pipeline_details.md`` — everything structural in that doc (field lists,
collapse sources, per-state mappings, vocab tables) is derived from
``items.py`` + ``normalization.py`` by
``scripts/generate_pipeline_details.py``.

When you add a new **common** field, add a 1–2 sentence explanation here. The
generator emits a visible ``TODO`` for any common/derived field missing an
entry, so gaps are obvious in the committed doc.
"""

FIELD_DOCS = {
    # Identity / names
    "provider_name": "The facility's name. ALL-CAPS values are converted to Title Case; whitespace is cleaned.",
    "doing_business_as": "Trade name (doing business as) alongside the licensed name, free text. FL, OK; sentinels "
    "such as `N/A` -> unset. FL's value always equals provider_name (its API already returns the DBA as the name). WA "
    "license name and MA umbrella org are not DBAs and are not mapped.",
    "source_provider_id": "The state system's own record key for the provider, as an opaque string (leading zeros "
    "kept, ints stringified). State-specific: not comparable across states and not guaranteed unique. One source per "
    "state (AK facility_gen_id; VT excluded, NV/WI have no fallback).",
    "license_holder": "Name of the license holder/licensee. ALL-CAPS values are Title-cased.",
    "license_holder_address": "The license holder's own single-line address, separate from the facility address. MI, "
    "NC; whitespace/comma cleanup via clean_address, case kept as-is, MI's literal `null` pieces dropped (all-null -> "
    "unset).",
    "administrator": "On-site administrator or director name. ALL-CAPS values are Title-cased.",
    "license_number": "State-issued license/registration identifier, kept as emitted (only whitespace-cleaned).",
    # Categorical
    "provider_type": "The raw state license category, preserved exactly. It "
    "drives the derived facility_category facet.",
    "facility_category": "Canonical cross-state facet derived from "
    "provider_type (center/family_home/group_home/"
    "school_age/exempt/other). Additive — provider_type "
    "is untouched.",
    "status": "License status mapped to the 5-bucket canonical vocabulary "
    "(active/provisional/pending/enforcement/closed, else unknown). "
    "Replaced in place.",
    # Dates
    "status_date": "Date the current status took effect, normalized to ISO 8601 (YYYY-MM-DD).",
    "license_begin_date": "Start date of the provider's CURRENT license term, normalized to ISO 8601 -- "
    "not necessarily the facility's first-ever license date (bug_11). Some states publish no "
    "current-term date at all, in which case this is left empty rather than falling back to an "
    "original/migration date; see that state's xx_original_license_date field, where published.",
    "license_expiration": "License expiration date, normalized to ISO 8601.",
    # Address
    "address": "Full street address; whitespace-cleaned, trailing country "
    '(", United States"/", USA") stripped, comma spacing '
    "normalized.",
    "city": "City parsed from address when unambiguous; otherwise left empty (never guessed).",
    "state": "USPS 2-letter state parsed from address (spelled-out names are "
    "converted); empty when no recognizable state is present.",
    "zip": "5-digit ZIP parsed from the end of address; empty when ambiguous.",
    # Geocoding provenance (set by the post-run enrichment step, not the
    # normalization pipeline; see scripts/geocode_enrich.py).
    "geocode_source": "Where latitude/longitude came from, set by the post-run "
    'geocoding step: "state" (supplied by the source '
    'state), "census" (derived from address via the US '
    'Census geocoder), or "unmatched" (attempted, no usable '
    "point). Empty when geocoding was not attempted.",
    "geocode_confidence": 'Confidence of a geocoded coordinate: "exact" or '
    '"approximate" for a Census match, or "tie"/'
    '"no_match" when unmatched. Empty for '
    "state-supplied coordinates.",
    # Numbers / types
    "capacity": "Licensed capacity coerced to an integer when it is a clean "
    "number; ranges/free text are left unchanged and logged.",
    "ages_served": 'Ages accepted, normalized to a single string (lists are joined with ", ").',
    "latitude": "Latitude kept as a trimmed string to avoid float precision drift.",
    "longitude": "Longitude kept as a trimmed string to avoid float precision drift.",
    # Collapsed common fields
    "license_type": "Type/category of license, collapsed from a single populated state-specific *_license_type field.",
    "school_district": "Associated school district, collapsed from a single populated state-specific source field.",
    "mailing_address": "Facility mailing address, collapsed from a single "
    "populated state source (owner/licensee addresses are "
    "excluded).",
    "accreditation": "Accreditation(s), collapsed from a single populated state source.",
    "meals": "Meals / food program, collapsed from a single populated state source.",
    "accepting_new_children": "Whether the provider is accepting new children, collapsed from a single state source.",
    "transportation": "Transportation offered, collapsed from a single populated state source.",
    "head_start": "Head Start participation, collapsed from a single state source and coerced to a boolean.",
    "early_head_start": "Early Head Start participation (federal infant/toddler program), coerced to a boolean. "
    "MS, NH, WA. Distinct from `head_start`: EHS sources never feed it.",
    "wheelchair_accessible": "Whether the facility is wheelchair accessible (True/False/None). CT bool, TN Yes/No; NH "
    "from the WHEELCHAIR ACCESSIBLE token in its environment checklist (token present -> True, absent -> None, never "
    "False).",
    "address_suppressed": "True = the state deliberately withheld the street address (typically family homes), so a "
    "blank address is not a data gap. CT, KS, MA bools; FL inverted from displayAddressOnWeb; NY `Y` -> True, blank -> "
    "False.",
    "phone_suppressed": "True = the state deliberately withheld the phone number. FL inverted from displayPhoneOnWeb; "
    "NY `Y` -> True, blank -> False; MA's single contact-redacted flag feeds this too.",
    "curriculum": "Curriculum, collapsed from a single populated state source (VPK-specific curriculum is excluded).",
    # Pass-through common fields (whitespace cleanup only)
    "phone": "Contact phone number (whitespace-cleaned only).",
    "phone_extension": "Extension that goes with `phone`, digits only, kept as a string (leading zeros survive). NJ, "
    "NY; NY input-mask underscores are stripped, and non-numeric junk (`NJ`, `class`) -> None.",
    "email": "Contact email (whitespace-cleaned only).",
    "provider_website": "Provider website URL (whitespace-cleaned only).",
    "county": "County (whitespace-cleaned only).",
    "hours": "Operating hours, as emitted by the state (whitespace-cleaned only).",
    "days_of_operation": "Weekdays the provider is open: list of full day names in Monday -> Sunday order (e.g. "
    '`["Monday", "Tuesday"]`). GA (`MO,TU`), IA (`Mon, Tue`) and VT (`Monday, Tuesday`) via day_names: case/whitespace '
    "tolerant, de-duplicated, unrecognised tokens logged and dropped, none left -> unset. CT/IN/NH/OK/WY/MA/FL derive "
    "it from `weekly_schedule` (distinct days present); a direct source always wins.",
    "weekly_schedule": 'Per-day opening hours: list of `{"day": "Monday", "open": "07:00", "close": "18:00"}` rows, '
    "24h `HH:MM`, full day names, sorted Monday -> Sunday (source order kept within a day; several rows per day "
    "allowed, e.g. NH sessions). CT, IN, NH, OK, WY, MA and FL (from its dict-shaped `hours`, which is left as-is) via "
    "one shared time parser. Closed days give no row; an unparseable row is logged and dropped; none left -> unset. "
    "MA lists several schedules: one is chosen (Full Year, else first School Year, else first Summer, else first "
    "listed) and its rows carry a `schedule_type` key. Overnight ranges (close earlier than open) are kept as "
    "reported.",
    "languages": "Languages, as emitted (string or list; whitespace-cleaned).",
    "deficiencies": "Deficiency/violation count for the provider, coerced to an "
    "integer (bug_10). Digit strings, comma-grouped counts, and integral "
    "floats (e.g. 2.0) all become int; blank/None stays None; negative or "
    "non-numeric values (and stray booleans) become None rather than a "
    "guessed number.",
    "infant": "Whether the provider serves infants: bool-or-None. States that "
    'emit a per-age count ("8"), a count string ("Y"/"N"), or "Yes"/absent '
    "are coerced via count/token > 0 (bug_05); an unparseable token becomes "
    "None rather than a guessed True.",
    "toddler": "Whether the provider serves toddlers: bool-or-None. See infant.",
    "preschool": "Whether the provider serves preschoolers: bool-or-None. See infant.",
    "school": "Whether the provider serves school-age children: bool-or-None. See infant.",
    "scholarships_accepted": "Whether the provider accepts child-care subsidies/scholarships: "
    'bool-or-None. States that emit "Yes"/"No" or NM\'s "Accepted"/"Not '
    'Accepted" are coerced via the shared strict token rule (bug_06); an '
    "unparseable token becomes None rather than a guessed True.",
    "licensing_specialist": "State staff member assigned to oversee the provider's license, as `First Last` "
    "(ALL-CAPS Title-cased; SC's `Last, First` flipped; `N/A` -> empty). Collapsed from AK, MA, ME, OK, SC, TN, "
    "WV; VA's packed `Name: phone` string is split by an adapter.",
    "inspector": "Inspection field: the staff member who ran the visit, as `First Last` (ALL-CAPS Title-cased; MO's "
    "`Last, First M` flipped; blank/`N/A` -> unset). Collapsed from AK, MA (licensor on routine visits, investigator "
    "on investigation rows), MO, MT, NH, RI. If a row carried two populated sources it would stay unset.",
    "inspection_id": "Inspection field: the state's own key for the visit/report, as an opaque string (ints "
    "stringified with no `.0`, strings only trimmed). State-specific: not comparable across states, and unique per "
    "provider record rather than guaranteed globally unique. Collapsed from CT, FL, HI, KY, MA (visit id on routine "
    "visits, investigation id on investigation rows), MO, NH, NV, KS (`ks_nosf_id` only; the survey number repeats) "
    "and AZ. CT occasionally has two provider records (different `ct_provider_id`, same license) carrying the same "
    "inspections, so one CT inspection_id can appear under both.",
    "announced": "Inspection field: whether the visit was announced (`True`), unannounced (`False`) or unknown "
    "(unset). Collapsed from AK, MA, NH and MO's dedicated fields; NC, CT and RI read it from the inspection `type` "
    "text. Matching is case-insensitive and typo tolerant (RI has `Uannounced`, `Unannounce`), unannounced checked "
    "first. Text naming neither word (follow-ups, complaints, `N/A`) stays unset; `type` itself is unchanged.",
    "violation_found": "Inspection field: whether this row recorded any noncompliance (`True`), none (`False`) or "
    "unknown (unset). Collapsed from FL, VA, MA (investigation rows only), WY and MO (inverted: MO's source says "
    "`in compliance`); AK and WI read the status text (`Non-Compliance`, `See Violations below` -> True; "
    "`In Compliance`, `No Violations found ***` -> False). MD has one row per finding: `No Noncompliances Found` "
    "-> False, any other narrative -> True (a cited regulation is a violation even when worded `no evidence of ...`). "
    "CT, DE, NV, KS, HI and SC have no explicit flag, so it is derived from `violation_count` "
    "(`> 0` -> True, `0` -> False, no count -> unset); an explicit flag is never overwritten. "
    "DE's count is always >= 1 by construction (its dataset holds only noncompliance visits), so DE is always "
    "True and must not be used for clean-inspection rates.",
    "violation_count": "Inspection field: number of violations / deficiencies / findings recorded on the visit "
    "(int; `0` is a real value). Collapsed from CT `ct_violations_count`, DE `de_violation_count`, "
    "NV `nv_deficiency_count`, KS `ks_findings_count`, HI `hi_requirements_not_met` and SC `sc_alert_count`. "
    "SC parses it from an `N Alert` badge; a missing badge is unverified and stays unset (not 0). "
    "DE's count is always >= 1 by construction (its dataset contains only noncompliance visits; a clean visit "
    "produces no row), so DE can never show a clean visit and must not be used for clean-inspection rates. "
    "MO is excluded (user decision): `mo_open_violations` counts only violations still open, not the total "
    "found, and MO already has `violation_found` via `mo_in_compliance`.",
    "regulation_cited": "Inspection field: the rule / code section cited on the row, as the state emits it "
    "(trimmed; `N/A`-style placeholders -> unset). Collapsed from AZ `az_regulation`, MD `md_regulation`, "
    "IN `in_rule_code` and WI `wi_rule_number`. Only for one-citation-per-row states; other states nest "
    "citations inside violation lists and leave it unset. AZ rows can hold several comma-separated citations "
    "(`R6-5-5203.4, R6-5-5203.4.B`) or a non-rule label (`DES CONTRACT`, `INCIDENT`), kept as emitted with no "
    "splitting or filtering. Citation formats are state-specific (`AAC R9-5-208.C.`, `13A.15.03.04A(3)`, "
    "`470 IAC 3-4.5-4(1)`, `251.05(2)(a)2.`), so values cannot be compared across states.",
    "compliance_domains": "Inspection field (MA and NH, which share a licensing platform): per-visit list of "
    'per-domain checklist roll-ups, `[{"domain": str, "compliant": int, "reviewed": int}]`, counts only. '
    "`compliant` / `reviewed` are checklist items in that domain, so any finding means `compliant < reviewed`. "
    "NH splits the source `x / y` ratio (`19 / 20` = 19 compliant of 20 reviewed; verified against a domain with "
    "one violation). MA rolls up per-domain status counts: `compliant` = Compliant + Complete, `reviewed` = every "
    "known status (Non-Compliant and Not Complete count as reviewed only; blank statuses are excluded). Rows are in "
    "source order (NH) or first-seen order (MA). Unset when the visit has no domain data. MA values appear only "
    "after an MA re-scrape that emits `ma_domain_counts`. Finding detail lives in the violation fields.",
    "licensing_specialist_phone": "Phone number of the assigned licensing specialist, as emitted by the state "
    "(OK, SC, and VA's split-out number); empty when not published.",
    "licensing_specialist_email": "Lower-cased email of the assigned licensing specialist (published by ME).",
    "vacancies_infant": "Open slots the provider reports for the infant age band (int; `0` = full, empty = not "
    "reported). Bands are each state's OWN age definitions (e.g. ND infant = 0-17 months, DC = 0-12 months, IA = "
    "0-23 months). Not the infant age-served flag. CO, DC, IA, ME, ND, NH, VT; IA sums full-time + part-time openings.",
    "vacancies_toddler": "Open slots for the toddler age band (int; `0` = full, empty = not reported). State-defined "
    "band; see `vacancies_infant`.",
    "vacancies_preschool": "Open slots for the preschool age band (int; `0` = full, empty = not reported). "
    "State-defined band; IA sums its 3-year-old and 4-5-year-old rows. See `vacancies_infant`.",
    "vacancies_school_age": "Open slots for the school-age band (int; `0` = full, empty = not reported). "
    "State-defined band; IA sums its before/after-school and school-age rows. See `vacancies_infant`.",
    "total_vacancies": "Total open slots the provider reports (int; `0` = full, empty = not reported). Only what the "
    "state publishes -- never summed from the per-age `vacancies_*`. IA, ID, ND, TN, UT, VT, WA, WI; WI's prose "
    "(`0 vacancies have been reported...`) is parsed for the leading count and `None Reported.` is empty.",
    "vacancies_updated_date": "Date the provider last updated its vacancy counts (`YYYY-MM-DD`; empty = not reported). "
    "Freshness for `total_vacancies` / `vacancies_*`. IA, ME, ND, VT; WI's date is parsed from the `Last updated on` "
    "text in its vacancy prose.",
}
