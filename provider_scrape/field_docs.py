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
    "age_min_months": "Youngest accepted age in whole months (int; 0 is valid). CT weeks use "
    "floor(weeks * 12 / 52) (6 weeks -> 1 month, 624 weeks -> 144); ND/WV `N Years M Months` strings are "
    "summed (`6 Weeks` uses the same weeks rule); IL takes the widest of the day and night ranges "
    "(`6W TO 12Y`; a bare `0` means not offered and is ignored); WY takes the min over `wy_programs[]`. "
    "Unparseable values are logged and left None. `ages_served` and the `xx_` sources are unchanged.",
    "age_max_months": "Oldest accepted age in whole months (int), same parsing as `age_min_months` (IL max of the "
    "day and night maxes, WY max over programs). A max of 0 or below the min is logged and set to None, keeping the "
    "min. Either bound may be None independently.",
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
    "operating_months": "Months the provider operates: list of 3-letter Title-case months (`Jan`..`Dec`) "
    "in calendar order, de-duplicated. Only where the state lists months: GA (`JAN,FEB,...`; GA's non-month "
    "`OtherSchoolBreak` marker is dropped, other unknown tokens logged), MS (list) and WI (range `Sep - Jun`, "
    "expanded inclusively and wrapping the year). Unparseable or empty -> unset.",
    "year_round": "Whether the provider operates all 12 months (bool). GA/MS/WI: True when all 12 months are "
    "listed, else False. SD (`12 Months` True; `School Year Only`/`Summer Only` False; `Other` unset), "
    "ND (`Full calendar year` True; `School year only` False), VT (`School Year Only`/`Summer Only` False; "
    "`In-Service Days` and blank unset -- never assumed True) and OK (`Year Round` in `ok_care_types` True; "
    "else `School Year Only`/`Summer Only`/`School Vacation Only` False; else unset) use explicit "
    "case-insensitive lookups; unknown labels are logged and leave it unset.",
    "serves_special_needs": "Whether the provider accommodates children with special needs (bool). IA "
    "(`Yes`/`No`) and NE (bool) are copied; NC is True when `nc_special_features` has the `Will enroll children "
    "with special needs` entry, else unset (never False). CO: `None` -> False, `Willing to Support` or any mapped "
    "label -> True, only unmapped labels -> unset. CT/ND: True when the list has a real entry. MA/NH: True for a "
    "mapped label or `Experience` (which is never a type). Absent/empty source -> unset.",
    "special_needs_types": "Kinds of special needs accommodated: list of canonical categories from a fixed "
    "11-category list, de-duplicated, in this order: Autism, Behavioral/Social-Emotional, ADHD, "
    "Developmental/Cognitive, Speech/Language, Physical, Hearing/Vision, Medical/Health, Dietary, Sensory, At Risk. "
    "From CO (`;`-separated), CT/ND (list), MA (`;`-separated) and NH (`, `-separated, UPPERCASE) by an explicit "
    "case-insensitive label lookup; `Experience`, `Willing to Support`, `None` and `N/A` are never types and an "
    "unmapped label is logged and dropped. Empty -> unset. The state labels stay in the `xx_` source fields.",
    "food_program": "Whether the provider participates in the USDA food program, CACFP (bool). HI/ID/WA copy "
    "their flag (`Yes`/`No` or bool; WA is `Yes` or blank, blank -> unset). DE (`de_financial_arrangements`), VT "
    "(`vt_program_participation`), WY (`wy_services`) and UT/PA/DC (the common `meals` text) are matched with one "
    "case-insensitive phrase, `food program|CACFP`: present -> True, absent -> unset (never False). `meals` is "
    "unchanged; the `xx_` sources are kept.",
    "state_prek": "Whether the provider participates in a publicly funded state pre-K program (bool). FL/RI/WA/DC/KY "
    "copy their flag (`Yes`/`No` or bool; blank -> unset, False is real). IN/GA/SC/VT/NV match one exact whole entry "
    "(case-insensitive) in their program list/text: present -> True, absent -> unset (never False). GA "
    "`Georgia's Pre-K Transition` does not count. The `xx_` sources are kept.",
    "state_prek_program": "The state's pre-K program brand (string), set only when `state_prek` is True: VPK, RI "
    "Pre-K, ECEAP, Pre-K Enhancement and Expansion, Preschool Partnership, On My Way Pre-K, Georgia's Pre-K, First "
    "Steps 4K, Vermont Universal Pre-K, Nevada Ready! Pre-K. One constant per state (`STATE_PREK_PROGRAMS`).",
    "nontraditional_hours": "Roll-up (bool): True if the record has night, evening or weekend care True, or DC's "
    "`dc_nontraditional_hours` is True. False ONLY from DC's explicit False; otherwise unset, even when every reported "
    "specific flag is False. Computed in each state's adapter after its specific flags.",
    "night_care": "Overnight / third-shift care (bool). LA `la_night_care`; AL any `al_nighttime_hours` range -> True "
    "(regardless of end time), `N/A - N/A` -> False; IL/WI night capacity and NC third-shift capacity -> capacity > 0 "
    "(`0` and junk `0 0` -> False); OK `Overnight Hours` -> True (absent -> unset). The `xx_` sources are kept. "
    "Caveat: for the capacity-based states (IL, WI, NC) True means the license permits night/shift care, not "
    "confirmed operating hours (IL night capacity equals day capacity for ~80% of its positives).",
    "evening_care": "Evening / second-shift care (bool). WY `wy_evening_care`; NC second-shift capacity > 0 "
    "(0 -> False); OK `Evening Hours` -> True (absent -> unset). The `xx_` sources are kept. For NC, True means "
    "licensed second-shift capacity, not confirmed operating hours.",
    "weekend_care": "Weekend care (bool). WY `wy_weekend_care`; OK `Weekend Hours` -> True (absent -> unset). The "
    "`xx_` sources are kept.",
    "care_schedules": "Care arrangements offered: list of canonical tokens from a fixed 12-token list, "
    "de-duplicated, in this order: before_school, after_school, drop_in, full_day, half_day, full_time, part_time, "
    "summer, school_holidays, emergency, flexible, sick_care. From FL/MS/SD/RI/WI/ND/MA/WY/OK/NJ (lists), "
    "IA/VT (`, `-separated), MI/NH (`;`-separated, NH UPPERCASE), GA (three `; `-separated sources merged) and LA "
    "(`la_before_care`/`la_after_care` True) by an explicit case-insensitive label table (`CARE_SCHEDULE_LABELS`); "
    "one label can give several tokens (`Before and After School`). Evening/overnight/weekend/year-round labels "
    "and non-schedule services are ignored silently (they feed other fields); an unknown label is logged and "
    "dropped. Empty -> unset. The state labels stay in the `xx_` source fields.",
    "rates": "Published tuition: list of rows `{age_group, unit, period, amount}` in source order. `age_group` is the "
    "state's own label (state-specific; no cross-state band mapping). `unit` full_time/part_time/None; `period` "
    "hour/day/week/month/None (an unknown period is kept as None, never guessed); `amount` is a float with `$`/`,` "
    "stripped. Rows with a blank, unparseable (logged) or <= 0 amount are dropped. Optional keys: `schedule_type` "
    "(MA) and `care_type` (TN, WY; the state's label when it is not a unit/period). From CT (one row per "
    "`{unit}_{period}` column; age_group `Infant (0-17 Months)`), GA (weekly full day), KY and PA (the period `day` "
    "is inferred from the amounts, not stated by the source), TN (`unit_of_care` Full/Part-time -> unit, "
    "Weekly/Daily/Monthly -> period, any other descriptor -> `care_type` with period week; `$0.00` skipped), MA "
    "(`rate_type` parsed, `Other Full/Part Time` -> unit only, period None; ONE schedule type per provider chosen "
    "with the same pick as `weekly_schedule`: Full Year, "
    "else School Year, else Summer, else first), NJ (16 `nj_tuition_*` columns, `0.0` skipped) and WY (program name "
    "as age_group). NH is skipped (`nh_rates` is free text and is left alone). Empty -> unset. The `xx_` sources "
    "are kept.",
    "registration_fee": "One-time enrollment/registration fee in dollars (float); 0.0 = no fee. From GA "
    "(`ga_registration_fee`, `$`/`,` stripped; `$0.00` -> 0.0, null -> unset) and WY (`wy_registration_fee`; "
    "the spider only sets it when a fee is charged, so absence is None, not 0.0). NJ is not mapped (`nj_fees` "
    "holds fee types only, no amount). The `xx_` sources are kept.",
    "discounts": "Price discounts offered: list of canonical tokens from a fixed list, de-duplicated, in this order: "
    "sibling, sliding_scale, employee, military, other. From GA `ga_financial_info` (`; `-separated), MA "
    "`ma_financial_assistance` (list), NH `nh_financial_assistance` (`, `-separated UPPERCASE), NJ "
    "`nj_discounts_labels` (resolved labels) by an explicit case-insensitive label table (`DISCOUNT_LABELS`), and "
    "VT `vt_sibling_discount` (Yes -> sibling). MA `Other` -> other. Subsidies and scholarships are not discounts: "
    "they are skipped silently and routed to `scholarships_accepted`. An unknown label is logged and dropped. "
    "No discount (VT No, subsidy-only, empty) is None, never an empty list. The `xx_` sources are kept.",
    "environment_features": "Facility amenities: list of canonical tokens from a fixed list, de-duplicated, in order: "
    "smoke_free, fenced_yard, outdoor_play, air_conditioned, peanut_free, secure_entry, video_surveillance, "
    "parent_webcam, pool, sports_facilities, separate_play_areas, field_trips, near_public_transit, no_tv. From GA "
    "`ga_environment` (`; `-separated), MA `ma_environment` (list), NH `nh_environment` (`, `-separated UPPERCASE), "
    "UT `ut_environment` and VT `vt_area_description` (`,`-separated), and NJ `nj_environmental_features_labels` "
    "(resolved labels) by an explicit case-insensitive label table (`ENVIRONMENT_LABELS`). Pet labels and "
    "wheelchair/handicap labels belong to other fields and are skipped silently, as are non-amenity labels "
    "(approved assistant, mixed age, school bus route, provide your own equipment). NJ codes 6-13 have no known "
    'label ("Unknown (code N)") and are skipped silently. An unknown label is logged and dropped. None found '
    "is None, never an empty list. The `xx_` sources are kept.",
    "has_pets": "Whether animals are present at the provider: True = pets present, False = explicitly none, None = "
    "not stated (absence is never False). From ID `id_pet_policy` (Pet Free = False; Dogs only / Cats only / Other = "
    "True), VT `vt_pets` (any recognised entry = True), GA `ga_environment` and NJ `nj_environmental_features_labels` "
    "(`No pets` = False), UT `ut_environment` (`No Pets` = False, `Pets` = True) and NH `nh_environment` (NO PETS = "
    "False; PETS / INDOOR PETS / OUTDOOR PETS / DOG / CAT = True). NH NO DOG / NO CAT alone do not set it. Conflicting "
    "entries in one record are logged and left None. The `xx_` sources are kept.",
    "pet_types": "Animal types present: list of `dogs`, `cats`, `other` (in that order, de-duplicated). Set only when "
    "the types are known (ID, VT, NH DOG / CAT entries); a bare `Pets` (UT, NH) leaves it None, never []. Unknown VT "
    "entries and ID values are logged and dropped.",
    "pets_excluded": "Animals the source explicitly says are absent: list of `dogs`, `cats`. From NH NO DOG / NO CAT "
    "(also NO DOGS / NO CATS) only, which never set `has_pets` or `pet_types`. None when none stated, never [].",
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
