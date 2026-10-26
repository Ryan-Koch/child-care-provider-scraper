# Field-Mapping Playbook (for new state spiders)

Read this **before** inventing a new `xx_` state-specific field. Most data a
new state emits already has a home in a **common field**. Putting it there (and
in the canonical format) means the downstream web app and cross-state filters
work without per-state special-casing.

> **Source of truth:** the field set lives in `provider_scrape/items.py`; the
> exact per-field transformations live in `provider_scrape/normalization.py`.
> For an auto-generated, always-current list of *every* field and what the
> pipeline does to it (plus per-state mappings), see
> [`../state_pipeline_details.md`](../state_pipeline_details.md). This playbook
> is the *decision guide*; that doc is the *as-built reference*.

---

## When you're adding a new state — checklist

1. **Map first, invent last.** For each datum the site exposes, scan the
   *Concept → common field* table below and the common-field catalog. If it
   fits a common field, populate the common field.
2. **Honor the canonical formats** (dates `YYYY-MM-DD`, `capacity` int, etc.) —
   but you usually don't have to do the work yourself: the normalization
   pipeline reformats common fields at scrape time. Emit your best raw value
   into the right field and let the pipeline normalize it.
3. **New shared concept?** If two or more states have a field for the same
   concept and there's no common field yet, add a *common* field (and, for a
   state-specific source, wire it into `FIELD_COLLAPSE_MAP`) rather than a
   second `xx_` field. See the rule of thumb below.
4. **Genuinely state-unique data** (and **all quality ratings**) → an `xx_`
   field, per `claude.md`'s `stateabbreviation_fieldname` rule.
5. If your new state introduces a new `provider_type` or `status` value, extend
   `FACILITY_CATEGORY_MAP` / `STATUS_MAP` so it doesn't fall to the logged
   `other` / `unknown` bucket. Re-run the Task 10 generator and commit.

---

## Common-field catalog

Meaning + canonical format for the common (non-`xx_`) fields. The list is
maintained in `items.py`; this table is the human-readable gloss.

| Field | Meaning | Canonical format |
|---|---|---|
| `provider_name` | Facility name | String; ALL-CAPS names → Title Case |
| `doing_business_as` | Trade name (DBA) | Free-text string (FL/OK via `none_if_sentinel`: trim, `N/A`-style sentinels -> unset). FL's equals `provider_name` |
| `license_number` | License/registration id | String (as emitted) |
| `source_provider_id` | State system's own record key | Opaque string via `to_id_string` (leading zeros kept); one source per state (AK `ak_facility_gen_id`); VT excluded; not unique/comparable across states |
| `license_holder` | Licensee / holder name | String; ALL-CAPS → Title Case |
| `license_holder_address` | Licensee / owner's own address | Single-line string (MI, NC, MO): `clean_address`, case kept; MI's literal `null` pieces dropped; MO's comes from the report's "Mailing Address" listed under Owner |
| `administrator` | On-site administrator/director | String; ALL-CAPS → Title Case |
| `provider_type` | Raw state license category | String (preserved exactly) |
| `facility_category` | Canonical facet of `provider_type` | Vocab: `center` / `family_home` / `group_home` / `school_age` / `exempt` / `other` |
| `status` | License status | Canonical vocab (5 buckets, see below) |
| `status_date` | Date of current status | `YYYY-MM-DD` |
| `address` | Full street address | Cleaned string (no `, United States`) |
| `city` / `state` / `zip` | Parsed address parts | `city` string, `state` USPS 2-letter, `zip` 5-digit — **only when unambiguous** |
| `latitude` / `longitude` | Coordinates | String (full precision preserved) |
| `phone` / `email` / `provider_website` | Contact | String |
| `phone_extension` | Extension for `phone` | Digits-only **string** (NJ/NY via `phone_extension_digits`: trim, strip `_` mask padding, non-digits -> None) |
| `capacity` | Licensed capacity | Integer (when a clean number) |
| `hours` | Operating hours | String |
| `days_of_operation` | Weekdays open | List of full day names, Mon->Sun (GA/IA/VT via `day_names`: 2-/3-letter/full names, de-duped, unknown tokens logged and dropped; CT/IN/NH/OK/WY/MA/FL derived from `weekly_schedule`, never overwriting a direct source) |
| `weekly_schedule` | Per-day open/close rows | List of `{day, open, close}`: full day names, 24h `HH:MM`, Mon->Sun, several rows per day allowed (CT/IN/NH/OK/WY/MA/FL via per-state adapters sharing `parse_time`/`split_time_range`; FL parsed from its dict `hours`, which stays as-is; MA picks one schedule - Full Year, else School Year, else Summer, else first - and tags rows with `schedule_type`) |
| `operating_months` | Months open | List of 3-letter months `Jan`..`Dec`, calendar order (GA/MS/WI via `month_names`/`month_range`: WI ranges wrap the year, GA `OtherSchoolBreak` dropped) |
| `year_round` | Operates all 12 months | Bool (GA/MS/WI: all 12 months listed; SD/ND/VT/OK by explicit label lookup, seasonal-only -> False, unknown/blank -> unset) |
| `serves_special_needs` | Accommodates special needs | Bool (IA/NE via `to_bool`; NC from the `Will enroll children with special needs` token, absence -> unset; CO/CT/MA/NH/ND via per-state adapters; CO `None` -> False, `Willing to Support`/`Experience` -> True with no type) |
| `special_needs_types` | Kinds of special needs | List from a fixed 11-category list (`SPECIAL_NEEDS_LABELS` in `common_fields.py`: explicit case-insensitive label lookup, canonical order, de-duped, unmapped labels logged and dropped; CO/CT/MA/NH/ND) |
| `food_program` | Participates in USDA CACFP | Bool (HI/ID/WA via `to_bool`, WA blank -> unset; DE/VT/WY via the shared `food program\|CACFP` phrase match in the map; UT/PA/DC via state-scoped adapters matching the same phrase in the common `meals`, which is unchanged; phrase absent -> unset, never False) |
| `faith_based` | Faith-based operator or religious programming | Bool (FL `fl_is_faith_based` and VT `vt_religious_activity` via `to_bool`, False real, blank -> unset; WI whole-word `Religious` in `wi_program_philosophy` and ID comma-separated entry `Faith Based` in `id_program_philosophy`, else unset; VA `Religious Exempt Child Day Center` and IN `Unlicensed Registered Ministry` exact `provider_type` via state-scoped adapters, else unset; WI/ID/VA/IN true-only, never False; meaning of True is per state, see `FIELD_DOCS`) |
| `accepts_diapered_children` | Provider takes children who are not toilet-trained | Bool (NM `nm_potty_training` inverted via explicit whole-value lookup: `Not Required` -> True, `Required` -> False, else unset; HI `hi_diapered_children_accepted` via `to_bool`, False real, None -> unset; unset = unknown; NC diaper supply not mapped) |
| `publicly_listed` | State public portal shows this provider (source visibility flag, not a provider attribute) | Bool (CT `ct_searchable` and DE `de_portal_listed` via `to_bool`, False real, None -> unset; DE unset when the advisory portal fetch failed; IA `ia_referral_listed` excluded: referral-network membership, not portal visibility) |
| `state_prek` | Publicly funded state pre-K participation | Bool (FL/RI/WA/DC/KY via `to_bool` in per-state adapters, False real, blank -> unset; IN/GA/SC/VT/NV exact whole-entry token match, absent -> unset, never False; GA `Georgia's Pre-K Transition` excluded) |
| `state_prek_program` | Pre-K program brand | String from `STATE_PREK_PROGRAMS` in `common_fields.py` (e.g. `VPK`, `ECEAP`), set only when `state_prek` is True |
| `nontraditional_hours` | Offers night, evening or weekend care (roll-up) | Bool; True if any specific flag is True or DC `dc_nontraditional_hours` True; False only from DC's explicit False, else unset |
| `night_care` | Overnight / third-shift care | Bool (LA bool; AL any time range True, `N/A - N/A` False; IL/WI/NC capacity > 0 via shared `capacity_to_bool`, `0`/`0 0` False; OK `Overnight Hours` True, absent unset). For IL/WI/NC, True = licensed night/shift capacity, not confirmed hours |
| `evening_care` | Evening / second-shift care | Bool (WY bool; NC second-shift capacity > 0; OK `Evening Hours` True, absent unset) |
| `weekend_care` | Weekend care | Bool (WY bool; OK `Weekend Hours` True, absent unset) |
| `care_schedules` | Care arrangements offered | List from a fixed 12-token list (`CARE_SCHEDULE_LABELS` in `common_fields.py`: explicit case-insensitive label table, one label may give several tokens, canonical order, de-duped; ignored labels silent, unknown logged and dropped; FL/MS/SD/RI/WI/ND/MA/WY/OK/NJ/IA/VT/MI/NH/GA/LA via per-state adapters, GA merges three sources, LA from two bools) |
| `rates` | Published tuition by age group | List of `{age_group, unit, period, amount}` (+ optional `schedule_type` MA, `care_type` TN/WY); float amounts, `$`/`,` stripped, <= 0/blank/unparseable dropped; state age labels kept; unknown period -> None. CT/GA/TN/MA/NJ/WY unpivoted via per-state adapters; KY/PA period `day` inferred from amounts; MA picks one schedule like `weekly_schedule`; NH skipped |
| `licensed` | Licensed vs license-exempt / approved-only | Bool (True licensed, False exempt/approved-only). CT copies `ct_licensed`; LA explicit vocab (`Licensed` True, `License Exempt` False); NH `Yes` True, absent unset; SC explicit vocab (`License` True, `Approval`/`Exempt` False). Any state: unset + `facility_category` `exempt` -> False; True never inferred |
| `profit_status` | Operator ownership type | String vocab `for_profit`/`nonprofit`/`public`. DE `Nonprofit` nonprofit, `Profit` and `Private` for_profit, `Publicly Operated` public. GA not mapped (`ga_profit_status` is null on every API record, bug_08) |
| `building_type` | Physical setting of care (not license class) | String vocab `residence`/`non_residential`/`workplace`/`other`. IA `house` residence, `building` non_residential; VT `House`/`Mobile Home`/`Apartment` residence, `Non-Residential` non_residential, `Workplace-Based` workplace. MN `mn_setting` not wired (never set). Nothing maps to `other` yet |
| `program_description` | Provider-written program description | String, verbatim apart from trimming; shared `description_text` filler check (blank/sentinel, punctuation-only, `x` runs, `Please ask provider`, `Please ask program`, `Ask provider`, `Coming soon` -> unset; short answers kept). ID adapter: `id_program_description`, else `id_philosophy_description` (`id_philosophy_comment` ignored); ND `nd_philosophy`; VT `vt_program_description` |
| `current_enrollment` | Children currently enrolled | Int; ND copies `nd_total_enrollment` with `0` -> unset (unreported); DC adapter sums numeric `dc_enrollment` rows (all non-numeric -> unset; `No Data Available` rows may undercount). Can exceed `capacity` (part-time children) |
| `capacity_by_age` | Licensed capacity per age group | List of `{age_group, capacity[, classrooms]}`; state's own labels (not banded); int via `to_int`, zero kept, blank/unparseable dropped, none -> unset; MA fixed key table (unknown key logged + humanized), RI copies label + `classrooms`, CT 3 buckets incl. `Full Time (any age)` (not an age group), IN `Start – End`; NY prose skipped |
| `license_conditions` | Restrictions/conditions on the license | List of the state's own strings, verbatim, trimmed, case-insensitively de-duped, sentinels/blanks dropped, none -> unset (never []). MN `;` split plus explicit run-together phrase split points (`MN_CONDITION_PHRASES`), literal `None` -> unset; NC strips leading `Other -` (bare one dropped); MO `;` split, ALL CAPS kept; DE `;` split |
| `registration_fee` | One-time registration/enrollment fee | Float dollars, 0.0 = no fee (GA `$100.00`/`$0.00` via `to_float`; WY amount copied, only present when a fee is charged so absence -> None; NJ not mapped: `nj_fees` is fee types only) |
| `discounts` | Price discounts offered | List from a fixed 5-token list (`DISCOUNT_LABELS` in `common_fields.py`: explicit case-insensitive label table, canonical order, de-duped; GA/MA/NH/NJ via per-state adapters, VT Yes -> `sibling`; subsidy/scholarship labels skipped silently (they feed `scholarships_accepted`), unknown logged and dropped; no discount = None, never []) |
| `environment_features` | Facility amenities (smoke-free, fenced yard, ...) | List from a fixed 14-token list (`ENVIRONMENT_LABELS` in `common_fields.py`: explicit case-insensitive label table, canonical order, de-duped; GA/MA/NH/UT/VT/NJ via per-state adapters; pet and wheelchair labels, non-amenity labels and NJ `Unknown (code N)` skipped silently, unknown logged and dropped; none found = None, never []) |
| `has_pets` | Animals present at the provider | Bool-or-None: True present, False explicitly none, None unknown (absence never False); ID `id_pet_policy`, VT `vt_pets`, GA/UT/NJ/NH environment lists via per-state adapters (NH NO DOG / NO CAT alone do not set it; conflicts logged, left None) |
| `pet_types` | Which animals are present | List of `dogs`, `cats`, `other` in that order, de-duped; only when types known (ID, VT, NH DOG/CAT), else None, never [] |
| `pets_excluded` | Animals stated absent | List of `dogs`, `cats` from NH NO DOG / NO CAT only; None when none stated, never [] |
| `ages_served` | Ages accepted | String (lists joined with `, `) |
| `age_min_months` | Youngest accepted age | Int months, 0 valid; weeks -> `floor(weeks * 12 / 52)` (6 wk -> 1, 624 wk -> 144); `age_to_months` parses `6 Weeks` / `2 Years 0 Months` / IL `6W`,`15M`,`12Y`; CT/ND/WV/IL/WY via per-state adapters; IL widest of day+night (bare `0` = not offered, ignored); WY min of program mins; unparseable logged -> None |
| `age_max_months` | Oldest accepted age | Int months, same parsing as min (IL/WY max of maxes); max of 0 or below min is logged and set to None, min kept |
| `county` | County | String |
| `scholarships_accepted` | Subsidy/scholarship acceptance | bool-or-None via `coerce_scholarships_accepted_flag`; FL/VA/WA collapsed from their subsidy flags (WA empty -> unset), NH from the `STATE CC ASSISTANCE (SCHOLARSHIP)` token as a fallback only; OK set by its spider |
| `temporarily_closed` | Licensed but not currently operating | bool-or-None via `to_bool`; ME `me_temporarily_closed` (Yes/No), IN `in_is_temporarily_closed` (only ever True; absent -> None). NH `nh_covid_closure` / MA `ma_temporary_status` are COVID-era, not mapped |
| `complaint_count` | Number of complaints the state lists (all, not substantiated-only) | Int, `0` kept; IA `ia_complaint_count` via `to_int`, CA `ca_complaint_info` (`No Complaints` -> 0, a date -> None: count unknown), IN `in_complaints` len() by adapter (absence -> 0). AL `al_substantiated_complaints` excluded: substantiated only, user decision |
| `serious_injury_count` | Reported serious injuries (`0` = none reported) | Int via `to_int`; MD `md_serious_injuries` (period unspecified on the page), DE `de_injuries_report` by adapter: only the most recent year listed in the record is counted (user decision; the year can differ per provider), `No facility injuries reported` -> 0, unrecognised -> None. LA `la_serious_injuries_url` excluded (URL only) |
| `fatality_count` | Reported deaths (`0` = none reported) | Int via `to_int`; MD `md_fatalities`, DE `de_injuries_report` by adapter (same most-recent-year rule; `N facility death(s)`, `No facility deaths reported` -> 0) |
| `license_history` | Prior and current license terms | List of rows with only `license_type`, `effective_date` (ISO), `expiration_date` (ISO), `license_id`, `status`, `capacity` (blank/`Unknown` keys omitted); chronological, undated last; empty -> unset; adapters for HI `hi_license_history` (`P`/`R` expanded to Provisional/Regular) and WA `wa_license_history` (`closure_date` -> `expiration_date`, a closure not a scheduled expiry); NC excluded (single current entry, not a history) |
| `license_begin_date` | License start | `YYYY-MM-DD`; NC collapsed from `nc_license_effective_date` (current license's effective date; a spider-set value wins) |
| `last_renewed_date` | Current-term start, only if renewed | `YYYY-MM-DD`; adapter for MN `mn_last_renewed_date`, RI `ri_most_recently_renewed`, MA `ma_last_issue_date`: set only when later than the state's `xx_original_license_date` (equal = first term -> None; missing original or earlier-than-original -> None). Equals `license_begin_date` when set. MO `mo_license_anniversary_date` excluded (no year) |
| `original_license_date` | First-ever license date | `YYYY-MM-DD` via `to_iso_date` (CA `ca_license_first_date`, NY `ny_facility_opened_date`, MN/VT/MA/RI `xx_original_license_date`); distinct from `license_begin_date` (current term). CT `ct_date_established` (founding date) and CO `co_license_issue_date` (current-license issue) excluded |
| `license_expiration` | License end | `YYYY-MM-DD` |
| `license_type` | Type/category of license | String (collapsed from `xx_license_type`) |
| `school_district` | Associated school district | String (collapsed) |
| `mailing_address` | Facility mailing address | String (collapsed) |
| `accreditation` | Accreditation(s) | String/list (collapsed) |
| `meals` | Meals / food program | String (collapsed) |
| `accepting_new_children` | Open enrollment flag | As emitted (collapsed) |
| `transportation` | Transportation offered | Mixed: bool (KY/SD/WY; NC `True` on the `Provides transportation` token, else unset), string/list/label text elsewhere |
| `enforcement_action` | Current enforcement action against the license | String, the state's own label; unset when none, never `False`. Overlay beside `status` (unchanged). By adapter from DE (`de_enforcement_action`, else `de_intent_to_revoke`; `Revoked` kept alongside `status` `closed`), KY (`process_type` of each `On-going` `ky_ongoing_processes` entry incl. `Directed Plan of Correction`, joined `; `), OK (fixed label per true flag `ok_revocation_sent` / `ok_denial_sent` / `ok_emergency_issued`: `Revocation sent`, `Denial sent`, `Emergency order issued`, joined `; `). AL excluded: `al_adverse_actions` is all past corrective action. Source fields unchanged |
| `head_start` | Head Start program | **Boolean** (collapsed + coerced) |
| `early_head_start` | Early Head Start program (separate from `head_start`) | **Boolean** (collapsed MS/NH/WA, coerced via `_coerce_bool`) |
| `wheelchair_accessible` | Wheelchair-accessible facility | **Boolean** (collapsed CT/TN via `_coerce_bool`; NH from `nh_environment` token, absent -> None not False) |
| `address_suppressed` / `phone_suppressed` | State deliberately withheld the address / phone (True = withheld) | **Boolean** (CT/KS/MA copy; FL `display_*_on_web` inverted; NY adapter: `Y` -> True, blank -> False; MA flag feeds both) |
| `curriculum` | Curriculum | String (GA/NJ free text copied as published; ID `id_program_philosophy` and WI `wi_program_philosophy` filtered through a fixed allow-list of Creative Curriculum / Montessori / HighScope / Reggio Emilia / Waldorf, canonical names de-duplicated and joined `; `, else unset) |
| `licensing_specialist` | Assigned state licensing specialist | `First Last` string (collapsed; SC `Last, First` flipped; VA split by adapter; `N/A` -> None) |
| `region` | State's own region label (no cross-state vocabulary; suits per-state filters) | String copy, trimmed (blank/sentinel -> None). IA `ia_region`, NV `nv_region`, NY `ny_region_code` and CA `ca_regional_office` (office codes, unexpanded; CA keeps leading zero), AZ `az_regionalpartnership` (First Things First funding region), HI `hi_island` (island). MA `ma_regional_office_address` excluded (an address) |
| `inspector` | Inspection row: staff member who ran the visit | `First Last` string (collapsed from AK, MA licensor/investigator, MO, MT, NH, RI; MO `Last, First M` flipped; ALL-CAPS Title-cased) |
| `inspection_id` | Inspection row: state's own key for the visit/report | Opaque string via `to_id_string` (collapsed from CT, FL, HI, KY, MA visit/investigation, MO, NH, NV, KS `ks_nosf_id` only, AZ); state-specific, unique per provider record, not guaranteed globally unique (CT can repeat across two provider records with the same license) |
| `announced` | Inspection row: was the visit announced | Bool: True announced, False unannounced, None unknown (collapsed from AK, MA, NH, MO via `announced_flag`; NC, CT, RI read the common `type` text by adapter; typo-tolerant, unannounced checked first; `type` unchanged; `N/A` or neither word -> unset) |
| `violation_found` | Inspection row: did the visit record any noncompliance | Bool: True violation, False none, None unknown (collapsed from FL, VA, MA, WY via `to_bool`, MO inverted via `invert`, MD `md_finding` text via `md_finding_flag`; AK and WI read the common `original_status` by adapter with explicit vocab tables; source fields unchanged; CT, DE, NV, KS, HI, SC derived as `violation_count > 0` by adapter, never overwriting an explicit flag; DE is always True because its count is >= 1 by construction, so it cannot show a clean visit) |
| `violation_count` | Inspection row: number of violations / deficiencies / findings on the visit | Int via `to_int`, `0` kept (collapsed from CT, DE, NV, KS, HI, SC; SC missing badge stays unset; MO `mo_open_violations` excluded: open-only count, user decision; DE always >= 1 by construction) |
| `compliance_domains` | Inspection row: per-domain checklist roll-up | List of `{domain, compliant, reviewed}` ints, counts only (MA, NH). NH adapter splits `nh_domains` `x / y` via the shared `parse_ratio` (bad rows dropped + logged); MA adapter rolls up `ma_domain_counts` (compliant = Compliant + Complete, reviewed = all known statuses, `Unknown` excluded); empty -> unset; sources unchanged |
| `items_compliant` / `items_reviewed` | Inspection row: numerator / denominator of the visit's compliant-over-reviewed ratio | Non-negative ints. MA `ma_level_of_compliance`, RI `ri_compliance`, NH `nh_level_of_compliance` split by adapter via `parse_ratio` (NH numerator verified as compliant); OK copies `ok_compliance_count` / `ok_compliance_total`; KS copies `ks_regulations_reviewed` to `items_reviewed` only, never `items_compliant` (findings are not one-per-regulation). Units differ: items (MA/RI/NH), areas (OK), regulations (KS). Bad ratio, numerator > denominator, negative or non-int -> both unset; sources unchanged |
| `regulation_cited` | Inspection row: rule/code section cited (one-citation-per-row states only) | State's own citation string, as emitted, via `none_if_sentinel` (trim; `N/A` -> unset) (collapsed from AZ, MD, IN, WI; AZ may hold several comma-separated citations or a label like `DES CONTRACT`, kept as emitted; formats are state-specific, not comparable across states; source fields unchanged) |
| `correction_date` | Inspection row: date the cited issue was corrected (one-issue-per-row states only) | ISO `YYYY-MM-DD` via `to_iso_date` (collapsed from AZ `az_date_resolved`, IN `in_correction_date`; WV `wv_issue_completed_date` by adapter, only when `wv_outcome_code` is exactly `Achieved`, and the `1/1/1753` null placeholder -> unset; NH `nh_corrective_action_accepted` excluded: plan accepted is not a correction; an unparseable date is kept as emitted and logged; source fields unchanged) |
| `licensing_period_start` / `licensing_period_end` | Inspection row: the license term stamped on the visit (the term it is associated with, not necessarily the one in force on the visit date; renewal visits usually precede it) | ISO `YYYY-MM-DD` via `to_iso_date`. HI copies `hi_licensing_period_start` / `_end`; ME converts `me_licensed_from` / `me_licensed_to`. One adapter per state fills both. Copied as published, no filter on the inspection date. Both parse and end < start -> both unset; an unparseable date is kept as emitted and logged; a missing side is left unset alone; sources unchanged |
| `visit_reason` | Inspection row: purpose of the visit in the state's own words | String as emitted after a whitespace trim, via `none_if_sentinel`; no canonical vocabulary, labels are state-specific. MA copies `ma_visit_reason`, OK copies `ok_visit_purpose` (both by adapter), KS copies `ks_survey_reason` (collapse entry). MA/OK fall back to `type` when the reason is unset (`Investigation` / `Complaint` rows); the fallback is state-scoped and never fires elsewhere. KS has no fallback (`Administrative Order` rows stay unset). `type` and source fields unchanged; blank or `N/A` -> unset |
| `finding_text` | Inspection row: inspector's narrative of what was found | String, trimmed and otherwise verbatim (no re-casing or truncation), via `none_if_sentinel` (collapsed from MD `md_finding`, IN `in_noncompliance`, WI `wi_description`, AZ `az_decision_correction`, DE `de_investigation_conclusion`; OK `ok_noncompliances` list joined with a blank line, blank entries dropped). Per citation in MD/IN/WI, per visit in AZ/OK, per complaint investigation in DE. MD `No Noncompliances Found` (same match as `violation_found`) and AZ exact `MONITOR VISIT COMPLETED; NO ISSUES` (case-insensitive, no fuzzy matching) -> unset; empty/all-blank OK list, blanks and `N/A` -> unset. IN narratives that repeat `in_rule_description` are kept as published. Source fields unchanged |
| `regulation_description` | Inspection row: text or summary of the cited rule | String, trimmed and otherwise verbatim, via `none_if_sentinel` (blanks and `N/A` -> unset) (collapsed from IN `in_rule_description`, WI `wi_rule_summary`, WV `wv_non_compliance_code`). Granularity differs: WI is a short title (`Supervision Of Children`, Violation rows only), IN is mostly full rule text with some short titles, WV is full rule text despite the field name. Pairs with `regulation_cited` (IN and WI also publish a code; WV has none). Wording is state-specific, not comparable across states. Source fields unchanged |
| `violations` | Inspection row: list of structured violations, one per violation | List of dicts with keys `regulation`, `regulation_description`, `finding_text`, `category`, `severity`, `status`, `corrected_date` (source order). By adapter from CT `ct_violations` (`regulation` with surrounding `[ ]` stripped, `statute`, `category`), DE `de_violations` (`regulation_code`, `description`, `correction_status`, `corrected_date`), MA `ma_violations` (`regulation`, `statement`; `result` not mapped), NH `nh_violations` (one row per violation; non-blank citations joined `; `, non-blank texts joined with a blank line; `observations`, `domain`, `result`), MO `mo_violations` (`rule`, `description`), WY `wy_violations` (combined `regulation` text -> `regulation_description`, not split; `compliance_achieved_date`), NC `nc_violations` strings (leading `Rule Violated: ` prefix removed, case-insensitive, -> `finding_text`; unprefixed strings kept), SC `sc_deficiencies` (`deficiency_type`, `severity`, `resolved`, `date_resolved`), OK `ok_allegations` (`requirement`, `requirement_description`, `noncompliance_observed`, `findings`). Strings trimmed and otherwise verbatim via `none_if_sentinel` (blank and `N/A` -> key omitted); `category` / `severity` / `status` are the state's own labels, no vocabulary; `corrected_date` via `to_iso_date` (unparseable kept as emitted and logged). Rows with no populated key dropped; no rows -> unset, never `[]`. Source fields unchanged |
| `licensing_specialist_phone` / `licensing_specialist_email` | Specialist contact | Phone as emitted / lower-cased email |
| `vacancies_infant` / `_toddler` / `_preschool` / `_school_age` | Open slots per age band (state's own band definitions) | Int, `0` kept (collapsed CO/ME/NH/VT via `to_int`; IA/ND/DC by label-table adapters, IA sums full+part time) |
| `total_vacancies` | Total open slots the provider reports (never summed from per-age bands) | Int, `0` kept (collapsed IA/ID/ND/TN/UT/VT/WA via `to_int`; WI parsed from prose by `wi_total_vacancies`, `None Reported.` -> empty) |
| `vacancies_updated_date` | Date the provider last updated its vacancy counts | `YYYY-MM-DD` (collapsed IA/ME/ND/VT via `to_iso_date`; WI date parsed from prose by `wi_vacancies_updated_date`, `None Reported.` -> empty) |
| `languages` | Languages | String/list |

---

## Canonical vocabularies

Normalization of categorical fields is **lossy** (the raw value is replaced).
To recover originals, re-run the spider with `NORMALIZE_ENABLED=False`.
`facility_category` is **additive**, so the raw `provider_type` is always kept.

### `status` → 5 buckets (+ `unknown`)

| Bucket | Meaning |
|---|---|
| `active` | Licensed / open / certified / operational |
| `provisional` | Provisional / initial / probationary-start permits |
| `pending` | Pending application / renewal / change |
| `enforcement` | Probation, suspension, pending revocation, refuse-to-renew |
| `closed` | Closed / inactive / revoked / surrendered |
| `unknown` | Unmapped (logged so the table can be extended) |

Full raw→bucket assignment: `STATUS_MAP` in `normalization.py`.

### `facility_category` (facet of `provider_type`)

`center`, `family_home`, `group_home`, `school_age`, `exempt`, `other`
(`other` is the logged fallback). Full raw→category assignment:
`FACILITY_CATEGORY_MAP` in `normalization.py`. Notable judgment calls:
center-based institutional care (public schools, Head Start, university, DoD)
→ `center`; care in a private residence (e.g. residential certificate) →
`family_home`; camps, placement agencies, and informal FFN/nanny care →
`other`.

---

## Concept → common field table

This is the heart of the playbook. If a new state's site exposes one of these
concepts, route it to the listed common field.

| Concept | Common field | Notes |
|---|---|---|
| Type / category of license | `license_type` | Watch overlap with `provider_type`. |
| School district | `school_district` | |
| Licensee / owner address | `license_holder_address` | Not `mailing_address`. |
| Facility mailing address | `mailing_address` | **Not** owner/licensee address — those go to `license_holder_address`. |
| Accreditation | `accreditation` | Negative answers ("NA", "No", "Not Accredited") are dropped; a bare bool with no accreditor names (KY) stays state-specific. |
| Meals / food program | `meals` | |
| Accepting new children | `accepting_new_children` | |
| Transportation | `transportation` | |
| Head Start | `head_start` | Normalized to a **boolean**. |
| Curriculum | `curriculum` | Keep VPK-specific curriculum (`fl_vpk_curriculum`) separate. |
| City / state / ZIP | `city` / `state` / `zip` | Parsed from `address` when unambiguous. |
| License status | `status` | Mapped to the 5-bucket vocab. |
| Any date | `status_date` / `license_begin_date` / `license_expiration` | ISO `YYYY-MM-DD`. Don't merge differing date *semantics* (issue vs effective vs first-licensed). |

To add a new source field to an existing common field, append it to that
field's list in `FIELD_COLLAPSE_MAP` (`normalization.py`). Collapse is
**additive** — the `xx_` source field is retained. A source that needs
conversion is listed as `(source, transform)` (e.g. `invert`, `to_int`; see
`provider_scrape/common_fields.py`). Cases a map entry can't express (one
source feeding several fields, or several sources feeding one) get a per-state
adapter registered in `common_fields.py`.

---

## The rule of thumb

> **If two or more states already have a field for a concept, it should be a
> common field — not a per-state `xx_` field.**

Lightweight process: check this doc → if a common field fits, use it → only
create an `xx_` field for genuinely state-unique data.

### The exception: quality ratings stay state-specific

Quality / QRIS ratings (`ut_quality_rating`, `pa_stars_rating`,
`nv_star_rating`, `ga_quality_rated_level`, `ri_brightstars_rating`,
`wa_early_achievers_status`, `md_excels_level`, Ohio's `oh_sutq_rating`, …) are
**deliberately not** collapsed into a common `quality_rating` field. Each
state's value set differs, and the web app enumerates each state's filter
choices independently. A new state's rating gets its **own** `xx_` field — even
though "two or more states have it" — *not* a common one.

---

## Cross-reference

This complements `claude.md`'s naming rule: new `xx_` fields use
`stateabbreviation_fieldname` (e.g. Ohio "cats" → `oh_cats`) and are for
**truly state-specific data only**. When in doubt, prefer a common field; reach
for `xx_` only after confirming nothing here fits.
