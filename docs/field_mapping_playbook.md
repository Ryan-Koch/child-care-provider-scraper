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
| `license_holder_address` | Licensee / owner's own address | Single-line string (MI, NC): `clean_address`, case kept; MI's literal `null` pieces dropped |
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
| `state_prek` | Publicly funded state pre-K participation | Bool (FL/RI/WA/DC/KY via `to_bool` in per-state adapters, False real, blank -> unset; IN/GA/SC/VT/NV exact whole-entry token match, absent -> unset, never False; GA `Georgia's Pre-K Transition` excluded) |
| `state_prek_program` | Pre-K program brand | String from `STATE_PREK_PROGRAMS` in `common_fields.py` (e.g. `VPK`, `ECEAP`), set only when `state_prek` is True |
| `nontraditional_hours` | Offers night, evening or weekend care (roll-up) | Bool; True if any specific flag is True or DC `dc_nontraditional_hours` True; False only from DC's explicit False, else unset |
| `night_care` | Overnight / third-shift care | Bool (LA bool; AL any time range True, `N/A - N/A` False; IL/WI/NC capacity > 0 via shared `capacity_to_bool`, `0`/`0 0` False; OK `Overnight Hours` True, absent unset). For IL/WI/NC, True = licensed night/shift capacity, not confirmed hours |
| `evening_care` | Evening / second-shift care | Bool (WY bool; NC second-shift capacity > 0; OK `Evening Hours` True, absent unset) |
| `weekend_care` | Weekend care | Bool (WY bool; OK `Weekend Hours` True, absent unset) |
| `care_schedules` | Care arrangements offered | List from a fixed 12-token list (`CARE_SCHEDULE_LABELS` in `common_fields.py`: explicit case-insensitive label table, one label may give several tokens, canonical order, de-duped; ignored labels silent, unknown logged and dropped; FL/MS/SD/RI/WI/ND/MA/WY/OK/NJ/IA/VT/MI/NH/GA/LA via per-state adapters, GA merges three sources, LA from two bools) |
| `ages_served` | Ages accepted | String (lists joined with `, `) |
| `county` | County | String |
| `scholarships_accepted` | Subsidy/scholarship acceptance | bool-or-None via `coerce_scholarships_accepted_flag`; FL/VA/WA collapsed from their subsidy flags (WA empty -> unset), NH from the `STATE CC ASSISTANCE (SCHOLARSHIP)` token as a fallback only; OK set by its spider |
| `license_begin_date` | License start | `YYYY-MM-DD`; NC collapsed from `nc_license_effective_date` (current license's effective date; a spider-set value wins) |
| `license_expiration` | License end | `YYYY-MM-DD` |
| `license_type` | Type/category of license | String (collapsed from `xx_license_type`) |
| `school_district` | Associated school district | String (collapsed) |
| `mailing_address` | Facility mailing address | String (collapsed) |
| `accreditation` | Accreditation(s) | String/list (collapsed) |
| `meals` | Meals / food program | String (collapsed) |
| `accepting_new_children` | Open enrollment flag | As emitted (collapsed) |
| `transportation` | Transportation offered | String (collapsed) |
| `head_start` | Head Start program | **Boolean** (collapsed + coerced) |
| `early_head_start` | Early Head Start program (separate from `head_start`) | **Boolean** (collapsed MS/NH/WA, coerced via `_coerce_bool`) |
| `wheelchair_accessible` | Wheelchair-accessible facility | **Boolean** (collapsed CT/TN via `_coerce_bool`; NH from `nh_environment` token, absent -> None not False) |
| `address_suppressed` / `phone_suppressed` | State deliberately withheld the address / phone (True = withheld) | **Boolean** (CT/KS/MA copy; FL `display_*_on_web` inverted; NY adapter: `Y` -> True, blank -> False; MA flag feeds both) |
| `curriculum` | Curriculum | String (collapsed) |
| `licensing_specialist` | Assigned state licensing specialist | `First Last` string (collapsed; SC `Last, First` flipped; VA split by adapter; `N/A` -> None) |
| `inspector` | Inspection row: staff member who ran the visit | `First Last` string (collapsed from AK, MA licensor/investigator, MO, MT, NH, RI; MO `Last, First M` flipped; ALL-CAPS Title-cased) |
| `inspection_id` | Inspection row: state's own key for the visit/report | Opaque string via `to_id_string` (collapsed from CT, FL, HI, KY, MA visit/investigation, MO, NH, NV, KS `ks_nosf_id` only, AZ); state-specific, unique per provider record, not guaranteed globally unique (CT can repeat across two provider records with the same license) |
| `announced` | Inspection row: was the visit announced | Bool: True announced, False unannounced, None unknown (collapsed from AK, MA, NH, MO via `announced_flag`; NC, CT, RI read the common `type` text by adapter; typo-tolerant, unannounced checked first; `type` unchanged; `N/A` or neither word -> unset) |
| `violation_found` | Inspection row: did the visit record any noncompliance | Bool: True violation, False none, None unknown (collapsed from FL, VA, MA, WY via `to_bool`, MO inverted via `invert`, MD `md_finding` text via `md_finding_flag`; AK and WI read the common `original_status` by adapter with explicit vocab tables; source fields unchanged; CT, DE, NV, KS, HI, SC derived as `violation_count > 0` by adapter, never overwriting an explicit flag; DE is always True because its count is >= 1 by construction, so it cannot show a clean visit) |
| `violation_count` | Inspection row: number of violations / deficiencies / findings on the visit | Int via `to_int`, `0` kept (collapsed from CT, DE, NV, KS, HI, SC; SC missing badge stays unset; MO `mo_open_violations` excluded: open-only count, user decision; DE always >= 1 by construction) |
| `compliance_domains` | Inspection row: per-domain checklist roll-up | List of `{domain, compliant, reviewed}` ints, counts only (MA, NH). NH adapter splits `nh_domains` `x / y` via the shared `parse_ratio` (bad rows dropped + logged); MA adapter rolls up `ma_domain_counts` (compliant = Compliant + Complete, reviewed = all known statuses, `Unknown` excluded); empty -> unset; sources unchanged |
| `regulation_cited` | Inspection row: rule/code section cited (one-citation-per-row states only) | State's own citation string, as emitted, via `none_if_sentinel` (trim; `N/A` -> unset) (collapsed from AZ, MD, IN, WI; AZ may hold several comma-separated citations or a label like `DES CONTRACT`, kept as emitted; formats are state-specific, not comparable across states; source fields unchanged) |
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
| Accreditation | `accreditation` | Normalize plural/singular naming. |
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
