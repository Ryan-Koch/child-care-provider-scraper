# Common-Field Candidates — Normalization Round 2 Inventory

Inventory of concepts that **two or more states** emit under state-specific
(`xx_`) fields and that are not yet common fields. It is input for choosing the
standardized shape of each field before anything is built. Nothing here has been
implemented.

## How this was built

- **Data:** the full Hugging Face dataset (`ryan-koch/US-Child-Care-Providers`,
  46 states, in `hf_sample/`). Every record in every state was profiled.
- **States not on HF yet** (MO, NH, OK, WY): a capped run of each spider
  (`CLOSESPIDER_ITEMCOUNT=60`, about 60 records per state), so their fill
  rates are indicative only.
- **Fill %** is the share of that state's records with a non-empty value
  (inspection fields: share of inspection rows). *code-only* means the spider
  declares the field but it was empty in every sampled record.
- **Out of scope:** quality/QRIS ratings stay state-specific per the playbook,
  so they are not listed.

### Confidence scale

| Level | Meaning |
|---|---|
| **High** | Same concept and same meaning in every listed state; formats map mechanically. |
| **Medium** | Same concept, but meaning, granularity or structure differs enough that some judgment or a vocabulary is needed. |
| **Low** | Related concept only; values aren't really comparable across states without a design decision. |

---

## Summary

| # | Proposed field | States | Confidence |
|---|---|---|---|
| P1 | `licensing_specialist` (+ phone/email) | AK, MA, ME, OK, SC, TN, VA, WV | High |
| P2 | `openings_infant` / `_toddler` / `_preschool` / `_school_age` | CO, ME, NH, VT (+ IA, ND, DC, RI structured) | High |
| P3 | `total_openings` | IA, ND, TN, UT, VT, WA, WI (+ ID code-only) | High |
| P4 | `openings_updated_date` | IA, ME, ND, VT (+ WI embedded) | High |
| P5 | `early_head_start` | MS, NH, WA | High |
| P6 | `wheelchair_accessible` | CT, TN (+ NH embedded) | High |
| P7 | `address_suppressed` | CT, KS, NY, FL (inverted), MA | High |
| P8 | `phone_extension` | NJ, NY | High |
| P9 | `doing_business_as` | FL, OK (+ WA, MA related) | High |
| P10 | `licensee_address` | MI, NC (+ MO possible) | High |
| P11 | `days_of_operation` | GA, IA, VT | High |
| P12 | `weekly_schedule` (structured hours) | CT, IN, NH, OK, WY, MA, FL (`hours` dict) | High |
| P13 | `months_of_operation` / `year_round` | GA, MS, WI, SD, ND, VT, OK | Medium |
| P14 | `serves_special_needs` (+ `special_needs_types`) | CO, CT, IA, MA, NE, NH, ND, NC | Medium |
| P15 | `food_program` (USDA CACFP) | HI, ID, WA, DE, VT, UT, PA, DC, WY | Medium |
| P16 | `state_prek` | FL, RI, WA, DC, KY, IN, GA, SC, VT, NV | Medium |
| P17 | `nontraditional_hours` (night/evening/weekend) | AL, IL, WI, LA, DC, WY, OK, NC | Medium |
| P18 | `care_options` (before/after school, drop-in, part-time, summer…) | FL, MS, SD, RI, WI, ND, IA, VT, OK, MI, GA, LA, MA, NH, NJ, WY | Medium |
| P19 | `rates` (structured tuition) | CT, GA, KY, TN, MA, PA, NH, NJ, WY | Medium |
| P20 | `registration_fee` | GA, WY (+ NJ codes) | Medium |
| P21 | `discounts` | GA, MA, NH, VT, NJ | Medium |
| P22 | `environment_features` | GA, MA, NH, UT, VT, NJ | Medium |
| P23 | `pets` | ID, VT (+ GA, UT, NH embedded) | Medium |
| P24 | `age_min` / `age_max` | CT, ND, WV, IL (+ WY programs) | Medium |
| P25 | `capacity_by_age` | MA, RI, CT, IN, NY | Medium |
| P26 | `license_conditions` | MN, NC, MO, DE | Medium |
| P27 | `original_license_date` | CA, CO, CT, NY | Medium |
| P28 | `last_renewed_date` | MN, RI, MO (anniversary) | Medium |
| P29 | `temporarily_closed` | ME, IN, NH, MA | Medium |
| P30 | `complaint_count` | IA, IN, AL, CA | Medium |
| P31 | `serious_injuries` / `fatalities` | MD, DE (+ LA code-only) | Medium |
| P32 | `license_history` | HI, NC, WA | Medium |
| P33 | `current_enrollment` | ND, DC | Medium |
| P34 | `source_provider_id` | ~20 states | Medium |
| P35 | `program_description` (free text) | ID, ND, VT | Medium |
| P36 | `licensed` / license-exempt flag | CT, LA, NH, SC | Medium |
| P37 | `building_type` (home vs non-residential) | IA, VT, MN | Medium |
| P38 | `profit_status` | DE, GA (code-only) | Medium |
| P39 | `enforcement_action` (current) | DE, AL, KY, OK | Low |
| P40 | `faith_based` | FL, VT, WI | Low |
| P41 | `potty_training_required` | NM, HI | Low |
| P42 | `region` | IA, NV, NY, CA, AZ, MA, HI | Low |
| P43 | `ccrr_agency` | MA, CO (code-only) | Low |
| P44 | `publicly_listed` | CT, DE, IA | Low |
| X1 | extend `scholarships_accepted` | FL, VA, WA, OK, NH | High |
| X2 | extend `license_begin_date` | NC | High |
| X3 | extend `accreditation` | ID, KY, NH | Medium |
| X4 | extend `mailing_address` | MO | Medium |
| X5 | extend `transportation` | NC | Medium |
| X6 | extend `curriculum` | ID, WI | Low |
| I1 | `inspector` | AK, MA, MO, MT, NH, RI | High |
| I2 | `inspection_id` | CT, FL, HI, KY, MA, MO, NH, NV, KS, AZ | High |
| I3 | `announced` | AK, MA, NH, MO, NC, CT, RI | High |
| I4 | `violation_found` | FL, VA, MA, MO, WY, AK, WI, MD | High |
| I5 | `violation_count` | CT, DE, NV, KS, HI, SC, MO | High |
| I6 | `regulation_cited` | AZ, MD, IN, WI | High |
| I7 | `compliance_domains` | MA, NH | High |
| I8 | `items_compliant` / `items_reviewed` | MA, RI, NH, OK, KS | Medium |
| I9 | `correction_date` | AZ, IN, WV (+ NH) | Medium |
| I10 | `licensing_period_start` / `_end` | HI, ME | Medium |
| I11 | `visit_reason` | MA, KS, OK | Medium |
| I12 | `finding_text` | MD, IN, WI, AZ, OK, DE | Medium |
| I13 | `regulation_description` | IN, WI, WV | Medium |
| I14 | `violations` (structured list) | CT, DE, MA, NH, MO, WY, NC, SC, OK | Medium |
| I15 | `enforcement_type` | WI, KS, AZ, MS | Low |

---

## Part 1 — Provider-level candidates (new common fields)

### P1. `licensing_specialist` — **High**

**Why it's the same:** Each field names the state staff member assigned to oversee the facility's license. ME, OK and SC also publish that person's contact details.

**Formats:** Name order and casing vary, and VA packs the phone into the name string. A companion `licensing_specialist_phone` / `_email` would take the ME/OK/SC extras.

| State | Source field(s) | Fill | Pattern |
|---|---|---|---|
| AK | `ak_licensing_specialist` | 75% | `Alyssa Pulice` |
| MA | `ma_licensor` | 100% | `Noelia Ramirez` |
| ME | `me_licensing_specialist`, `me_licensing_specialist_email` | 100% | `Karen Quinn` / `karen.e.quinn@maine.gov` |
| OK | `ok_licensing_specialist`, `ok_licensing_specialist_phone` | 98% | `Peyton Crawford` / `(405) 343-2186` |
| SC | `sc_licensing_specialist_name`, `sc_licensing_specialist_phone` | 97% | `Woodward, Rebecca` (Last, First) / `(803) 898-9001` |
| TN | `tn_regulatory_individual` | 100% | `Sharon Neely` |
| VA | `va_inspector` | 100% | `Angela Farmer: (276) 608-2283`; `N/A` sentinel (21%) |
| WV | `wv_licensing_specialist` | 100% | `TODD MCDANIEL` (ALL CAPS) |

### P2. `openings_infant` / `openings_toddler` / `openings_preschool` / `openings_school_age` — **High**

**Why it's the same:** Each is the provider's self-reported count of open slots for one of the four standard age bands. Several other states publish the same counts inside an age-band list.

**Formats:** CO and NH emit numeric strings (NH mixes str/int); ME and VT emit ints. Age-band boundaries differ by state, and IA/ND/DC/RI nest the counts in lists keyed by their own band labels.

| State | Source field(s) | Fill | Pattern |
|---|---|---|---|
| CO | `co_{infant,toddler,preschool,school_age}_openings` | 76% | `"0"`, `"2"` (string) |
| ME | `me_{infant,toddler,preschool,school_age}_slots` | 100% | `0`, `1` (int) |
| NH | `nh_{infant,toddler,preschool,school_age}_openings` | 75–85% | `"0"` or `2` (mixed) |
| VT | `vt_{infant,toddler,preschool,school_age}_vacancies` | 84% | `0`, `1` (int) |
| IA | `ia_openings_by_age` | 96% | `[{"ageGroup": "Infant (0-12 mo.)", "fullTime": 0, "partTime": 0}, …]` |
| ND | `nd_vacancies_by_age` | 18% | `[{"age_group": "Infant (0-17 months)", "vacancies": 1}, …]` |
| DC | `dc_enrollment` | 100% | `[{"age_group": "Infant (0 - 12 Months)", "openings": "0", "current_enrollment": …}]` |
| RI | `ri_availability` | 100% | `[{"age_group": "Infant (6 weeks up to 18 months)", "slot_info": null}]` (`slot_info` mostly null) |

### P3. `total_openings` — **High**

**Why it's the same:** Each is the total number of open slots the provider reports. Several states already derive `accepting_new_children` from it.

**Formats:** Mostly ints, but TN and WA use strings and WI buries the number in prose. A canonical int needs a regex for WI and a decision on how to treat `None Reported.`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_total_openings` | 94% | `0`, `2` (int) |
| ND | `nd_total_vacancies` | 67% | int |
| TN | `tn_vacancy` | 39% | `"5"` (string) |
| UT | `ut_vacancies` | 100% | int |
| VT | `vt_current_vacancy` | 86% | int |
| WA | `wa_available_slots` | 39% | `"2"` (string) |
| WI | `wi_vacancies` | 100% | `None Reported.` (86%) / `0 vacancies have been reported. Last updated on 9/1/2026Vacancies Age Range …` |
| ID | `id_number_of_openings`, `id_openings_available` | code-only | — |

### P4. `openings_updated_date` — **High**

**Why it's the same:** Each is the "as of" date on the provider's self-reported openings. Without it, the openings counts in P2/P3 can't be judged for staleness.

**Formats:** IA and VT use `MM/DD/YYYY`; ME and ND are already ISO. WI embeds the date in the `wi_vacancies` / `wi_waitlist` prose.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_openings_as_of` | 94% | `09/15/2026` |
| ME | `me_openings_updated` | 95% | `2025-05-29` |
| ND | `nd_vacancies_updated` | 68% | `2025-05-29` |
| VT | `vt_vacancy_as_of` | 85% | `09/08/2026` |
| WI | inside `wi_vacancies` | — | `…Last updated on 9/1/2026…` |

### P5. `early_head_start` — **High**

**Why it's the same:** Each flags participation in the federal Early Head Start program, which is distinct from the existing `head_start` field. All three are yes/no flags.

**Formats:** MS is a bool; NH and WA are `Yes`/`No` strings. It maps to a bool using the existing `_coerce_bool`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MS | `ms_early_head_start` | 100% | `true`/`false` |
| NH | `nh_early_head_start` | 13% | `Yes`/`No` |
| WA | `wa_early_head_start` | 100% | `Yes`/`No` |

### P6. `wheelchair_accessible` — **High**

**Why it's the same:** Both are a direct facility-accessibility flag. NH lists the same attribute as one token in its environment list.

**Formats:** CT is a bool; TN is `Yes`/`No`. NH would need a token check on `nh_environment`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_wheelchair_accessible` | 100% | `true`/`false` |
| TN | `tn_wheelchair_accessible` | 58% | `Yes`/`No` |
| NH | inside `nh_environment` | — | `…, WHEELCHAIR ACCESSIBLE, …` |

### P7. `address_suppressed` — **High**

**Why it's the same:** Each says the state deliberately withheld the location (typically for family homes). That explains blank addresses and missing geocodes better than treating them as data gaps.

**Formats:** CT/KS/MA are bools, NY is `Y`-or-absent, and FL is the inverse (`display_address_on_web`). NY and FL also have phone-suppression flags (`ny_phone_number_omitted`, `fl_display_phone_on_web`), which could become a sibling `phone_suppressed`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_address_suppressed` | 100% | bool (15 true) |
| KS | `ks_address_suppressed` | 100% | bool (42% true) |
| NY | `ny_address_omitted` | 4% | `Y` only |
| FL | `fl_display_address_on_web` | 100% | bool, **inverted** (false = suppressed) |
| MA | `ma_contact_redacted` | 100% | bool (all false in sample) |

### P8. `phone_extension` — **High**

**Why it's the same:** Both carry the extension that goes with `phone`.

**Formats:** Both are strings. NJ has junk values such as `NJ`, which need filtering to digits.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| NJ | `nj_phone_extension` | 5% | `321`; junk `NJ` |
| NY | `ny_phone_extension` | 5% | `120` |

### P9. `doing_business_as` — **High**

**Why it's the same:** FL and OK both publish an explicit "doing business as" name alongside the licensed name. WA's license name and MA's umbrella organization are related, but they are different concepts (legal name and parent org).

**Formats:** Free-text strings in both core states.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| FL | `fl_dba` | 23% | `The Learning Experience` |
| OK | `ok_doing_business_as` | 38% | `Lil Bison Learning and Development` |
| WA *(related)* | `wa_license_name` | 100% | legal license name, e.g. `Rodriguez Maria` |
| MA *(related)* | `ma_umbrella_name` | 31% | parent org, e.g. `KinderCare Education, LLC` |

### P10. `licensee_address` — **High**

**Why it's the same:** Both are the license holder's own mailing address, which the playbook keeps out of `mailing_address`. NC also publishes the owner's phone, email and fax, which could become siblings.

**Formats:** MI is comma-joined with a literal `null, null, null,` sentinel; NC is ALL CAPS with no comma before the city.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MI | `mi_licensee_address` | 100% | `5005 Meadows Road, Lake Oswego, OR, 97035`; sentinel `null, null, null,` |
| NC | `nc_owner_mailing_address` (+ `nc_owner_phone/email/fax`) | 100% | `1901 HERBERT SPAUGH LANE CHARLOTTE, NC 28208` |
| MO *(possible)* | `mo_mailing_address` | 48% | 23 of 30 values are one Overland Park, KS corporate address, so it is likely licensee rather than facility (see X4) |

### P11. `days_of_operation` — **High**

**Why it's the same:** Each lists the weekdays the provider is open. It can also be derived for states that have a structured schedule (P12).

**Formats:** GA uses 2-letter caps codes without spaces, IA uses 3-letter abbreviations and VT spells the days out. A canonical list (e.g. `["Mon", …]`) maps mechanically from each.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| GA | `ga_operating_days` | 98% | `MO,TU,WE,TH,FR` |
| IA | `ia_days_of_operation` | 95% | `Mon, Tue, Wed, Thu, Fri` |
| VT | `vt_days_of_operation` | 79% | `Monday, Tuesday, Wednesday, Thursday, Friday` |

### P12. `weekly_schedule` (structured per-day hours) — **High**

**Why it's the same:** Each is a per-day open/close structure that the state also flattens into the free-text `hours` field. A structured common field would allow cross-state "open at 6 AM" or "open Saturday" filters.

**Formats:** Key names (`open`/`close` vs `start`/`end` vs a single `hours` string) and time formats (24h vs `6:00 AM` vs `7:00AM`) differ. NH allows several sessions per day, and FL's common `hours` is already a dict.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_schedule` | 81% | `[{"day": "Monday", "open": "07:00", "close": "18:00"}]` |
| IN | `in_schedule` | 100% | `[{"day": "Monday", "open": "6:00 AM", "close": "6:00 PM"}]` |
| NH | `nh_schedule` | 85% | `[{"day": "Monday", "start": "07:00 AM", "end": "08:30 AM"}, {"day": "Monday", "start": "03:00 PM", …}]` |
| OK | `ok_schedule` | 100% | `[{"day": "Monday", "hours": "7:00AM - 5:30PM"}]` |
| WY | `wy_schedule` | 100% | `[{"day": "Monday", "start": "07:00", "end": "17:30"}]` |
| MA | `ma_schedules` | 85% | `[{"schedule_type": "Full Year Schedule", "drop_in": …, "days": [{"day": "Monday", …}]}]` |
| FL | `hours` (common field) | 100% | `{"monday": "6:30AM to 6:00PM", …}` |

### P13. `months_of_operation` / `year_round` — **Medium**

**Why it's the same:** Each says which part of the year the provider operates. GA, MS and WI give explicit months; the other states give only a category.

**Formats:** GA uses caps CSV, MS a list, and WI a range (`Jan - Dec`). SD, ND, VT and OK use categorical labels (`12 Months`, `School Year Only`, `Summer Only`), so a derived `year_round` bool plus a schedule category may be the realistic common shape.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| GA | `ga_operating_months` | 98% | `JAN,FEB,MAR,…,DEC` |
| MS | `ms_months_of_operation` | 100% | `["Jan", "Feb", …]` |
| WI | `wi_months_open` | 99% | `Jan - Dec`, `Sep - Jun` |
| SD | `sd_months_of_operation` | 84% | `["12 Months"]`, `["School Year Only"]`, `["Summer Only"]` |
| ND | `nd_enrollment_schedule` | 69% | `Full calendar year`, `School year only (typically September-May)` |
| VT | `vt_special_schedule` | 24% | `School Year Only`, `Summer Only`, `In-Service Days` |
| OK | `ok_care_types` | 100% | list token `Year Round` / `School Year Only` |

### P14. `serves_special_needs` (+ `special_needs_types`) — **Medium**

**Why it's the same:** Each says whether the provider accommodates children with special needs. Some states also list which needs.

**Formats:** IA, NE and NC are flags (Yes/No, bool, a phrase in a list); CO, CT, MA, NH and ND are condition lists in several delimiter and casing styles. CO and MA mix sentinels (`Willing to Support`, `None`, `Experience`) into the list.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_serves_special_needs` | 96% | `Yes`/`No` |
| NE | `ne_serves_special_needs` | 36% | bool |
| NC | inside `nc_special_features` | 63% | list item `Will enroll children with special needs` |
| CO | `co_special_needs` | 63% | `Social Emotional Behaviors;Respiratory Illness;…`, `Willing to Support`, `None` |
| CT | `ct_special_needs` | 34% | `["Autism Spectrum Disorders", …]` |
| MA | `ma_special_needs` | 14% | `Experience;Behavioral;Developmental;…` |
| NH | `nh_special_needs` | 57% | `EXPERIENCE, BEHAVIORAL, DEVELOPMENTAL, …` (caps, comma) |
| ND | `nd_special_populations` | 17% | `["ADHD/ADD", "Developmental Delay", …]` |

### P15. `food_program` (USDA CACFP participation) — **Medium**

**Why it's the same:** Each indicates participation in the USDA Child and Adult Care Food Program. It's medium because DE, DC, UT and PA already put the same fact inside `meals`, so you need to choose between a dedicated bool and leaving it in `meals`.

**Formats:** HI is a bool, ID is `Yes`/`No` and WA is `Yes`-only. The other states put it in mixed lists as a phrase such as `Child Care Food Program` or `On USDA Food Program`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| HI | `hi_usda_food_program` | 100% | bool |
| ID | `id_usda_food_program` | 100% | `Yes`/`No` |
| WA | `wa_food_program` | 14% | `Yes` only |
| DE | inside `de_financial_arrangements` (and `meals`) | 91% | `Child Care Food Program;Purchase of Care` |
| VT | inside `vt_program_participation` | 25% | `Food Program, Pre-K Program` |
| UT / PA / DC | inside `meals` | — | `On USDA Food Program,…` / `…Child and Adult Care Food Program (CACFP)…` |
| WY | inside `wy_services` | — | `Child and Adult Food Program` token |

### P16. `state_prek` — **Medium**

**Why it's the same:** Each flags participation in a publicly funded pre-K program. The programs carry state brands (VPK, ECEAP, On My Way Pre-K, First Steps 4K), so a bool plus the retained program name is the likely shape.

**Formats:** FL, DC and KY are bools; RI and WA are `Yes`/`No`. IN, GA, SC, VT and NV only mention the program inside list or text fields.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| FL | `fl_is_vpk` | 100% | bool |
| RI | `ri_state_prek` | 46% | `Yes`/`No` |
| WA | `wa_eceap` | 100% | `Yes`/`No` |
| DC | `dc_prek_enhancement` | 100% | bool |
| KY | `ky_prek_partnership` | 100% | bool (2 true) |
| IN | inside `in_programs` | 90% | `["CCDF Provider", "On My Way Pre-K"]` |
| GA | inside `ga_other_care_type` | 82% | `Georgia's Pre-K` |
| SC | inside `sc_program_participation` | 18% | `["First Steps 4K"]` |
| VT | inside `vt_program_participation` | 25% | `Pre-K Program` |
| NV | inside `nv_site_characteristic` | 48% | `Nevada Ready Pre-K` |

### P17. `nontraditional_hours` (night / evening / weekend care) — **Medium**

**Why it's the same:** Each says whether the provider offers care outside standard daytime hours. Some states split this into night, evening and weekend, so you need to choose between one bool and three.

**Formats:** LA, DC and WY are bools. AL gives separate night hours and ages with `N/A` sentinels; IL, WI and NC give a night/shift capacity where `0` means no.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| LA | `la_night_care` | 100% | bool |
| DC | `dc_nontraditional_hours` | 100% | bool |
| WY | `wy_evening_care`, `wy_weekend_care` | 100% | bool |
| AL | `al_nighttime_hours`, `al_nighttime_ages` | 100% | `07:00 PM - 11:00 PM`; `N/A - N/A` sentinel (95%) |
| IL | `il_night_capacity`, `il_night_age_range` | 100% | `"8"`; `"0"` = none |
| WI | `wi_night_capacity` | 86% | `"8"`, `"0 0"` (junk) |
| NC | `nc_capacity_second_shift`, `nc_capacity_third_shift` | 100% | int; `0` = none |
| OK | inside `ok_care_types` | 100% | list token `Evening Hours` |

### P18. `care_options` (before/after school, drop-in, part-time, full-day, summer…) — **Medium**

**Why it's the same:** Each is a list of the care arrangements the provider offers, and the same vocabulary shows up everywhere (before school, after school, drop-in, part-time, full-day, summer). A canonical vocabulary list, or a handful of bools, would need to be defined.

**Formats:** Some states use lists and others delimited strings, in different casing (NH ALL CAPS, WI full sentences such as `Before School Care provided.`). NJ uses unresolved integer codes, and LA already has three separate bools.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| FL | `fl_services` | 96% | `["After School", "Before School", "Drop In", "Full Day", "Half Day", …]` |
| MS | `ms_services` | 98% | `["School Age After School", "Full Day"]` |
| SD | `sd_services_offered` | 84% | `["Drop-in", "After School"]` |
| RI | `ri_services_offered` | 31% | `["Full day care", "Half-day care (…)", "Partial week (…)"]` |
| WI | `wi_special_care_types` | 61% | `["Before School Care provided.", "Drop In Care provided."]` |
| ND | `nd_supplemental_care` | 49% | `["Before School", "After School", "Drop-in/Hourly", "Temporary/Emergency"]` |
| IA | `ia_care_types` | 95% | `After School, Before School, Full-time, Part-time, Daytime` |
| VT | `vt_type_of_care` | 75% | `Daytime, Full-Time, Part-Time` |
| OK | `ok_care_types` | 100% | `["Year Round", "Daytime Hours"]` |
| MI | `mi_services_provided` | 66% | `Pre-School;School Age;Infant/Toddler;Food Service;Before/After School` |
| GA | `ga_special_hours`, `ga_accepts_children_type`, `ga_summer_camp` | 34–84% | `Drop in; Open school holidays`; `Full Time; Part Time`; `Before Camp Care; Summer Camp` |
| LA | `la_before_care`, `la_after_care` | 100% | bool each |
| MA | `ma_schedule_options` | 27% | `["Full day", "Open school vacation week", "Full week"]` |
| NH | `nh_schedule_options` | 84% | `PART WEEK;PART DAY;FULL DAY;OPEN SCHOOL VACATION WEEK` |
| NJ | `nj_special_schedules` | 22% | `[6]`, `[4, 6]` (integer codes) |
| WY | `wy_services` | 98% | `["Full Day Care", "Half Day Care", "Drop-In Care"]` |

### P19. `rates` (structured tuition) — **Medium**

**Why it's the same:** Each is the provider's published price by age group and/or care unit. A common list shape such as `[{age_group, unit (full/part time), period (hour/day/week/month), amount}]` would fit all of them.

**Formats:** Amounts appear as `"$150.00"` strings, bare strings and floats, and the period is sometimes implied (KY looks daily, GA weekly). NJ flattens into 16 `nj_tuition_{age}_{period}` float columns, and NH flattens the table into one text string.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_rates` | 45% | `[{"age_group": "Infant", "label": "0-17 Months", "full_time_weekly": "250.00"}]` |
| GA | `ga_weekly_rates` | 26% | `[{"age": "Under 1 year", "weekly_full_day": "$150.00"}]` |
| KY | `ky_service_costs` | 90% | `[{"age_group": "Children under the age of 1", "full_time_cost": 47.0, "part_time_cost": 43.0}]` |
| TN | `tn_age_group_rates` | 71% | `[{"age_group": "3 Years", "weekly_rate": "$144.00", "unit_of_care": "Full-time", …}]` |
| MA | `ma_cost_table` | 83% | `[{"schedule_type": "Full Year Schedule", "age_group": "Infant Age Group", "rate_type": "Daily Full Time", …}]` |
| PA | `pa_cost_table` | 92% | `[{"age_group": "Newborn (Under 6 Weeks)", "full_time_rate": "$67.83", "part_time_rate": "$55.72", …}]` |
| NH | `nh_rates` | 80% | flattened text `Age Group Hourly Daily Weekly Monthly School Age $97.00`; `Please contact…` |
| NJ | `nj_tuition_{infant,toddler,preschool,school_age}_{hourly,daily,weekly,monthly}` | 33% | float; `0.0` = blank |
| WY | `wy_programs[].rates` | 57% | `[{"rate", "period", "type"}]` |

### P20. `registration_fee` — **Medium**

**Why it's the same:** Both are a one-time enrollment/registration fee amount.

**Formats:** GA uses `"$100.00"` strings (with `$0.00` for none); WY emits a float only when a fee is charged. NJ's `nj_fees` is a list of integer codes, likely fee types, and needs its lookup resolved before it can join.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| GA | `ga_registration_fee` | 72% | `$100.00`, `$0.00` |
| WY | `wy_registration_fee` | 25% | `50.0` (float) |
| NJ *(maybe)* | `nj_fees` | 37% | `[1, 2]` (codes) |

### P21. `discounts` — **Medium**

**Why it's the same:** Each lists price reductions the provider offers, such as sibling/multi-child discounts and sliding scales. In GA, MA and NH the same field also mixes in subsidy acceptance, which belongs in `scholarships_accepted`.

**Formats:** GA uses `;`-delimited text, MA a list, NH a caps CSV and VT a single `Yes`/`No` for sibling discount. NJ uses codes.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| GA | `ga_financial_info` | 43% | `Multi-Child Discount; Scholarship; Sliding Scale Fee` |
| MA | `ma_financial_assistance` | 22% | `["EEC Subsidies", "Sibling Discounts", "Employee Discounts"]` |
| NH | `nh_financial_assistance` | 90% | `MULTI-CHILD DISCOUNT, SLIDING FEE SCALE, STATE CC ASSISTANCE (SCHOLARSHIP)` |
| VT | `vt_sibling_discount` | 79% | `Yes`/`No` |
| NJ | `nj_discounts` | 28% | `[1, 2]` (codes) |

### P22. `environment_features` — **Medium**

**Why it's the same:** Each is a checklist of facility amenities from the same family of options (smoke-free, fenced yard, air-conditioned, outdoor play, pets). MA and NH appear to share a vendor vocabulary.

**Formats:** GA uses `;` text, MA a list, NH a caps CSV, UT a comma string without spaces and VT a comma string. NJ uses codes; pets and wheelchair access could be split out (P23, P6).

| State | Source field | Fill | Pattern |
|---|---|---|---|
| GA | `ga_environment` | 79% | `No pets; Outdoor Play area; Secure Grounds/Keypad Access; Smoke Free Environment` |
| MA | `ma_environment` | 27% | `["Air Conditioned", "Fenced Yard", "Smoke Free"]` |
| NH | `nh_environment` | 84% | `AIR CONDITIONED, PEANUT FREE, WHEELCHAIR ACCESSIBLE, NO PETS` |
| UT | `ut_environment` | 72% | `Smoke-free Environment,No Pets` |
| VT | `vt_area_description` | 76% | `Fenced Yard, Smoke Free` |
| NJ | `nj_environmental_features` | 34% | `[2, 5, 10]` (codes) |

### P23. `pets` — **Medium**

**Why it's the same:** Each describes the presence of animals at the facility, which matters to families with allergies. ID and VT are dedicated fields; three more states encode it in environment lists.

**Formats:** ID is a policy category and VT lists animal types. GA, UT and NH carry only a `No pets` / `Pets` token.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ID | `id_pet_policy` | 95% | `Pet Free`, `Dogs only`, `Cats only`, `Other` |
| VT | `vt_pets` | 31% | `Dog, Cat`, `Other Non-Furry Animal` |
| GA / UT / NH | inside `*_environment` | — | `No pets` / `Pets` / `NO PETS, NO DOG, NO CAT` |

### P24. `age_min` / `age_max` (structured) — **Medium**

**Why it's the same:** Each gives explicit youngest and oldest accepted ages, which `ages_served` only has as free text. A numeric pair (e.g. in months) would allow cross-state age filtering.

**Formats:** CT uses integer weeks, ND `"0 Months"`/`"12 Years"`, WV `"2 Years 0 Months"`, IL `6W TO 12Y` and WY integer months. Every one converts to months mechanically.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_age_range_min_weeks`, `ct_age_range_max_weeks` | 100% / 91% | `6`, `624` (weeks) |
| ND | `nd_min_age`, `nd_max_age` | 69% | `0 Months`, `12 Years` |
| WV | `wv_age_from`, `wv_age_to` | 49% | `2 Years 0 Months`, `13 Years 0 Months` |
| IL | `il_day_age_range` | 100% | `6W TO 12Y`, `0 TO 12Y` |
| WY | `wy_programs[].min_age_months / max_age_months` | 57% | int months |

### P25. `capacity_by_age` — **Medium**

**Why it's the same:** Each breaks licensed capacity down by age group. The age groups and structures are all different, though, and NY and VT already put per-age counts into the common `infant`/`toddler`/… flags (see D1).

**Formats:** MA uses a camelCase dict, RI a list with classroom counts, IN a list of age-span quantities and CT three fixed bucket fields. NY has only prose.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MA | `ma_capacity_by_age` | 32% | `{"schoolAge": 52}` |
| RI | `ri_age_group_capacity` | 100% | `[{"age_group": "Infant (…)", "classrooms": 1, "capacity": 8}]` |
| CT | `ct_capacity_three_and_under`, `ct_capacity_school_aged`, `ct_capacity_full_time` | 35–69% | int |
| IN | `in_licensed_ages` | 65% | `[{"start_age": "Infant", "end_age": "Twelve", "quantity": 12}]` |
| NY | `ny_capacity_description` | 100% | `12 children, ages 6 weeks to 12 years AND 4 additional school-aged children` |

### P26. `license_conditions` — **Medium**

**Why it's the same:** Each lists restrictions, limitations or special conditions attached to the license (e.g. "no transporting children under nine", "variance"). Analytically it's one concept: constraints on how the provider may operate.

**Formats:** MN is a string with a `None` sentinel (77%), NC a list prefixed `Other - `, MO ALL CAPS `;`-joined text and DE a short category label.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MN | `mn_restrictions` | 82% | `No transporting of children under age nine.`; `None` |
| NC | `nc_license_restrictions` | 44% | `["Other - Meets reduced ratios"]` |
| MO | `mo_limitations` | 34% | `UP TO 10 CHILDREN IN CARE NO MORE THAN 2 CHILDREN UNDER AGE 2…` |
| DE | `de_special_conditions` | 9% | `Variance`, `Agreement of Understanding`, `High Nitrate Water` |

### P27. `original_license_date` — **Medium**

**Why it's the same:** Each appears to be when the facility was first licensed or established, as opposed to the current license term. The playbook warns against merging date semantics, and NY's "facility opened" values cluster on recent dates, so semantics need checking state by state.

**Formats:** All are US-style `M/D/YYYY` or `MM/DD/YYYY` strings, so ISO conversion is mechanical.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CA | `ca_license_first_date` | 95% | `11/18/1997` |
| CO | `co_license_issue_date` | 100% | `1/1/1995` (CO has no `license_begin_date`) |
| CT | `ct_date_established` | 45% | `04/04/2003` |
| NY | `ny_facility_opened_date` | 100% | `08/21/2026` (distinct from `license_begin_date` = "License Issue Date") |

### P28. `last_renewed_date` — **Medium**

**Why it's the same:** Each records the latest renewal of an ongoing license. In MN this differs from `license_begin_date`, which there holds the original date (`2014-07-01` ×2,519).

**Formats:** MN and RI use `MM/DD/YYYY`. MO gives only a month/day anniversary with no year, so it is related rather than equivalent.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MN | `mn_last_renewed_date` | 99% | `01/01/2026` |
| RI | `ri_most_recently_renewed` | 100% | `11/01/2025` |
| MO *(related)* | `mo_license_anniversary_date` | 89% | `05/01` (no year) |

### P29. `temporarily_closed` — **Medium**

**Why it's the same:** Each flags a licensed provider that is temporarily not operating while its license remains active. NH's and MA's fields are COVID-era artifacts, so their usefulness is mostly historical.

**Formats:** ME is `Yes`/`No`, IN a bool plus a message, NH a bool and MA a status string (`Reopened`/`Provisionally Reopened`).

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ME | `me_temporarily_closed` | 100% | `Yes`/`No` (17 yes) |
| IN | `in_is_temporarily_closed`, `in_temporarily_closed_message` | code-only | bool + text |
| NH | `nh_covid_closure` | 100% | bool (all false) |
| MA | `ma_temporary_status` | 55% | `Reopened`, `Provisionally Reopened` |

### P30. `complaint_count` — **Medium**

**Why it's the same:** Each records complaints against the provider; a count can be derived for all four. AL lists only substantiated complaints while IA/IN count all, so the semantics differ.

**Formats:** IA is an int, IN a list of dated complaints and AL a list of concatenated text. CA gives a `No Complaints` string or a last-complaint date, not a count.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_complaint_count` | 100% | int |
| IN | `in_complaints` | 14% | `[{"complaint_date": "7/20/2026", "issue": …, "closed_date": "9/23/2026"}]` |
| AL | `al_substantiated_complaints` | 17% | `["ComplaintCHARACTER AND SUITABILITY06-22-2026"]` (substantiated only) |
| CA | `ca_complaint_info` | 100% | `No Complaints` (77%) or a date |

### P31. `serious_injuries` / `fatalities` — **Medium**

**Why it's the same:** Both states publish counts of serious injuries and deaths at the facility. DE puts both in one sentence per year, so it needs parsing.

**Formats:** MD uses separate numeric strings; DE uses prose such as `2026: 1 facility injury. No facility deaths reported.`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MD | `md_serious_injuries`, `md_fatalities` | 62% | `"0"`, `"1"` |
| DE | `de_injuries_report` | 100% | `No facility injuries reported. No facility deaths reported.` |
| LA | `la_serious_injuries_url` | code-only | URL |

### P32. `license_history` — **Medium**

**Why it's the same:** Each is a chronological list of prior licenses or terms for the facility. The list schemas differ, and HI also has a separate status history.

**Formats:** HI uses camelCase ISO dates, NC snake_case `M/D/YYYY` with a license type and age range, and WA a license-id/regulation-type list.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| HI | `hi_license_history` (+ `hi_status_history`) | 100% | `[{"effectiveDate": "2000-12-01", "expirationDate": "2002-11-30", …}]` |
| NC | `nc_license_history` | 96% | `[{"license_type": "Temporary License", "effective_date": "5/1/2024", "age_range": "0 through 12", …}]` |
| WA | `wa_license_history` | 100% | `[{"license_id": "PL-65132", "regulation_type": "DCYF Licensed", …}]` |

### P33. `current_enrollment` — **Medium**

**Why it's the same:** Both report how many children are currently enrolled, as opposed to capacity.

**Formats:** ND is a total int; DC is per-age inside a list and is mostly `No Data Available`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ND | `nd_total_enrollment` | 100% | int |
| DC | `dc_enrollment[].current_enrollment` | 100% | `No Data Available` or number string |

### P34. `source_provider_id` — **Medium**

**Why it's the same:** Each is the state system's own record key for the facility, separate from `license_number`. It's needed to join re-scrapes and link back to the source; it's medium only because some states have two candidate IDs and one must be chosen.

**Formats:** Plain ints, zero-padded numeric strings (IL, LA), Salesforce IDs (AZ, MA, NH), hex/ObjectIDs (ND), prefixed IDs (MA `P-261705`) and UUIDs (CT). Keep it as an opaque string.

| State | Source field (recommended) | Alternate | Pattern |
|---|---|---|---|
| AK | `ak_facility_number` | `ak_facility_gen_id` | `1000652` |
| AZ | `az_facility_id` | — | `0bScs000000PmHeEAK` |
| CT | `ct_provider_id` | `ct_provider_uid` (UUID; 26 dupes) | `61` (int) |
| HI | `hi_service_id` | `hi_provider_id` (not unique) | int |
| IL | `il_provider_id` | — | `003251` |
| IN | `in_provider_id` | `in_location_id` | `682844` |
| KY | `ky_provider_id` | — | int |
| LA | `la_unique_id` | — | `001003` |
| MA | `ma_program_number` | `ma_account_id` | `P-261705` |
| MS | `ms_facility_id` | — | `20014077` |
| NE | `ne_resource_id` | — | int (81%) |
| NH | `nh_account_id` | — | Salesforce ID |
| NJ | `nj_unique_program_id` | `nj_doh_id` | `711671` |
| NV | `nv_operation_id` | `nv_license_base` | `626` (80%) |
| NY | `ny_facility_id` | — | `41021` |
| ND | `nd_program_id` | `nd_org_id` | `68483dadcd50c1c655e13b1b` |
| SC | `sc_provider_id` | — | `2465` |
| TN | `tn_provider_id` | — | `116896` |
| TX | `tx_operation_id` | — | `1404306` |
| VA | `va_ID` | — | `35291` |
| VT | `vt_provider_id` | — | not unique (13 dupes) |
| WA | `wa_provider_id` | — | `961182` |
| WI | `wi_facility_number` | `wi_location_number` | `2006942` (88%) |
| WY | `wy_facility_id` | — | `goa2ejdubka4h4o` |

### P35. `program_description` — **Medium**

**Why it's the same:** Each is a free-text description or philosophy statement written by the provider. Low analytic value, but it's the same concept and useful for display.

**Formats:** Free text in all three, with filler values (`Please ask provider.`, `x`). ID splits it into description and philosophy fields.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ID | `id_program_description`, `id_philosophy_description`, `id_philosophy_comment` | 37–62% | prose |
| ND | `nd_philosophy` | 44% | prose |
| VT | `vt_program_description` | 49% | prose; `Please ask provider.`, `x` |

### P36. `licensed` (licensed vs license-exempt) — **Medium**

**Why it's the same:** Each distinguishes licensed providers from exempt or approved-only ones. It overlaps the `exempt` bucket of `facility_category`, so check whether it adds anything beyond that.

**Formats:** CT is a bool, LA uses `Licensed`/`License Exempt`, NH is `Yes`-only and SC uses `License`/`Approval`/`Exempt`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_licensed` | 100% | bool |
| LA | `la_licensed` | 67% | `Licensed`, `License Exempt` |
| NH | `nh_licensed` | 97% | `Yes` |
| SC | `sc_license_category` | 71% | `License`, `Approval`, `Exempt` |

### P37. `building_type` — **Medium**

**Why it's the same:** Each says whether care happens in a residence or in a non-residential building. It overlaps `facility_category` (family_home vs center) but describes the physical setting rather than the license class.

**Formats:** IA uses lowercase `house`/`building`, VT has five categories including `Workplace-Based` and MN is code-only.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_business_type` | 96% | `house`, `building` |
| VT | `vt_building_type` | 72% | `Non-Residential`, `House`, `Workplace-Based` |
| MN | `mn_setting` | code-only | — |

### P38. `profit_status` — **Medium**

**Why it's the same:** Both carry the operator's for-profit / nonprofit / public classification. It's medium only because GA was empty in every sampled record.

**Formats:** DE uses `Nonprofit`, `Private`, `Profit` and `Publicly Operated`. GA reads `profitStatus` from its API, so the format can't be confirmed.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| DE | `de_profit_status` | 18% | `Nonprofit`, `Private`, `Profit`, `Publicly Operated` |
| GA | `ga_profit_status` | code-only | — |

### P39. `enforcement_action` (current) — **Low**

**Why it's the same:** Each signals an active adverse or enforcement process against the license. The states model it very differently (status label, dated history, process list, notice flags), and it overlaps the `status` → `enforcement` bucket.

**Formats:** DE uses a label, AL a list of concatenated text with dates, KY a list of `{process_type, status}` and OK three bools (all false in sample).

| State | Source field | Fill | Pattern |
|---|---|---|---|
| DE | `de_enforcement_action`, `de_intent_to_revoke` | 4% / 1% | `Suspended`, `Warning of Probation`; `Intent to Place on Probation Extension` |
| AL | `al_adverse_actions` | 7% | `["Adverse Action CORRECTIVE ACTION 09-30-2025 12-15-2025"]` |
| KY | `ky_ongoing_process`, `ky_ongoing_processes` | 100% / 2% | bool; `[{"process_type": "Directed Plan of Correction", "status": "On-going"}]` |
| OK | `ok_revocation_sent`, `ok_denial_sent`, `ok_emergency_issued` | 100% | bool |

### P40. `faith_based` — **Low**

**Why it's the same:** All three relate to religious affiliation or programming. "Faith-based operator" (FL) is not the same thing as "offers religious activity" (VT) or "religious philosophy" (WI).

**Formats:** FL uses bools (plus `fl_is_religious_exempt`, a licensing exemption), VT is `Yes`/`No` and WI has one category value inside a philosophy list.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| FL | `fl_is_faith_based` (+ `fl_is_religious_exempt`) | 100% | bool |
| VT | `vt_religious_activity` | 79% | `Yes`/`No` |
| WI | inside `wi_program_philosophy` | — | `Religious` |

### P41. `potty_training_required` — **Low**

**Why it's the same:** Both describe whether the provider takes children who aren't toilet-trained. The fields are logical inverses (NM: is potty training required; HI: are diapered children accepted).

**Formats:** NM uses `Required`/`Not Required`; HI is a bool.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| NM | `nm_potty_training` | 24% | `Required`, `Not Required` |
| HI | `hi_diapered_children_accepted` | 100% | bool (inverse sense) |

### P42. `region` — **Low**

**Why it's the same:** Each places the provider in a state-defined administrative or licensing region. The values only mean something within each state, so a common field mainly helps generic display.

**Formats:** Every state uses its own code or label vocabulary.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| IA | `ia_region` | 96% | `Region 4` |
| NV | `nv_region` | 65% | `South`, `North`, `Northeast` |
| NY | `ny_region_code` | 100% | `NYCDOH`, `LIRO` |
| CA | `ca_regional_office` | 100% | `07` |
| AZ | `az_regionalpartnership` | 100% | `North Pima` |
| MA | `ma_regional_office_address` | 100% | full office address |
| HI | `hi_island` | 100% | `Oahu` |

### P43. `ccrr_agency` — **Low**

**Why it's the same:** Both name the Child Care Resource & Referral agency serving the provider. CO was empty in every sampled record, so this is effectively MA-only for now.

**Formats:** MA spreads the agency across four fields (name, city, phone, website).

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MA | `ma_ccrr_name` (+ `_city`, `_phone`, `_website`) | 100% | `Seven Hills Child Care Resources` |
| CO | `co_ccrr` | code-only | — |

### P44. `publicly_listed` — **Low**

**Why it's the same:** Each says whether the state's own public search/referral portal lists the provider. It's a source-system visibility flag rather than a provider attribute.

**Formats:** All three are bools.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| CT | `ct_searchable` | 100% | bool (31% false) |
| DE | `de_portal_listed` | 100% | bool |
| IA | `ia_referral_listed` | 96% | bool |

---

## Part 2 — Extensions to existing common fields

These sources should feed a common field that already exists (via
`FIELD_COLLAPSE_MAP` or in-spider), rather than becoming a new field.

### X1. `scholarships_accepted` ← FL, VA, WA, OK, NH — **High**

**Why it's the same:** Each is a subsidy-acceptance flag, and these five states currently leave `scholarships_accepted` empty. Twenty-two other states already fill it.

**Formats:** FL is a bool, VA `Yes`/`No` and WA `Yes`-only. OK has only a contract number (presence implies yes), and NH has a token inside a list.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| FL | `fl_is_offering_school_readiness` | 100% | bool |
| VA | `va_current_subsidy_provider` | 100% | `Yes`/`No` |
| WA | `wa_subsidy` | 25% | `Yes` only |
| OK | `ok_subsidy_contract_number` | 62% | `59555` → presence = yes |
| NH | inside `nh_financial_assistance` | 90% | `STATE CC ASSISTANCE (SCHOLARSHIP)` |

### X2. `license_begin_date` ← NC — **High**

**Why it's the same:** `nc_license_effective_date` is the current license's effective date, and NC doesn't populate `license_begin_date` at all.

**Formats:** `M/D/YYYY`; the existing ISO normalizer already handles it.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| NC | `nc_license_effective_date` | 100% | `8/24/2026` |

### X3. `accreditation` ← ID, KY, NH — **Medium**

**Why it's the same:** These are accreditation fields not yet in the collapse map. They mostly carry a negative sentinel or a bare flag rather than accreditor names, so they add little until those are handled.

**Formats:** ID uses an `NOT Nationally Accredited` sentinel (sometimes duplicated), KY is a bool and NH a single `No`. FL's `fl_vpk_accreditation` is VPK-specific and should stay separate, like `fl_vpk_curriculum`.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ID | `id_national_accreditation` | 82% | `NOT Nationally Accredited, NOT Nationally Accredited`, `Other` |
| KY | `ky_accreditation_available` | 37% | bool (no names) |
| NH | `nh_accreditation` | 2% | `No` |

### X4. `mailing_address` ← MO — **Medium**

**Why it's the same:** The field is labelled as a mailing address. But 23 of 30 sampled values are one Kansas corporate address, so it may be the licensee's address (P10) rather than the facility's; check the source page before mapping.

**Formats:** ALL CAPS, ZIP+4.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| MO | `mo_mailing_address` | 48% | `6901 SHAWNEE MISSION PKWY STE 300, OVERLAND PARK, KS 66202-4029` |

### X5. `transportation` ← NC — **Medium**

**Why it's the same:** NC's special-features list carries `Provides transportation`, and NC has no `transportation` value today.

**Formats:** A list token, which would need to become a bool or string.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| NC | inside `nc_special_features` | 63% | `["Provides transportation"]` |

### X6. `curriculum` ← ID, WI — **Low**

**Why it's the same:** These "program philosophy" fields often name a curriculum (e.g. `Creative Curriculum`). They also hold non-curriculum values (`Religious`, `Other`, `None Reported.`).

**Formats:** ID and WI use categorical strings; WI uses `None Reported.` as a sentinel (85%).

| State | Source field | Fill | Pattern |
|---|---|---|---|
| ID | `id_program_philosophy` | 82% | `Creative Curriculum`, `Other`, `No Philosophy Curriculum` |
| WI | `wi_program_philosophy` | 100% | `Religious`, `Cognitive Based Philosophy`, `None Reported.` |

---

## Part 3 — Inspection-level candidates (`InspectionItem`)

### I1. `inspector` — **High**

**Why it's the same:** Each names the staff member who ran the visit.

**Formats:** Plain names; MO uses ALL CAPS. MA also has a separate `ma_investigator` on investigation rows.

| State | Source field | Fill | Pattern |
|---|---|---|---|
| AK | `ak_licensing_specialist` | 100% | `Juliana Burya` |
| MA | `ma_licensor` (+ `ma_investigator`) | 88% | `Madelyn Marichal` |
| MO | `mo_specialist` | 99% | `PENNY SNODGRASS` |
| MT | `mt_inspector_name` | 89% | `Jodi Linne` |
| NH | `nh_licensor` | 100% | `Alison Fitz` |
| RI | `ri_licensor` | 29% | `Jessica Ullrich` |

### I2. `inspection_id` — **High**

**Why it's the same:** Each is the source system's key for one visit or report.

**Formats:** Ints (CT, HI, KY), numeric strings (NV, MO, KS), Salesforce IDs (MA, NH), UUIDs (FL, not unique: ~4.5k repeats) and a prefixed code (AZ `D-2029537`). Keep it as an opaque string.

| State | Source field | Pattern |
|---|---|---|
| CT | `ct_inspection_id` | int |
| FL | `fl_inspection_id` | UUID |
| HI | `hi_visit_id` | int |
| KY | `ky_inspection_id` | int |
| MA | `ma_visit_id` | Salesforce ID |
| MO | `mo_inspection_id` | `2507905226` |
| NH | `nh_visit_id` | Salesforce ID |
| NV | `nv_inspection_number` | `79452` |
| KS | `ks_nosf_id` (row key) / `ks_survey_number` | `161446` / `25-001610` |
| AZ | `az_enforcement_name` | `D-2029537` |

### I3. `announced` — **High**

**Why it's the same:** Each says whether the visit was announced or unannounced. Three states give it in a dedicated field; NC, CT and RI encode it in `type`.

**Formats:** Title-case labels, MO ALL CAPS with an `N/A` sentinel, or a substring of `type`. All of them map to a bool.

| State | Source field | Pattern |
|---|---|---|
| AK | `ak_visit_type` | `Announced` / `Unannounced` |
| MA | `ma_announcement_type` | `Announced` / `Unannounced` |
| NH | `nh_announcement_type` | `Announced` / `Unannounced` |
| MO | `mo_notice` | `ANNOUNCED` / `UNANNOUNCED` / `N/A` |
| NC | `type` | `Announced` / `Unannounced` |
| CT / RI | substring of `type` | `UNANNOUNCED INSPECTION - FULL`, `Unannounced Monitoring Visit` |

### I4. `violation_found` — **High**

**Why it's the same:** Each says whether the visit found any noncompliance. Several states put this in `original_status` or `md_finding` text instead.

**Formats:** Bools (MO's has inverted polarity: `in_compliance`), `Yes`/`No` and status phrases. All of them map to a bool.

| State | Source field | Pattern |
|---|---|---|
| FL | `fl_has_violation` | bool |
| VA | `va_violations` | `Yes`/`No` |
| MA | `ma_noncompliance_identified` | bool (investigations only) |
| MO | `mo_in_compliance` | bool, **inverted** |
| WY | `wy_violation_found` | bool (visit rows) |
| AK | `original_status` | `In Compliance` / `Non-Compliance` |
| WI | `original_status` | `See Violations below` / `No Violations found ***` |
| MD | `md_finding` | `No Noncompliances Found` vs a finding |

### I5. `violation_count` — **High**

**Why it's the same:** Each is the number of violations, deficiencies or findings recorded for the visit.

**Formats:** All ints. HI counts "requirements not met" and SC counts "alerts", both of which are close equivalents.

| State | Source field | Pattern |
|---|---|---|
| CT | `ct_violations_count` | int |
| DE | `de_violation_count` | int |
| NV | `nv_deficiency_count` | int |
| KS | `ks_findings_count` | int |
| HI | `hi_requirements_not_met` | int |
| SC | `sc_alert_count` | int |
| MO | `mo_open_violations` | int (open, not total) |

### I6. `regulation_cited` — **High**

**Why it's the same:** In these four states each inspection row is one citation, and the field is the cited rule/code section. Other states nest citations inside violation lists (I14).

**Formats:** Free-form legal citations in each state's own code (`AAC R9-5-208.C.`, `13A.15.03.04A(3)`, `470 IAC 3-4.5-4(1)`, `251.05(2)(a)2.`). Keep them as strings.

| State | Source field | Fill |
|---|---|---|
| AZ | `az_regulation` | 73% |
| MD | `md_regulation` | 74% |
| IN | `in_rule_code` | 55% |
| WI | `wi_rule_number` | 64% |

### I7. `compliance_domains` — **High**

**Why it's the same:** MA and NH both publish per-domain compliance breakdowns from what looks like the same licensing platform, with matching domain names (`Administration`, `Care of Children`, …).

**Formats:** NH uses `{domain, level_of_compliance: "12 / 12"}`; MA uses `{domain, indicator, description}`.

| State | Source field | Fill |
|---|---|---|
| MA | `ma_domains` | 88% |
| NH | `nh_domains` | 96% |

### I8. `items_compliant` / `items_reviewed` — **Medium**

**Why it's the same:** Each gives a compliant-items over items-reviewed ratio for the visit. It's not confirmed whether NH's numerator counts compliant or non-compliant items (`0 / 1` is common).

**Formats:** MA and RI use `x/y`, NH `x / y`, OK two ints and KS only the denominator (plus findings count).

| State | Source field | Pattern |
|---|---|---|
| MA | `ma_level_of_compliance` | `17/17` |
| RI | `ri_compliance` | `87/87` |
| NH | `nh_level_of_compliance` | `0 / 1` |
| OK | `ok_compliance_count`, `ok_compliance_total` | int, int |
| KS | `ks_regulations_reviewed` | int (denominator) |

### I9. `correction_date` — **Medium**

**Why it's the same:** Each is when the cited issue was resolved or corrected. NH records when the corrective-action plan was *accepted*, which is a nearby but different event.

**Formats:** AZ and IN are already ISO. WV uses `M/D/YYYY` with a `1/1/1753` null sentinel (13%), and NH uses `MM/DD/YYYY`.

| State | Source field | Pattern |
|---|---|---|
| AZ | `az_date_resolved` | ISO |
| IN | `in_correction_date` | ISO |
| WV | `wv_issue_completed_date` | `8/9/2024`; sentinel `1/1/1753` |
| NH *(related)* | `nh_corrective_action_accepted` | `06/25/2025` |

### I10. `licensing_period_start` / `licensing_period_end` — **Medium**

**Why it's the same:** Both stamp each inspection with the license term it fell under.

**Formats:** HI uses ISO; ME uses `MM/DD/YYYY`.

| State | Source field | Pattern |
|---|---|---|
| HI | `hi_licensing_period_start`, `hi_licensing_period_end` | `2024-12-01` |
| ME | `me_licensed_from`, `me_licensed_to` | `09/09/2024` |

### I11. `visit_reason` — **Medium**

**Why it's the same:** Each gives the purpose of the visit (annual, complaint, renewal) separately from `type`. In most other states `type` already holds the purpose, so this may be better folded into `type`.

**Formats:** Each state uses its own vocabulary.

| State | Source field | Pattern |
|---|---|---|
| MA | `ma_visit_reason` | `Monitoring`, `Renewal - Full Review` |
| KS | `ks_survey_reason` | `Annual Survey`, `To investigate a complaint` |
| OK | `ok_visit_purpose` | `Periodic`, `Permit`, `Complaint` |

### I12. `finding_text` — **Medium**

**Why it's the same:** Each is the narrative of what the inspector found. Granularity varies: one per citation in MD, IN and WI, one per visit in AZ, OK and DE.

**Formats:** MD mixes in a `No Noncompliances Found` sentinel, OK is a list of strings and the rest are prose strings.

| State | Source field |
|---|---|
| MD | `md_finding` |
| IN | `in_noncompliance` |
| WI | `wi_description` |
| AZ | `az_decision_correction` |
| OK | `ok_noncompliances` |
| DE | `de_investigation_conclusion` |

### I13. `regulation_description` — **Medium**

**Why it's the same:** Each is the text of the rule that was cited. WV's field is named "non compliance code" but holds rule text, which is why it's grouped here.

**Formats:** IN has the full rule text, WI a short summary and WV the rule text.

| State | Source field | Pattern |
|---|---|---|
| IN | `in_rule_description` | full rule text |
| WI | `wi_rule_summary` | `Supervision Of Children` |
| WV | `wv_non_compliance_code` | rule text |

### I14. `violations` (structured list) — **Medium**

**Why it's the same:** Each is a nested list of violations or deficiencies for one visit. A common `[{regulation, description, status, corrected_date}]` shape would cover most of them.

**Formats:** Keys and nesting differ by state, and NC holds flat strings with a `Rule Violated:` prefix. **DE's list currently holds only all-null dicts (4,080 rows), which looks like a scraper bug.**

| State | Source field | Shape |
|---|---|---|
| CT | `ct_violations` | `[{regulation, category, status, …}]` |
| DE | `de_violations` | `[{regulation_code, description, corrective_action, correction_status}]` (all null) |
| MA | `ma_violations` | `[{regulation, result, statement}]` |
| NH | `nh_violations` | `[{domain, item, …}]` |
| MO | `mo_violations` | `[{rule, description}]` |
| WY | `wy_violations` | `[{regulation, …}]` |
| NC | `nc_violations` | `["Rule Violated: …"]` |
| SC | `sc_deficiencies` | `[{severity, deficiency_type, resolved, date_resolved}]` |
| OK | `ok_allegations` | `[{requirement, requirement_description, …}]` |

### I15. `enforcement_type` — **Low**

**Why it's the same:** Each is a formal enforcement or penalty action, recorded as an inspection row. The vocabularies (warning letter, civil fine, monetary penalty) and the row semantics differ a lot.

**Formats:** WI and KS use labels, AZ a float penalty amount and MS a `type` value.

| State | Source field | Pattern |
|---|---|---|
| WI | `wi_enforcement_type` | `Orders Letter`, `Warning Letter` |
| KS | `ks_order_type` | `Intent to Assess Civil Fine` |
| AZ | `az_civil_penalty` | `100.0` |
| MS | `type` / `ms_description` | `Monetary Penalty` / `Monetary Penalty Letter` |

---

## Appendix — Format drift in *existing* common fields

These problems showed up while profiling. They aren't new-field candidates, but they will affect whatever standard you choose.

- **D1. `infant` / `toddler` / `preschool` / `school`:** most states emit bools, but GA emits `Yes`, TX `Y`/`N`, and NY and VT emit **counts** (per-age capacity) under the same names.
- **D2. `scholarships_accepted`:** bools, `Yes`/`No` (AK, CO, GA, RI, TN, TX, UT, VT, DC), and `Accepted`/`Not Accepted` (NM). It needs bool coercion like `head_start`.
- **D3. `accepting_new_children`:** bools vs `Yes`/`No`. NH has three states (`Accepting Children on Wait List`), and **GA is `No` for 100% of 7,838 records, which looks like a bug.**
- **D4. `license_type`:** it mixes two concepts: license *term/stage* (AK `Biennial`, CO `Permanent`, HI/IN/WV `Regular`, VA `Two Year`, WA `Full`) vs *facility type* (AZ, CT, MT, NC, UT). NJ emits integer codes. Splitting it into a `license_stage` field may be worthwhile.
- **D5. `hours`:** roughly 15 string formats, and FL emits a dict. P12 would give a structured alternative.
- **D6. `languages`:** lists vs comma or semicolon strings. IL has duplicate list entries (`["ENGLISH", "ENGLISH"]`), GA adds annotations (`English (Spoken by Staff)`), and NM uses a full boilerplate sentence.
- **D7. NJ integer codes:** the code lookups were never resolved, so NJ's `meals`, `transportation`, `accreditation`, `license_type` and several `nj_*` lists hold integer codes.
- **D8. `mailing_address`:** AL drops the separator between street and city (`…MISSY LEIGH LANEATHENS, AL`) and uses a bare `,` as an empty value (38%).
- **D9. `deficiencies`:** TX emits strings while CT, KS and WI emit ints.
- **D10. `license_begin_date`:** MN and VT values look like original-license dates rather than current-term starts, which is inconsistent with other states (see P27/P28).
