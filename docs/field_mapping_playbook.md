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
| `license_number` | License/registration id | String (as emitted) |
| `license_holder` | Licensee / holder name | String; ALL-CAPS → Title Case |
| `administrator` | On-site administrator/director | String; ALL-CAPS → Title Case |
| `provider_type` | Raw state license category | String (preserved exactly) |
| `facility_category` | Canonical facet of `provider_type` | Vocab: `center` / `family_home` / `group_home` / `school_age` / `exempt` / `other` |
| `status` | License status | Canonical vocab (5 buckets, see below) |
| `status_date` | Date of current status | `YYYY-MM-DD` |
| `address` | Full street address | Cleaned string (no `, United States`) |
| `city` / `state` / `zip` | Parsed address parts | `city` string, `state` USPS 2-letter, `zip` 5-digit — **only when unambiguous** |
| `latitude` / `longitude` | Coordinates | String (full precision preserved) |
| `phone` / `email` / `provider_website` | Contact | String |
| `capacity` | Licensed capacity | Integer (when a clean number) |
| `hours` | Operating hours | String |
| `ages_served` | Ages accepted | String (lists joined with `, `) |
| `county` | County | String |
| `scholarships_accepted` | Subsidy/scholarship acceptance | As emitted (string/bool) |
| `license_begin_date` | License start | `YYYY-MM-DD` |
| `license_expiration` | License end | `YYYY-MM-DD` |
| `license_type` | Type/category of license | String (collapsed from `xx_license_type`) |
| `school_district` | Associated school district | String (collapsed) |
| `mailing_address` | Facility mailing address | String (collapsed) |
| `accreditation` | Accreditation(s) | String/list (collapsed) |
| `meals` | Meals / food program | String (collapsed) |
| `accepting_new_children` | Open enrollment flag | As emitted (collapsed) |
| `transportation` | Transportation offered | String (collapsed) |
| `head_start` | Head Start program | **Boolean** (collapsed + coerced) |
| `curriculum` | Curriculum | String (collapsed) |
| `languages` | Languages | String/list |

> `sutq_rating` also sits in the common block but is **Ohio's** Step Up To
> Quality rating — a legacy quality-rating field. Don't map other states into
> it; quality ratings stay state-specific (see the exception below).

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
| Facility mailing address | `mailing_address` | **Not** owner/licensee address — keep those `xx_`. |
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
**additive** — the `xx_` source field is retained.

---

## The rule of thumb

> **If two or more states already have a field for a concept, it should be a
> common field — not a per-state `xx_` field.**

Lightweight process: check this doc → if a common field fits, use it → only
create an `xx_` field for genuinely state-unique data.

### The exception: quality ratings stay state-specific

Quality / QRIS ratings (`ut_quality_rating`, `pa_stars_rating`,
`nv_star_rating`, `ga_quality_rated_level`, `ri_brightstars_rating`,
`wa_early_achievers_status`, `md_excels_level`, Ohio's `sutq_rating`, …) are
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
