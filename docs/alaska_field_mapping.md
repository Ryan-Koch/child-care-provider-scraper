# Alaska (AKCCIS) — Field Mapping

Reference for the Alaska rebuild (see `tasks/alaska-rebuild/`). This is the
single decision log: for every field the AKCCIS API returns, where it lands
in `ProviderItem` / `InspectionItem`, and (for anything not obvious) why.

## Source

| Verb | Endpoint | Purpose |
|---|---|---|
| `POST` | `https://akccis.com/server/api/Facility/Search` | Full state roster. Body: `[]` (no filters). Returns ~700 records in one call. |
| `GET`  | `https://akccis.com/server/api/Facility/GetSearchFacilityById?facilityGenId={id}` | Same-shape single record — used as the errback fallback. |
| `GET`  | `https://akccis.com/server/api/Inspection/GetFacilityInspectionTasksPublicView?facilityGenId={id}` | Per-facility inspection visits (visit-level only; no deficiency detail publicly). |

Required request headers on both search and inspection calls: `Accept: application/json`,
`Content-Type: application/json` (search only), `Referer: https://akccis.com/client/map`.
Without the Referer, some environments have observed 403 responses.

## Fixtures

Committed under `provider_scrape/spiders/fixtures/` (matching the existing
convention for other states like North Dakota and Hawaii):

- **`ak_search_sample.json`** — 16 curated roster records, covering every
  `facilityType` and every observed `providerStatus`. Includes a record
  (`facilityGenId=10035`) that also appears in the inspection fixture so
  tests can round-trip parent→children.
- **`ak_inspection_sample.json`** — 4 visits from `facilityGenId=10035`.
  Covers both `compliance` values (`C` ×3, `NC` ×1), both `visitType` values
  (`Announced`, `Unannounced`), and three `purposeOfVisit` values (`H&SMonitor`,
  `Annual`, `Renewal`).

Roster diversity by curated id:

| id | facilityName | facilityType | providerStatus | licenseType | Coverage note |
|---|---|---|---|---|---|
| 10000 | Little Peoples Learning World | Licensed Center | Active/Open | Biennial | Golden path: license + expiredLicense + capacity + ages + CCAP + admin + specialist + legacyLN |
| 10003 | Kids R Fun | Licensed Home | Active/Open | Biennial | Standard licensed home |
| 10006 | Liz's Daycare | Licensed Group Home | Active/Open | Biennial | Group home |
| 10652 | Paula Sieghart | License Exempt | *null* | *null* | Exempt-only, no providerStatus |
| 10737 | Unlicensed Facility Jessica Conaway | Illegally Unlicensed | *null* | *null* | State-flagged illegal operation |
| 10330 | Kendrick, Pamela Jean | CCAP Certified/Accredited | Active/Open | *null* | Non-licensed but CCAP-vendor |
| 10005 | Ray's Child Care & Learning Center | Licensed Center | Closed | *null* | Closed center — retains expiredLicense history |
| 10031 | International Backpackers Hostel | License Exempt | Denied | *null* | Enforcement outcome |
| 10001 | Yesenia's Baby Care | Licensed Home | Revoked/Not Renewed | *null* | Revoked home |
| 10136 | Katrina's Home Daycare | Illegally Unlicensed | Suspended | *null* | Suspended illegal operation |
| 10542 | Noni's Place | License Exempt | Pending | *null* | Pending application |
| 10617 | Auke Lake Preschool & Afterschool - Glacier Valley | License Exempt | Exempt | *null* | Status = "Exempt" (needs its own status-bucket decision) |
| 10518 | Mill Pond Child Care | Licensed Home | App Withdrawn | *null* | Withdrawn |
| 10360 | Little Stretches Child Care | Licensed Home | Active/Open | Provisional | Provisional licensee |
| 10700 | Little Dreamers Academy | Licensed Home | *null* | *null* | **Prefixed licenseNumber "MOA-H-1000700"** |
| 10035 | Play All Day Care | Licensed Home | Active/Open | Biennial | Parent for the inspection fixture |

## Search response — top-level fields

Every key present in a roster record. Rows are grouped by decision.

### Mapped to common `ProviderItem` fields

| AKCCIS field | Sample value | ProviderItem target | Notes |
|---|---|---|---|
| `facilityName` | `"Little Peoples Learning World"` | `provider_name` | Pipeline title-cases ALL-CAPS values automatically. |
| `licenseNumber` | `970002` / `"MOA-H-1000700"` | `license_number` | **Cast to `str`, don't `int()`.** Prefixed forms (`MOA-H-…`) are legitimate — preserve as emitted. |
| `phoneNumber` | `"(907)262-4113"` | `phone` | |
| `licensedCapacity` | `60` (int) or `null` | `capacity` | Already an int on the wire — no parsing needed (contrast the old spider, which extracted digits from `"60 Children"`). |
| `facilityAdmin` | `"Rachel Jenkins"` or `null` | `administrator` | Populated for licensed facilities; null for exempt/illegal. |
| `doingBusinessAs` | `"Jeff Baker"` (or occasionally a legal entity like `"D. N. Ray Inc."`) | `license_holder` | **The name misleads.** In AKCCIS this field is used inconsistently: on 510/572 populated records it holds the licensee/owner name (e.g. `facilityName="Little Peoples Learning World"` → `doingBusinessAs="Jeff Baker"`), not a true DBA. The natural home for the licensee name is the common `license_holder` field. Populate `license_holder` only when the value differs from `facilityName` (the ~62 identity cases add no signal). |
| `facilityType` | `"Licensed Center"` | `provider_type` | Also drives `facility_category` via `FACILITY_CATEGORY_MAP` (Task 4). |
| `providerStatus` | `"Active/Open"` | `status` | Emit raw; pipeline buckets to `active`/`provisional`/`pending`/`enforcement`/`closed`. When `null` (13 records), emit `None` — the pipeline logs `unknown` and moves on. |
| `providerStatusEffectiveDate` | `"1998-07-02T09:00:00Z"` | `status_date` | Convert to ISO `YYYY-MM-DD`. |
| `address` | `"35095 Huntington Drive"` | (combined) | Concatenated with `address2`, `city`, `state`, `zipCode` into `address`. |
| `address2` | `""` or `"Suite 4"` | (combined) | Often empty string — the combiner must skip empties or it emits `"35095 Huntington Drive  Soldotna, AK 99669"` (double space). |
| `city` | `"Soldotna"` | `address` (combined) + `city` | Emit unmodified — pipeline preserves. |
| `stateDescAbbr` | `"AK"` | `address` (combined) + `state` | Always `"AK"` on this spider; use the field literal (don't hard-code). |
| `zipCode` | `"99669"` | `address` (combined) + `zip` | 5-digit; a few empties in the wild. |
| `county` | `"Kenai Peninsula Borough"` | `county` | Alaska's counties are boroughs / census areas. Emit as-is. |
| `latitude` | `60.488985199709` | `latitude` | **Coerce to `str`, keep full precision.** `normalization.canonical_coordinate` requires strings. |
| `longitude` | `-151.146877804374` | `longitude` | Same. |
| `isCCAP` | `true` / `false` | `scholarships_accepted` | Convert `true` → `"Yes"`, `false` → `"No"`. Matches how the old spider emitted `acceptsCCAP`. |
| `agesAcceptedMonthsStart` | `0.0` | `ages_served`, `infant`/`toddler`/`preschool`/`school` | Combined with `agesAcceptedMonthsEnd` into a human string like `"0 Months - 12 Years, 11 Months"` (155 months = 12y 11m). See "Age handling" below. |
| `agesAcceptedMonthsEnd` | `155.0` | same | |
| `license.effectiveDate` | `"2025-06-01T08:00:00Z"` | `license_begin_date` | ISO-truncate to `YYYY-MM-DD`. The nested `license` object represents the **current** license (isClosed is always False when present). |
| `license.endDate` | `"2027-02-28T09:00:00Z"` | `license_expiration` | Same treatment. |

### Mapped to new `ak_*` fields

| AKCCIS field | Sample | New field | Rationale |
|---|---|---|---|
| `facilityGenId` | `"10000"` | `ak_facility_gen_id` | AKCCIS internal id; also used to build `provider_url`. Exposed so downstream can rejoin to AKCCIS API responses. |
| `facilityNumber` | `1000000` | `ak_facility_number` | 7-digit facility number distinct from both `licenseNumber` and `facilityGenId`. Preserved for future cross-referencing. |
| `legacyLicenseNumber` | `"970002"` (or `null`) | `ak_legacy_license_number` | Pre-migration license id (populated on 426/700 records). Bridges historical records — some external systems still key on this. |
| `vendorId` | `"LPL98252"` | `ak_vendor_id` | CCAP vendor code (587/700 populated). Not a license id — a state-side subsidy-billing identifier. Unique to AK, no common-field home. |
| `facilityTypeSubTypeDescription` | `"License Exempt - Home (less than 4 unrelated) – MOA"` | `ak_facility_subtype` | Granular subtype available only for exempt/illegal records (Licensed Center/Home/Group Home records leave this null). Not suitable for `facility_category` bucketing — kept as supplementary detail. |
| `licenseType` | `"Biennial"` / `"Provisional"` / `"Biennial Extension"` / `"Provisional Extension"` / `null` | `ak_license_type` | The state's license *cadence* (biennial vs. provisional). Distinct from `provider_type`, which is the facility category. Wire into `FIELD_COLLAPSE_MAP` so it also populates common `license_type` (Task 4). |
| `facilityLicSpecialist` | `"Venus Siemens"` | `ak_licensing_specialist` | State-side inspector/specialist assigned to the facility. Populated on 511/700 records. Uniquely granular — no common home. |

### Skipped (dropped, with reason)

| AKCCIS field | Sample | Reason |
|---|---|---|
| `stateCode` | `7` | AKCCIS internal state code; `stateDescAbbr` gives us the USPS form. |
| `stateDesc` | `"Alaska"` | Redundant with the spider's hard-coded `source_state`. |
| `caseStatus` | `null` (always) | Never populated in the observed roster; reserved. |
| `createDate` | `"2025-12-03T01:02:07.32Z"` | Record-creation timestamp for the AKCCIS row itself, not a licensing event. No `ProviderItem` home. |
| `expirationDate` | `null` (always) | Duplicates `license.endDate` conceptually, but never populated at the top level in observed data. |
| `geoCode` | `null` (always) | Reserved; never populated. |
| `mapMarkerColorId` / `mapMarkerColorName` / `mapMarkerColorHex` / `mapMarkerNameId` / `mapMarkerNameURL` | `3953` / `"green"` / `"#31911A"` / `605379` / `"green"` | Pure UI theming for the AKCCIS map — no product value. |
| `distance` | `null` | Only populated in a proximity-filtered search; N/A for the empty-body full roster. |
| `isVerified` | `null` (always) | Reserved; never populated. |
| `licenseType` (top-level, when represented redundantly on `license` object) | see above | The top-level string is mapped; the nested `license.licenseType` int code is skipped (`licenseType: "Biennial"` is redundant with `license.licenseType == "Biennial"` when license is present, and the state ships the same string on both). |
| `facilityTypePrefix` | `"C"` / `"H"` | One-letter facility-type prefix used inside `licenseNumber` strings — offers no additional signal beyond `facilityType`. |
| `facilityTypeGenId` | `1` | Internal numeric code for `facilityType` — the string form is what we use. |
| `facilityTypeSubTypeId` | `35` | Numeric subtype code; `facilityTypeSubTypeDescription` is the human form. |
| `programSubType` | `null` (100% null in observed roster) | Reserved field; never populated in AK. |
| `providerStatusCode` | `2881` | Numeric code for `providerStatus` — the string form is what buckets in `STATUS_MAP`. |
| `countyGenId` | `169` | Numeric code for `county` — the string form is what downstream expects. |
| `subTypes` | `[35]` or `[]` | Array of `facilityTypeSubTypeId` values — redundant with `facilityTypeSubTypeDescription` for the subset that has subtypes. |
| `agesAcceptedMonths` | `[0.0, 155.0]` (or `[]`) | Same data as `agesAcceptedMonthsStart`/`End` in array form. |
| `agesServed` | `{startAge, endAge, statusCode, effectiveDate, ...}` | Nested object. On 700/700 records the payload is either all-null or ~redundant with `agesAcceptedMonths*`; keep the top-level ints. |
| `isLicensedType` | `true`/`false` | Facet already implied by `provider_type` / `facility_category`. |
| `hasFutureLicense` | `false` (or `true` for 23 records) | Convenience boolean; the underlying `futureLicense` object is not exposed either — see below. |
| `hasExpiredLicense` | `true` for 447 records | Same — convenience boolean, no downstream use. |
| `futureLicense` | `{...}` (23 records) | The **next** scheduled license period. No `ProviderItem` field exposes future-scheduled licensing; skip for now. Can be revisited if a use surfaces. |
| `expiredLicense` | `{effectiveDate, endDate, capacity, ...}` (447 records) | The **most recent expired** license period. Historical — the current `license` object is what `license_begin_date` / `license_expiration` come from. Skip for now; a `license_history` facet is a project-wide decision, not an AK-specific one. |

### Nested-object shapes for reference

`license` / `futureLicense` / `expiredLicense` all share this shape (only `license` is consumed):

| Field | Sample | Consumed? |
|---|---|---|
| `facilityLicenseStatusHistoryGenId` | `953` | no (internal id) |
| `licenseType` | `"Biennial"` | no (redundant with top-level `licenseType`) |
| `licenseTypeCode` | `604317` | no |
| `actionTypeCode` | `9` | no |
| `status` | `"Complete"` | no (workflow status of the license record, not the provider) |
| `statusCode` | `604572` | no |
| `effectiveDate` | `"2025-06-01T08:00:00Z"` | **yes** → `license_begin_date` (from `license` only) |
| `endDate` | `"2027-02-28T09:00:00Z"` | **yes** → `license_expiration` (from `license` only) |
| `deleteDate` | `null` | no |
| `capacity` | `60` | no (redundant with top-level `licensedCapacity`) |
| `facilityLicenseStatusInformationGenId` | `953` | no |
| `startAge` | `0.0` | no (redundant with top-level `agesAcceptedMonthsStart`; occasionally diverges — see "Age handling") |
| `endAge` | `155.0` | no |
| `isClosed` | `false` (always for `license`) | no |

## Inspection response — field mapping

Endpoint: `GET /server/api/Inspection/GetFacilityInspectionTasksPublicView?facilityGenId={id}`.
Returns a JSON array. Empty list is normal — only ~57 of the first 120 facility
ids sampled had any visits.

### Mapped

| AKCCIS field | Sample | InspectionItem target | Notes |
|---|---|---|---|
| `visitDate` | `"6/23/2026 1:00 PM"` | `date` | US format `M/D/YYYY H:MM AM/PM`. Convert to ISO `YYYY-MM-DD` (drop the time). |
| `purposeOfVisit` | `"Annual"` / `"Renewal"` / `"H&SMonitor"` / `"Complaint"` | `type` | The **reason** for the visit — the closest analogue to the "type" of inspection the item has historically held. |
| `compliance` | `"C"` / `"NC"` | `original_status` | Expand at parse time: `"C"` → `"In Compliance"`, `"NC"` → `"Non-Compliance"`. Unknown values pass through. |
| `visitType` | `"Announced"` / `"Unannounced"` | `ak_visit_type` | Orthogonal to purpose — kept as AK-specific detail so `type` can hold the (more informative) purpose. |
| `licensingSpecialist` | `"Daphne Mikes"` | `ak_licensing_specialist` | Inspector's name for this visit. New field on `InspectionItem` (also used as a spider-side dedup fingerprint component). |

### Skipped

| AKCCIS field | Sample | Reason |
|---|---|---|
| `formDescription` | `"Home Inspection - MOA"` | Internal form template name; not user-facing. |
| `formGenId` | `18` | Internal id. |
| `taskGenId` | `131` | Internal id. Would be needed to hit `GetTaskNonBinaryGridDataPublicView`, but that endpoint returns `[]` for every sampled task — no useful data. |
| `deviceGenId` | `45` | Internal id (partner of `taskGenId` for the same dead endpoint). |
| `entityGenId` | `11891` | Internal id (the licensee entity). |
| `visitTime` | `null` (always) | Reserved; the time-of-day is embedded in `visitDate`. |
| `caseStatus` | `null` (always) | Reserved. |
| `summaryInd` / `inspectionInd` / `qrisInd` | `false` / `true` / `false` | Workflow flags; every observed record has `inspectionInd=true` and the others `false`. |
| `inspectionGenId` | `1101` | Internal id (would be needed for a deeper detail call — no such public endpoint exists). |
| `licenseType` | `"Biennial"` | Redundant with the parent facility's top-level `licenseType`. |
| `facilityGenId` | `10035` | Redundant with the parent request. |

### Compliance-code expansion

Observed values in the raw feed: only `"C"` and `"NC"`. Task 2's spider should
implement `_expand_compliance`:

```python
_COMPLIANCE = {"C": "In Compliance", "NC": "Non-Compliance"}

def _expand_compliance(code):
    code = (code or "").strip()
    return _COMPLIANCE.get(code, code) or None
```

This matches the state's own map-view labels and keeps `original_status`
human-readable.

### Future-dated visits

**Not observed in the current fixture.** Sampled 120 facilities across the roster
and found no `visitDate` strictly greater than today (2026-07-24). The prior
concern (that scheduled/upcoming inspections might appear here) turned out not
to reproduce in this snapshot, at least for these facilities.

**Recommendation:** the spider should still parse `visitDate` defensively and
emit whatever the endpoint returns — including any future date it happens to
serve. Downstream consumers can filter by `date <= today` if they only want
completed visits. Do **not** filter server-side in the spider; that hides state
data that may become present in the future without a code change.

## New fields introduced

Added to `provider_scrape/items.py` by **Task 2** (not this task):

`ProviderItem`:

- `ak_facility_gen_id` — AKCCIS internal facility id; also encoded in `provider_url`.
- `ak_facility_number` — 7-digit AKCCIS facility number (distinct from `licenseNumber` and `facilityGenId`).
- `ak_legacy_license_number` — pre-migration license id (bridges historical records).
- `ak_vendor_id` — CCAP subsidy-billing vendor code.
- `ak_facility_subtype` — granular subtype for exempt / illegal / MOA-scoped records.
- `ak_license_type` — biennial vs. provisional license cadence; wired into `FIELD_COLLAPSE_MAP` so it also feeds common `license_type`.
- `ak_licensing_specialist` — assigned state licensing specialist.

`InspectionItem`:

- `ak_visit_type` — `"Announced"` / `"Unannounced"`.
- `ak_licensing_specialist` — inspector name for this visit.

## New STATUS_BUCKETS values (Task 4 wires these)

Full set of `providerStatus` values observed and their target bucket:

| Value | Count | Bucket | Notes |
|---|---|---|---|
| `Active/Open` | 392 | `active` | New addition. |
| `Closed` | 174 | `closed` | Already covered by the generic `"CLOSED"` / `"Closed"` entries. |
| `Exempt` | 46 | `active` | **Decision:** map to `active`. The project's canonical vocab has no `exempt` **status** bucket (only `exempt` **category**). Exempt providers *are* operationally open — bucketing them anywhere but `active` would misrepresent the data. Called out for explicit review in Task 4. |
| `Denied` | 32 | `enforcement` | New. Denial is a regulatory outcome that blocked the license — enforcement, not closure. |
| `Pending` | 27 | `pending` | Already covered by the generic `"PENDING"` / `"Pending"` entries. |
| *(null)* | 13 | *(unmapped → `unknown`)* | Passing `None` to `canonical_status` returns `None`; the pipeline emits nothing. Not a bug. |
| `App Withdrawn` | 8 | `enforcement` | New. Withdrawn applications indicate a blocked license path — enforcement. |
| `Revoked/Not Renewed` | 3 | `closed` | New. |
| `Temporary Closure` | 3 | `closed` | Already covered by the existing entry. |
| `Suspended` | 2 | `enforcement` | Already covered by the existing entry. |

## New FACILITY_CATEGORY_BUCKETS values (Task 4 wires these)

Full set of `facilityType` values observed and their target category:

| Value | Count | Category | Notes |
|---|---|---|---|
| `Licensed Center` | 270 | `center` | Already covered. |
| `Licensed Home` | 204 | `family_home` | Already covered by the generic `"Family Home"` / etc. entries — verify in Task 4. |
| `License Exempt` | 139 | `exempt` | New. |
| `Licensed Group Home` | 67 | `group_home` | New. **Decision:** `group_home`, matching the precedent set by North Dakota's `HHS-Licensed Group Child Care Home`. |
| `CCAP Certified/Accredited` | 11 | `exempt` | New. CCAP-accredited providers are not state-*licensed*; they participate in the subsidy program without a licensing category. `exempt` is the closest fit. |
| `Illegally Unlicensed` | 9 | `other` | New. Not a legitimate category; the state uses this to flag investigation targets. `other` keeps them separate from real exempt care. |

## Fixture-driven test coverage summary

What each fixture record was included to exercise (for the Task 3 tests):

| id | Exercises |
|---|---|
| 10000 | Full golden-path build_item: license + capacity + ages + admin + CCAP + specialist + legacyLN + expiredLicense |
| 10003 | Ages populated on a family home |
| 10006 | Group home → group_home category |
| 10652 | License Exempt with no `providerStatus` (`None` path); `ak_facility_subtype` populated |
| 10737 | Illegally Unlicensed → `other` category |
| 10330 | CCAP Certified/Accredited → `exempt` category |
| 10005 | Closed status still emits provider; expiredLicense present |
| 10031 | Denied → `enforcement` |
| 10001 | Revoked/Not Renewed → `closed` |
| 10136 | Suspended → `enforcement`; also Illegally Unlicensed |
| 10542 | Pending → `pending` |
| 10617 | Exempt status → `active` (bucket decision under review) |
| 10518 | App Withdrawn → `enforcement` |
| 10360 | Provisional licenseType |
| 10700 | Prefixed `licenseNumber` (`"MOA-H-1000700"`) — must not be `int()`-cast |
| 10035 | Parent for inspection fixture |
