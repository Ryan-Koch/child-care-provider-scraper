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
    "provider_name": "The facility's name. ALL-CAPS values are converted to "
                     "Title Case; whitespace is cleaned.",
    "license_holder": "Name of the license holder/licensee. ALL-CAPS values "
                      "are Title-cased.",
    "administrator": "On-site administrator or director name. ALL-CAPS values "
                     "are Title-cased.",
    "license_number": "State-issued license/registration identifier, kept as "
                      "emitted (only whitespace-cleaned).",

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
    "status_date": "Date the current status took effect, normalized to ISO "
                   "8601 (YYYY-MM-DD).",
    "license_begin_date": "License start date, normalized to ISO 8601.",
    "license_expiration": "License expiration date, normalized to ISO 8601.",

    # Address
    "address": "Full street address; whitespace-cleaned, trailing country "
               "(\", United States\"/\", USA\") stripped, comma spacing "
               "normalized.",
    "city": "City parsed from address when unambiguous; otherwise left empty "
            "(never guessed).",
    "state": "USPS 2-letter state parsed from address (spelled-out names are "
             "converted); empty when no recognizable state is present.",
    "zip": "5-digit ZIP parsed from the end of address; empty when ambiguous.",

    # Geocoding provenance (set by the post-run enrichment step, not the
    # normalization pipeline; see scripts/geocode_enrich.py).
    "geocode_source": "Where latitude/longitude came from, set by the post-run "
                      "geocoding step: \"state\" (supplied by the source "
                      "state), \"census\" (derived from address via the US "
                      "Census geocoder), or \"unmatched\" (attempted, no usable "
                      "point). Empty when geocoding was not attempted.",
    "geocode_confidence": "Confidence of a geocoded coordinate: \"exact\" or "
                          "\"approximate\" for a Census match, or \"tie\"/"
                          "\"no_match\" when unmatched. Empty for "
                          "state-supplied coordinates.",

    # Numbers / types
    "capacity": "Licensed capacity coerced to an integer when it is a clean "
                "number; ranges/free text are left unchanged and logged.",
    "ages_served": "Ages accepted, normalized to a single string (lists are "
                   "joined with \", \").",
    "latitude": "Latitude kept as a trimmed string to avoid float precision "
                "drift.",
    "longitude": "Longitude kept as a trimmed string to avoid float precision "
                 "drift.",

    # Collapsed common fields
    "license_type": "Type/category of license, collapsed from a single "
                    "populated state-specific *_license_type field.",
    "school_district": "Associated school district, collapsed from a single "
                       "populated state-specific source field.",
    "mailing_address": "Facility mailing address, collapsed from a single "
                       "populated state source (owner/licensee addresses are "
                       "excluded).",
    "accreditation": "Accreditation(s), collapsed from a single populated "
                     "state source.",
    "meals": "Meals / food program, collapsed from a single populated state "
             "source.",
    "accepting_new_children": "Whether the provider is accepting new "
                              "children, collapsed from a single state source.",
    "transportation": "Transportation offered, collapsed from a single "
                      "populated state source.",
    "head_start": "Head Start participation, collapsed from a single state "
                  "source and coerced to a boolean.",
    "curriculum": "Curriculum, collapsed from a single populated state source "
                  "(VPK-specific curriculum is excluded).",

    # Pass-through common fields (whitespace cleanup only)
    "phone": "Contact phone number (whitespace-cleaned only).",
    "email": "Contact email (whitespace-cleaned only).",
    "provider_website": "Provider website URL (whitespace-cleaned only).",
    "county": "County (whitespace-cleaned only).",
    "hours": "Operating hours, as emitted by the state (whitespace-cleaned "
             "only).",
    "languages": "Languages, as emitted (string or list; whitespace-cleaned).",
    "scholarships_accepted": "Subsidy/scholarship acceptance, as emitted "
                             "(whitespace-cleaned only).",
    "deficiencies": "Deficiency count/summary, as emitted (whitespace-cleaned "
                    "only).",
    "infant": "Infant care indicator/count, as emitted (whitespace-cleaned).",
    "toddler": "Toddler care indicator/count, as emitted (whitespace-cleaned).",
    "preschool": "Preschool indicator/count, as emitted (whitespace-cleaned).",
    "school": "School-age indicator/count, as emitted (whitespace-cleaned).",
}
