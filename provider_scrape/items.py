# items.py

import scrapy


class InspectionItem(scrapy.Item):
    date = scrapy.Field()
    type = scrapy.Field()
    original_status = scrapy.Field()
    corrective_status = scrapy.Field()
    status_updated = scrapy.Field()
    report_url = scrapy.Field()
    va_shsi = scrapy.Field()
    va_complaint_related = scrapy.Field()
    va_violations = scrapy.Field()
    mt_inspector_name = scrapy.Field()
    md_regulation = scrapy.Field()
    md_finding = scrapy.Field()
    md_inspection_status = scrapy.Field()
    # South Carolina specific
    sc_alert_count = scrapy.Field()
    sc_alert_resolved_count = scrapy.Field()
    sc_deficiencies = scrapy.Field()
    # North Carolina specific
    nc_violations = scrapy.Field()
    # Rhode Island specific
    ri_compliance = scrapy.Field()
    ri_licensor = scrapy.Field()
    # West Virginia specific
    wv_corrective_action_plan_start = scrapy.Field()
    wv_corrective_action_plan_end = scrapy.Field()
    wv_non_compliance_code = scrapy.Field()
    wv_outcome_code = scrapy.Field()
    wv_issue_completed_date = scrapy.Field()

    # Arizona specific
    az_regulation = scrapy.Field()
    az_decision_correction = scrapy.Field()
    az_date_resolved = scrapy.Field()
    az_civil_penalty = scrapy.Field()
    az_enforcement_name = scrapy.Field()

    # Florida specific
    fl_has_violation = scrapy.Field()
    fl_inspection_id = scrapy.Field()

    # Nevada specific
    nv_deficiency_count = scrapy.Field()
    nv_inspection_number = scrapy.Field()

    # Hawaii specific
    hi_visit_id = scrapy.Field()
    hi_licensing_period_start = scrapy.Field()
    hi_licensing_period_end = scrapy.Field()
    # Count of requirements marked "not met" on the visit, when the visit's
    # detail is embedded in the inspections page (latest visit only).
    hi_requirements_not_met = scrapy.Field()

    # Alaska specific inspection fields (AKCCIS
    # GetFacilityInspectionTasksPublicView -- see docs/alaska_field_mapping.md).
    ak_visit_type = scrapy.Field()  # "Announced" | "Unannounced"
    ak_licensing_specialist = scrapy.Field()

    # Wisconsin specific inspection fields (childcarefinder.wisconsin.gov).
    # The detail page publishes three tables; each row becomes one
    # InspectionItem discriminated by `type` ("Monitoring" | "Enforcement" |
    # "Violation").
    wi_rule_number = scrapy.Field()  # admin-code cite, e.g. "251.055(1)(a)"
    wi_rule_summary = scrapy.Field()  # e.g. "Supervision Of Children"
    wi_description = scrapy.Field()  # violation / enforcement narrative
    wi_enforcement_type = scrapy.Field()  # "Orders Letter" | "Warning Letter" | ...
    wi_appeal = scrapy.Field()  # "Yes" | "No"
    wi_decision = scrapy.Field()  # enforcement appeal decision
    wi_correction_plan_url = scrapy.Field()  # monitoring "View Correction Plan" doc

    # Indiana specific inspection fields (secure.in.gov providersearch JSON API).
    # Each inspection is a regulatory visit; a rule citation
    # (centerRule.code/description) and a non-compliance narrative are present
    # only on visits that recorded a violation -- clean visits carry just the
    # survey date, department, and the health-violation flag.
    in_rule_code = scrapy.Field()  # e.g. "470 IAC 3-4.7-100(e)"
    in_rule_description = scrapy.Field()  # centerRule.description
    in_noncompliance = scrapy.Field()  # noncomplianceStatement narrative
    in_is_health_violation = scrapy.Field()  # bool
    in_correction_date = scrapy.Field()  # date the non-compliance was corrected

    # Kansas specific inspection fields (khap.kdhe.ks.gov OIDS). Each licensing
    # or complaint survey row becomes one InspectionItem (`type` = "Licensing
    # Survey" / "Complaint Survey"); ``ks_nosf_id`` is the row key -- one
    # Survey ID can produce several NOSF rows with different findings counts
    # (see kansas_plan.md Sec 2.5). Administrative order rows are a third
    # discriminated `type` ("Administrative Order") with their own fields.
    ks_survey_id = scrapy.Field()  # Survey ID -- NOT unique per row
    ks_nosf_id = scrapy.Field()  # NOSF ID -- the true row key
    ks_survey_number = scrapy.Field()  # e.g. "25-005368"
    ks_survey_reason = scrapy.Field()  # "Annual Survey" / "Initial Survey" / ...
    ks_findings_count = scrapy.Field()  # numerator of "View Findings (3/568)"
    ks_regulations_reviewed = scrapy.Field()  # denominator of "View Findings (3/568)"
    ks_survey_template_url = scrapy.Field()  # blank-template link (claris.kdhe.state.ks.us)
    ks_facility_response = scrapy.Field()  # "No Response" / "Facility Response" / "Not Received"
    ks_order_number = scrapy.Field()  # administrative order Number
    ks_order_type = scrapy.Field()  # e.g. "Intent to Assess Civil Fine"
    ks_order_reason = scrapy.Field()  # order Reason (often blank)
    ks_order_final_status = scrapy.Field()  # e.g. "Appeal not filed order is final"
    # Findings-tier detail (opt-in, `-a findings=1`): K.A.R. citation +
    # "Description :" narrative pairs from OIDS_ViewFacilityFindings.aspx.
    ks_findings = scrapy.Field()  # [{regulation, description}]

    # Maine specific inspection fields (gateway.maine.gov licensing history).
    me_licensed_from = scrapy.Field()
    me_licensed_to = scrapy.Field()

    # Kentucky specific inspection fields (kynect.ky.gov). Each row of the
    # provider's KICCS inspection history; `ky_poc_id` is present only on the
    # subset that produced a plan of correction.
    ky_inspection_id = scrapy.Field()  # KICCS InspectionId
    ky_report_name = scrapy.Field()  # form id, e.g. "KID013A"
    ky_poc_id = scrapy.Field()  # Plan-of-Correction id

    # Connecticut specific inspection fields (www.211childcare.org). Each
    # entry of a provider's embedded `inspections[]` becomes one InspectionItem
    # from summary fields alone (free -- no extra request); `ct_violations`/
    # `ct_documents`/`report_url` are filled in only by the Phase 3 detail
    # fan-out to /inspections/{id}.json (default on, `-a violations=0` to
    # skip -- see connecticut_plan.md Sec 4.3/6.3).
    ct_inspection_id = scrapy.Field()  # the /inspections/{id} key
    ct_case_uid = scrapy.Field()  # CT OEC case id
    ct_inspection_status = scrapy.Field()  # "CLOSED" (97%) / "PENDING"
    ct_severity = scrapy.Field()  # "Low" / "High" / "N/A" / "PENDING"
    ct_reason = scrapy.Field()
    ct_resolution = scrapy.Field()
    ct_violations_count = scrapy.Field()  # int, 0 on ~35% of inspections
    ct_document_count = scrapy.Field()  # int, 0 on ~7% of inspections
    # [{regulation, category, statute}] -- NOTE the source API's own field
    # names are swapped: `description` holds the regulation cite and
    # `statute` holds the requirement text (Sec 6.3). Unset when
    # ct_violations_count is 0 (the API still returns a "No Violations"
    # sentinel row in that case, which is deliberately filtered out).
    ct_violations = scrapy.Field()
    ct_documents = scrapy.Field()  # [{description, document_type, visited_on, link}]

    # Delaware specific inspection fields (data.delaware.gov Socrata). Two
    # discriminated kinds share the `inspections` list, told apart by `type`:
    #   * a compliance visit (wb83-pkcv), `type` = facility_visit_type, e.g.
    #     "Unannounced Full Compliance Review" -- one item per (license,
    #     date), carrying every citation from that visit in de_violations.
    #   * a complaint investigation (pnbd-85r6), `type` = investigation_type
    #     ("OCCL Standards Complaint" / "IA Investigation"), with the result
    #     in `original_status` and the narrative in
    #     de_investigation_conclusion.
    # NOTE: wb83-pkcv contains ONLY non-compliance rows -- a clean visit
    # produces no row at all, so de_violation_count is >= 1 by construction
    # and the visit list is not a complete inspection history
    # (delaware_plan.md Sec 6.3).
    de_violation_count = scrapy.Field()  # int, always >= 1
    de_violations = scrapy.Field()  # [{regulation_code, description,
    #   corrective_action, correction_status,
    #   correction_due, corrected_date,
    #   how_corrected}]
    de_investigation_conclusion = scrapy.Field()  # complaint narrative; absent on IA Investigations

    # Idaho specific inspection fields (www.idahochildcarecheck.org, a
    # separate Drupal 10 site). Two discriminated kinds share the
    # `inspections` list:
    #   * Health inspections -- `type` is the inspection activity (e.g.
    #     "Investigation", "Follow-Up", "Annual"), `original_status` is
    #     "Passed"/"Failed", and every one of the ~20-31 numbered inspection
    #     criteria is captured in `id_criteria`.
    #   * Incidents -- `type` = "Incident", `original_status` carries the
    #     incident category (e.g. "Supervision Concern").
    id_investigation_resolved = scrapy.Field()  # "Resolved" / "Not Resolved"; only on investigations
    id_criteria = scrapy.Field()  # [{name, passed, comment}]
    id_incident_title = scrapy.Field()
    id_incident_description = scrapy.Field()
    id_incident_resolution = scrapy.Field()

    # Massachusetts specific inspection fields (childcare.mass.gov Salesforce
    # Aura Apex API -- EEC_ProviderDetailsController.getProviderDetails). Two
    # discriminated kinds share the `inspections` list, told apart by `type`:
    #   * a monitoring visit -- `type` is the visit type (e.g. "Renewal -
    #     Monitoring Visit"); D-1 keeps EVERY per-domain compliance row from
    #     `visitDomainList`, not just the non-compliant subset.
    #   * an investigation/complaint -- `type` is the literal "Investigation"
    #     discriminator; the outcome lives in `original_status` and D-2 keeps
    #     the (source-redacted) violation narratives in `ma_violations`.
    ma_visit_id = scrapy.Field()  # visit `id`
    ma_announcement_type = scrapy.Field()  # "Unannounced" / "Announced"
    ma_visit_reason = scrapy.Field()  # `isPreLicensing` -- misnamed by the
    # source; actually the visit reason, e.g. "Renewal - Monitoring"
    ma_level_of_compliance = scrapy.Field()  # e.g. "41/42"
    ma_licensor = scrapy.Field()  # licensor assigned to this visit
    # Every visitDomainList row (D-1): [{domain, indicator, description,
    # level_of_compliance, is_key_indicator, regulation_name,
    # regulations: [{name, article_text}]}]
    ma_domains = scrapy.Field()
    ma_investigation_id = scrapy.Field()  # investigation `id`
    ma_investigator = scrapy.Field()  # investigator assigned
    ma_noncompliance_identified = scrapy.Field()  # bool
    # D-2: [{regulation, result, statement, corrective_action_plan}] -- the
    # statement/plan text is source-redacted ("[REDACTED]") where applicable.
    ma_violations = scrapy.Field()


class ProviderItem(scrapy.Item):
    # This defines all the possible columns for your final CSV file.
    provider_name = scrapy.Field()
    license_number = scrapy.Field()
    license_holder = scrapy.Field()
    provider_type = scrapy.Field()
    # Canonical cross-state facet derived from provider_type (normalization
    # pipeline). Additive: provider_type keeps its exact state value.
    facility_category = scrapy.Field()
    status = scrapy.Field()
    status_date = scrapy.Field()
    address = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    phone = scrapy.Field()
    email = scrapy.Field()
    provider_website = scrapy.Field()
    administrator = scrapy.Field()
    capacity = scrapy.Field()
    hours = scrapy.Field()
    ages_served = scrapy.Field()
    infant = scrapy.Field()
    toddler = scrapy.Field()
    preschool = scrapy.Field()
    school = scrapy.Field()
    county = scrapy.Field()
    scholarships_accepted = scrapy.Field()
    license_begin_date = scrapy.Field()
    license_expiration = scrapy.Field()
    deficiencies = scrapy.Field()
    languages = scrapy.Field()

    # Common fields populated by the normalization pipeline's field-collapse
    # step (additive: the source state-specific fields are preserved, D2).
    # See FIELD_COLLAPSE_MAP in normalization.py.
    license_type = scrapy.Field()
    school_district = scrapy.Field()
    mailing_address = scrapy.Field()
    accreditation = scrapy.Field()
    meals = scrapy.Field()
    accepting_new_children = scrapy.Field()
    transportation = scrapy.Field()
    head_start = scrapy.Field()  # normalized to a boolean
    curriculum = scrapy.Field()

    # Best-effort address components parsed from `address` by the normalization
    # pipeline (additive; `address` is preserved). Left None when ambiguous.
    city = scrapy.Field()
    state = scrapy.Field()  # USPS 2-letter
    zip = scrapy.Field()

    # Coordinate provenance, populated by the post-run geocoding enrichment step
    # (see provider_scrape/geocoding.py). `latitude`/`longitude` are filled from
    # the address for states that don't publish coordinates; these two fields
    # record where each coordinate came from so downstream consumers can filter.
    geocode_source = scrapy.Field()  # "state" | "census" | "unmatched" | None
    geocode_confidence = scrapy.Field()  # "exact" | "approximate" | "tie" | "no_match" | None

    # Ohio specific fields
    oh_sutq_rating = scrapy.Field()

    # Virginia specific fields
    va_license_type = scrapy.Field()
    va_inspector = scrapy.Field()
    va_current_subsidy_provider = scrapy.Field()
    va_quality_rating = scrapy.Field()
    va_ID = scrapy.Field()
    va_public_funding = scrapy.Field()
    va_interactions = scrapy.Field()
    va_interactions_points = scrapy.Field()
    va_curriculum_points = scrapy.Field()
    va_total_points = scrapy.Field()

    # Texas specific fields
    tx_rising_star = scrapy.Field()
    tx_operation_id = scrapy.Field()
    tx_agency_number = scrapy.Field()

    # California specific fields
    ca_regional_office = scrapy.Field()
    ca_license_first_date = scrapy.Field()
    ca_closed_date = scrapy.Field()
    ca_citation_numbers = scrapy.Field()
    ca_poc_dates = scrapy.Field()
    ca_all_visit_dates = scrapy.Field()
    ca_inspection_visit_dates = scrapy.Field()
    ca_other_visit_dates = scrapy.Field()
    ca_complaint_info = scrapy.Field()
    ca_inspect_typea = scrapy.Field()
    ca_inspect_typeb = scrapy.Field()
    ca_other_typea = scrapy.Field()
    ca_other_typeb = scrapy.Field()

    # IL specific
    il_provider_id = scrapy.Field()
    il_facility_type = scrapy.Field()
    il_day_age_range = scrapy.Field()
    il_night_age_range = scrapy.Field()
    il_day_capacity = scrapy.Field()
    il_night_capacity = scrapy.Field()

    # Alabama specific fields
    al_quality_rating = scrapy.Field()
    al_rating_expiration = scrapy.Field()
    al_nighttime_hours = scrapy.Field()
    al_nighttime_ages = scrapy.Field()
    al_mailing_address = scrapy.Field()
    al_accreditations = scrapy.Field()
    al_adverse_actions = scrapy.Field()
    al_substantiated_complaints = scrapy.Field()
    al_deficiency_reports = scrapy.Field()

    # Arkansas specific fields
    ar_quality_rating = scrapy.Field()
    ar_program_type = scrapy.Field()
    ar_regulation_type = scrapy.Field()
    ar_total_capacity = scrapy.Field()

    # Colorado specific fields
    co_quality_rating = scrapy.Field()
    co_award_date = scrapy.Field()
    co_governing_body = scrapy.Field()
    co_cccap_fa_status_d1 = scrapy.Field()
    co_cccap_authorization_status = scrapy.Field()
    co_school_district = scrapy.Field()
    co_ecc = scrapy.Field()
    co_ccrr = scrapy.Field()
    co_license_type = scrapy.Field()
    co_licensed_to_serve = scrapy.Field()
    co_special_needs = scrapy.Field()
    co_accepting_new_children = scrapy.Field()
    co_infant_openings = scrapy.Field()
    co_toddler_openings = scrapy.Field()
    co_preschool_openings = scrapy.Field()
    co_school_age_openings = scrapy.Field()
    co_head_start = scrapy.Field()
    co_license_issue_date = scrapy.Field()

    # New York specific fields
    ny_facility_id = scrapy.Field()
    ny_region_code = scrapy.Field()
    ny_facility_opened_date = scrapy.Field()
    ny_address_omitted = scrapy.Field()
    ny_phone_number_omitted = scrapy.Field()
    ny_phone_extension = scrapy.Field()
    ny_school_district_name = scrapy.Field()
    ny_capacity_description = scrapy.Field()

    # Pennsylvania specific fields
    pa_stars_rating = scrapy.Field()
    pa_certificate_status = scrapy.Field()
    pa_school_district = scrapy.Field()
    pa_meal_options = scrapy.Field()
    pa_schedule = scrapy.Field()
    pa_cost_table = scrapy.Field()

    # New Mexico specific fields
    nm_star_level = scrapy.Field()
    nm_potty_training = scrapy.Field()
    nm_pay_schedules = scrapy.Field()
    nm_snacks = scrapy.Field()
    nm_meals = scrapy.Field()
    nm_schedule = scrapy.Field()

    # Utah specific fields
    ut_license_type = scrapy.Field()
    ut_quality_rating = scrapy.Field()
    ut_vacancies = scrapy.Field()
    ut_licensed_since = scrapy.Field()
    ut_environment = scrapy.Field()
    ut_meals = scrapy.Field()
    ut_school_district = scrapy.Field()

    # Montana specific fields
    mt_license_type = scrapy.Field()

    # Maryland specific fields
    md_approved_education = scrapy.Field()
    md_accreditation = scrapy.Field()
    md_fatalities = scrapy.Field()
    md_serious_injuries = scrapy.Field()
    md_excels_level = scrapy.Field()
    md_school_name = scrapy.Field()

    # Michigan specific fields
    mi_license_status = scrapy.Field()
    mi_licensee_address = scrapy.Field()
    mi_services_provided = scrapy.Field()
    mi_full_day = scrapy.Field()

    # Minnesota specific fields
    mn_last_renewed_date = scrapy.Field()
    mn_licensed_to_provide = scrapy.Field()
    mn_restrictions = scrapy.Field()
    mn_setting = scrapy.Field()
    mn_gender = scrapy.Field()
    mn_license_info_misc = scrapy.Field()
    mn_type_of_license = scrapy.Field()
    mn_license_holder_onsite = scrapy.Field()

    # Georgia specific fields
    ga_quality_rated_level = scrapy.Field()
    ga_accepting_new_children = scrapy.Field()
    ga_liability_insurance = scrapy.Field()
    ga_mailing_address = scrapy.Field()
    ga_registration_fee = scrapy.Field()
    ga_activity_fee = scrapy.Field()
    ga_program_status = scrapy.Field()
    ga_compliance_status = scrapy.Field()
    ga_services = scrapy.Field()
    ga_transportation = scrapy.Field()
    ga_meals = scrapy.Field()
    ga_accreditation = scrapy.Field()
    ga_environment = scrapy.Field()
    ga_summer_camp = scrapy.Field()
    ga_accepts_children_type = scrapy.Field()
    ga_profit_status = scrapy.Field()
    ga_weekly_rates = scrapy.Field()
    ga_activities = scrapy.Field()
    ga_other_care_type = scrapy.Field()
    ga_financial_info = scrapy.Field()
    ga_special_hours = scrapy.Field()
    ga_curriculum = scrapy.Field()
    ga_family_engagement = scrapy.Field()
    ga_operating_months = scrapy.Field()
    ga_operating_days = scrapy.Field()
    ga_transportation_notes = scrapy.Field()
    ga_school_break_notes = scrapy.Field()

    # Washington specific fields
    wa_provider_id = scrapy.Field()
    wa_license_name = scrapy.Field()
    wa_license_type = scrapy.Field()
    wa_early_achievers_status = scrapy.Field()
    wa_school_district = scrapy.Field()
    wa_head_start = scrapy.Field()
    wa_early_head_start = scrapy.Field()
    wa_eceap = scrapy.Field()
    wa_available_slots = scrapy.Field()
    wa_slot_age_groups = scrapy.Field()
    wa_food_program = scrapy.Field()
    wa_subsidy = scrapy.Field()
    wa_provider_status = scrapy.Field()
    wa_languages_of_instruction = scrapy.Field()
    wa_contacts = scrapy.Field()
    wa_license_history = scrapy.Field()

    # South Carolina specific fields
    sc_provider_id = scrapy.Field()
    sc_abc_quality_rating = scrapy.Field()
    sc_abc_rating_history = scrapy.Field()
    sc_program_participation = scrapy.Field()
    sc_license_category = scrapy.Field()
    sc_licensing_specialist_name = scrapy.Field()
    sc_licensing_specialist_phone = scrapy.Field()

    # New Jersey specific fields (from GetProviders.aspx)
    nj_unique_program_id = scrapy.Field()
    nj_program_facility_type = scrapy.Field()
    nj_facility_type = scrapy.Field()
    nj_license_type = scrapy.Field()
    nj_quality_rating = scrapy.Field()
    nj_accreditation = scrapy.Field()
    nj_yearly_schedule = scrapy.Field()
    nj_doh_id = scrapy.Field()
    nj_phone_extension = scrapy.Field()
    nj_participation_programs = scrapy.Field()
    nj_curriculum = scrapy.Field()
    nj_child_assessment = scrapy.Field()
    nj_environmental_features = scrapy.Field()
    nj_meal_options = scrapy.Field()
    nj_special_needs_training = scrapy.Field()
    nj_transportation = scrapy.Field()
    nj_special_schedules = scrapy.Field()
    nj_discounts = scrapy.Field()
    nj_fees = scrapy.Field()
    nj_mccyn_plus = scrapy.Field()
    nj_social_media = scrapy.Field()
    nj_tuition_infant_hourly = scrapy.Field()
    nj_tuition_infant_daily = scrapy.Field()
    nj_tuition_infant_weekly = scrapy.Field()
    nj_tuition_infant_monthly = scrapy.Field()
    nj_tuition_toddler_hourly = scrapy.Field()
    nj_tuition_toddler_daily = scrapy.Field()
    nj_tuition_toddler_weekly = scrapy.Field()
    nj_tuition_toddler_monthly = scrapy.Field()
    nj_tuition_preschool_hourly = scrapy.Field()
    nj_tuition_preschool_daily = scrapy.Field()
    nj_tuition_preschool_weekly = scrapy.Field()
    nj_tuition_preschool_monthly = scrapy.Field()
    nj_tuition_school_age_hourly = scrapy.Field()
    nj_tuition_school_age_daily = scrapy.Field()
    nj_tuition_school_age_weekly = scrapy.Field()
    nj_tuition_school_age_monthly = scrapy.Field()

    # Rhode Island specific fields
    ri_brightstars_rating = scrapy.Field()
    ri_license_decision = scrapy.Field()
    ri_most_recently_renewed = scrapy.Field()
    ri_ccap_status = scrapy.Field()
    ri_ccap_expiration_date = scrapy.Field()
    ri_head_start = scrapy.Field()
    ri_state_prek = scrapy.Field()
    ri_is_lea = scrapy.Field()
    ri_provider_contact_name = scrapy.Field()
    ri_provider_email = scrapy.Field()
    ri_services_offered = scrapy.Field()
    ri_availability = scrapy.Field()
    ri_age_group_capacity = scrapy.Field()

    # North Carolina specific fields
    nc_license_type = scrapy.Field()
    nc_license_effective_date = scrapy.Field()
    nc_star_rating_total_points = scrapy.Field()
    nc_star_rating_max_points = scrapy.Field()
    nc_program_standards_points = scrapy.Field()
    nc_educational_standards_points = scrapy.Field()
    nc_capacity_first_shift = scrapy.Field()
    nc_capacity_second_shift = scrapy.Field()
    nc_capacity_third_shift = scrapy.Field()
    nc_license_restrictions = scrapy.Field()
    nc_license_history = scrapy.Field()
    nc_special_features = scrapy.Field()
    nc_staff_child_ratios = scrapy.Field()
    nc_owner_name = scrapy.Field()
    nc_owner_mailing_address = scrapy.Field()
    nc_owner_phone = scrapy.Field()
    nc_owner_fax = scrapy.Field()
    nc_owner_email = scrapy.Field()
    nc_sanitation_inspection_date = scrapy.Field()
    nc_sanitation_classification = scrapy.Field()
    nc_sanitation_score = scrapy.Field()

    # West Virginia specific fields
    wv_licensing_specialist = scrapy.Field()
    wv_license_type = scrapy.Field()
    wv_contact = scrapy.Field()
    wv_contact_title = scrapy.Field()
    wv_age_from = scrapy.Field()
    wv_age_to = scrapy.Field()

    # Arizona specific fields
    az_facility_id = scrapy.Field()
    az_operatinghourid = scrapy.Field()
    az_affiliation = scrapy.Field()
    az_regionalpartnership = scrapy.Field()
    az_shiftcomment = scrapy.Field()
    az_headstart = scrapy.Field()
    az_desprovider = scrapy.Field()
    az_status_label = scrapy.Field()
    az_first_slot_start = scrapy.Field()
    az_first_slot_end = scrapy.Field()
    az_license_type = scrapy.Field()
    az_quality_rating = scrapy.Field()

    # Florida specific fields
    fl_dba = scrapy.Field()
    fl_license_status = scrapy.Field()
    fl_alternate_provider_number = scrapy.Field()
    fl_city = scrapy.Field()
    fl_zip_code = scrapy.Field()
    fl_display_address_on_web = scrapy.Field()
    fl_display_email_on_web = scrapy.Field()
    fl_display_phone_on_web = scrapy.Field()
    fl_is_religious_exempt = scrapy.Field()
    fl_is_faith_based = scrapy.Field()
    fl_is_head_start = scrapy.Field()
    fl_is_offering_school_readiness = scrapy.Field()
    fl_is_vpk = scrapy.Field()
    fl_is_gold_seal = scrapy.Field()
    fl_is_public_school = scrapy.Field()
    fl_wels_rating_date = scrapy.Field()
    fl_vpk_composite_score = scrapy.Field()
    fl_vpk_school_year_composite_score = scrapy.Field()
    fl_vpk_school_year_wels_rating_date = scrapy.Field()
    fl_vpk_summer_composite_score = scrapy.Field()
    fl_vpk_summer_wels_rating_date = scrapy.Field()
    fl_is_trauma_badge = scrapy.Field()
    fl_is_inclusion_badge = scrapy.Field()
    fl_is_dll_badge = scrapy.Field()
    fl_is_infant_toddler_badge = scrapy.Field()
    fl_trauma_badge_date = scrapy.Field()
    fl_inclusion_badge_date = scrapy.Field()
    fl_dll_badge_date = scrapy.Field()
    fl_infant_toddler_badge_date = scrapy.Field()
    fl_services = scrapy.Field()
    fl_programs = scrapy.Field()
    fl_gold_seal = scrapy.Field()
    fl_vpk_accreditation = scrapy.Field()
    fl_vpk_classrooms = scrapy.Field()
    fl_vpk_curriculum = scrapy.Field()
    fl_vpk_instructor_credentials = scrapy.Field()

    # Hawaii specific fields
    hi_service_id = scrapy.Field()  # serviceId - the per-service primary key
    hi_provider_id = scrapy.Field()  # providerId - the parent org id
    hi_service_type_code = scrapy.Field()  # raw serviceType code, e.g. "05"
    hi_provider_kind = scrapy.Field()  # "OR" (org/center) or "CG" (caregiver/home)
    hi_license_type = scrapy.Field()  # "Provisional" | "Regular" (from P/R)
    hi_area_code = scrapy.Field()  # fully-qualified area code, e.g. "ABAHBW"
    hi_island = scrapy.Field()  # island name (top-level area description)
    hi_mailing_address = scrapy.Field()  # mailing address (distinct from location)
    hi_usda_food_program = scrapy.Field()  # bool
    hi_diapered_children_accepted = scrapy.Field()  # bool
    hi_demonstration_project = scrapy.Field()  # bool
    hi_meals = scrapy.Field()  # list of meal descriptions
    hi_accreditations = scrapy.Field()  # list of accreditation descriptions
    hi_license_history = scrapy.Field()  # list of prior license periods
    hi_status_history = scrapy.Field()  # list of {status, statusDate}

    # These fields help with tracking and debugging.
    provider_url = scrapy.Field()
    source_state = scrapy.Field()

    # Maine specific fields (search.childcarechoices.me + gateway.maine.gov).
    me_star_rating = scrapy.Field()
    me_infant_slots = scrapy.Field()
    me_toddler_slots = scrapy.Field()
    me_preschool_slots = scrapy.Field()
    me_school_age_slots = scrapy.Field()
    me_openings_updated = scrapy.Field()
    me_licensing_specialist = scrapy.Field()
    me_licensing_specialist_email = scrapy.Field()
    me_temporarily_closed = scrapy.Field()
    me_times_renewed = scrapy.Field()

    # Nevada specific fields
    nv_credential_type = scrapy.Field()
    nv_facility_type = scrapy.Field()
    nv_operation_id = scrapy.Field()
    # Base license number (no year suffix) used as the quality-data join key.
    nv_license_base = scrapy.Field()
    # Silver State Stars QRIS enrichment (Power BI quality dashboard).
    nv_star_rating = scrapy.Field()
    nv_program_type = scrapy.Field()
    nv_region = scrapy.Field()
    nv_qris_status = scrapy.Field()
    nv_rating_period_start = scrapy.Field()
    nv_rating_period_end = scrapy.Field()
    nv_qris_enrollment_date = scrapy.Field()
    nv_rating_period_name = scrapy.Field()
    nv_site_characteristic = scrapy.Field()
    nv_rating_priority = scrapy.Field()

    # North Dakota specific fields (search.ec.hhs.nd.gov JSON API)
    # Quality rating stays state-specific per the field-mapping playbook.
    nd_quality_rating = scrapy.Field()  # qualityRatingLabel, e.g. "Step 1"
    # Vacancy data (the registry's high-value, ND-unique content).
    nd_total_vacancies = scrapy.Field()
    nd_vacancies_by_age = scrapy.Field()  # list of {ageGroupLabel, numberVacancies}
    nd_vacancies_details = scrapy.Field()  # free-text vacancies note
    nd_vacancies_updated = scrapy.Field()  # vacanciesTimestamp
    # Enrollment / capacity detail beyond the common `capacity`.
    nd_desired_capacity = scrapy.Field()
    nd_total_enrollment = scrapy.Field()
    nd_enrollment_schedule = scrapy.Field()  # programEnrollmentScheduleLabel
    nd_special_populations = scrapy.Field()  # specialPopulationsLabels
    nd_supplemental_care = scrapy.Field()  # supplementalCareTypesLabels
    nd_min_age = scrapy.Field()  # e.g. "0 Months"
    nd_max_age = scrapy.Field()  # e.g. "12 Years"
    nd_program_id = scrapy.Field()  # registry program id (also in provider_url)
    nd_org_id = scrapy.Field()  # orgId
    nd_philosophy = scrapy.Field()  # philosophyStatement

    # Alaska specific fields (AKCCIS -- see docs/alaska_field_mapping.md).
    ak_facility_gen_id = scrapy.Field()  # AKCCIS internal facility id
    ak_facility_number = scrapy.Field()  # 7-digit facility number
    ak_legacy_license_number = scrapy.Field()  # pre-migration license id
    ak_vendor_id = scrapy.Field()  # CCAP subsidy-billing vendor code
    ak_facility_subtype = scrapy.Field()  # facilityTypeSubTypeDescription
    ak_license_type = scrapy.Field()  # "Biennial"|"Provisional"|...
    ak_licensing_specialist = scrapy.Field()  # assigned state specialist

    # Washington DC specific fields (mychildcare.dc.gov). No license dates,
    # status, or inspections are published by this source; the internal facility
    # id is emitted as the common `license_number` (the closest registration id).
    # Capital Quality designation -- DC's QRIS rating; stays state-specific.
    dc_capital_quality_designation = scrapy.Field()
    dc_capital_quality_participant = scrapy.Field()  # bool
    dc_pay_equity_fund = scrapy.Field()  # bool (Pay Equity Fund participant)
    dc_prek_enhancement = scrapy.Field()  # bool (Pre-K Enhancement program)
    dc_nontraditional_hours = scrapy.Field()  # bool (nontraditional hours)
    # Per-age-group enrollment/openings/tuition table (DC-unique detail):
    # list of {age_group, openings, current_enrollment, desired_enrollment,
    # monthly_tuition}.
    dc_enrollment = scrapy.Field()

    # Wisconsin specific fields (childcarefinder.wisconsin.gov). The common
    # `license_number` carries the Provider # (e.g. "0000555710"); a provider
    # number spans multiple locations, so the per-facility identity is
    # `license_number` + `wi_location_number` (or the standalone
    # `wi_facility_number`). YoungStar rating stays state-specific per the
    # field-mapping playbook.
    wi_youngstar_rating = scrapy.Field()  # e.g. "5 Stars" | "Not Yet Rated"
    wi_location_number = scrapy.Field()  # e.g. "016"
    wi_facility_number = scrapy.Field()  # licensed facility #, e.g. "120162"
    wi_night_capacity = scrapy.Field()  # night capacity (day cap -> `capacity`)
    wi_months_open = scrapy.Field()  # e.g. "Jan - Dec"
    wi_unique_services = scrapy.Field()  # YoungStar "Unique Program Services" list
    wi_special_care_types = scrapy.Field()  # provider-reported special care list
    wi_program_philosophy = scrapy.Field()  # provider-reported philosophy text
    wi_vacancies = scrapy.Field()  # provider-reported vacancies text
    wi_waitlist = scrapy.Field()  # provider-reported waitlist text

    # Vermont specific fields (brightfutures.dcf.state.vt.us). VT publishes no
    # explicit license number, so the internal Provider ID is emitted as the
    # common `license_number` (DC precedent) and duplicated here as an explicit
    # join key. The STARS quality rating stays state-specific per the
    # field-mapping playbook. The `infant`/`toddler`/`preschool`/`school` common
    # fields carry the per-age *capacities*; the per-age *vacancies* (the
    # registry's frequently-updated, VT-unique content) live in the vt_* fields.
    vt_provider_id = scrapy.Field()  # PARTY_ID, e.g. "3053"
    vt_star_level = scrapy.Field()  # STARS rating, e.g. "4 Star"
    vt_type_of_care = scrapy.Field()  # e.g. "Full-Time, Part-Time, Daytime"
    vt_days_of_operation = scrapy.Field()  # e.g. "Monday, Tuesday, ..."
    vt_special_schedule = scrapy.Field()
    vt_building_type = scrapy.Field()  # Building Type/Setting, e.g. "House"
    vt_area_description = scrapy.Field()  # e.g. "Fenced Yard"
    vt_religious_activity = scrapy.Field()  # "Yes" | "No"
    vt_sibling_discount = scrapy.Field()  # "Yes" | "No"
    vt_special_services = scrapy.Field()
    vt_program_participation = scrapy.Field()
    vt_guidance = scrapy.Field()  # provider-reported guidance philosophy
    vt_program_description = scrapy.Field()  # provider-reported daily program text
    vt_pets = scrapy.Field()
    vt_vacancy_as_of = scrapy.Field()  # "Current as of" date for vacancies
    vt_current_vacancy = scrapy.Field()  # total current vacancy count
    vt_infant_vacancies = scrapy.Field()
    vt_toddler_vacancies = scrapy.Field()
    vt_preschool_vacancies = scrapy.Field()
    vt_school_age_vacancies = scrapy.Field()

    # Tennessee specific fields (onedhs.tn.gov ServiceNow provider maps). The
    # public feed is coarse (provider_type is only Child Care / DOE / Exempt,
    # and there are no license dates or ZIP). QRIS quality data stays
    # state-specific per docs/field_mapping_playbook.md.
    tn_provider_id = scrapy.Field()  # registry "Provider ID", e.g. "84171"
    tn_regulatory_agency = scrapy.Field()  # "DHS Child Care" | "Department of Education"
    tn_regulatory_individual = scrapy.Field()  # assigned state licensing rep
    tn_vacancy = scrapy.Field()  # raw vacancy string
    tn_participates_certificate = scrapy.Field()  # bool (Certificate program)
    tn_wheelchair_accessible = scrapy.Field()  # "Yes" | "No"
    tn_age_group_rates = scrapy.Field()  # list of {age_group, weekly_rate, unit_of_care, vacancy}
    tn_quality_rating = scrapy.Field()  # overall QRIS score, e.g. "91/100"
    tn_rating_effective_date = scrapy.Field()  # rating effective date (MM/DD/YYYY)
    tn_rating_expiration = scrapy.Field()  # rating expiration date (MM/DD/YYYY)
    tn_rating_scorecard = scrapy.Field()  # dict of per-domain score + percent

    # South Dakota specific fields (olapublic.sd.gov). The detail page's
    # Services Offered and Months of Operation multi-selects have no common
    # field; each is a small SD-specific vocabulary. (Ages of Children Served
    # maps to the common `ages_served`; Nationally Accredited -> `accreditation`.)
    sd_services_offered = scrapy.Field()  # list, e.g. ["After School"]
    sd_months_of_operation = scrapy.Field()  # list, e.g. ["12 Months"]

    # Indiana specific fields (secure.in.gov/apps/fssa/providersearch JSON API).
    # Indiana publishes no license number, so the internal providerId is emitted
    # as the common `license_number` (DC/VT precedent) and duplicated here as an
    # explicit join key; `in_location_id` is the second half of the detail key.
    # The Paths to QUALITY (PTQ) rating stays state-specific per the field-mapping
    # playbook. Capacity is published only as a per-age breakdown, so the common
    # `capacity` carries the sum and the exact breakdown is preserved here.
    in_provider_id = scrapy.Field()  # providerId (== license_number)
    in_location_id = scrapy.Field()  # locationId (2nd half of detail key)
    in_ptq_level = scrapy.Field()  # "0"-"4"; 0 = not rated
    in_health_violation_count = scrapy.Field()  # int
    in_is_ccdf = scrapy.Field()  # bool (also -> scholarships_accepted)
    in_is_temporarily_closed = scrapy.Field()  # bool
    in_temporarily_closed_message = scrapy.Field()
    in_programs = scrapy.Field()  # e.g. ["CCDF Provider", "On My Way Pre-K"]
    in_licensed_ages = scrapy.Field()  # [{start_age, end_age, quantity}]
    in_schedule = scrapy.Field()  # [{day, open, close}]
    in_complaints = scrapy.Field()  # [{complaint_date, issue, closed_date}]

    # Iowa specific fields. C3 (search.iachildcareconnect.org) publishes live
    # vacancy counts, a quality level, and two service-description
    # vocabularies; Titan (secureapp.dhs.state.ia.us) contributes the
    # per-provider report counts (see tasks/iowa_epic/iowa_plan.md).
    ia_iq4k_level = scrapy.Field()  # "IQ4K Level 1".."5" (82.5% null)
    ia_region = scrapy.Field()  # "Region 1".."Region 5"
    ia_total_openings = scrapy.Field()  # int, live vacancy count (0..131)
    ia_openings_by_age = scrapy.Field()  # list of {ageGroup, fullTime, partTime}
    ia_openings_as_of = scrapy.Field()  # date the openings were reported
    ia_days_of_operation = scrapy.Field()  # "Mon, Tue, ..."
    ia_care_types = scrapy.Field()  # hoursOfOperation service vocab
    ia_serves_special_needs = scrapy.Field()  # "Yes"/"No"
    ia_business_type = scrapy.Field()  # "house"/"building" pin glyph
    ia_padded_license_id = scrapy.Field()  # C3 display form, zero-padded
    ia_referral_listed = scrapy.Field()  # bool: in the referral network
    ia_compliance_report_count = scrapy.Field()  # Titan ComplianceCount
    ia_complaint_count = scrapy.Field()  # Titan ComplaintCount
    ia_regulation_checklist_count = scrapy.Field()  # Titan RegulationCheckListCount

    # Kansas specific fields (khap.kdhe.ks.gov/OIDS -- OIDS_Search.aspx /
    # OIDS_ViewFacility.aspx). The site is POST-only (no per-facility GET url;
    # `provider_url` is the base search page per DC/Indiana precedent), so
    # ``ks_facility_token`` -- the stable SearchLink id -- is the real durable
    # identity (see kansas_plan.md Sec 4.6). ``provider_type`` is taken from
    # the detail page; when the listing disagrees (rare) the listing value is
    # kept here rather than discarded (Sec 4.1).
    ks_facility_token = scrapy.Field()  # stable "SearchLink.<token>" id
    ks_listing_program_type = scrapy.Field()  # listing Program Type, only if != detail
    ks_address_suppressed = scrapy.Field()  # bool: owner opted out (~38% of rows)

    # Kentucky specific fields (kynect.ky.gov Salesforce Aura API). The All
    # STARS quality rating stays state-specific per the field-mapping playbook.
    # `ky_provider_id` is the internal id and the detail-call join key (the
    # public licence number is emitted as `license_number`).
    ky_provider_id = scrapy.Field()  # ProviderId, e.g. 403
    ky_stars_rating = scrapy.Field()  # All STARS 0-5; 0 = not rated
    ky_prek_partnership = scrapy.Field()  # bool (state pre-K partnership)
    ky_ongoing_process = scrapy.Field()  # bool: open licensing action
    # Open regulatory actions, e.g. [{"process_type": "Adverse Action",
    # "status": "On-going"}]. Present on ~1.5% of providers.
    ky_ongoing_processes = scrapy.Field()  # [{process_type, status}]
    ky_food_permit = scrapy.Field()  # bool; null in source -> unset
    ky_accreditation_available = scrapy.Field()  # bool; null in source -> unset
    # (source typo'd "Acceditations")
    ky_service_costs = scrapy.Field()  # [{age_group, full_time_cost, part_time_cost}]

    # Connecticut specific fields (www.211childcare.org -- 211 Child Care, a
    # Rails JSON API front for CT's Office of Early Childhood). Per D-1 the
    # spider sweeps every provider id (1..max_id) and emits every record,
    # including the ~34% CT hides from its own public search -- `ct_searchable`
    # flags those; the inference that a non-searchable record is a lapsed/
    # closed listing is documented on `status`/`STATUS_BUCKETS`, not asserted
    # here (connecticut_plan.md Sec 5.3). Per D-3, `status` derives from
    # `searchable` ("Listed"/"Not Listed") -- CT publishes no license status.
    ct_provider_id = scrapy.Field()  # int -- the /providers/{id} key
    ct_provider_uid = scrapy.Field()  # uuid str -- CT OEC join key
    ct_searchable = scrapy.Field()  # bool -- visible in CT's own directory
    ct_licensed = scrapy.Field()  # bool (`license`); false == exempt/unlisted
    # Derived from the license_number prefix (Sec 5.6) -- a clean, fully
    # correlated taxonomy. NOTE: a DCEX (license-exempt) record's
    # type_of_provider is still a plain "Child Care Center", so the pipeline's
    # facility_category buckets it as `center`, not `exempt` -- an accepted,
    # documented limitation (~2.3% of searchable records). This field is the
    # one place the exemption signal survives; do not "fix" facility_category
    # with a global override for it.
    ct_license_type = scrapy.Field()
    ct_elevate_membership_level = scrapy.Field()  # CT's Elevate QRIS; stays state-specific
    ct_type_of_care = scrapy.Field()  # coarser vocab; emitted only when != provider_type
    ct_accreditations = scrapy.Field()  # list, e.g. NAEYC, NAFCC, Head Start
    ct_school_districts = scrapy.Field()  # list
    ct_transportation = scrapy.Field()  # list
    ct_accepting_referrals = scrapy.Field()  # bool
    ct_head_start = scrapy.Field()  # headstart_funding OR "Head Start" in accreditations
    ct_education_levels = scrapy.Field()  # list of staff credentials
    ct_special_needs = scrapy.Field()  # list
    ct_administers_meds = scrapy.Field()  # bool
    ct_wheelchair_accessible = scrapy.Field()  # bool
    ct_capacity_three_and_under = scrapy.Field()  # int
    ct_capacity_full_time = scrapy.Field()  # int
    ct_capacity_school_aged = scrapy.Field()  # int
    # Licensed age range in WEEKS, not months/years (6 == 6 weeks old). Never a
    # fallback for ages_served/age flags -- age_range_max: 0 and shifts: [] are
    # the exact same 38 (of 4,114) records, so that fallback path is broken in
    # the one case that would trigger it (Sec 5.8). 0 on the max is normalized
    # to unset here.
    ct_age_range_min_weeks = scrapy.Field()
    ct_age_range_max_weeks = scrapy.Field()
    ct_date_established = scrapy.Field()  # business founding date, NOT a licence date
    ct_oec_contact_id = scrapy.Field()  # CT Office of Early Childhood contact id
    ct_schedule = scrapy.Field()  # [{day, open, close}] (cf. in_schedule)
    ct_rates = scrapy.Field()  # [{age_group, label, full_time_weekly, ...}]
    # bool: the provider opted out of publishing a street address -- the source
    # `address` is the literal sentence "This provider's address has been
    # hidden" rather than a street (15 of 6,910 on the 2026-08-21 run). When
    # true, `address` is left unset; `city`/`zip`/coordinates are still
    # published and are kept. Mirrors ks_address_suppressed.
    ct_address_suppressed = scrapy.Field()

    # Delaware specific fields (data.delaware.gov iuzd-3dbt, published by
    # DSCYF / Office of Child Care Licensing). Delaware publishes no license
    # dates and no QRIS rating -- Delaware Stars is suspended pending a
    # system redesign.
    de_enforcement_action = scrapy.Field()  # "Suspended" / "Probation" / "Revoked" / ...
    de_intent_to_revoke = scrapy.Field()  # notice of a pending action, e.g. "Intent to Revoke"
    de_special_conditions = scrapy.Field()  # "Variance" / "High Nitrate Water" / "Foster Care" / ...
    de_financial_arrangements = scrapy.Field()  # raw ";"-joined token string (see de_profit_status)
    de_profit_status = scrapy.Field()  # "Nonprofit" / "Private" / "Profit" / "Publicly Operated"
    # Multi-year injury/death narrative, e.g. "2024: 5 facility injuries. No
    # facility deaths reported.; 2023: 1 facility injury. ...". 914 of 1,243
    # read "No facility injuries reported. No facility deaths reported."
    # Kept as the published sentence -- no run had any non-zero death count,
    # so there is nothing to parse into md_fatalities-style counters.
    de_injuries_report = scrapy.Field()
    # True if the provider appears in education.delaware.gov's own facility
    # table (890 of 1,243). The portal's filter is not reproducible from any
    # published column and is lossy in both directions -- it omits 353
    # records, 22 of which are newer than anything it lists. Treat as a
    # display hint, never as a licensure signal. See delaware_plan.md Sec 4.
    de_portal_listed = scrapy.Field()

    # Idaho specific fields (idahostars.org ActionGrid listing API +
    # DnnSharp ActionForm detail API -- see tasks/idaho_story/idaho_plan.md).
    # Idaho publishes no license number, so the internal facility `Id` is
    # emitted as the common `license_number` (DC/VT/Indiana precedent) and
    # duplicated as the AlternateRiseId join key used by the separate Idaho
    # Child Care Check (inspections) site. The Quality Achiever Status is
    # Idaho's QRIS-style designation and stays state-specific per the
    # field-mapping playbook.
    id_alternate_rise_id = scrapy.Field()  # AlternateRiseId -- idahochildcarecheck.org join key
    id_license_status = scrapy.Field()  # "State Licensed" / "License Exempt" / "Pending Renewal"
    id_national_accreditation = scrapy.Field()
    id_quality_achiever_status = scrapy.Field()  # "Eligible" / "Growing Star" / "Star Achiever" / ...
    id_quality_achievements = scrapy.Field()
    # "Are there openings available" / "Number of Openings" / "Is there a
    # waitlist" are always present in the detail HTML template but, as of the
    # 2026-08-25/26 live check, wrapped in an HTML comment on every sampled
    # provider (i.e. disabled site-wide, not a per-provider gap). The parser
    # strips comments before extracting fields, so these stay unset today;
    # kept in the schema in case the site re-enables the feature.
    id_openings_available = scrapy.Field()
    id_number_of_openings = scrapy.Field()
    id_waitlist = scrapy.Field()
    id_program_philosophy = scrapy.Field()
    id_philosophy_comment = scrapy.Field()
    id_philosophy_description = scrapy.Field()
    id_program_description = scrapy.Field()
    id_usda_food_program = scrapy.Field()
    id_family_style_dining = scrapy.Field()
    id_other_opportunities = scrapy.Field()
    id_opportunities_comment = scrapy.Field()
    id_consistent_schedule = scrapy.Field()
    id_consistent_schedule_comment = scrapy.Field()
    id_pet_policy = scrapy.Field()
    id_pet_policy_comment = scrapy.Field()

    # Massachusetts specific fields (childcare.mass.gov Salesforce Aura Apex
    # API -- see tasks/massachusettes_story/massachusettes_plan.md). Two Apex
    # methods: the search (ZIP-exact, one request per ZIP, no cap) supplies
    # the core record; the detail call (keyed by `Encrypted_Id__c`) supplies
    # the rich D-5 provider-reported extras plus the full inspection history.
    # MA publishes no county (only city + CC R&R region), so `county` is left
    # unset (plan §4). Per D-4, informal/exempt records have no public licence
    # number -- `license_number` falls back to the `P-######` program number,
    # which is always kept here too.
    ma_account_id = scrapy.Field()  # Salesforce accountId -- the dedupe key
    ma_encrypted_id = scrapy.Field()  # Encrypted_Id__c -- the detail-call key
    ma_program_number = scrapy.Field()  # providerNumber, e.g. "P-176763"
    ma_last_issue_date = scrapy.Field()  # most-recent licence issuance (D-3
    # keeps the FIRST issuance on the common `license_begin_date`)
    ma_temporary_status = scrapy.Field()  # Temporary_Status__c, e.g. "Reopened"
    ma_capacity_by_age = scrapy.Field()  # dict of the non-zero per-age-group counts
    ma_availability = scrapy.Field()  # raw availability text
    ma_schedule_options = scrapy.Field()  # list, e.g. ["Full day", "Full week"]
    ma_environment = scrapy.Field()  # list, e.g. ["Fenced Yard", "Smoke Free"]
    ma_financial_assistance = scrapy.Field()  # list, e.g. ["EEC Subsidies"]
    ma_special_needs = scrapy.Field()
    ma_special_skills = scrapy.Field()
    ma_licensor = scrapy.Field()  # currently assigned EEC licensor
    ma_regional_office_address = scrapy.Field()
    ma_regional_website = scrapy.Field()
    ma_umbrella_name = scrapy.Field()  # parent/umbrella organization name
    ma_is_informal = scrapy.Field()  # bool: license-exempt informal care
    ma_is_gsa = scrapy.Field()  # bool
    ma_dph_summer_camp = scrapy.Field()  # bool (isUnderDphSummerCamp Yes/No)
    ma_contact_redacted = scrapy.Field()  # bool: owner opted out of public contact info
    ma_ccrr_name = scrapy.Field()  # assigned CC R&R agency name
    ma_ccrr_phone = scrapy.Field()
    ma_ccrr_website = scrapy.Field()
    ma_ccrr_city = scrapy.Field()
    # Every schedule (Temporary/Summer/Full Year), not just the one collapsed
    # into the common `hours`: [{schedule_type, drop_in, extended_day,
    # days: [{day, start, end}]}]
    ma_schedules = scrapy.Field()
    # Only the populated (non-blank amount) per-age-group fee rows across all
    # schedules: [{schedule_type, age_group, rate_type, amount}]
    ma_cost_table = scrapy.Field()

    # This will hold the list of inspections.
    inspections = scrapy.Field()
