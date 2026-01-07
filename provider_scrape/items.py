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


class ProviderItem(scrapy.Item):
    # This defines all the possible columns for your final CSV file.
    provider_name = scrapy.Field()
    license_number = scrapy.Field()
    license_holder = scrapy.Field()
    provider_type = scrapy.Field()
    status = scrapy.Field()
    status_date = scrapy.Field()
    sutq_rating = scrapy.Field()
    address = scrapy.Field()
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
    # Virginia specific fields
    va_license_type = scrapy.Field()
    va_inspector = scrapy.Field()
    va_current_subsidy_provider = scrapy.Field()
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

    # These fields help with tracking and debugging.
    provider_url = scrapy.Field()
    source_state = scrapy.Field()

    # This will hold the list of inspections.
    inspections = scrapy.Field()
