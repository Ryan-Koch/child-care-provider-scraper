import json
import logging
import os

import pytest
from scrapy.http import Request, TextResponse

from provider_scrape import normalization as norm
from provider_scrape.items import InspectionItem, ProviderItem
from provider_scrape.spiders.connecticut import (
    INSPECTION_URL,
    PROVIDER_URL,
    SEARCH_URL,
    ConnecticutSpider,
    ages_from_shifts,
    clean_zip_full,
    compose_address,
    ct_license_type_from_number,
    extend_miss_streak,
    first_schedule,
    format_hours,
    is_suppressed_address,
    pad_zip_base,
    rates_from_shifts,
    zip5_from_clean,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_fixture(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


@pytest.fixture
def spider():
    return ConnecticutSpider()


# --- response builders ----------------------------------------------------- #


def provider_response(payload, provider_id=1):
    url = PROVIDER_URL.format(provider_id)
    req = Request(url, meta={"provider_id": provider_id})
    body = b"null" if payload is None else json.dumps(payload).encode()
    return TextResponse(url=url, body=body, encoding="utf-8", request=req)


def inspection_response(payload, provider_id=1, inspection_id=1):
    url = INSPECTION_URL.format(inspection_id)
    req = Request(url, meta={"provider_id": provider_id, "inspection_id": inspection_id})
    body = b"null" if payload is None else json.dumps(payload).encode()
    return TextResponse(url=url, body=body, encoding="utf-8", request=req)


def discovery_response(id_, exists):
    url = PROVIDER_URL.format(id_)
    req = Request(url, meta={"discovery_id": id_})
    body = b"{}" if exists else b"null"
    return TextResponse(url=url, body=body, encoding="utf-8", request=req)


def town_search_response(payload, town="Kent"):
    req = Request(f"{SEARCH_URL}?town={town}", meta={"town": town})
    return TextResponse(url=req.url, body=json.dumps(payload).encode(), encoding="utf-8", request=req)


def split_provider_outputs(outputs):
    """Partition parse_provider's yielded values into (detail_requests, items)."""
    requests, items = [], []
    for out in outputs:
        (items if isinstance(out, ProviderItem) else requests).append(out)
    return requests, items


# --- pure helper unit tests ------------------------------------------------ #


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("06825-1062\t", "06825-1062"),
        (" 06010-8521", "06010-8521"),
        ("06002", "06002"),
        (None, None),
    ],
)
def test_clean_zip_full(raw, expected):
    assert clean_zip_full(raw) == expected


@pytest.mark.parametrize(
    "cleaned,expected",
    [
        ("06825-1062", "06825"),
        ("06002", "06002"),
        (None, None),
        ("", None),
    ],
)
def test_zip5_from_clean(cleaned, expected):
    assert zip5_from_clean(cleaned) == expected


def test_compose_address_full():
    assert compose_address("778 Long Ridge Road", "Stamford", "06902") == "778 Long Ridge Road, Stamford, CT 06902"


def test_compose_address_keeps_dirty_zip_dash_but_no_tab():
    result = compose_address("220 Jefferson Street", "Fairfield", "06825-1062")
    assert result == "220 Jefferson Street, Fairfield, CT 06825-1062"
    assert "\t" not in result


def test_compose_address_missing_pieces():
    assert compose_address(None, None, None) is None
    assert compose_address("1 Main St", None, None) == "1 Main St, CT"


@pytest.mark.parametrize(
    "license_number,expected",
    [
        ("DCCC.15079", "Licensed Child Care Center"),
        ("DCFH.56737", "Licensed Family Child Care Home"),
        ("DCGH.80040", "Licensed Group Child Care Home"),
        ("YCYC.00424", "Licensed Youth Camp"),
        ("DCEX.80638", "License-Exempt Child Care Program"),
        ("YCEX.00001", "Exempt Youth Camp"),
        (None, None),
        ("", None),
        ("BOGUS.123", None),
    ],
)
def test_ct_license_type_from_number(license_number, expected):
    assert ct_license_type_from_number(license_number) == expected


def test_ages_from_shifts_empty_is_none_and_no_flags():
    ages, flags = ages_from_shifts([])
    assert ages is None
    assert flags == {}
    ages, flags = ages_from_shifts(None)
    assert ages is None
    assert flags == {}


def test_ages_from_shifts_skips_null_group():
    shifts = [
        {
            "population_by_age": [
                {"group": None, "label": "should be skipped"},
                {"group": "Infant", "label": "0-17 Months"},
            ]
        }
    ]
    ages, flags = ages_from_shifts(shifts)
    assert ages == "0-17 Months"
    assert flags == {"infant": True}


def test_ages_from_shifts_toddler_preschool_sets_both_flags():
    shifts = [
        {
            "population_by_age": [
                {"group": "Toddler/Preschool", "label": "18-71 Months"},
            ]
        }
    ]
    ages, flags = ages_from_shifts(shifts)
    assert flags == {"toddler": True, "preschool": True}


def test_format_hours_collapses_uniform_week():
    schedule = {
        "Friday": {"start_time": "07:15:00", "end_time": "17:30:00"},
        "Monday": {"start_time": "07:15:00", "end_time": "17:30:00"},
        "Thursday": {"start_time": "07:15:00", "end_time": "17:30:00"},
        "Tuesday": {"start_time": "07:15:00", "end_time": "17:30:00"},
        "Wednesday": {"start_time": "07:15:00", "end_time": "17:30:00"},
    }
    assert format_hours(schedule) == "Monday-Friday 07:15-17:30"


def test_format_hours_mixed_days_lists_each():
    schedule = {
        "Monday": {"start_time": "07:00:00", "end_time": "17:00:00"},
        "Saturday": {"start_time": "08:00:00", "end_time": "12:00:00"},
    }
    out = format_hours(schedule)
    assert "Monday 07:00-17:00" in out
    assert "Saturday 08:00-12:00" in out


def test_format_hours_empty_is_none():
    assert format_hours(None) is None
    assert format_hours({}) is None


def test_first_schedule_none_when_no_shifts():
    assert first_schedule([]) is None
    assert first_schedule(None) is None


def test_rates_from_shifts_skips_all_null_buckets():
    data = _load_fixture("ct_provider_center_rich.json")
    rates = rates_from_shifts(data["shifts"])
    # School Age's rates_by_age band is all-null in this fixture -> excluded.
    groups = [r["age_group"] for r in rates]
    assert "School Age" not in groups
    assert "Infant" in groups
    infant = next(r for r in rates if r["age_group"] == "Infant")
    assert infant["full_time_weekly"] == "285.00"
    assert "full_time_daily" not in infant  # null bucket omitted


@pytest.mark.parametrize(
    "start,end,max_hit,trailing,results,exp_max,exp_trail",
    [
        # A clean run of hits keeps resetting the streak.
        (1, 3, 0, 0, {1: True, 2: True, 3: True}, 3, 0),
        # Misses accumulate, then a hit resets it.
        (1, 5, 0, 0, {1: True, 2: False, 3: False, 4: True, 5: False}, 4, 1),
        # All misses just accumulate onto the running streak.
        (10, 12, 5, 2, {10: False, 11: False, 12: False}, 5, 5),
    ],
)
def test_extend_miss_streak(start, end, max_hit, trailing, results, exp_max, exp_trail):
    got_max, got_trail = extend_miss_streak(results, start, end, max_hit, trailing)
    assert (got_max, got_trail) == (exp_max, exp_trail)


# --- _item_from_provider field mapping (plan Sec 8, tests 1-11) ----------- #


def test_rich_center_full_mapping(spider):
    data = _load_fixture("ct_provider_center_rich.json")
    item = spider._item_from_provider(data, 772)

    assert item["provider_name"] == "Long Ridge Child Development Center"
    assert item["license_number"] == "DCCC.15079"
    assert item["provider_type"] == "Child Care Center"
    assert "ct_type_of_care" not in item  # equal to provider_type -> skipped
    assert item["ct_searchable"] is True
    assert item["status"] == "Listed"
    assert item["ct_licensed"] is True
    assert item["ct_license_type"] == "Licensed Child Care Center"
    assert item["address"] == "778 Long Ridge Road, Stamford, CT 06902"
    assert item["city"] == "Stamford"
    assert item["state"] == "CT"
    assert item["zip"] == "06902"
    assert item["latitude"] and item["longitude"]
    assert item["phone"] == "203-461-8653"
    assert item["email"] == "lrr@brighthorizons.com"
    assert item["provider_website"] == "http://www.brighthorizons.com"
    assert item["capacity"] == 126
    assert item["ct_capacity_three_and_under"] == 48
    assert item["ct_capacity_school_aged"] == 26
    assert item["hours"] == "Monday-Friday 07:30-18:30"
    assert item["ages_served"] == "0-17 Months, 18-35 Months, 3-5 Years, 5-12 Years"
    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is True
    assert item["scholarships_accepted"] is False
    assert item["ct_head_start"] is False
    assert item["ct_elevate_membership_level"] == "member_accredited"
    # first_name "Tj" + last_name "Mcnamara" != business name -> the naive
    # split differs from business_name, so license_holder IS set here (this
    # record is not the naive-split-guard case; contrast test_multi_shift_*).
    assert item["license_holder"] == "Tj Mcnamara"
    assert "geocode_source" not in item


def test_family_home_license_holder_set(spider):
    data = _load_fixture("ct_provider_family_home.json")
    item = spider._item_from_provider(data, 2653)
    assert item["license_holder"] == "Lisa Newman"
    assert item["provider_name"] == "Lola's Little Ones"
    assert norm.facility_category_from_type(item["provider_type"]) == "family_home"
    assert item["zip"] == "06385"


def test_multi_shift_naive_split_leaves_license_holder_unset(spider):
    data = _load_fixture("ct_provider_multi_shift.json")
    item = spider._item_from_provider(data, 970)
    assert "license_holder" not in item
    # Both shifts' populations union into the age flags.
    assert item["infant"] is True
    assert item["toddler"] is True
    assert item["preschool"] is True
    assert item["school"] is True
    # hours renders once (both shifts share the same window).
    assert item["hours"] == "Monday-Friday 07:15-17:30"


def test_camp_facility_category_other(spider):
    data = _load_fixture("ct_provider_camp.json")
    item = spider._item_from_provider(data, 594)
    assert item["provider_type"] == "Summer Camp/Program"
    assert norm.facility_category_from_type(item["provider_type"]) == "other"


def test_exempt_provider(spider):
    data = _load_fixture("ct_provider_exempt.json")
    item = spider._item_from_provider(data, 1759)
    assert item["ct_licensed"] is False
    assert item["ct_license_type"] == "License-Exempt Child Care Program"
    # Documented limitation (Sec 5.6): a DCEX record's provider_type is still
    # a plain "Child Care Center", so facility_category buckets it as
    # `center`, not `exempt`. Asserted here so a future change is deliberate.
    assert item["provider_type"] == "Child Care Center"
    assert norm.facility_category_from_type(item["provider_type"]) == "center"


def test_group_home_facility_category(spider):
    data = _load_fixture("ct_provider_group_home.json")
    item = spider._item_from_provider(data, 6925)
    assert item["provider_type"] == "Group Home"
    assert norm.facility_category_from_type(item["provider_type"]) == "group_home"


def test_zero_coords_not_set(spider):
    data = _load_fixture("ct_provider_zero_coords.json")
    item = spider._item_from_provider(data, 4225)
    assert "latitude" not in item
    assert "longitude" not in item


def test_dirty_zip_cleaned(spider):
    data = _load_fixture("ct_provider_dirty_zip.json")
    item = spider._item_from_provider(data, 2830)
    assert item["zip"] == "06825"
    assert "\t" not in item["address"]
    assert item["address"] == "220 Jefferson Street, Fairfield, CT 06825-1062"


def test_sparse_no_shifts_no_crash_no_bogus_fields(spider):
    data = _load_fixture("ct_provider_sparse_no_shifts.json")
    item = spider._item_from_provider(data, 1506)
    assert "hours" not in item
    assert "ages_served" not in item
    for flag in ("infant", "toddler", "preschool", "school"):
        assert flag not in item
    assert "license_number" not in item
    assert "ct_age_range_max_weeks" not in item  # 0 in source -> unset
    assert item["ct_age_range_min_weeks"] == 6


def test_not_searchable_status_and_naive_split(spider):
    data = _load_fixture("ct_provider_not_searchable.json")
    item = spider._item_from_provider(data, 3235)
    assert item["ct_searchable"] is False
    assert item["status"] == "Not Listed"
    assert norm.canonical_status(item["status"]) == "closed"
    # Accepted false negative (Sec 5.5): business_name IS the license
    # holder's own name here, so the naive-split rule leaves it unset.
    assert "license_holder" not in item


# --- parse_provider dispatch + hold-and-join (Sec 4.3) --------------------- #


def test_missing_id_emits_nothing(spider):
    payload = _load_fixture("ct_provider_missing_null.json")
    assert payload is None
    outputs = list(spider.parse_provider(provider_response(None, provider_id=5000)))
    assert outputs == []
    assert spider.missing == 1
    assert spider.emitted == 0


def test_violations_off_emits_immediately_with_summaries_only(spider):
    spider.do_violations = False
    data = _load_fixture("ct_provider_center_rich.json")
    outputs = list(spider.parse_provider(provider_response(data, provider_id=772)))
    requests, items = split_provider_outputs(outputs)
    assert requests == []  # zero /inspections/ requests scheduled
    assert len(items) == 1
    item = items[0]
    assert len(item["inspections"]) == 14
    assert all(isinstance(i, InspectionItem) for i in item["inspections"])
    for insp in item["inspections"]:
        assert "ct_violations" not in insp
        assert "ct_documents" not in insp
    assert item["deficiencies"] == 47
    assert spider.pending == {}
    assert spider.emitted == 1


def test_no_inspections_emits_immediately(spider):
    data = _load_fixture("ct_provider_sparse_no_shifts.json")
    outputs = list(spider.parse_provider(provider_response(data, provider_id=1506)))
    requests, items = split_provider_outputs(outputs)
    assert requests == []
    assert len(items) == 1
    assert "inspections" not in items[0]
    assert "deficiencies" not in items[0]


def test_rich_center_holds_and_fans_out_14_detail_requests(spider):
    data = _load_fixture("ct_provider_center_rich.json")
    outputs = list(spider.parse_provider(provider_response(data, provider_id=772)))
    requests, items = split_provider_outputs(outputs)
    assert items == []  # not yielded yet -- still pending
    assert len(requests) == 14
    inspection_ids = {r.meta["inspection_id"] for r in requests}
    assert len(inspection_ids) == 14
    assert 14856 in inspection_ids  # matches inspection_with_violations.json
    assert 772 in spider.pending
    assert spider.pending[772]["outstanding"] == 14
    assert spider.pending[772]["item"]["deficiencies"] == 47


def test_full_hold_and_join_cycle_emits_once_all_resolve(spider):
    """Drive the rich-center hold-and-join to completion: two real detail
    fixtures (the key-swap + report_url cases), the rest via the errback
    guard path (Sec 4.3) -- the item must still be emitted exactly once."""
    data = _load_fixture("ct_provider_center_rich.json")
    outputs = list(spider.parse_provider(provider_response(data, provider_id=772)))
    requests, _ = split_provider_outputs(outputs)
    assert len(requests) == 14

    with_violations = _load_fixture("ct_inspection_with_violations.json")
    no_violations = _load_fixture("ct_inspection_no_violations.json")
    assert with_violations["id"] == 14856
    assert no_violations["id"] == 14861

    emitted = []
    for req in requests:
        insp_id = req.meta["inspection_id"]
        if insp_id == 14856:
            emitted.extend(
                spider.parse_inspection_detail(
                    inspection_response(with_violations, provider_id=772, inspection_id=insp_id)
                )
            )
        elif insp_id == 14861:
            emitted.extend(
                spider.parse_inspection_detail(
                    inspection_response(no_violations, provider_id=772, inspection_id=insp_id)
                )
            )
        else:
            # Simulate every other detail request failing outright -- the
            # guarded failure path must still decrement the counter.
            class _Failure:
                request = req
                value = TimeoutError("boom")

            emitted.extend(spider.inspection_errback(_Failure()))

    assert len(emitted) == 1
    item = emitted[0]
    assert isinstance(item, ProviderItem)
    assert 772 not in spider.pending
    assert spider.emitted == 1
    assert spider.inspection_detail_failures == 12

    inspections_by_id = {i["ct_inspection_id"]: i for i in item["inspections"]}
    with_v = inspections_by_id[14856]
    assert len(with_v["ct_violations"]) == 11
    assert with_v["ct_violations"][0]["regulation"] == "[19a-79-10(d)(1)(A-C)]"
    assert with_v["ct_violations"][0]["statute"].startswith("In child care centers there shall be a sink")
    assert with_v["ct_violations"][0]["category"] == " Adequate sinks-handwashing-diapering/other use-food prep"
    # No "Inspection Report" doc on this inspection (only "Corrective Action
    # Plan") -> report_url stays unset.
    assert "report_url" not in with_v
    assert len(with_v["ct_documents"]) == 1

    no_v = inspections_by_id[14861]
    assert "ct_violations" not in no_v  # violations_count 0 -> sentinel row dropped
    assert len(no_v["ct_documents"]) == 1
    # This fixture's one document IS an "Inspection Report" -> report_url set.
    assert no_v["report_url"] == (
        "https://elicense.ct.gov/Lookup/ViewPublicLookupDocument.aspx"
        "?DocumentIdnt=8260380&GUID=2AE57EFA-5A20-4812-83F2-9A160518AA04"
    )


def test_inspection_id_none_still_decrements_pending(spider):
    """A summary entry with no `id` can't be fetched -- must not stall the
    provider forever (Sec 4.3 guard)."""
    item = ProviderItem()
    item["inspections"] = [InspectionItem(ct_inspection_id=1)]
    spider.pending[42] = {"item": item, "outstanding": 1}
    outputs = list(spider._maybe_emit_pending(42))
    assert len(outputs) == 1
    assert outputs[0] is item
    assert 42 not in spider.pending


# --- from_crawler concurrency (plan Sec 4.5) -------------------------------- #


@pytest.mark.parametrize("arg,expected", [(None, 8), ("8", 8), ("2", 2)])
def test_concurrency_arg_reaches_the_crawler_settings(arg, expected):
    """Regression guard: setting ``self.custom_settings`` from ``__init__``
    looks like it works but is a silent no-op -- ``Crawler.__init__`` reads
    ``custom_settings`` off the *class* before any instance exists. Assert
    against ``crawler.settings``, never the spider's own dict."""
    from scrapy.crawler import Crawler
    from scrapy.utils.project import get_project_settings

    crawler = Crawler(ConnecticutSpider, get_project_settings())
    kwargs = {"max_id": 10}
    if arg is not None:
        kwargs["concurrency"] = arg
    ConnecticutSpider.from_crawler(crawler, **kwargs)

    assert crawler.settings.getint("CONCURRENT_REQUESTS") == expected
    assert crawler.settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN") == expected


# --- max_id discovery / override (plan Sec 4.1) ----------------------------- #


def test_max_id_override_skips_discovery_and_sweeps_directly():
    spider = ConnecticutSpider(max_id=10)
    outputs = list(spider.start_requests())
    assert spider.max_id == 10
    assert len(outputs) == 10
    assert {r.meta["provider_id"] for r in outputs} == set(range(1, 11))


def test_discovery_chains_blocks_then_hands_off_to_sweep(monkeypatch):
    import provider_scrape.spiders.connecticut as ct_module

    monkeypatch.setattr(ct_module, "DISCOVERY_BLOCK_SIZE", 5)
    monkeypatch.setattr(ct_module, "MISS_STREAK_LIMIT", 6)
    monkeypatch.setattr(ct_module, "KNOWN_MAX_ID", 100)

    spider = ct_module.ConnecticutSpider()
    block1 = list(spider.start_requests())
    assert len(block1) == 5
    assert {r.meta["discovery_id"] for r in block1} == set(range(100, 105))

    # id 100 exists, 101-104 miss -> trailing_miss=4, still under the limit.
    block2 = []
    for req in block1:
        id_ = req.meta["discovery_id"]
        exists = id_ == 100
        block2.extend(spider.parse_discovery(discovery_response(id_, exists)))
    assert len(block2) == 5
    assert {r.meta["discovery_id"] for r in block2} == set(range(105, 110))

    # All 5 of the second block miss too -> trailing_miss=9 >= 6 -> stop.
    sweep_requests = []
    for req in block2:
        id_ = req.meta["discovery_id"]
        sweep_requests.extend(spider.parse_discovery(discovery_response(id_, False)))

    assert spider.max_id == 100
    assert len(sweep_requests) == 100
    assert {r.meta["provider_id"] for r in sweep_requests} == set(range(1, 101))


def test_discovery_errback_counts_as_a_miss_and_warns_below_baseline(monkeypatch, caplog):
    import provider_scrape.spiders.connecticut as ct_module

    monkeypatch.setattr(ct_module, "DISCOVERY_BLOCK_SIZE", 2)
    monkeypatch.setattr(ct_module, "MISS_STREAK_LIMIT", 2)
    monkeypatch.setattr(ct_module, "KNOWN_MAX_ID", 50)

    spider = ct_module.ConnecticutSpider()
    block1 = list(spider.start_requests())
    assert len(block1) == 2

    class _Failure:
        request = block1[0]
        value = TimeoutError("boom")

    with caplog.at_level(logging.WARNING):
        outputs = list(spider.discovery_errback(_Failure()))
        outputs.extend(spider.parse_discovery(discovery_response(block1[1].meta["discovery_id"], False)))
    # Both ids in the block missed (one via errback) -> streak hits the
    # limit immediately -> sweep begins at max_id=0 (nothing was ever a hit).
    assert spider.max_id == 0
    assert len(outputs) == 0  # range(1, 0 + 1) sweeps nothing
    assert any("BELOW the known baseline" in r.getMessage() for r in caplog.records)


# --- normalization pipeline integration (plan Sec 7) ------------------------ #


def test_not_listed_status_maps_to_closed():
    assert norm.canonical_status("Not Listed") == "closed"


@pytest.mark.parametrize(
    "provider_type,category",
    [
        ("Nursery School", "center"),
        ("Summer Camp/Program", "other"),
        ("Child Care Center", "center"),
        ("Family Child Care", "family_home"),
        ("Group Home", "group_home"),
    ],
)
def test_connecticut_facility_category_mapping(provider_type, category):
    assert norm.facility_category_from_type(provider_type) == category


def test_field_collapse_covers_all_ct_source_fields():
    item = {
        "ct_license_type": "Licensed Child Care Center",
        "ct_school_districts": ["Hart"],
        "ct_accreditations": ["NAEYC"],
        "ct_accepting_referrals": True,
        "ct_transportation": ["Near Public Bus Stop"],
        "ct_head_start": False,
    }
    norm.collapse_state_fields(item)
    assert item["license_type"] == "Licensed Child Care Center"
    assert item["school_district"] == ["Hart"]
    assert item["accreditation"] == ["NAEYC"]
    assert item["accepting_new_children"] is True
    assert item["transportation"] == ["Near Public Bus Stop"]
    assert item["head_start"] is False  # boolean-coerced, and False survives


# --- closed() logging (plan Sec 10) ----------------------------------------- #


def test_closed_warns_when_emitted_below_baseline(spider, caplog):
    spider.emitted = 100
    with caplog.at_level(logging.WARNING):
        spider.closed("finished")
    assert any("baseline" in r.getMessage() for r in caplog.records)


def test_closed_flushes_and_warns_on_leftover_pending(spider, caplog):
    spider.emitted = 7000
    spider.pending[999] = {"item": ProviderItem(), "outstanding": 1}
    with caplog.at_level(logging.WARNING):
        spider.closed("finished")
    assert any("still pending at shutdown" in r.getMessage() for r in caplog.records)


def test_closed_verify_audit_reports_gap(spider, caplog):
    spider.emitted = 7000
    spider.verify_audit = True
    spider.town_ids = {1, 2, 3}
    spider.sweep_ids = {1, 2}  # 3 was found by the town search but not swept
    with caplog.at_level(logging.WARNING):
        spider.closed("finished")
    assert any("gaps" not in r.getMessage() and "missed" in r.getMessage() for r in caplog.records)


def test_closed_verify_audit_clean_when_no_gap(spider, caplog):
    spider.emitted = 7000
    spider.verify_audit = True
    spider.town_ids = {1, 2}
    spider.sweep_ids = {1, 2, 3}
    with caplog.at_level(logging.INFO):
        spider.closed("finished")
    assert any("no gaps" in r.getMessage() for r in caplog.records)
    assert not any("missed" in r.getMessage() for r in caplog.records)


# --- optional -a verify=1 town-frontier audit (plan Sec 4.4) ---------------- #


def test_parse_town_search_harvests_ids_no_new_towns(spider):
    spider.towns_queried.add("kent")
    payload = _load_fixture("ct_search_town_kent.json")
    outputs = list(spider.parse_town_search(town_search_response(payload, town="Kent")))
    assert outputs == []  # every record's own town is "Kent" -- nothing new
    assert spider.town_ids == {2491, 6315, 3588, 7558, 3337, 2675, 6243, 1602, 1930}


def test_parse_town_search_discovers_new_town():
    spider = ConnecticutSpider()
    spider.towns_queried.add("someplace")
    payload = [{"id": 1, "town": "Newtown"}, {"id": 2, "town": "Someplace"}]
    outputs = list(spider.parse_town_search(town_search_response(payload, town="Someplace")))
    assert spider.town_ids == {1, 2}
    assert len(outputs) == 1
    assert "town=Newtown" in outputs[0].url


# --- Sec 10.1 follow-ups: zip padding, address suppression, report fallback - #


@pytest.mark.parametrize(
    "cleaned,expected",
    [
        ("6516", "06516"),  # the lost leading zero (West Haven)
        ("6516-1234", "06516-1234"),  # pad the base, leave the +4 alone
        ("06516", "06516"),  # already 5 digits -- untouched
        ("06825-1062", "06825-1062"),
        ("", ""),
        (None, None),
    ],
)
def test_pad_zip_base(cleaned, expected):
    assert pad_zip_base(cleaned) == expected


def test_pad_zip_base_feeds_zip5():
    """A 4-digit source zip must survive as a real 5-digit zip, not be dropped."""
    assert zip5_from_clean(pad_zip_base("6516")) == "06516"
    # ...and the composed address carries the padded form too.
    assert (
        compose_address("17 Lattanzi St", "West Haven", pad_zip_base("6516")) == "17 Lattanzi St, West Haven, CT 06516"
    )


@pytest.mark.parametrize(
    "street,expected",
    [
        ("This provider's address has been hidden", True),
        ("this provider's address has been HIDDEN", True),
        ("  This provider's   address has been hidden  ", True),
        ("200 Bloomfield", False),
        ("123 Main St", False),  # a placeholder, but not the sentinel
        ("", False),
        (None, False),
    ],
)
def test_is_suppressed_address(street, expected):
    assert is_suppressed_address(street) is expected


def test_suppressed_address_dropped_but_city_zip_kept(spider):
    data = _load_fixture("ct_provider_address_suppressed.json")
    item = spider._item_from_provider(data, 7578)
    assert item["ct_address_suppressed"] is True
    # the sentence must NOT be composed into `address`
    assert "address" not in item
    # ...while the real, published components survive
    assert item["city"] == "Wallingford"
    assert item["zip"] == "06492"
    assert item["state"] == "CT"


def test_address_not_suppressed_flag_is_false(spider):
    data = _load_fixture("ct_provider_center_rich.json")
    item = spider._item_from_provider(data, 772)
    assert item["ct_address_suppressed"] is False
    assert item["address"].startswith("778 Long Ridge Road")


def test_report_url_prefers_inspection_report():
    """An exact "Inspection Report" wins even when a follow-up doc is listed first."""
    insp = InspectionItem()
    ConnecticutSpider._apply_inspection_detail(
        insp,
        {
            "documents": [
                {"description": "Follow-up Inspection Report", "link": "https://x/follow"},
                {"description": "Inspection Report", "link": "https://x/main"},
            ],
        },
    )
    assert insp["report_url"] == "https://x/main"


def test_report_url_falls_back_to_followup():
    """A follow-up visit publishes only its own report -- use it (Sec 10.1)."""
    insp = InspectionItem()
    ConnecticutSpider._apply_inspection_detail(
        insp,
        {
            "documents": [
                {"description": "Corrective Action Plan", "link": "https://x/cap"},
                {"description": "Follow-up Inspection Report", "link": "https://x/follow"},
            ],
        },
    )
    assert insp["report_url"] == "https://x/follow"


def test_report_url_unset_when_no_report_document():
    """CAP / Legal Resolution / Addendum alone must not masquerade as the report."""
    insp = InspectionItem()
    ConnecticutSpider._apply_inspection_detail(
        insp,
        {
            "documents": [
                {"description": "Corrective Action Plan", "link": "https://x/cap"},
                {"description": "Legal Resolution", "link": "https://x/legal"},
                {"description": "Inspection Report Addendum", "link": "https://x/add"},
            ],
        },
    )
    assert "report_url" not in insp
    # every link is still reachable
    assert len(insp["ct_documents"]) == 3


# --- exception containment: one bad inspection must not cost the parent ---- #


def _provider_with_two_inspections(spider):
    """Put one provider into `pending` with two outstanding inspections."""
    data = _load_fixture("ct_provider_center_rich.json")
    data = dict(
        data,
        inspections=[
            {
                "id": 901,
                "visited_on": "2025-01-02T00:00:00.000Z",
                "visit_type": "UNANNOUNCED INSPECTION - FULL",
                "violations_count": 1,
                "document_count": 1,
            },
            {
                "id": 902,
                "visited_on": "2025-03-04T00:00:00.000Z",
                "visit_type": "Follow-Up Inspection",
                "violations_count": 0,
                "document_count": 0,
            },
        ],
    )
    outputs = list(spider.parse_provider(provider_response(data, provider_id=772)))
    # the two detail Requests go out; the ProviderItem is held back
    assert [o for o in outputs if isinstance(o, ProviderItem)] == []
    assert len(outputs) == 2
    assert spider.pending[772]["outstanding"] == 2
    return outputs


def test_raising_inspection_merge_still_emits_parent(spider, monkeypatch):
    """A parse exception costs that inspection's detail, never the provider."""
    _provider_with_two_inspections(spider)

    def boom(*_a, **_kw):
        raise ValueError("simulated bad inspection payload")

    monkeypatch.setattr(spider, "_merge_inspection_detail", boom)
    emitted = []
    emitted += list(
        spider.parse_inspection_detail(inspection_response({"id": 901}, provider_id=772, inspection_id=901))
    )
    assert emitted == []  # still one outstanding, nothing emitted
    emitted += list(
        spider.parse_inspection_detail(inspection_response({"id": 902}, provider_id=772, inspection_id=902))
    )

    # the parent survived both exceptions
    assert len(emitted) == 1
    item = emitted[0]
    assert item["ct_provider_id"] == 772
    assert len(item["inspections"]) == 2  # summaries intact
    assert spider.pending == {}  # nothing stranded
    assert spider.inspection_detail_failures == 2


def test_malformed_inspection_json_still_emits_parent(spider):
    """A non-JSON detail body is contained the same way."""
    _provider_with_two_inspections(spider)
    from scrapy.http import Request, TextResponse

    url = INSPECTION_URL.format(901)
    bad = TextResponse(
        url=url,
        body=b"<html>gateway error</html>",
        encoding="utf-8",
        request=Request(url, meta={"provider_id": 772, "inspection_id": 901}),
    )
    assert list(spider.parse_inspection_detail(bad)) == []
    emitted = list(spider.parse_inspection_detail(inspection_response({"id": 902}, provider_id=772, inspection_id=902)))
    assert len(emitted) == 1
    assert spider.pending == {}
    assert spider.inspection_detail_failures == 1


def test_exception_is_logged_not_swallowed(spider, monkeypatch, caplog):
    """Containment must stay visible -- a silent drop would be worse."""
    _provider_with_two_inspections(spider)
    monkeypatch.setattr(spider, "_merge_inspection_detail", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    with caplog.at_level(logging.ERROR):
        list(spider.parse_inspection_detail(inspection_response({"id": 901}, provider_id=772, inspection_id=901)))
    assert any("keeping the provider" in r.message or "keeping the provider" in r.getMessage() for r in caplog.records)
