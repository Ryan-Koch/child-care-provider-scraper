"""Unit tests for provider_scrape.geocoding (pure helpers, no network).

Run with the project virtualenv: ``.venv/bin/pytest``.

Covers the golden path plus the ambiguous / missing-data cases the plan calls
out, and explicitly guards the Census ``lon,lat`` -> our ``lat,long`` swap.
"""
from provider_scrape import geocoding


# --------------------------------------------------------------------------- #
# has_coordinates
# --------------------------------------------------------------------------- #
def test_has_coordinates_true_when_both_present():
    assert geocoding.has_coordinates(
        {"latitude": "39.78", "longitude": "-89.65"}) is True


def test_has_coordinates_false_when_one_missing():
    assert geocoding.has_coordinates(
        {"latitude": "39.78", "longitude": None}) is False


def test_has_coordinates_false_on_empty_strings():
    assert geocoding.has_coordinates(
        {"latitude": "  ", "longitude": ""}) is False


def test_has_coordinates_false_on_empty_item():
    assert geocoding.has_coordinates({}) is False


# --------------------------------------------------------------------------- #
# split_address_for_geocode / build_batch_row
# --------------------------------------------------------------------------- #
def test_split_uses_parsed_components_and_peels_street():
    item = {
        "address": "123 Main St, Springfield, IL 62704",
        "city": "Springfield", "state": "IL", "zip": "62704",
    }
    assert geocoding.split_address_for_geocode(item) == (
        "123 Main St", "Springfield", "IL", "62704")


def test_split_falls_back_to_full_address_when_unparsed():
    item = {"address": "123 Main St Apt 4"}
    assert geocoding.split_address_for_geocode(item) == (
        "123 Main St Apt 4", "", "", "")


def test_split_keeps_full_address_when_street_would_be_empty():
    # Address is only city/state/zip: street must not come back empty.
    item = {
        "address": "Springfield, IL 62704",
        "city": "Springfield", "state": "IL", "zip": "62704",
    }
    street, city, state, zip_code = geocoding.split_address_for_geocode(item)
    assert street == "Springfield, IL 62704"
    assert (city, state, zip_code) == ("Springfield", "IL", "62704")


def test_split_returns_none_without_address():
    assert geocoding.split_address_for_geocode({"city": "Springfield"}) is None
    assert geocoding.split_address_for_geocode({"address": "   "}) is None


def test_build_batch_row_prefixes_id():
    item = {
        "address": "123 Main St, Springfield, IL 62704",
        "city": "Springfield", "state": "IL", "zip": "62704",
    }
    assert geocoding.build_batch_row(7, item) == [
        "7", "123 Main St", "Springfield", "IL", "62704"]


def test_build_batch_row_none_when_no_address():
    assert geocoding.build_batch_row(1, {"provider_name": "X"}) is None


# --------------------------------------------------------------------------- #
# parse_response_line
# --------------------------------------------------------------------------- #
def test_parse_match_exact_swaps_lon_lat():
    fields = [
        "3", "123 Main St, Springfield, IL, 62704", "Match", "Exact",
        "123 MAIN ST, SPRINGFIELD, IL, 62704", "-89.6501,39.7817",
        "12345678", "L",
    ]
    result = geocoding.parse_response_line(fields)
    assert result["id"] == "3"
    assert result["match"] == "Match"
    assert result["match_type"] == "Exact"
    # The axis swap is the whole point: Census gives lon,lat.
    assert result["latitude"] == "39.7817"
    assert result["longitude"] == "-89.6501"


def test_parse_match_non_exact():
    fields = [
        "1", "in", "Match", "Non_Exact", "matched", "-90.1,40.2", "id", "R",
    ]
    result = geocoding.parse_response_line(fields)
    assert result["match"] == "Match"
    assert result["match_type"] == "Non_Exact"
    assert (result["latitude"], result["longitude"]) == ("40.2", "-90.1")


def test_parse_no_match_short_row():
    result = geocoding.parse_response_line(["9", "bogus address", "No_Match"])
    assert result["match"] == "No_Match"
    assert result["match_type"] is None
    assert result["latitude"] is None and result["longitude"] is None


def test_parse_tie_short_row():
    result = geocoding.parse_response_line(["4", "ambiguous", "Tie"])
    assert result["match"] == "Tie"
    assert result["latitude"] is None


def test_parse_empty_row_is_none():
    assert geocoding.parse_response_line([]) is None


# --------------------------------------------------------------------------- #
# apply_result
# --------------------------------------------------------------------------- #
def test_apply_result_exact_fills_coords_and_provenance():
    item = {"latitude": None, "longitude": None}
    result = {
        "match": "Match", "match_type": "Exact",
        "latitude": "39.7817", "longitude": "-89.6501",
    }
    geocoding.apply_result(item, result)
    assert item["latitude"] == "39.7817"
    assert item["longitude"] == "-89.6501"
    assert item["geocode_source"] == "census"
    assert item["geocode_confidence"] == "exact"


def test_apply_result_non_exact_is_approximate():
    item = {}
    result = {
        "match": "Match", "match_type": "Non_Exact",
        "latitude": "40.2", "longitude": "-90.1",
    }
    geocoding.apply_result(item, result)
    assert item["geocode_confidence"] == "approximate"
    assert item["geocode_source"] == "census"


def test_apply_result_no_match_leaves_coords_empty():
    item = {"latitude": None, "longitude": None}
    geocoding.apply_result(item, {"match": "No_Match"})
    assert item["latitude"] is None and item["longitude"] is None
    assert item["geocode_source"] == "unmatched"
    assert item["geocode_confidence"] == "no_match"


def test_apply_result_tie():
    item = {}
    geocoding.apply_result(item, {"match": "Tie"})
    assert item["geocode_source"] == "unmatched"
    assert item["geocode_confidence"] == "tie"


def test_apply_result_never_overwrites_existing_coords():
    item = {"latitude": "1.0", "longitude": "2.0"}
    result = {
        "match": "Match", "match_type": "Exact",
        "latitude": "39.7817", "longitude": "-89.6501",
    }
    geocoding.apply_result(item, result)
    # Provenance is still marked census, but the spider's coords are preserved.
    assert item["latitude"] == "1.0" and item["longitude"] == "2.0"


# --------------------------------------------------------------------------- #
# mark_state_source
# --------------------------------------------------------------------------- #
def test_mark_state_source_sets_state_when_coords_present():
    item = {"latitude": "1.0", "longitude": "2.0"}
    geocoding.mark_state_source(item)
    assert item["geocode_source"] == "state"


def test_mark_state_source_noop_without_coords():
    item = {"latitude": None, "longitude": None}
    geocoding.mark_state_source(item)
    assert "geocode_source" not in item
