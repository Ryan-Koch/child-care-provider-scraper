"""I/O tests for scripts/geocode_enrich.py (no network).

Focuses on the format layer added for CSV support: reading/writing JSON and CSV,
CSV column ordering, and a full ``enrich_file`` round-trip on a CSV with the
Census HTTP call stubbed out. The pure geocoding decisions are covered by
``provider_scrape/spiders/test_geocoding.py``.

Run with the project virtualenv: ``.venv/bin/pytest scripts/test_geocode_enrich.py``.
"""
import csv
import json

import geocode_enrich


# --------------------------------------------------------------------------- #
# format detection + read/write round-trips
# --------------------------------------------------------------------------- #
def test_is_csv_by_extension():
    assert geocode_enrich._is_csv("state_output/ohio.csv") is True
    assert geocode_enrich._is_csv("state_output/OHIO.CSV") is True
    assert geocode_enrich._is_csv("state_output/ohio.json") is False


def test_read_csv_returns_string_records_and_header(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text(
        "provider_name,address,latitude,longitude\n"
        "Acme,123 Main St,,\n"
        "Bright Kids,,40.1,-81.5\n",
        encoding="utf-8")

    records, fieldnames = geocode_enrich._read_records(str(path))

    assert fieldnames == ["provider_name", "address", "latitude", "longitude"]
    assert records[0] == {
        "provider_name": "Acme", "address": "123 Main St",
        "latitude": "", "longitude": ""}
    # An empty cell reads back as "" so has_coordinates() treats it as missing.
    assert records[1]["latitude"] == "40.1"


def test_read_json_returns_records_and_no_header(tmp_path):
    path = tmp_path / "in.json"
    path.write_text(json.dumps([{"provider_name": "Acme"}]), encoding="utf-8")

    records, fieldnames = geocode_enrich._read_records(str(path))

    assert fieldnames is None
    assert records == [{"provider_name": "Acme"}]


def test_csv_fieldnames_appends_new_keys_after_header():
    records = [
        {"provider_name": "A", "geocode_source": "state"},
        {"provider_name": "B", "geocode_source": "census",
         "geocode_confidence": "exact"},
    ]
    base = ["provider_name", "address"]
    assert geocode_enrich._csv_fieldnames(records, base) == [
        "provider_name", "address", "geocode_source", "geocode_confidence"]


def test_write_csv_preserves_header_order_and_fills_missing(tmp_path):
    path = tmp_path / "out.csv"
    records = [
        {"provider_name": "A", "geocode_source": "census"},
        {"provider_name": "B"},  # missing geocode_source -> blank cell
    ]
    geocode_enrich._write_records(str(path), records, ["provider_name"])

    rows = list(csv.reader(path.read_text(encoding="utf-8").splitlines()))
    assert rows[0] == ["provider_name", "geocode_source"]
    assert rows[1] == ["A", "census"]
    assert rows[2] == ["B", ""]


# --------------------------------------------------------------------------- #
# enrich_file end-to-end on CSV, with the Census call stubbed
# --------------------------------------------------------------------------- #
def _args(**overrides):
    args = geocode_enrich.build_arg_parser().parse_args(["placeholder", "--no-cache"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_enrich_file_csv_end_to_end(tmp_path, monkeypatch):
    path = tmp_path / "ohio.csv"
    path.write_text(
        "provider_name,address,city,state,zip,latitude,longitude\n"
        "Has Coords,1 A St,Columbus,OH,43004,40.0,-82.0\n"          # -> state
        "Needs Geocode,123 Main St,Springfield,IL,62704,,\n"       # -> census
        "No Address,,,,,,\n",                                       # -> skipped
        encoding="utf-8")

    # The single candidate is query position "0"; return a canned Census match.
    def fake_post(chunk, *rest):
        return [["0", "in", "Match", "Exact", "123 MAIN ST",
                 "-89.6501,39.7817", "id", "L"]]

    monkeypatch.setattr(geocode_enrich, "_post_batch", fake_post)

    counters = geocode_enrich.enrich_file(
        str(path), geocode_enrich.NullCache(), _args())

    assert counters["state"] == 1
    assert counters["skipped_no_address"] == 1
    assert counters["geocoded_exact"] == 1

    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    # New provenance columns are appended after the originals.
    assert rows[0].keys() >= {"geocode_source", "geocode_confidence"}
    by_name = {r["provider_name"]: r for r in rows}
    assert by_name["Has Coords"]["geocode_source"] == "state"
    assert by_name["Needs Geocode"]["geocode_source"] == "census"
    assert by_name["Needs Geocode"]["geocode_confidence"] == "exact"
    assert by_name["Needs Geocode"]["latitude"] == "39.7817"
    assert by_name["Needs Geocode"]["longitude"] == "-89.6501"
    # A record with no usable address gets no provenance stamp (blank cell).
    assert by_name["No Address"]["geocode_source"] == ""


def test_enrich_file_csv_to_json_conversion(tmp_path, monkeypatch):
    # -o with a different extension converts format; no network needed here
    # because the only record already has coordinates.
    src = tmp_path / "in.csv"
    src.write_text(
        "provider_name,latitude,longitude\nAcme,40.0,-82.0\n", encoding="utf-8")
    dst = tmp_path / "out.json"

    geocode_enrich.enrich_file(
        str(src), geocode_enrich.NullCache(), _args(output=str(dst)))

    written = json.loads(dst.read_text(encoding="utf-8"))
    assert written[0]["geocode_source"] == "state"
