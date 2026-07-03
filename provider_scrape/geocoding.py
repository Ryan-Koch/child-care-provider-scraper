"""Pure geocoding helpers for provider items.

Like ``normalization``, every public function here is **pure**: it takes plain
values / ``dict`` items in and returns values out, with no Scrapy objects,
network, or file I/O. All HTTP, caching, and file handling lives in the
``scripts/geocode_enrich.py`` CLI that consumes these helpers.

The backend is the free US Census Bureau batch geocoder. The flow is:

  1. :func:`build_batch_row` turns a candidate item into one CSV row for the
     batch request (``[id, street, city, state, zip]``).
  2. The CLI POSTs those rows to the Census endpoint and reads back a CSV.
  3. :func:`parse_response_line` turns one response CSV row into a result dict.
  4. :func:`apply_result` merges that result (coordinates + provenance) back
     into the item; :func:`mark_state_source` stamps provenance on records that
     already carried coordinates from their spider.

See ``tasks/geocoding_epic/geocoding_plan.md`` for the epic plan.
"""
import logging

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Census batch geocoder configuration (consumed by the CLI; kept here so the
# one geocoding module owns the backend contract).
# --------------------------------------------------------------------------- #
CENSUS_BATCH_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
)
CENSUS_BENCHMARK = "Public_AR_Current"
# Census caps a batch at 10,000 rows; we chunk smaller because large batches
# time out. Sequential chunks keep us polite to a free public service.
MAX_BATCH_SIZE = 3000

# `geocode_source` values.
SOURCE_STATE = "state"          # coordinate came from the spider / source state
SOURCE_CENSUS = "census"        # coordinate derived by the Census geocoder
SOURCE_UNMATCHED = "unmatched"  # geocoding attempted but produced no usable point


def _present(value):
    """True when a value carries a non-empty coordinate/string signal."""
    return value is not None and str(value).strip() != ""


def has_coordinates(item):
    """True when the item already has both a latitude and a longitude."""
    return _present(item.get("latitude")) and _present(item.get("longitude"))


def _strip_trailing_piece(text, piece):
    """Remove ``piece`` (and any adjacent comma/space) from the end of ``text``.

    Used to peel the parsed city/state/zip off the full address so the leftover
    is the street line. Case-insensitive; a non-match leaves ``text`` untouched.
    """
    if not piece:
        return text
    stripped = text.rstrip()
    if stripped.lower().endswith(piece.lower()):
        cut = len(stripped) - len(piece)
        return stripped[:cut].rstrip().rstrip(",").rstrip()
    return text


def split_address_for_geocode(item):
    """Best-effort ``(street, city, state, zip)`` for a Census batch row.

    Reuses the components the normalization pipeline already parsed
    (``city``/``state``/``zip``) and derives ``street`` by peeling those off the
    end of ``address``. When components are missing the whole cleaned address is
    sent as the street and the rest are blank — Census tolerates that (with a
    lower match rate). Returns ``None`` when there is no address to send.
    """
    address = item.get("address")
    if not isinstance(address, str) or not address.strip():
        return None
    address = address.strip()
    city = (str(item["city"]).strip() if _present(item.get("city")) else "")
    state = (str(item["state"]).strip() if _present(item.get("state")) else "")
    zip_code = (str(item["zip"]).strip() if _present(item.get("zip")) else "")

    street = address
    for piece in (zip_code, state, city):
        street = _strip_trailing_piece(street, piece)
    # Never send an empty street (e.g. an address that was only "City, ST ZIP").
    if not street:
        street = address
    return (street, city, state, zip_code)


def build_batch_row(unique_id, item):
    """Return ``[id, street, city, state, zip]`` for the batch CSV, or ``None``.

    ``None`` means the item has no usable address and should be skipped (its
    ``geocode_source`` stays unset). ``unique_id`` is assigned by the caller and
    is what ties a response row back to its item.
    """
    parts = split_address_for_geocode(item)
    if parts is None:
        return None
    street, city, state, zip_code = parts
    return [str(unique_id), street, city, state, zip_code]


def cache_key(row):
    """Stable cache key for a batch row (ignores the leading id).

    ``row`` is ``[id, street, city, state, zip]``. Values are lowercased and
    whitespace-collapsed so identical addresses across states and runs share a
    single cache entry (and are queried once).
    """
    parts = (" ".join(str(p).split()).lower() for p in row[1:])
    return "|".join(parts)


def parse_response_line(fields):
    """Normalize one Census batch response row (already CSV-split) to a dict.

    The Census output has no header and a variable column count: a matched row
    has 8 columns, while ``No_Match`` / ``Tie`` rows have only the first 3. The
    coordinate column is ``"longitude,latitude"`` (X,Y) — this is the one place
    the axes must be swapped into our ``latitude`` / ``longitude``.

    Returns ``{id, match, match_type, matched_address, latitude, longitude}``
    (missing pieces are ``None``), or ``None`` for an empty row.
    """
    if not fields:
        return None
    result = {
        "id": fields[0].strip() if len(fields) > 0 else None,
        "match": fields[2].strip() if len(fields) > 2 else None,
        "match_type": fields[3].strip() if len(fields) > 3 else None,
        "matched_address": fields[4].strip() if len(fields) > 4 else None,
        "latitude": None,
        "longitude": None,
    }
    if len(fields) > 5 and fields[5].strip():
        coords = fields[5].split(",")
        if len(coords) == 2:
            longitude, latitude = coords[0].strip(), coords[1].strip()
            if longitude and latitude:
                # Census returns X,Y (lon,lat); store swapped into our fields.
                result["longitude"] = longitude
                result["latitude"] = latitude
            else:
                logger.warning(
                    "parse_response_line: blank coordinate in %r", fields)
    return result


def apply_result(item, result):
    """Merge one parsed geocode ``result`` back into ``item`` (in place).

    Sets ``geocode_source`` / ``geocode_confidence`` from the match, and fills
    ``latitude`` / ``longitude`` on a successful match. Never overwrites
    coordinates the item already has (defensive; such items are not sent to the
    geocoder in the first place). Returns the same ``item``.
    """
    match = result.get("match")
    has_point = _present(result.get("latitude")) and _present(
        result.get("longitude"))
    if match == "Match" and has_point:
        item["geocode_source"] = SOURCE_CENSUS
        item["geocode_confidence"] = (
            "exact" if result.get("match_type") == "Exact" else "approximate")
        if not has_coordinates(item):
            item["latitude"] = result["latitude"]
            item["longitude"] = result["longitude"]
    elif match == "Tie":
        item["geocode_source"] = SOURCE_UNMATCHED
        item["geocode_confidence"] = "tie"
    else:  # "No_Match", or a "Match" that carried no usable coordinate.
        item["geocode_source"] = SOURCE_UNMATCHED
        item["geocode_confidence"] = "no_match"
    return item


def mark_state_source(item):
    """Stamp ``geocode_source='state'`` on an item that already has coordinates.

    Called for records the geocoder skips because their spider supplied
    coordinates, so provenance is complete for every record. Returns the item.
    """
    if has_coordinates(item):
        item["geocode_source"] = SOURCE_STATE
    return item
