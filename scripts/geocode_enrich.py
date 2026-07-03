"""Post-run geocoding enrichment for scraped state output (JSON).

Reads a state's scrape output, fills ``latitude`` / ``longitude`` for records
that lack them by querying the free US Census Bureau batch geocoder, records
provenance (``geocode_source`` / ``geocode_confidence``), and writes the result
back. Records that already carry coordinates are stamped ``source="state"``;
records with no usable address are skipped.

All the pure decision logic lives in ``provider_scrape/geocoding.py``; this file
owns the I/O: reading/writing JSON, the SQLite cache, and the HTTP calls.

Usage:
    .venv/bin/python scripts/geocode_enrich.py state_output/ohio.json
    .venv/bin/python scripts/geocode_enrich.py -o out.json ohio.json alabama.json
    .venv/bin/python scripts/geocode_enrich.py --dry-run --limit 100 texas.json

See ``tasks/geocoding_epic/geocoding_plan.md``.
"""
import argparse
import csv
import io
import json
import logging
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from provider_scrape import geocoding  # noqa: E402

logger = logging.getLogger("geocode_enrich")

DEFAULT_CACHE_PATH = os.path.join(REPO_ROOT, "geocode_cache.sqlite")
DEFAULT_TIMEOUT = 120       # seconds; Census batch responses can be slow
DEFAULT_MAX_RETRIES = 4
BACKOFF_BASE = 3            # seconds; exponential: 3, 6, 12, ...


# --------------------------------------------------------------------------- #
# SQLite cache
# --------------------------------------------------------------------------- #
class GeocodeCache:
    """Address-keyed cache of geocode outcomes (incl. unmatched, to skip retry).

    Stores the *outcome* (source/confidence/coords), not the raw Census row, so
    a hit can be applied to an item directly.
    """

    def __init__(self, path):
        # A generous busy timeout + WAL keeps the shared cache safe when
        # run_spiders.sh runs several geocode steps in parallel (each writing
        # this one file).
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS geocode_cache ("
            " address_key TEXT PRIMARY KEY,"
            " latitude TEXT, longitude TEXT,"
            " source TEXT, confidence TEXT,"
            " matched_address TEXT, fetched_at TEXT)"
        )
        self.conn.commit()

    def get(self, key):
        cur = self.conn.execute(
            "SELECT latitude, longitude, source, confidence"
            " FROM geocode_cache WHERE address_key = ?", (key,))
        row = cur.fetchone()
        if row is None:
            return None
        return {"latitude": row[0], "longitude": row[1],
                "source": row[2], "confidence": row[3]}

    def put(self, key, latitude, longitude, source, confidence, matched):
        self.conn.execute(
            "INSERT OR REPLACE INTO geocode_cache"
            " (address_key, latitude, longitude, source, confidence,"
            "  matched_address, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (key, latitude, longitude, source, confidence, matched,
             datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def close(self):
        self.conn.close()


class NullCache:
    """Drop-in cache that never hits (for ``--no-cache``)."""

    def get(self, key):
        return None

    def put(self, *args):
        pass

    def close(self):
        pass


# --------------------------------------------------------------------------- #
# Census batch client
# --------------------------------------------------------------------------- #
def _chunks(seq, size):
    for start in range(0, len(seq), size):
        yield seq[start:start + size]


def _rows_to_csv(rows):
    """Serialize batch rows to a CSV string (no header), quoting as needed."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerows(rows)
    return buffer.getvalue()


def _post_batch(rows, benchmark, timeout, max_retries):
    """POST one chunk of rows to Census; return parsed CSV response rows.

    Retries on network errors / 5xx with exponential backoff. Raises the last
    exception if every attempt fails (the caller decides how fatal that is).
    """
    payload = _rows_to_csv(rows).encode("utf-8")
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                geocoding.CENSUS_BATCH_URL,
                files={"addressFile": ("addresses.csv", payload, "text/csv")},
                data={"benchmark": benchmark},
                timeout=timeout,
            )
            if response.status_code >= 500:
                raise requests.HTTPError(
                    "Census returned %s" % response.status_code)
            response.raise_for_status()
            return list(csv.reader(io.StringIO(response.text)))
        except (requests.RequestException, ) as error:
            last_error = error
            if attempt < max_retries:
                delay = BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning(
                    "Census batch attempt %d/%d failed (%s); retrying in %ds",
                    attempt, max_retries, error, delay)
                time.sleep(delay)
    raise last_error


# --------------------------------------------------------------------------- #
# Enrichment of one file
# --------------------------------------------------------------------------- #
def _outcome_bucket(record):
    """Fine-grained stats bucket for an enriched (non-state) record."""
    source = record.get("geocode_source")
    confidence = record.get("geocode_confidence")
    if source == geocoding.SOURCE_CENSUS:
        return "geocoded_exact" if confidence == "exact" else "geocoded_approx"
    return "unmatched_tie" if confidence == "tie" else "unmatched_no_match"


def _apply_cached(record, cached):
    """Apply a cached outcome to a candidate record (no network)."""
    record["geocode_source"] = cached["source"]
    record["geocode_confidence"] = cached["confidence"]
    if (cached.get("latitude") and cached.get("longitude")
            and not geocoding.has_coordinates(record)):
        record["latitude"] = cached["latitude"]
        record["longitude"] = cached["longitude"]


def enrich_records(records, cache, args):
    """Enrich a list of records in place; return a counters dict."""
    counters = defaultdict(int)

    # 1. Partition: already-has-coords (stamp state), no-address (skip), or a
    #    geocoding candidate.
    candidates = []  # (index, row, key)
    for index, record in enumerate(records):
        if geocoding.has_coordinates(record):
            geocoding.mark_state_source(record)
            counters["state"] += 1
            continue
        row = geocoding.build_batch_row(index, record)
        if row is None:
            counters["skipped_no_address"] += 1
            continue
        candidates.append((index, row, geocoding.cache_key(row)))
    counters["candidates"] = len(candidates)

    # 2. Cache pass: apply hits, collect misses grouped by unique address key so
    #    a repeated address is only queried once.
    key_to_indices = defaultdict(list)
    representative_row = {}
    for index, row, key in candidates:
        cached = cache.get(key)
        if cached is not None:
            _apply_cached(records[index], cached)
            counters["cache_hit"] += 1
            counters[_outcome_bucket(records[index])] += 1
            continue
        key_to_indices[key].append(index)
        representative_row.setdefault(key, row)

    unique_keys = list(key_to_indices)
    if args.limit is not None:
        unique_keys = unique_keys[:args.limit]
    counters["to_query"] = len(unique_keys)
    counters["dedup_saved"] = (
        sum(len(key_to_indices[k]) for k in unique_keys) - len(unique_keys))

    if args.dry_run or not unique_keys:
        if args.dry_run:
            logger.info("Dry run: would query %d unique address(es).",
                        len(unique_keys))
        return counters

    # 3. Network pass. Query rows carry a positional id into ``unique_keys`` so
    #    responses map back regardless of order.
    query_rows = [
        [str(position)] + representative_row[key][1:]
        for position, key in enumerate(unique_keys)
    ]
    total_chunks = (len(query_rows) + args.batch_size - 1) // args.batch_size
    for chunk_number, chunk in enumerate(_chunks(query_rows, args.batch_size), 1):
        logger.info("Census batch %d/%d: querying %d addresses",
                    chunk_number, total_chunks, len(chunk))
        try:
            response_rows = _post_batch(
                chunk, args.benchmark, args.timeout, args.max_retries)
        except requests.RequestException as error:
            logger.error("Census batch %d/%d failed permanently: %s; leaving "
                         "these records unresolved", chunk_number, total_chunks,
                         error)
            counters["query_failed"] += len(chunk)
            continue
        results_by_id = {}
        for fields in response_rows:
            parsed = geocoding.parse_response_line(fields)
            if parsed and parsed.get("id") is not None:
                results_by_id[parsed["id"]] = parsed

        # Each query row's id (row[0]) is its position into ``unique_keys``.
        for row in chunk:
            unique_key = unique_keys[int(row[0])]
            result = results_by_id.get(row[0]) or {"match": "No_Match"}
            for index in key_to_indices[unique_key]:
                geocoding.apply_result(records[index], result)
                counters[_outcome_bucket(records[index])] += 1
            enriched = records[key_to_indices[unique_key][0]]
            is_census = (
                enriched.get("geocode_source") == geocoding.SOURCE_CENSUS)
            cache.put(
                unique_key,
                enriched.get("latitude") if is_census else None,
                enriched.get("longitude") if is_census else None,
                enriched.get("geocode_source"),
                enriched.get("geocode_confidence"),
                result.get("matched_address"))
    return counters


def _print_stats(path, counters):
    total = (counters["state"] + counters["skipped_no_address"]
             + counters["candidates"])
    geocoded = counters["geocoded_exact"] + counters["geocoded_approx"]
    unmatched = (counters["unmatched_tie"] + counters["unmatched_no_match"]
                 + counters["query_failed"])
    attempted = geocoded + unmatched
    rate = (100.0 * geocoded / attempted) if attempted else 0.0
    print("")
    print("=== %s ===" % path)
    print("  total records         : %d" % total)
    print("  state-provided coords : %d" % counters["state"])
    print("  no usable address     : %d" % counters["skipped_no_address"])
    print("  geocode candidates    : %d" % counters["candidates"])
    print("    cache hits          : %d" % counters["cache_hit"])
    print("    duplicates deduped  : %d" % counters["dedup_saved"])
    print("    geocoded (exact)    : %d" % counters["geocoded_exact"])
    print("    geocoded (approx)   : %d" % counters["geocoded_approx"])
    print("    unmatched (no_match): %d" % counters["unmatched_no_match"])
    print("    unmatched (tie)     : %d" % counters["unmatched_tie"])
    if counters["query_failed"]:
        print("    query failed        : %d" % counters["query_failed"])
    print("  match rate (of attempted): %.1f%%" % rate)


def enrich_file(path, cache, args):
    with open(path, "r", encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        raise ValueError("%s is not a JSON array of records" % path)

    counters = enrich_records(records, cache, args)

    out_path = args.output or path
    if not args.dry_run:
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(records, handle, ensure_ascii=False, indent=2)
        logger.info("Wrote %s", out_path)
    _print_stats(path, counters)
    return counters


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("inputs", nargs="+",
                        help="state output JSON file(s) to enrich")
    parser.add_argument("-o", "--output",
                        help="write to this path instead of in place "
                             "(only valid with a single input)")
    parser.add_argument("--cache", default=DEFAULT_CACHE_PATH,
                        help="SQLite cache path (default: %(default)s)")
    parser.add_argument("--no-cache", action="store_true",
                        help="ignore the cache (do not read or write it)")
    parser.add_argument("--benchmark", default=geocoding.CENSUS_BENCHMARK,
                        help="Census benchmark (default: %(default)s)")
    parser.add_argument("--batch-size", type=int,
                        default=geocoding.MAX_BATCH_SIZE,
                        help="addresses per Census request (default: %(default)s)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="per-request timeout seconds (default: %(default)s)")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help="retries per chunk (default: %(default)s)")
    parser.add_argument("--limit", type=int,
                        help="cap the number of unique addresses queried "
                             "(for testing on large files)")
    parser.add_argument("--dry-run", action="store_true",
                        help="partition and report only; no network, no write")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="debug-level logging")
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.output and len(args.inputs) > 1:
        build_arg_parser().error("-o/--output is only valid with one input")

    cache = NullCache() if args.no_cache else GeocodeCache(args.cache)
    try:
        for path in args.inputs:
            enrich_file(path, cache, args)
    finally:
        cache.close()


if __name__ == "__main__":
    main()
