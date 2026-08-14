#!/bin/bash
LOG_LEVEL='INFO'
# Max retries for each spider individually
MAX_RETRIES=6
# Default concurrency (override with -c)
DEFAULT_CONCURRENCY=5
CONCURRENCY=$DEFAULT_CONCURRENCY
# Default directory path (override with -d). An OUTPUT_DIR set in the
# environment changes the default while still letting -d win — the Docker image
# uses this to point output at a mounted volume.
DEFAULT_OUTPUT_DIR="./"
OUTPUT_DIR="${OUTPUT_DIR:-$DEFAULT_OUTPUT_DIR}"
# Default format (override with -f)
DEFAULT_FORMAT="json"
FORMAT=$DEFAULT_FORMAT
# Post-run geocoding enrichment (opt-in with -g); enriches each format from -f.
GEOCODE=false
GEOCODE_SCRIPT="$(dirname "$0")/scripts/geocode_enrich.py"
# Optional shared geocode cache path (opt-in via env). Unset -> the enrich
# script uses its own default (repo-root geocode_cache.sqlite). Point it at a
# mounted volume to persist the cache across runs (the Docker image does this).
GEOCODE_CACHE="${GEOCODE_CACHE:-}"
# Post-run upload of output files to Hugging Face (opt-in with -u); runs once
# after all spiders finish. Repo/token come from huggingface.env.
UPLOAD=false
UPLOAD_SCRIPT="$(dirname "$0")/scripts/upload_to_huggingface.py"
# Generates the per-state source-provenance doc shipped with the upload (-u).
SOURCES_SCRIPT="$(dirname "$0")/scripts/generate_sources.py"
# Optional pre-run refresh of the Webshare proxy pool: pulls the account's
# current ip:port list into webshare.env before crawling (Webshare rotates IPs
# as they lose reputation or go down). No-ops if webshare.env is absent.
# Triggered by the -p flag OR a truthy REFRESH_PROXIES in the environment — the
# Docker image sets the latter (see docker-compose.yml) so a container run
# refreshes the writable, bind-mounted webshare.env automatically at startup.
REFRESH_PROXIES="${REFRESH_PROXIES:-false}"
PROXY_SCRIPT="$(dirname "$0")/scripts/update_webshare_proxies.py"
WEBSHARE_ENV="$(dirname "$0")/webshare.env"
# Space-separated list of spiders that require a virtual display
XVFB_SPIDERS="new_jersey rhode_island arizona wisconsin"

usage() {
  echo "Usage: $0 [-c concurrency] [spider ...]" >&2
  echo "  -c   number of spiders to run in parallel (default: $DEFAULT_CONCURRENCY)" >&2
  echo "  -d   directory to use for spider logging and output files (default: $DEFAULT_OUTPUT_DIR)" >&2
  echo "  -f   output format(s): json, csv, or both as a comma/space list, e.g. -f json,csv (default: $DEFAULT_FORMAT)" >&2
  echo "  -g   after each spider, geocode records missing coordinates (enriches each -f format)" >&2
  echo "  -p   before crawling, refresh the Webshare proxy pool in webshare.env (no-op if absent)" >&2
  echo "  -u   after all spiders finish, upload the output files to a Hugging Face dataset" >&2
  echo "  spider names default to the output of 'scrapy list'" >&2
}

while getopts ":c:d:f:gpuh" opt; do
  case $opt in
  c) CONCURRENCY=$OPTARG ;;
  d) OUTPUT_DIR=$OPTARG ;;
  f) FORMAT=$OPTARG ;;
  g) GEOCODE=true ;;
  p) REFRESH_PROXIES=true ;;
  u) UPLOAD=true ;;
  h)
    usage
    exit 0
    ;;
  \?)
    usage
    exit 1
    ;;
  esac
done
shift $((OPTIND - 1))

if [ $# -gt 0 ]; then
  SPIDERS_TO_RUN=("$@")
  echo "Using provided spider list: ${SPIDERS_TO_RUN[*]}"
else
  echo "No spiders specified, discovering via scrapy list..."
  SPIDERS_TO_RUN=($(scrapy list))
fi
echo "Found ${#SPIDERS_TO_RUN[@]} spiders to run: ${SPIDERS_TO_RUN[*]}"
echo "Running with concurrency: $CONCURRENCY"

# Force to lower case because I prefer it
if [ -n "$FORMAT" ]; then
  FORMAT=${FORMAT,,}
fi

# -f accepts one or more formats separated by commas or spaces (e.g. "json",
# "csv", or "json,csv") so a single crawl can emit every requested format.
# Normalize to a validated, de-duped, space-separated list.
FORMAT=${FORMAT//,/ }
FORMATS=()
for fmt in $FORMAT; do
  case $fmt in
  json | csv)
    [[ " ${FORMATS[*]} " == *" $fmt "* ]] || FORMATS+=("$fmt")
    ;;
  *)
    echo "Invalid format '$fmt': use json, csv, or both (e.g. -f json,csv)." >&2
    exit 1
    ;;
  esac
done
if [ ${#FORMATS[@]} -eq 0 ]; then
  echo "No valid output format specified (use json, csv, or both)." >&2
  exit 1
fi
# Space-separated list; run_spider re-splits it into an array (arrays don't
# survive the export into the xargs subshells).
FORMAT="${FORMATS[*]}"

# Make sure we use a trailing slash in the path
if [ $OUTPUT_DIR ]; then
  [[ "${OUTPUT_DIR}" != */ ]] && OUTPUT_DIR="${OUTPUT_DIR}/"
fi

run_spider() {
  local spider_name=$1
  local log_file="${spider_name}.log"
  local retry_count=0

  # One -O target per requested format lets a single crawl emit them all.
  local formats=($FORMAT)
  local output_args=()
  local fmt
  for fmt in "${formats[@]}"; do
    output_args+=(-O "${OUTPUT_DIR}${spider_name}.${fmt}")
  done

  # the loop where we'll run a spider and do retries if needed.
  while [ $retry_count -lt $MAX_RETRIES ]; do
    echo "Crawling $spider_name..."
    local cmd_prefix=()
    grep -qw "$spider_name" <<<"$XVFB_SPIDERS" && cmd_prefix=(xvfb-run -a -s "-screen 0 1920x1080x24")
    "${cmd_prefix[@]}" scrapy crawl $spider_name \
      "${output_args[@]}" \
      -s LOG_FILE="${OUTPUT_DIR}${log_file}" \
      -s LOG_LEVEL=$LOG_LEVEL \
      -s LOG_FILE_APPEND=False

    if grep -q "Retryable Error" "${OUTPUT_DIR}${log_file}"; then
      echo "Retryable Error detected for $spider_name."
      retry_count=$((retry_count + 1))
      echo "Retrying $spider_name in 5 seconds. Retry count: $retry_count"
      sleep 5
    else
      echo "Crawling $spider_name completed successfully."
      # Geocoding is best-effort enrichment: a failure here must not fail
      # the (already successful) scrape, so we swallow its exit status.
      if [ "$GEOCODE" = true ]; then
        # Enrich every format we emitted. The shared geocode cache means
        # the second file's addresses are already resolved, so a both-run
        # costs one network pass, not one per format.
        echo "Geocoding $spider_name..."
        # Opt-in: persist/reuse the cache at a caller-provided path (e.g.
        # a mounted volume). Unset -> the script's default location.
        local cache_args=()
        [ -n "$GEOCODE_CACHE" ] && cache_args=(--cache "$GEOCODE_CACHE")
        local geocode_ok=true
        for fmt in "${formats[@]}"; do
          python "$GEOCODE_SCRIPT" \
            "${OUTPUT_DIR}${spider_name}.${fmt}" \
            "${cache_args[@]}" \
            >>"${OUTPUT_DIR}${log_file}" 2>&1 || geocode_ok=false
        done
        if [ "$geocode_ok" = true ]; then
          echo "Geocoding $spider_name completed."
        else
          echo "Geocoding $spider_name failed (see ${log_file})."
        fi
      fi
      return 0
    fi
  done

  echo "Failed to crawl $spider_name after $MAX_RETRIES attempts."
  return 1
}

export -f run_spider
export LOG_LEVEL
export MAX_RETRIES
export OUTPUT_DIR FORMAT XVFB_SPIDERS
export GEOCODE GEOCODE_SCRIPT GEOCODE_CACHE

# Optional pre-run proxy refresh (-p flag or a truthy REFRESH_PROXIES env var).
# Best-effort and only when webshare.env exists — a stale/empty fetch leaves the
# existing list untouched (the script exits non-zero but we don't abort the run
# over it). Bannered so it's clearly visible in `docker compose run` output.
case "${REFRESH_PROXIES,,}" in 1 | true | yes | on) REFRESH_PROXIES=true ;; esac
if [ "$REFRESH_PROXIES" = true ]; then
  echo "======================="
  if [ -f "$WEBSHARE_ENV" ]; then
    echo "Refreshing Webshare proxy pool from $WEBSHARE_ENV ..."
    if python "$PROXY_SCRIPT" --env-file "$WEBSHARE_ENV"; then
      echo "Proxy pool refresh finished."
    else
      echo "Proxy refresh failed; continuing with the existing endpoints."
    fi
  else
    echo "REFRESH_PROXIES requested but $WEBSHARE_ENV is absent; skipping (single-IP mode)."
  fi
  echo "======================="
fi

# Main
echo "Starting spiders run..."
echo "======================="

# Run spiders in parallel using xargs
printf "%s\n" "${SPIDERS_TO_RUN[@]}" | xargs -P "$CONCURRENCY" -I {} -n 1 bash -c 'run_spider "$@"' _ {}

echo "Spider runs completed."

# Upload is opt-in (-u) and best-effort: it runs once, after every spider has
# finished, so the dataset repo gets a single commit instead of one per state.
# A failure here must not fail the (already completed) run.
if [ "$UPLOAD" = true ]; then
  # Ship a state -> source-URL provenance table with the dataset. Best-effort:
  # a failure here must not abort the (already completed) upload, so we just
  # skip the extra file if generation fails.
  SOURCES_FILE="${OUTPUT_DIR}SOURCES.md"
  extra_upload_args=()
  echo "Generating source list ${SOURCES_FILE}..."
  if python "$SOURCES_SCRIPT" --output "$SOURCES_FILE"; then
    extra_upload_args=(--extra-file "$SOURCES_FILE")
  else
    echo "Source list generation failed; uploading without it."
  fi

  # The upload script takes one format at a time, so push each requested
  # format (one commit per format) rather than one commit for the whole run.
  # SOURCES.md is format-independent; it rides along with every format's commit.
  for fmt in $FORMAT; do
    echo "Uploading ${OUTPUT_DIR} (${fmt}) to Hugging Face..."
    if python "$UPLOAD_SCRIPT" -f "$fmt" "${extra_upload_args[@]}" "$OUTPUT_DIR"; then
      echo "Upload completed."
    else
      echo "Upload failed."
    fi
  done
fi
