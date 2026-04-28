#!/bin/bash
LOG_LEVEL='INFO'
# Max retries for each spider individually
MAX_RETRIES=6
# Default concurrency (override with -c)
DEFAULT_CONCURRENCY=5
CONCURRENCY=$DEFAULT_CONCURRENCY
# Default directory path (override with -d)
DEFAULT_OUTPUT_DIR="./"
OUTPUT_DIR=$DEFAULT_OUTPUT_DIR
# Default format (override with -f)
DEFAULT_FORMAT="json"
FORMAT=$DEFAULT_FORMAT
# Space-separated list of spiders that require a virtual display
XVFB_SPIDERS="new_jersey"

usage() {
    echo "Usage: $0 [-c concurrency] [spider ...]" >&2
    echo "  -c   number of spiders to run in parallel (default: $DEFAULT_CONCURRENCY)" >&2
    echo "  -d   directory to use for spider logging and output files (default: $DEFAULT_OUTPUT_DIR)" >&2
    echo "  -f   file format to use for spider output can be json or csv (default: $DEFAULT_FORMAT)" >&2
    echo "  spider names default to the output of 'scrapy list'" >&2
}

while getopts ":c:d:f:h" opt; do
    case $opt in
        c) CONCURRENCY=$OPTARG ;;
        d) OUTPUT_DIR=$OPTARG ;;
        f) FORMAT=$OPTARG ;;
        h) usage; exit 0 ;;
        \?) usage; exit 1 ;;
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
if [ $FORMAT ]; then
    FORMAT=${FORMAT,,}
fi

# Make sure we use a trailing slash in the path
if [ $OUTPUT_DIR ]; then
   [[ "${OUTPUT_DIR}" != */ ]] && OUTPUT_DIR="${OUTPUT_DIR}/"
fi

run_spider() {
    local spider_name=$1
    local log_file="${spider_name}.log"
    local retry_count=0

    # the loop where we'll run a spider and do retries if needed.
    while [ $retry_count -lt $MAX_RETRIES ]; do
      echo "Crawling $spider_name..."
      local cmd_prefix=()
      grep -qw "$spider_name" <<< "$XVFB_SPIDERS" && cmd_prefix=(xvfb-run -a)
      "${cmd_prefix[@]}" scrapy crawl $spider_name \
      -o "${OUTPUT_DIR}${spider_name}.${FORMAT}" \
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

# Main
echo "Starting spiders run..."
echo "======================="

# Run spiders in parallel using xargs
printf "%s\n" "${SPIDERS_TO_RUN[@]}" | xargs -P "$CONCURRENCY" -I {} -n 1 bash -c 'run_spider "$@"' _ {}

echo "Spider runs completed."
