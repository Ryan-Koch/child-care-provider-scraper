#!/bin/bash
LOG_LEVEL='INFO'
# Max retries for each spider individually
MAX_RETRIES=6
# Default concurrency (can be overridden by first argument)
DEFAULT_CONCURRENCY=5
CONCURRENCY=${1:-$DEFAULT_CONCURRENCY}

echo "Checking what spiders we have..."
SPIDERS_TO_RUN=($(scrapy list))
echo "Found ${#SPIDERS_TO_RUN[@]} spiders to run: ${SPIDERS_TO_RUN[*]}"
echo "Running with concurrency: $CONCURRENCY"

run_spider() {
    local spider_name=$1
    local log_file="${spider_name}.log"
    local retry_count=0

    # the loop where we'll run a spider and do retries if needed.
    while [ $retry_count -lt $MAX_RETRIES ]; do
      echo "Crawling $spider_name..."
      scrapy crawl $spider_name \
      -o "$spider_name.json" \
      -s LOG_FILE="$log_file" \
      -s LOG_LEVEL=$LOG_LEVEL \
      -s LOG_FILE_APPEND=False

      if grep -q "Retryable Error" "$log_file"; then
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

# Main
echo "Starting spiders run..."
echo "======================="

# Run spiders in parallel using xargs
printf "%s\n" "${SPIDERS_TO_RUN[@]}" | xargs -P "$CONCURRENCY" -I {} -n 1 bash -c 'run_spider "$@"' _ {}

echo "Spider runs completed."
