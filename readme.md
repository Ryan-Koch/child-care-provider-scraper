# Provider web scrapers
The goal of this repo is to get web spiders together that can scrape the publically available license databases out there with
child care provider information.

# Setup Instructions

1. Clone the repo using git clone
2. Use a python virtual environment. One way to do this (assuming you have python 3.x installed already) is to run `python3 -m venv .venv` . 
3. Activate the virtual environment like so: `source .venv/bin/activate`
4. Install dependencies using pip: `pip install -r requirements.txt`
5. Install playwright browsers: `playwright install` and then `playwright install chrome`
6. (Linux servers only) Install xvfb: `sudo apt install xvfb`. This is used for cases where we're running a more realistic headless browser and on a server with no display it needs xvfb to provide a virtual one. `run_spiders.sh` automatically uses `xvfb-run` for selected spiders (configured within the script) when its available; for ad-hoc runs use `xvfb-run -a scrapy crawl new_jersey -o new_jersey.json`. macOS/Windows/desktop Linux users can skip this — the browser window will just open locally.
7. If you are going to run the Maryland scraper make sure you take a look at `Maryland.md` for instructions on how to do that. It uses tesseract.
8. Some spiders (Rhode Island, New Jersey, Arizona, Minnesota) drive a real browser to get past reCAPTCHA v3. If one of them suddenly returns zero rows, or you need to touch the browser-fingerprint / anti-bot setup, see `docs/browser_signature.md` — it documents the fingerprint patches, the behavioral warm-up, and the requirement to run from an IP whose timezone matches the spider (e.g. Eastern-time for Rhode Island).

## Running the spiders
The spiders can be run directly using the `scrapy` command, for example one could do `scrapy crawl ohio -o ohio.json` to begin a run of the Ohio spider and output it to a file named `ohio.json`. There is also a script here called `run_spiders.sh` which can be used to run several spiders together in one go. It has a `-c` argument for concurrency (how many states we'll run at once) and takes a list of state spider names separated by spaces. Like in this example: `./run_spiders.sh ohio florida new_jersey texas new_york`. If you don't provide a concurrency argument like in that example it assumes 5 at a time.

### Customizing your run of run_spiders.sh
Check out the current usage, at the time of this update it's:
```
Usage: ./run_spiders.sh [-c concurrency] [spider ...]
  -c   number of spiders to run in parallel (default: 5)
  -d   directory to use for spider logging and output files (default: ./)
  -f   file format to use for spider output can be json or csv (default: json)
  -g   after each spider, geocode records missing coordinates (JSON output only)
  -u   after all spiders finish, upload the output files to a Hugging Face dataset
  spider names default to the output of 'scrapy list'
```
What this means is that you can add in options before the list of spiders to run (or you can provide no spiders and let it run on all of them). So an example command can look like: `./run_spider.sh -c 3 -d /some/path/you/can/write/to/ -f csv ohio new_jersey new_york texas north_carolina illinois` in the example we're setting the concurrency to 3, customizing the output path for output and logging, and choosing to use the CSV format. 

### Geocoding records that are missing coordinates
Some states don't publish latitude/longitude. For those, a post-run enrichment step derives coordinates from the scraped address using the free [US Census Bureau batch geocoder](https://geocoding.geo.census.gov/) and records where each coordinate came from in two fields: `geocode_source` (`state` when the spider supplied it, `census` when we derived it, `unmatched` when geocoding found nothing) and `geocode_confidence` (`exact`/`approximate` for a match, `tie`/`no_match` otherwise).

Pass `-g` to `run_spiders.sh` to geocode each state's output right after it finishes (JSON output only — with `-f csv` the step is skipped). Geocoding failures are logged but never fail the scrape. Example: `./run_spiders.sh -g -c 3 ohio texas alabama`.

You can also run it standalone on any JSON output file:
```
.venv/bin/python scripts/geocode_enrich.py state_output/alabama.json
```
Results are cached in `geocode_cache.sqlite` (git-ignored) keyed by address, so re-runs only geocode new or changed records. Useful flags: `--dry-run` (report candidates without calling the geocoder), `--limit N` (cap unique addresses queried), `--no-cache`, and `-o PATH` (write elsewhere instead of in place). Note: geocoding only helps states that emit an address — address-less states are reported as skipped.

### Uploading the output to Hugging Face
Pass `-u` to `run_spiders.sh` to upload the run's data files to a Hugging Face dataset repo once every spider has finished. Unlike `-g` (which runs per-spider), the upload runs a single time at the end so the repo gets one commit instead of one per state. It uploads the data files matching `-f` (the `.json` files by default, `.csv` with `-f csv`) and never uploads the `.log` files. Upload failures are logged but never fail the (already completed) scrape. Pair it with `-d` so it uploads a specific run's directory, e.g. `./run_spiders.sh -u -c 3 -d state_output/ ohio texas alabama`.

The token and target repo are read from `huggingface.env` at the repo root (git-ignored):
```
hugging_face_token=hf_xxxxxxxx
hugging_face_repo=owner/dataset-name
```
Use a **write** token, and create the dataset repo on the Hugging Face website first — the script uploads to an existing repo and does not create one.

Because each state file has its own set of columns, loading them as one table fails Hugging Face's "all files must have the same columns" check. So a JSON upload also writes a `README.md` whose YAML frontmatter declares one dataset **configuration** per state file (`config_name` = the file's stem, e.g. `alabama`). Hugging Face then parses each state independently — pick a state in the dataset viewer, or `load_dataset("owner/dataset-name", "alabama")`. The card's body and any other frontmatter keys you've written are preserved; only the `configs` key is regenerated each upload. This is on by default for JSON and off for CSV; use `--no-readme` / `--readme` to override.

The `-u` upload also ships a `SOURCES.md` provenance table (state → source website(s)), regenerated at upload time from each spider's `allowed_domains`/`start_urls` by `scripts/generate_sources.py`. To refresh the committed copy after adding or changing a spider's source, run `.venv/bin/python scripts/generate_sources.py` (a pytest drift-guard enforces it stays current). Standalone uploads can include it with `--extra-file SOURCES.md` (repeatable for any extra file).

You can also run it standalone on a directory or specific files:
```
.venv/bin/python scripts/upload_to_huggingface.py state_output/
.venv/bin/python scripts/upload_to_huggingface.py --dry-run state_output/alabama.json
```
Useful flags: `--dry-run` (list what would be uploaded without pushing), `--repo owner/name` and `--token …` (override the env file), `-f csv` (upload CSVs instead of JSON), `--path-in-repo subdir/` (upload into a subdirectory of the repo), and `--no-readme` (skip the per-state dataset card).

## Running with Docker

You can run the scrapers as a containerized job without installing Python, the Playwright browsers, xvfb, or the Tesseract model on the host — the image bakes all of that in. It's built on Microsoft's official Ubuntu-based Playwright image, so the browser system dependencies are handled for you.

### One-time setup

1. Install Docker and the Docker Compose plugin.
2. (Optional) Copy the Hugging Face config template. The compose file mounts this file, so it must exist even if you leave it blank; fill it in only if you plan to upload with `-u`:
   ```bash
   cp huggingface.env.example huggingface.env
   ```
3. Build the image (the first build downloads the browsers and Tesseract model, so give it a few minutes):
   ```bash
   docker compose build
   ```

### Running a job

Use `docker compose run` and pass the same arguments you'd give `run_spiders.sh` (see the usage above). For example, crawl three states at concurrency 3, emitting JSON + CSV and geocoding as you go:

```bash
docker compose run --rm scraper -g -c 3 -f json,csv ohio texas alabama
```

Output files and logs land in `./state_output/` on the host, and the geocode cache persists there too, so re-runs only geocode new records. A bare `docker compose run --rm scraper` prints the usage help rather than running every spider. To upload to Hugging Face at the end, add `-u` (requires a filled-in `huggingface.env`).

### How the container is wired up

- **Output & cache** — `./state_output/` on the host is mounted into the container and is the default output directory, so scraped files, logs, and `geocode_cache.sqlite` all persist there. Override the in-container path with `-d` as usual.
- **Secret** — `huggingface.env` is bind-mounted read-only at runtime and is never copied into the image, so the token stays on the host.
- **Permissions** — the container runs as your host user (UID 1000 by default), so files written to `./state_output/` are owned by you. If you aren't UID 1000 on your host, pass your own IDs, e.g. `DOCKER_UID=$(id -u) DOCKER_GID=$(id -g) docker compose run --rm scraper ...` (or set `DOCKER_UID`/`DOCKER_GID` in a `.env` file next to the compose file).
- **Chrome** — the Cloudflare-sensitive spiders (`new_jersey`, `rhode_island`, `arizona`, `minnesota`) use real Google Chrome under a virtual display (xvfb); both are in the image and used automatically. The container gets a 2 GB `/dev/shm` so Chrome doesn't crash.

## Running Tests
The project uses pytest, so one can simply run `pytest` to go through the whole run of it.


# Progress tracking

| Name                     | Scraper | 
|--------------------------|---------|
| Alabama                  | [x]     | 
| Alaska                   | [x]     | 
| Arizona                  | [x]     | 
| Arkansas                 | [x]     | 
| California               | [x]     | 
| Colorado                 | [x]     | 
| Connecticut              | [ ]     | 
| Delaware                 | [ ]     | 
| Florida                  | [x]     | 
| Georgia                  | [x]     | 
| Hawaii                   | [x]     | 
| Idaho                    | [ ]     | 
| Illinois                 | [x]     | 
| Indiana                  | [ ]     | 
| Iowa                     | [ ]     | 
| Kansas                   | [ ]     | 
| Kentucky                 | [ ]     | 
| Louisiana                | [ ]     | 
| Maine                    | [ ]     | 
| Maryland                 | [x]     | 
| Massachusetts            | [ ]     | 
| Michigan                 | [x]     | 
| Minnesota                | [x]     | 
| Mississippi              | [ ]     | 
| Missouri                 | [ ]     | 
| Montana                  | [x]     | 
| Nebraska                 | [ ]     | 
| Nevada                   | [x]     | 
| New Hampshire            | [ ]     | 
| New Jersey               | [x]     | 
| New Mexico               | [x]     | 
| New York                 | [x]     | 
| North Carolina           | [x]     | 
| North Dakota             | [x]     | 
| Ohio                     | [x]     | 
| Oklahoma                 | [ ]     | 
| Oregon                   | [ ]     | 
| Pennsylvania             | [x]     | 
| Rhode Island             | [x]     | 
| South Carolina           | [x]     | 
| South Dakota             | [ ]     | 
| Tennessee                | [ ]     | 
| Texas                    | [x]     | 
| Utah                     | [x]     | 
| Vermont                  | [ ]     | 
| Virginia                 | [x]     | 
| Washington               | [x]     | 
| Washington DC            | [x]     | 
| West Virginia            | [x]     | 
| Wisconsin                | [ ]     | 
| Wyoming                  | [ ]     | 
| American Samoa           | [ ]     | 
| Guam                     | [ ]     | 
| Northern Mariana Islands | [ ]     | 
| Puerto Rico              | [ ]     | 
| U.S. Virgin Islands      | [ ]     |
