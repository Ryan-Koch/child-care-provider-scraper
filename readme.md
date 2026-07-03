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
| West Virginia            | [x]     | 
| Wisconsin                | [ ]     | 
| Wyoming                  | [ ]     | 
| American Samoa           | [ ]     | 
| Guam                     | [ ]     | 
| Northern Mariana Islands | [ ]     | 
| Puerto Rico              | [ ]     | 
| U.S. Virgin Islands      | [ ]     |
