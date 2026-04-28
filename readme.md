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

## Running Tests
The project uses pytest, so one can simply run `pytest` to go through the whole run of it.


# Progress tracking

| Name                     | Scraper | Unit Tests | Item updated   |
|--------------------------|---------|------------|----------------|
| Alabama                  | [x]     | [x]        | [x]            |
| Alaska                   | [x]     | [x]        | [x]            |
| Arizona                  | [ ]     | [ ]        | [ ]            |
| Arkansas                 | [x]     | [x]        | [x]            |
| California               | [x]     | [x]        | [x]            |
| Colorado                 | [x]     | [x]        | [x]            |
| Connecticut              | [ ]     | [ ]        | [ ]            |
| Delaware                 | [ ]     | [ ]        | [ ]            |
| Florida                  | [ ]     | [ ]        | [ ]            |
| Georgia                  | [x]     | [x]        | [x]            |
| Hawaii                   | [ ]     | [ ]        | [ ]            |
| Idaho                    | [ ]     | [ ]        | [ ]            |
| Illinois                 | [x]     | [x]        | [x]            |
| Indiana                  | [ ]     | [ ]        | [ ]            |
| Iowa                     | [ ]     | [ ]        | [ ]            |
| Kansas                   | [ ]     | [ ]        | [ ]            |
| Kentucky                 | [ ]     | [ ]        | [ ]            |
| Louisiana                | [ ]     | [ ]        | [ ]            |
| Maine                    | [ ]     | [ ]        | [ ]            |
| Maryland                 | [x]     | [x]        | [x]            |
| Massachusetts            | [ ]     | [ ]        | [ ]            |
| Michigan                 | [x]     | [x]        | [x]            |
| Minnesota                | [x]     | [x]        | [x]            |
| Mississippi              | [ ]     | [ ]        | [ ]            |
| Missouri                 | [ ]     | [ ]        | [ ]            |
| Montana                  | [x]     | [x]        | [x]            |
| Nebraska                 | [ ]     | [ ]        | [ ]            |
| Nevada                   | [ ]     | [ ]        | [ ]            |
| New Hampshire            | [ ]     | [ ]        | [ ]            |
| New Jersey               | [x]     | [x]        | [x]            |
| New Mexico               | [x]     | [x]        | [x]            |
| New York                 | [x]     | [x]        | [x]            |
| North Carolina           | [x]     | [x]        | [x]            |
| North Dakota             | [ ]     | [ ]        | [ ]            |
| Ohio                     | [x]     | [x]        | [x]            |
| Oklahoma                 | [ ]     | [ ]        | [ ]            |
| Oregon                   | [ ]     | [ ]        | [ ]            |
| Pennsylvania             | [x]     | [x]        | [x]            |
| Rhode Island             | [ ]     | [ ]        | [ ]            |
| South Carolina           | [x]     | [x]        | [x]            |
| South Dakota             | [ ]     | [ ]        | [ ]            |
| Tennessee                | [ ]     | [ ]        | [ ]            |
| Texas                    | [x]     | [x]        | [x]            |
| Utah                     | [x]     | [x]        | [x]            |
| Vermont                  | [ ]     | [ ]        | [ ]            |
| Virginia                 | [x]     | [x]        | [x]            |
| Washington               | [x]     | [x]        | [x]            |
| West Virginia            | [ ]     | [ ]        | [ ]            |
| Wisconsin                | [ ]     | [ ]        | [ ]            |
| Wyoming                  | [ ]     | [ ]        | [ ]            |
| American Samoa           | [ ]     | [ ]        | [ ]            |
| Guam                     | [ ]     | [ ]        | [ ]            |
| Northern Mariana Islands | [ ]     | [ ]        | [ ]            |
| Puerto Rico              | [ ]     | [ ]        | [ ]            |
| U.S. Virgin Islands      | [ ]     | [ ]        | [ ]            |
