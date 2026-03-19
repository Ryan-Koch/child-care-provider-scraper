# Child Care Provider Scraper Technical Overview

This document provides a technical description of this product as well as its purpose.

## Why does this project exist?

The United States has individual States with Child Care Provider information stored in individual databases.
These store information like licensing, quality ratings, capacity, and other related data points, which can be
used for a variety of purposes. This project seeks to build a series of web scrapers to provide infrastructure
to bring this data together.

## Foundational Technologies

- **Programming Language:** Python 3.x
- **Scraper Library:** scrapy
- **Other tools**
  - Playwright
  - Scrapy-playwright

## Project Structure
- **./** The project root. You'll find the readme, requirements.txt, scrapy.cfg and other base level configuration files
  and scripts here.
- **./provider_scrape** Where our implementation using scrapy lives. At this level you'll find our items.py file which 
  contains data structure definitions as well as the settings.py which has scrapy configuration options.
- **./provider_scrape/spiders** This is where our individual spiders that define our scrapers live. Each state has 
  one of these, which is customized to function with the unique environment that retrieving the data there demands.

## General Instructions:

- Before you write any code propose a plan. It's important for us both to understand what we're about to try to accomplish.
- This work mostly involves one of the following activities:
  - Scraping and parsing HTML
  - Interacting with an API
  - Pulling a file like a CSV and parsing
  - Integrating data from remote source into items.py structure.
- When writing python code please follow Pep8 guidelines.
- Ensure any spider written is independent from any other spider.
- Keep methodologies simple and focus on readability and maintainability.
- When we discover new fields that don't yet exist in the item we have in items.py then add a new one using the
  following naming convention: stateabbreviation_fieldname. For example a 'cats' fields for 'Ohio' would be 
  oh_cats.
- Always write basic unit tests for spiders created. Cover at least a golden path scenario as well as ones where 
  data elements are missing.
- Do not add new dependencies to this project unless absolutely necessary.
- When building a spider that requires navigating paginated results or complex hierarhical navigation make sure to log each transition. For example, for pagination let's log the transition point, the current page number, and the total number of pages.


## Context gathering tools
- When you need to check source HTML context use curl.
- When you need to search for specific strings within larger contexts use grep.

## Running tests
This project uses pytest for testing. To run all tests use `pytest`. To run a specific test file use `pytest <test_file_name>`.

## Running a spider
- Use `scrapy crawl <spider_name>` to run a spider.
- When running a spider don't redirect logging output to a file. Redirecting results to a json file is okay.
