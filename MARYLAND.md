# Maryland Spider

The Maryland spider scrapes child care provider data from [checkccmd.org](https://www.checkccmd.org/). The licensing site omits the street **number** from its HTML (it only shows street name + city + ZIP), so a precise address has to be sourced elsewhere.

## Address enrichment (EXCELS API)

For the precise address the spider queries the public **Maryland EXCELS** "Find a Program" API, keyed by license number:

```
GET https://findaprogram.marylandexcels.org/api/fap/search?license=<license_number>
```

This one fast JSON call returns **rooftop-accurate latitude/longitude** for every provider, plus — **for center-type providers** — the full street address with house number.

- **Centers** (`CTR`, `LOC`): adopt the EXCELS house-numbered address + coordinates.
- **Family homes** (`FCCH`, `LFCCH`): EXCELS withholds the house number (private residences), so the address stays street-name level, but the **coordinates are rooftop-accurate** (verified: they reverse-geocode to the exact house, and the number is recoverable from the coordinate if ever needed). No PDF is fetched for these.

EXCELS runs against a separate, non-throttling domain, so this enrichment does not contend with the licensing-site crawl.

### PDF + OCR fallback

Only providers with **no EXCELS record at all** optionally fall back to OCR-ing the address out of the first inspection-report PDF (slow: each report is ~1 MB and server-rendered, and this endpoint is the historical bottleneck). This is a small residual set.

- `-a ocr_fallback=false` disables the PDF/OCR fallback entirely for a run that downloads zero PDFs (EXCELS-miss providers keep the street-name-level address).
- `-a counties="Howard,Carroll"` restricts the crawl to counties whose label contains one of the given terms (case-insensitive) — handy for limited verification runs.

The OCR fallback requires a Tesseract trained data file that is not included in `pip install`. Note the OCR crop is calibrated for the **center** report layout; family-home reports use a different layout (and are not normally fetched, since EXCELS covers them).

## Additional Setup

The Tesseract model is only needed if the OCR fallback is enabled (the default). After installing Python dependencies (`pip install -r requirements.txt`), download the Tesseract English language model:

```bash
mkdir -p /tmp/tessdata
curl -L -o /tmp/tessdata/eng.traineddata \
  https://github.com/tesseract-ocr/tessdata_fast/raw/main/eng.traineddata
```

To use a different location, set the `TESSDATA_PREFIX` environment variable:

```bash
export TESSDATA_PREFIX=/path/to/your/tessdata
```

No system-level Tesseract installation is required — `tesserocr` bundles its own `libtesseract` and `libleptonica`.

## Running the Spider

```bash
scrapy crawl maryland -o maryland.json
```

## Running Tests

```bash
pytest provider_scrape/spiders/test_maryland.py -v
```

The tests do not require the tessdata file — OCR calls are mocked.
