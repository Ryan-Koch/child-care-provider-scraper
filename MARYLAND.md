# Maryland Spider

The Maryland spider scrapes child care provider data from [checkccmd.org](https://www.checkccmd.org/). It uses OCR to extract precise street addresses from inspection report PDFs, which requires a Tesseract trained data file that is not included in `pip install`.

## Additional Setup

After installing Python dependencies (`pip install -r requirements.txt`), download the Tesseract English language model:

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
