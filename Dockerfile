# Microsoft's official Playwright image, Ubuntu 24.04 (noble). It ships every
# browser OS dependency, which is the painful part to reproduce by hand. Tag is
# pinned to the Playwright version in requirements.txt (1.55.0); keep the two
# roughly in sync. `playwright install --with-deps` below re-fetches browsers to
# match whatever pip actually resolves, so minor drift is handled.
FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

ENV DEBIAN_FRONTEND=noninteractive

# System packages:
#  - xvfb: the channel=chrome + headless=False spiders (new_jersey,
#    rhode_island, arizona, minnesota) need a virtual display. run_spiders.sh
#    wraps them in xvfb-run automatically.
#  - tesseract/leptonica + build-essential + pkg-config: the Maryland OCR uses
#    tesserocr. Its wheel usually bundles libtesseract, but these guarantee the
#    install works even if it has to build from source. (Trim later with a
#    multi-stage build if image size matters.)
#  - python-is-python3: run_spiders.sh and the enrich step call bare `python`.
#  - curl: fetch the Tesseract model below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        xvfb \
        curl \
        python-is-python3 \
        build-essential \
        pkg-config \
        tesseract-ocr \
        libtesseract-dev \
        libleptonica-dev \
    && rm -rf /var/lib/apt/lists/*

# Tesseract language model. maryland.py reads eng.traineddata from
# TESSDATA_PREFIX (the fast model, per MARYLAND.md). Baked in so runs need no
# network for OCR setup.
ENV TESSDATA_PREFIX=/opt/tessdata
RUN mkdir -p "$TESSDATA_PREFIX" \
    && curl -fsSL -o "$TESSDATA_PREFIX/eng.traineddata" \
        https://github.com/tesseract-ocr/tessdata_fast/raw/main/eng.traineddata

WORKDIR /app

# Install Python deps first (own layer) so code changes don't invalidate the
# dependency cache.
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Browser binaries matching the installed Playwright: bundled Chromium plus
# real Google Chrome (channel=chrome). --with-deps pulls any OS libs the
# resolved browser build needs on top of what the base image provides.
RUN playwright install --with-deps chromium chrome

# Application code.
COPY . .

# Run as the non-root user shipped by the Playwright image (UID 1000). Chrome
# refuses to run as root without --no-sandbox (which the spiders don't pass), so
# pwuser sidesteps that and is more secure. It must own the app dir and the
# tessdata cache for runtime writes.
RUN chown -R pwuser:pwuser /app "$TESSDATA_PREFIX"
USER pwuser

# Pass args straight through to run_spiders.sh:
#   docker compose run --rm scraper -g -c 3 ohio texas
# A bare run prints usage rather than crawling every spider.
ENTRYPOINT ["./run_spiders.sh"]
CMD ["-h"]
