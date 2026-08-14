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

# CHROME VERSION PIN — bumped 2026-08-14 from 150.0.7871.114 to current stable
# 151.0.7922.137. This is an explicit *version* pin, no longer a *downgrade*.
#
# Why keep a pin at all instead of floating on current stable — TWO footguns,
# both found while retesting on 2026-08-14:
#  1. Build-cache staleness. The `playwright install` line above resolves to
#     *current stable at layer-build time* and then Docker CACHES that layer.
#     A routine rebuild reuses the cache, so "unpinned" does NOT mean "current"
#     — as of 2026-08-14 the cached layer still baked in 150.0.7871.128, the
#     EXACT version that breaks RI's v3. Only `--no-cache` refetches. A floating
#     Chrome is therefore silently whatever happened to be cached.
#  2. A Chrome PATCH bump is a real v3 variable and `-a audit=1` is BLIND to it
#     (Chrome's reduced UA reports "NNN.0.0.0", hiding the patch). 150.0.7871.128
#     failed v3 on every attempt where .114 passed. A future stable bump could
#     silently do the same and zero out RI, with no signal. See
#     docs/browser_signature.md.
# The explicit fetch below is deterministic (always this exact .deb) and
# cache-immune for correctness, and it installs Chrome via apt so its full
# dependency+recommends closure is pulled (libxft2, libxcb-shape0, x11-utils,
# x11-xserver-utils — these ALSO affect the v3 score and are invisible to the
# audit; NEVER add --no-install-recommends here). Keep the `playwright install`
# line above too. This pinned build is live-verified: 151.0.7922.137 passed RI
# v3 7/7, dead-even with .114 (6/6), on a fresh NYC egress before self-inflicted
# IP-reputation degradation set in.
#
# Retest a newer Chrome before bumping the pin (do this periodically — a pin is
# an unpatched browser). Vary ONLY CHROME_VERSION so the install path is held
# constant, and interleave a known-good control at the same moment to separate a
# version regression from IP-reputation flakiness (RI's v3 is ~50%/attempt on a
# marginal IP, and heavy testing from one datacenter IP degrades it fast):
#   docker build --build-arg CHROME_VERSION=<version>-1 -t cc-test .
#   docker run --rm --init --shm-size=2gb --user 1000:1000 -e HOME=/tmp \
#     --entrypoint bash cc-test -c 'xvfb-run -a -s "-screen 0 1920x1080x24" \
#     scrapy crawl rhode_island -a max_providers=3 -s LOG_LEVEL=INFO'
# No isV3Failed in the log => that version is good; bump CHROME_VERSION to it.
ARG CHROME_VERSION=151.0.7922.137-1
RUN curl -fsSL -o /tmp/chrome.deb \
        "https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb" \
    && apt-get update \
    && apt-get install -y --allow-downgrades /tmp/chrome.deb \
    && rm -f /tmp/chrome.deb \
    && apt-mark hold google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && google-chrome --version

# Application code.
COPY . .

# NB: the Webshare proxy-pool refresh does NOT run here. webshare.env is
# docker-ignored and bind-mounted read-write at runtime, so a build step can
# neither see it nor persist changes back to the host file. It runs instead at
# container startup in run_spiders.sh (triggered by REFRESH_PROXIES, set in
# docker-compose.yml). See scripts/update_webshare_proxies.py.

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
