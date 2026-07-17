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

# !!! TEMPORARY PIN (2026-07-17). Remove once a newer Chrome passes v3. !!!
#
# Chrome 150.0.7871.128 drops rhode_island's reCAPTCHA v3 score below the
# threshold: the Aura search returns isCaptchaInvalid/isV3Failed on every
# attempt. 150.0.7871.114 passes. Established by A/B inside this image — same
# code, same egress IP, channel="chrome" both times, only the Chrome version
# varying: .114 scraped providers on 3/3 runs, .128 failed 3/3 attempts.
# The line above resolves to *current* stable, which is what makes the break
# recur on every rebuild; this layer pins the version back down.
#
# The `-a audit=1` fingerprint is IDENTICAL on both versions (Chrome's reduced
# UA reports "150.0.0.0", hiding the patch), so a clean audit does NOT clear
# Chrome and cannot diagnose this. See docs/browser_signature.md.
#
# Why downgrade-in-place instead of just installing the pinned .deb directly:
# doing that with --no-install-recommends yields an image that still FAILS v3
# despite carrying an identical .114 binary and an identical JS fingerprint —
# it silently drops packages Chrome's own dep closure pulls in (libxft2,
# libxcb-shape0, x11-utils, x11-xserver-utils). Whatever v3 reads there, it is
# invisible to the audit. So: let upstream install Chrome and its full
# dependency+recommends closure, then swap only the binary's version and hold
# the package so a later apt layer can't bump it.
#
# Retest a newer Chrome (do this periodically — a pin is an unpatched browser):
#   docker build --build-arg CHROME_VERSION=<version>-1 -t cc-test .
#   docker run --rm --init --shm-size=2gb --user 1000:1000 -e HOME=/tmp \
#     --entrypoint bash cc-test -c 'xvfb-run -a -s "-screen 0 1920x1080x24" \
#     scrapy crawl rhode_island -a max_providers=3 -s LOG_LEVEL=INFO'
# No isV3Failed in the log => that version is good; delete this whole layer.
ARG CHROME_VERSION=150.0.7871.114-1
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
