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

### Rate limiting (checkccmd.org)

The licensing site enforces a **hard per-IP request-rate limit** (IIS Dynamic IP
Restrictions): a **trailing ~60s window that allows only 2 requests**, returning
a stock IIS `403 - Forbidden` once exceeded — site-wide per IP. While over the
limit the block is *self-sustaining* (each blocked request keeps the window
saturated); it clears only after ~60s of silence. Safe condition: no 3 requests
in any 60s window, i.e. spacing strictly **> 30s**. 30.0s sits exactly on the
edge (a 30s-spaced crawl tripped on its 3rd request), so the spider crawls
checkccmd **strictly single-flight** (`CONCURRENT_REQUESTS_PER_DOMAIN=1`) at a
fixed **33s** spacing (~10% margin). Every checkccmd request (search, detail,
pagination, PDF fallback) shares that one throttled slot; the EXCELS API is on a
separate, non-throttling host and keeps its own fast slot.

- `-a delay=<seconds>` tunes the per-request spacing (default 33). Do **not** go
  `<= 30` without a proxy pool — 2 requests per 60s is the ceiling from one IP.
- A per-IP 403 is **recovered, not dropped**: `RateLimitBackoffMiddleware` pauses
  the checkccmd slot for a 60s cooldown (real silence clears the window) then
  retries, bounded by `RATELIMIT_BACKOFF_MAX_RETRIES`. (403 is not in Scrapy's
  default `RETRY_HTTP_CODES`, so without this a blocked request is silently lost —
  a prior run shed ~50% of providers exactly this way.)

**Run time:** at 33s/request a full ~12k-request run is **~4–5 days** from one IP
(up from ~53h before the site tightened the limit). The per-IP wall, not
concurrency, is the limiter *from one IP*, so a real speed-up requires **IP
rotation / a proxy pool** — the block is per-IP and clears in ~60s, so N exit IPs
each just under 2/min gives ~N× throughput. **Caveat:** past enough IPs a second,
server-side limit takes over — see *Detail-endpoint concurrency limit* below —
so N× does **not** hold indefinitely; total concurrency must be tuned to the
origin, not just maximized.

### Optional proxy pool (multi-IP, opt-in)

The spider can rotate a pool of egress IPs to beat the per-IP wall. This is
**opt-in** — with nothing configured it runs single-IP exactly as above.

Enable it by creating `webshare.env` at the repo root (see
`webshare.env.example`):

```
webshare_proxy_username=<user>
webshare_proxy_password=<pass>
webshare_proxy_endpoints=host1:port1,host2:port2,host3:port3,host4:port4
```

Then run normally — `scrapy crawl maryland -o maryland.json` picks the pool up
and logs `proxy pool ENABLED — N egress IPs`. How it works:

- **Each proxy gets its own download slot**, so the 33s single-flight cadence is
  enforced *per IP* and the proxies run in parallel — N IPs ≈ N× throughput
  (4 free Webshare US proxies ≈ ~1 day vs ~4–5 days).
- **Detail/PDF GETs rotate** across all proxies (they're session-independent —
  the only requirement is the SearchResults referer, which the crawl always
  sends). **Each county's pagination chain sticks to one proxy** so its
  ViewState/session and IP stay consistent.
- `RateLimitBackoffMiddleware` cools down **per slot**, so one throttled proxy
  pauses only itself while the others keep flowing.
- The EXCELS API stays **direct** (separate non-throttling host).

Toggles:

- `-a proxies=off` — force single-IP even if `webshare.env` exists.
- `-a proxies="host:port,host:port"` — supply endpoints inline (credentials
  still come from `webshare.env`, or embed full `http://user:pass@host:port`).
- `-a proxy_env=/path/to/file` — use a different env file.

Under Docker, mount the file (see the commented line in `docker-compose.yml`).
Datacenter IPs are fine here — the site only rate-limits; there is no reputation
block or captcha. Traffic transits the proxy operator, but this is public
government data, so that's not a confidentiality concern.

### Detail-endpoint concurrency limit (the *other* bottleneck)

Beating the per-IP 403 wall with a big proxy pool exposes a second, independent
limit: **checkccmd's `FacilityDetail` endpoint is concurrency-limited on the
*server* side.** A single detail GET returns in **~1 second**, but a handful
fired at once queue/serialize on the origin and blow past the 60s timeout.
Measured live (2026-07-21, 20-IP run): **8 parallel detail GETs all took >100s
while a solo GET was ~1s**; endpoint latency swings 1s ↔ 45s ↔ timeout purely
with how many requests are in flight, and it's **global across egress IPs** (a
direct request from the host queues too when the pool is hammering). Throughout,
**zero 403s** — this is *not* the rate limit, and no number of extra IPs fixes
it. Driving 20 proxy slots at 16-way concurrency drove the origin into
**congestion collapse**: ~83% of details timed out, and each timeout retrying
`RETRY_TIMES`-deep poured more load on (a self-sustaining storm).

The endpoint is also **just slow**: measured 2026-07-22 at low concurrency a
detail page renders in **~17–34s** (only rarely ~1s in a lull), on top of the
concurrency scaling. So it's slow *and* concurrency-sensitive — both must be
accommodated. Once you have enough IPs to clear the 403 wall, **total in-flight
concurrency and the detail timeout are the primary levers — not the per-IP
delay.** Three controls:

- **`-a concurrency=<n>`** sets `CONCURRENT_REQUESTS` (default **4**). Hold it near
  the origin's knee. Counterintuitively, *fewer* concurrent detail requests yield
  *more* completed details — past the knee you get a timeout storm, not speed.
  Start low and raise it while watching the `[proxy-pool]` per-IP `err` rate in
  the log; the origin tolerates more **off-peak** (evenings/overnight US Eastern).
- **`-a detail_timeout=<seconds>`** sets the per-request detail timeout (default
  **120**, ~3.5× the observed slow baseline). Because the page is genuinely slow,
  a *tight* timeout makes valid-but-slow pages time out and churn (timeout → 45s
  slot pause → retry) instead of just finishing a few seconds later. Raise it if
  the log shows timeout pauses on pages that would have completed; lower it only
  if genuinely-hung requests tie up slots too long.
- **Detail-timeout backoff:** a detail GET carries `meta['timeout_backoff']`, so a
  timeout (now only for pages exceeding the generous `detail_timeout`) is treated
  as *origin saturation* — `RateLimitBackoffMiddleware` pauses that slot
  (`RATELIMIT_BACKOFF_TIMEOUT_COOLDOWN`, default 45s) and retries a bounded few
  times (`..._MAX_RETRIES`, default 4) instead of re-firing immediately. As slots
  trip and pause, in-flight concurrency drops adaptively, letting the origin
  drain. Chain-critical **pagination is not opted in** and keeps its patient 180s
  + full retry budget.

Diagnosing it: the per-IP report line (`[proxy-pool] last …s per IP
(ok/blocked/err) … [TimeoutError xN]`) is the tell — **`err` dominated by
`TimeoutError` with `blocked`/403s at zero means the origin is saturated, so
*lower* `-a concurrency`, don't add IPs.**

## Running Tests

```bash
pytest provider_scrape/spiders/test_maryland.py -v
```

The tests do not require the tessdata file — OCR calls are mocked.
