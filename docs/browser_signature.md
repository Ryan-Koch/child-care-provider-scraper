# Browser Signature Playbook (beating reCAPTCHA v3 on Playwright spiders)

**Audience: future me, months from now, staring at a state spider that suddenly
returns zero rows and needs to remember how this whole anti-bot dance works.**

Some state sites (Salesforce/Aura communities, mostly) sit behind **reCAPTCHA
v3**. v3 is invisible — there's no puzzle to solve. It silently scores each
request 0.0–1.0 and the site rejects anything below its threshold. We never see
the score; we only see the search come back **empty**. To pass, we have to make
a Playwright-driven Chrome look and behave enough like a real human's browser on
a real network that the score clears the bar.

There are **two independent levers**, and both have to be right at once:

1. **Browser signature** — the fingerprint the page can read via JS (WebGL, UA,
   canvas, plugins, timezone, screen…). Covered in most of this doc.
2. **Network identity** — the reputation *and geolocation* of the egress IP.
   This one bit us hardest. See [Network identity](#network-identity-the-ip-matters-as-much-as-the-browser).

> **Reference implementation:** `provider_scrape/spiders/rhode_island.py`. It's
> the most refined version of this pattern and the one whose reasoning is fully
> documented inline. `new_jersey.py`, `minnesota.py`, and `arizona.py` share the
> shape but predate the RI fingerprint audit — see [Cross-spider status](#cross-spider-status).

---

## Recognize the failure first

When v3 blocks us, the Aura search response comes back `SUCCESS` at the HTTP
layer but with an empty result set and this tell in the payload:

```
responseWrap={'isCaptchaInvalid': True, 'isV3Failed': True, 'isValid': False}
```

RI logs this verbatim (`_parse_search_response`), then the retry loop kicks in,
and if all attempts fail:

```
RI search returned no results — possible reCAPTCHA block or response shape change. Aborting.
```

If you see that, it's a **score problem**, not a code bug. Don't go rewriting
parsers. Work the two levers below.

---

## Step 1 — dump the fingerprint (`-a audit=1`)

Before changing anything, see what the page currently sees:

```bash
source .venv/bin/activate
xvfb-run -a -s "-screen 0 1920x1080x24" \
  scrapy crawl rhode_island -a audit=1 -s LOG_LEVEL=INFO
```

This runs `_dump_fingerprint` and logs a `FINGERPRINT_AUDIT {…}` JSON blob
instead of searching. **What "good" looks like on the Linux server:**

| Field | Want to see | Bad (bot tell) |
|---|---|---|
| `webdriver` | `false` | `true` |
| `cdpHooks` | `[]` | any `cdc_…` keys |
| `pluginsLength` | `5` (real names) | `0` |
| `permissionsState` | `notifications` **==** `notificationApi` | mismatch |
| `webglRenderer` | an Intel/Mesa string | contains **`SwiftShader`** or `ERR`/null |
| `userAgent` vs `appVersion` vs `userAgentData.brands` | all the **same** Chrome major version | version disagreement |
| `timezone` | matches the state (`America/New_York` for RI) | anything else |

The classic automation tells (`webdriver`, `cdcHooks`, empty plugins) are all
handled by playwright-stealth. The ones we hand-tuned are WebGL, timezone, and
UA consistency.

---

## The browser signature, patch by patch

All of these are applied in `rhode_island.py`. Each exists to kill a *specific*
inconsistency the audit surfaced — the guiding principle is **internal
consistency beats "impressive" spoofing**. A value that disagrees with another
value (UA says Chrome 124 but userAgentData says 150; platform says MacIntel but
the UA says Linux) is a *stronger* bot signal than the honest value would have
been.

### Real, headed Chrome under a virtual display
- `PLAYWRIGHT_LAUNCH_OPTIONS`: `channel: "chrome"` (real Google Chrome, not
  bundled Chromium) + `headless: False`. Headless leaks fingerprint signals that
  tank the v3 score.
- On the Linux server there's no display, so it runs under **xvfb**.
  `run_spiders.sh` does this automatically for spiders listed in `XVFB_SPIDERS`
  (currently `new_jersey rhode_island arizona`) by prefixing
  `xvfb-run -a -s "-screen 0 1920x1080x24"`.

### Launch args (OS-conditional)
- **Linux:** `--ozone-platform=x11` (use the X11 backend xvfb provides) and
  **`--enable-unsafe-swiftshader`**. The latter is load-bearing — see
  [The SwiftShader saga](#the-swiftshader-saga-a-cautionary-tale).
- **macOS (dev):** `--window-size=1440,900` and `--force-device-scale-factor=2`
  to match a real Retina MacBook. We deliberately do **not** pass these on
  Linux — Retina DPR on a non-Retina virtual screen is itself an inconsistency.

### Browser-context settings — mind the key name
- Set on `PLAYWRIGHT_CONTEXTS["default"]`, **not** `PLAYWRIGHT_CONTEXT_ARGS`.
  ⚠️ There is no such setting as `PLAYWRIGHT_CONTEXT_ARGS`; the audit caught our
  viewport/locale/timezone being **silently dropped** because they were written
  to the wrong key. scrapy-playwright reads `PLAYWRIGHT_CONTEXTS` (plural).
- Values: `viewport 1440x900`, `device_scale_factor 2`, `locale en-US`,
  **`timezone_id "America/New_York"`** (see the network section — this has to
  agree with the egress IP's geolocation), `ignore_https_errors True`.

### Stealth init scripts (applied at context level)
`StealthContextMiddleware` monkeypatches the scrapy-playwright handler's
`_create_browser_context` to inject three init scripts into every context. This
is the same pattern in all four spiders.

1. **playwright-stealth (`_STEALTH_SCRIPT`)** — patches `navigator.webdriver`,
   plugin arrays, permissions, etc. We configure it carefully:
   - `navigator_platform_override` is **derived from the host OS**
     (`_NAV_PLATFORM_BY_OS`: `Linux x86_64` / `MacIntel` / `Win32`). Hardcoding
     `MacIntel` on the Linux box contradicts the real UA and
     `userAgentData.platform` (both "Linux") — a cross-check v3 weighs heavily.
   - **We do NOT override `navigator.userAgent`.** Let real Chrome's native UA
     flow through. Spoofing it created a UA-version mismatch against
     `appVersion` / `userAgentData.brands` = instant "spoofed UA" signal.
   - **WebGL vendor/renderer mask is OS-conditional:**
     - **macOS:** `webgl_vendor=False` — disabled, so the genuine Apple-Silicon
       strings (`Google Inc. (Apple)` / `ANGLE (Apple, Apple M…)`) flow through.
       playwright-stealth's defaults (`Intel Inc.` / `Intel Iris OpenGL Engine`)
       are *wrong* on Apple hardware.
     - **Linux:** enabled with `webgl_vendor_override="Google Inc. (Intel)"` and
       `webgl_renderer_override="ANGLE (Intel, Mesa Intel(R) UHD Graphics 630
       (CFL GT2), OpenGL 4.6 (Core Profile) Mesa 23.2.1)"` — a plausible
       integrated-GPU string that hides the SwiftShader tell (below).
2. **`_CANVAS_PATCH`** — wraps `HTMLCanvasElement.toDataURL` to XOR the low bit
   of each pixel's red channel, so canvas fingerprints aren't stable/identifiable
   across runs.
3. **`_HW_PATCH`** — sets `navigator.deviceMemory => 8` (the spec caps/buckets it
   at 8 anyway). We deliberately **do not** patch `hardwareConcurrency` — letting
   the real core count surface avoids yet another inconsistency.

---

## Behavior matters too (v3 scores *how* you act)

A pristine fingerprint that clicks Search 5 seconds after load with zero mouse
movement still scores low. RI spends time looking human before the protected
click:

- **`_humanize_warmup`** (~20s): random non-linear mouse paths, a light scroll
  down/up, and focus+Tab on the program-name input (a real keystroke event).
  The baseline was raised from ~10s to ~20s after intermittent failures.
- **`_post_form_jitter`**: a couple more mouse moves + idle after ticking the
  age-group checkboxes, so the Search click isn't suspiciously adjacent to the
  form changes.
- **Retry loop (`_submit_search`)**: v3 scores are non-deterministic, so we retry
  up to `search_retries + 1` times (default 3), **fully reloading the page** and
  re-running warm-up between attempts. Reload is required because after a v3
  failure the component swaps in the visible v2 widget; clicking again would
  invoke v2, not a fresh v3 token.
- **Manual fallback (`-a manual_captcha=1`)**: on final failure, wait for a human
  to solve the visible v2 challenge. Only useful with a real display / someone
  watching — not for unattended scheduled runs.

---

## Network identity — the IP matters as much as the browser

**This was the actual blocker for RI, and the least obvious.** A flawless
fingerprint won't save you if the egress IP looks wrong to v3.

Two IP properties matter:

1. **Reputation.** v3 penalizes VPN / datacenter / hosting exit ranges heavily —
   they're shared and abused. Residential IPs score best.
2. **Geolocation vs. browser timezone.** v3 cross-checks the browser's
   `timezone_id` against where the IP geolocates. A mismatch is a strong negative
   signal that stacks on top of reputation.

### The RI diagnosis (2026-07-13)

The spider was failing v3 on every attempt even with a clean fingerprint. The
host egresses through **Proton VPN**. The audit and an `ipinfo.io` lookup showed:

- With a **Seattle (Pacific)** exit → browser said `America/New_York`, IP said
  Pacific. **Failed every time.**
- Switching to an **Eastern-US** exit (Columbus/NYC, `America/New_York`) →
  **passed v3 cleanly on the first attempt**, despite still being a Proton/M247
  *hosting* IP.

So the **timezone↔geo mismatch was doing more damage than the VPN reputation
itself.** Masking WebGL (the SwiftShader fix) was necessary hygiene but did *not*
move the needle on its own — the network fix is what unblocked it.

### The rule

> **The browser `timezone_id` must match the egress IP's timezone** — not the
> state being scraped. A Minnesota childcare site is perfectly happy being
> browsed from the US East Coast; what v3 dislikes is the browser claiming one
> timezone while the IP sits in another.

**Our operating model:** the whole job runs from a single homelab server (in
Korea, as of this writing) tunneled through one **US Eastern** VPN endpoint. So
every browser-driven spider is standardized on **`America/New_York`** to match
that one endpoint, rather than each using its own state's zone. This holds even
after a move back to the US — just keep egressing from an Eastern-time exit.

Verify before a run:

```bash
curl -s https://ipinfo.io/json   # "timezone" should be America/New_York
```

RI is the state where this was *confirmed* load-bearing (a Pacific exit blocked
it outright). If you ever need spiders on genuinely different timezones at the
same time, you'd need per-timezone exits or a residential proxy — but the single
Eastern endpoint keeps everything consistent today.

**Caveat:** a Proton/hosting IP still isn't bulletproof — Proton rotates the IP
behind each exit, and if a given IP's reputation degrades you can see
intermittent failures. The retry loop and `manual_captcha` are the safety nets.

---

## The SwiftShader saga (a cautionary tale)

A concrete example of how a *dependency bump* silently broke the fingerprint —
worth remembering because it'll happen again with the next Chrome bump.

1. **Chrome 150 removed the automatic SwiftShader fallback for WebGL.** On the
   GPU-less server under xvfb, `canvas.getContext('webgl')` started returning
   **null** — the page saw *no WebGL at all*, a strong headless/bot signal. This
   is what started tripping `isCaptchaInvalid`/`isV3Failed` after the bump.
2. Fix #1: add **`--enable-unsafe-swiftshader`** to re-enable software WebGL via
   ANGLE+SwiftShader. WebGL came back — but the honest renderer string was now
   `"ANGLE (Google, Vulkan 1.3.0 (SwiftShader Device …), SwiftShader driver)"`,
   which literally says *"GPU-less datacenter box."* Another tell.
3. Fix #2: the **OS-conditional WebGL mask** above, hiding `SwiftShader` behind a
   plausible Intel/Mesa string on Linux.

Lesson: after any Chrome/Playwright/stealth upgrade, **re-run `-a audit=1`** and
diff the fingerprint against a real Chrome on the same machine before trusting a
scheduled run.

---

## Cross-spider status

All four use `StealthContextMiddleware` + `_CANVAS_PATCH` + `deviceMemory` patch.
Per the operating model above, every spider whose context is actually applied is
standardized on **`America/New_York`**. They differ in maturity:

| Spider | UA override | WebGL | Timezone | Notes |
|---|---|---|---|---|
| **rhode_island** | none (real UA) | OS-conditional mask | `America/New_York` | **Reference.** Post-audit; most consistent. |
| new_jersey | **hardcoded** `Chrome/124` macOS UA + `MacIntel` | `Intel Inc.` / `Intel Iris` | `America/New_York` | **Do NOT "modernize" this.** The coherent macOS profile is **load-bearing for Cloudflare**. A 2026-07 A/B proved it: swapping to RI's honest-Linux fingerprint got an instant `403` on `/Search`, while the macOS profile cleared CF and returned 6.2k providers from the same IP seconds later. NJ faces Cloudflare, not reCAPTCHA v3 — the two want opposite things (see below). |
| minnesota | hardcoded macOS-style | `Intel Inc.` / `Intel Iris` | **dropped** (see note) | ⚠️ Writes its context to the nonexistent `PLAYWRIGHT_CONTEXT_ARGS` key, so its `user_agent`/`viewport`/`locale`/`timezone_id` are all **silently dropped** — the browser reports the *host's* timezone, not Central. Left as-is deliberately: MN is headless and not v3-checked, so its timezone is moot, and fixing the key would newly activate a macOS UA on headless Linux (a bot tell) for no benefit. Modernize the RI way (fix key **and** drop the macOS UA) if it ever needs it. |
| arizona | (see file) | `webgl_vendor=False` | `America/New_York` | Standardized to Eastern (2026-07). Has the Linux `--enable-unsafe-swiftshader` fix; no WebGL mask yet. |

### reCAPTCHA v3 and Cloudflare want *opposite* fingerprints

The single most important cross-spider lesson, learned the hard way (2026-07):

- **RI / reCAPTCHA v3** punishes *internal inconsistency*. A macOS UA on a Linux
  host (UA claims macOS while `userAgentData.platform` says Linux) tanks the
  score. RI's fix is to be honestly Linux: real UA, host-derived platform, WebGL
  masked only to hide the SwiftShader tell.
- **NJ / Cloudflare** does the opposite. The honest-Linux fingerprint gets an
  instant `403` on `/Search`; the coherent *macOS* profile (spoofed UA +
  `MacIntel` + Mac-style WebGL) is what earns `cf_clearance`. A/B-tested from one
  IP: macOS → 6.2k providers, RI-style → 403.

So **there is no universal "modern" fingerprint.** Match the tuning to the bot
system: copy RI's honest-Linux approach only for other **reCAPTCHA-v3** sites.
**Leave NJ's macOS profile alone**, and treat any Cloudflare-guarded spider as its
own tuning problem. (MN is a third case again — headless and unchecked; see its
row.)

> Aside: NJ logs `AttributeError: 'NoneType' object has no attribute 'logger'`
> at startup — its `StealthContextMiddleware` uses the `spider=None` startup arg
> instead of RI's captured `log_spider`. It's **benign**: the per-request context
> still gets stealth and CF still clears (verified — NJ returns 6.2k providers
> with the traceback present). RI has the clean version if you ever want to copy
> it, but don't bundle unrelated changes into the finicky NJ spider casually.

---

## Quick runbook

Spider returns zero rows / you suspect a v3 block:

1. Grep the log for `isV3Failed` / `isCaptchaInvalid`. Present → score problem,
   not a parser bug.
2. `curl -s https://ipinfo.io/json` — is the exit IP's timezone/city consistent
   with the spider's `timezone_id`? **Fix this first**; it's the cheapest and was
   the actual RI cause. (For RI: use an **Eastern-time** exit.)
3. `-a audit=1` under xvfb — check the table in [Step 1](#step-1--dump-the-fingerprint--a-audit1).
   Look especially for `SwiftShader` in `webglRenderer` and any UA-version
   disagreement (a Chrome upgrade may have shifted things — see the SwiftShader
   saga).
4. Still failing? Try a different / cleaner exit IP (residential beats hosting),
   or fall back to `-a manual_captcha=1` for an attended run.

**Cloudflare spiders (NJ) fail differently:** the symptom is an HTTP `403` on the
page load, not `isV3Failed`, and the fix is **not** fingerprint modernization —
NJ's macOS profile is exactly what CF accepts. If NJ 403s, suspect the exit IP /
CF reputation first and leave the fingerprint alone (see Cross-spider status).

**Key files:** `provider_scrape/spiders/rhode_island.py` (reference impl,
`StealthContextMiddleware`, `_STEALTH_SCRIPT`, `_humanize_warmup`,
`_submit_search`, `_dump_fingerprint`); `run_spiders.sh` (`XVFB_SPIDERS`).
