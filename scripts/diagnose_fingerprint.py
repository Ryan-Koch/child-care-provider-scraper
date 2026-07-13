#!/usr/bin/env python3
"""Diagnose why a browser-driven spider (RI reference impl) is failing
reCAPTCHA v3 on a given host — WITHOUT touching the live state site.

It checks the two independent levers from docs/browser_signature.md against
the *actual config baked into whatever image/checkout this runs in*:

  1. NETWORK identity  — the egress IP's geolocation/timezone, fetched through
     the same headed Chrome the spider uses (so it's the exact egress v3 sees).
  2. BROWSER signature — the fingerprint the page can read (WebGL renderer,
     webdriver, plugins, claimed timezone), with the spider's real launch args,
     context, and playwright-stealth scripts applied.

Then it cross-checks them and prints a verdict pointing at the likely cause.

Run it wherever the spider runs. On Linux the spider is headed, so wrap it in
xvfb exactly like run_spiders.sh does. In the Docker image:

    docker run --rm --init --shm-size=2gb -e HOME=/tmp -e PYTHONPATH=/app \\
      --entrypoint bash child-care-provider-scraper:latest \\
      -c 'xvfb-run -a -s "-screen 0 1920x1080x24" python /app/scripts/diagnose_fingerprint.py'

(If the script isn't baked into the image yet, add
 `-v "$(pwd)/scripts/diagnose_fingerprint.py":/app/scripts/diagnose_fingerprint.py:ro`.)

Exit code: 0 if both levers look good, 1 if a likely-blocking problem is found.
"""
import asyncio
import json
import sys
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.async_api import async_playwright

# Pull the spider's REAL config so this reflects exactly what will run.
from provider_scrape.spiders.rhode_island import (
    RhodeIslandSpider,
    _STEALTH_SCRIPT,
    _CANVAS_PATCH,
    _HW_PATCH,
)

_CS = RhodeIslandSpider.custom_settings
LAUNCH = _CS["PLAYWRIGHT_LAUNCH_OPTIONS"]
CTX = _CS["PLAYWRIGHT_CONTEXTS"]["default"]
CLAIMED_TZ = CTX.get("timezone_id", "America/New_York")

FP_PROBE = r"""
() => {
    const gl = document.createElement('canvas').getContext('webgl');
    const dbg = gl && gl.getExtension('WEBGL_debug_renderer_info');
    return {
        webglRenderer: (gl && dbg) ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL)
                                   : 'NO_WEBGL',
        webdriver: navigator.webdriver,
        pluginsLength: navigator.plugins.length,
        platform: navigator.platform,
        hardwareConcurrency: navigator.hardwareConcurrency,
        browserTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        timezoneOffsetMin: new Date().getTimezoneOffset(),
    };
}
"""


def _utc_offset_hours(tz_name):
    try:
        off = datetime.now(ZoneInfo(tz_name)).utcoffset()
        return round(off.total_seconds() / 3600, 1)
    except Exception:
        return None


async def _egress_via_browser(page):
    """Fetch egress geo through the spider's own Chrome (same network path)."""
    for url in ("https://ipinfo.io/json", "http://ip-api.com/json"):
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            txt = await page.evaluate("() => document.body.innerText")
            data = json.loads(txt)
            # Normalize ip-api.com's field names to ipinfo's.
            if "timezone" in data and "query" in data:  # ip-api shape
                data = {
                    "ip": data.get("query"),
                    "city": data.get("city"),
                    "region": data.get("regionName"),
                    "country": data.get("countryCode"),
                    "timezone": data.get("timezone"),
                }
            if data.get("timezone"):
                data["_source"] = url
                return data
        except Exception as e:  # noqa: BLE001
            last = f"{url}: {e}"
    return {"_error": f"could not resolve egress ({last})"}


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(**LAUNCH)
        context = await browser.new_context(**CTX)
        await context.add_init_script(_STEALTH_SCRIPT)
        await context.add_init_script(_CANVAS_PATCH)
        await context.add_init_script(_HW_PATCH)
        page = await context.new_page()

        egress = await _egress_via_browser(page)
        await page.goto("about:blank")
        fp = await page.evaluate(FP_PROBE)

        await context.close()
        await browser.close()

    return egress, fp


def report(egress, fp):
    problems = []
    warnings = []

    egress_tz = egress.get("timezone")
    claimed_off = _utc_offset_hours(CLAIMED_TZ)
    egress_off = _utc_offset_hours(egress_tz) if egress_tz else None

    print("=" * 66)
    print("EGRESS (network identity — what reCAPTCHA v3 actually sees)")
    print("=" * 66)
    if egress.get("_error"):
        print("  !! could not determine egress:", egress["_error"])
        warnings.append("egress geo lookup failed — check connectivity/VPN")
    else:
        print(f"  ip           : {egress.get('ip')}")
        print(f"  location     : {egress.get('city')}, "
              f"{egress.get('region')}, {egress.get('country')}")
        print(f"  egress tz    : {egress_tz}  (UTC{egress_off:+})"
              if egress_off is not None else f"  egress tz    : {egress_tz}")
        print(f"  (via {egress.get('_source')})")

    print()
    print("=" * 66)
    print("BROWSER SIGNATURE (from the spider's real launch/context/stealth)")
    print("=" * 66)
    rend = fp["webglRenderer"]
    if "SwiftShader" in rend:
        rend_state = "!! SwiftShader LEAK (mask absent — stale image?)"
        problems.append(
            "WebGL renderer leaks 'SwiftShader' — the mask isn't applied. "
            "The image is stale; rebuild it (docker build -t "
            "child-care-provider-scraper:latest .)."
        )
    elif rend == "NO_WEBGL":
        rend_state = "!! NO WebGL context (missing --enable-unsafe-swiftshader?)"
        problems.append("No WebGL context at all — a strong headless/bot tell.")
    else:
        rend_state = "OK (masked, no SwiftShader tell)"
    print(f"  claimed tz   : {CLAIMED_TZ}  (UTC{claimed_off:+})"
          if claimed_off is not None else f"  claimed tz   : {CLAIMED_TZ}")
    print(f"  browser tz   : {fp['browserTimezone']}  "
          f"(offset {-fp['timezoneOffsetMin']//60:+}h)")
    print(f"  webglRenderer: {rend}")
    print(f"                 -> {rend_state}")
    print(f"  webdriver    : {fp['webdriver']}  "
          f"({'OK' if fp['webdriver'] is False else '!! should be false'})")
    print(f"  plugins      : {fp['pluginsLength']}  "
          f"({'OK' if fp['pluginsLength'] else '!! 0 = bot tell'})")
    print(f"  platform     : {fp['platform']}")
    print(f"  cpu cores    : {fp['hardwareConcurrency']}  "
          f"(host-dependent; informational)")

    if fp["webdriver"] is not False:
        problems.append("navigator.webdriver is not false.")
    if not fp["pluginsLength"]:
        problems.append("navigator.plugins is empty (0).")

    # The decisive cross-check: browser timezone vs egress IP timezone.
    print()
    print("=" * 66)
    print("CROSS-CHECK: browser timezone  vs  egress IP timezone")
    print("=" * 66)
    if egress_off is None:
        print("  ?? egress timezone unknown — cannot verify the #1 lever.")
        warnings.append("Could not verify timezone<->IP match.")
    elif claimed_off is not None and abs(claimed_off - egress_off) < 0.01:
        print(f"  OK  browser {CLAIMED_TZ} (UTC{claimed_off:+}) matches "
              f"egress (UTC{egress_off:+}).")
    else:
        print(f"  !! MISMATCH  browser claims {CLAIMED_TZ} (UTC{claimed_off:+}) "
              f"but egress IP is UTC{egress_off:+} ({egress_tz}).")
        problems.append(
            f"TIMEZONE<->IP MISMATCH: browser={CLAIMED_TZ} (UTC{claimed_off:+}) "
            f"vs egress={egress_tz} (UTC{egress_off:+}). Per browser_signature.md "
            "this is the #1 cause of RI v3 failure. Fix: egress this host through "
            "a US-Eastern exit, OR change the spiders' timezone_id to match this "
            "host's real egress."
        )

    print()
    print("=" * 66)
    print("VERDICT")
    print("=" * 66)
    if problems:
        print("  LIKELY BLOCKED. Problems, most important first:")
        for i, msg in enumerate(problems, 1):
            print(f"    {i}. {msg}")
    else:
        print("  Both levers look good. If v3 still fails, suspect IP "
              "reputation (hosting/VPN range) — try a cleaner/residential exit, "
              "or use -a manual_captcha=1 for an attended run.")
    for w in warnings:
        print(f"  (warn) {w}")
    return 1 if problems else 0


if __name__ == "__main__":
    egress, fp = asyncio.run(main())
    sys.exit(report(egress, fp))
