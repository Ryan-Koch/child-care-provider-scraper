#!/usr/bin/env python3
"""Refresh ``webshare_proxy_endpoints`` in ``webshare.env`` from Webshare.

Webshare rotates proxies: an IP can lose reputation or go down and get replaced
by a fresh one. The account's "Download proxy list" URL always returns the
current set, one ``ip:port:user:pass`` per line. This script fetches that list,
keeps the ``ip:port`` of each line (the username/password are shared and already
live in the env file), and rewrites ONLY the ``webshare_proxy_endpoints=`` line
of the env file in place — every comment, blank line, and other variable is
preserved.

It is safe to call unconditionally:

* No env file, or an env file with no ``webshare_download_url`` -> prints a note
  and exits 0. So a Docker build or a ``run_spiders.sh`` pre-step can invoke it
  without first checking whether the pool is configured.
* A fetch that fails or yields zero endpoints is a hard error (exit 1) and the
  existing list is left UNTOUCHED — a transient outage never wipes a working
  pool. Wrap the call in ``|| true`` at a build/run site that must not abort on
  a Webshare hiccup.

Usage::

    python scripts/update_webshare_proxies.py                # rewrite webshare.env
    python scripts/update_webshare_proxies.py --dry-run      # show changes only
    python scripts/update_webshare_proxies.py --env-file X   # a different file
"""

import argparse
import os
import sys
import urllib.error
import urllib.request

# Reuse the exact env-file parser the pool itself uses, so the two never drift.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from provider_scrape.proxy_pool import load_env_file

DEFAULT_ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "webshare.env")
ENDPOINTS_KEY = "webshare_proxy_endpoints"
DOWNLOAD_URL_KEY = "webshare_download_url"


def fetch_endpoints(url, timeout):
    """Download the proxy list and return its ``ip:port`` endpoints, in order.

    Each response line is ``ip:port:user:pass``; only the first two fields are
    kept. Blank and malformed (< 2 field) lines are skipped with a warning.
    """
    request = urllib.request.Request(url, headers={"User-Agent": "webshare-refresh"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")

    endpoints = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) < 2 or not parts[0] or not parts[1]:
            print(f"  skipping malformed line: {line!r}", file=sys.stderr)
            continue
        endpoints.append(f"{parts[0]}:{parts[1]}")
    return endpoints


def rewrite_endpoints_line(text, endpoints):
    """Return ``text`` with the ``webshare_proxy_endpoints=`` value replaced.

    Only that one line changes; if the key is absent it is appended. The file's
    trailing-newline state is preserved.
    """
    new_line = f"{ENDPOINTS_KEY}={','.join(endpoints)}"
    had_trailing_newline = text.endswith("\n")
    lines = text.splitlines()

    replaced = False
    for i, line in enumerate(lines):
        if line.lstrip().startswith(f"{ENDPOINTS_KEY}=") and not line.lstrip().startswith("#"):
            lines[i] = new_line
            replaced = True
            break
    if not replaced:
        lines.append(new_line)

    out = "\n".join(lines)
    if had_trailing_newline or not text:
        out += "\n"
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=DEFAULT_ENV_FILE,
        help=f"path to the env file to update (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and report changes but do not write the file",
    )
    args = parser.parse_args(argv)

    # Skip cleanly when there is nothing to do — building/running without a pool
    # is normal, so these are not errors.
    if not os.path.exists(args.env_file):
        print(f"{args.env_file} not found; nothing to update (proxy pool not configured).")
        return 0
    env = load_env_file(args.env_file)
    url = env.get(DOWNLOAD_URL_KEY)
    if not url:
        print(f"No {DOWNLOAD_URL_KEY} in {args.env_file}; nothing to update.")
        return 0

    print(f"Fetching proxy list for {args.env_file} ...")
    try:
        endpoints = fetch_endpoints(url, args.timeout)
    except (urllib.error.URLError, OSError) as exc:
        print(f"ERROR: could not download proxy list: {exc}", file=sys.stderr)
        print("Existing endpoints left unchanged.", file=sys.stderr)
        return 1

    if not endpoints:
        print("ERROR: proxy list was empty; existing endpoints left unchanged.", file=sys.stderr)
        return 1

    old = [e for e in (env.get(ENDPOINTS_KEY) or "").split(",") if e]
    added = [e for e in endpoints if e not in set(old)]
    removed = [e for e in old if e not in set(endpoints)]
    print(f"Fetched {len(endpoints)} endpoints ({len(added)} new, {len(removed)} gone, {len(old)} previously listed).")
    for e in added:
        print(f"  + {e}")
    for e in removed:
        print(f"  - {e}")

    if endpoints == old:
        print("Endpoints already up to date; no change.")
        return 0

    with open(args.env_file, encoding="utf-8") as handle:
        text = handle.read()
    new_text = rewrite_endpoints_line(text, endpoints)

    if args.dry_run:
        print("--dry-run: not writing. New line would be:")
        print(f"  {ENDPOINTS_KEY}={','.join(endpoints)}")
        return 0

    with open(args.env_file, "w", encoding="utf-8") as handle:
        handle.write(new_text)
    print(f"Updated {ENDPOINTS_KEY} in {args.env_file}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
