#!/usr/bin/env python3
"""Refresh ``webshare_proxy_endpoints`` in ``webshare.env`` from Webshare.

Webshare rotates proxies: an IP can lose reputation or go down and get replaced
by a fresh one. The account's "Download proxy list" URL always returns the
current set, one ``ip:port:user:pass`` per line. This script fetches that list,
and rewrites ONLY the ``webshare_proxy_endpoints=`` line (each line's
``ip:port``) plus the ``webshare_proxy_username=`` / ``webshare_proxy_password=``
lines (the shared credentials, which Webshare also rotates — a stale pair makes
every proxy answer HTTP 407) in place. Every comment, blank line, and other
variable is preserved, and the credential values are never printed.

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
USERNAME_KEY = "webshare_proxy_username"
PASSWORD_KEY = "webshare_proxy_password"
DOWNLOAD_URL_KEY = "webshare_download_url"


def parse_proxy_list(body):
    """Parse a Webshare download body into ``(endpoints, credentials)``.

    Each line is ``ip:port:user:pass``. ``endpoints`` is the ordered ``ip:port``
    list; ``credentials`` is the distinct ``(user, pass)`` pairs seen, in first-
    seen order (normally exactly one — Webshare shares them across the list).
    Blank and malformed (< 2 field) lines are skipped with a warning; a line
    with no credential fields still contributes its endpoint.
    """
    endpoints, credentials = [], []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        # maxsplit=3: a password containing ':' stays whole.
        parts = line.split(":", 3)
        if len(parts) < 2 or not parts[0] or not parts[1]:
            # Don't echo the line: it may carry a password.
            print(f"  skipping malformed line ({len(parts)} field(s))", file=sys.stderr)
            continue
        endpoints.append(f"{parts[0]}:{parts[1]}")
        if len(parts) == 4 and parts[2] and parts[3]:
            pair = (parts[2], parts[3])
            if pair not in credentials:
                credentials.append(pair)
    return endpoints, credentials


def fetch_proxy_list(url, timeout):
    """Download the proxy list and return :func:`parse_proxy_list`'s result."""
    request = urllib.request.Request(url, headers={"User-Agent": "webshare-refresh"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")
    return parse_proxy_list(body)


def rewrite_env_value(text, key, value):
    """Return ``text`` with the ``key=`` line's value replaced by ``value``.

    Only that one (first, uncommented) line changes; if the key is absent it is
    appended. Every other line and the file's trailing-newline state are kept.
    """
    new_line = f"{key}={value}"
    had_trailing_newline = text.endswith("\n")
    lines = text.splitlines()

    for i, line in enumerate(lines):
        if line.lstrip().startswith(f"{key}="):
            lines[i] = new_line
            break
    else:
        lines.append(new_line)

    out = "\n".join(lines)
    if had_trailing_newline or not text:
        out += "\n"
    return out


def rewrite_endpoints_line(text, endpoints):
    """Return ``text`` with the ``webshare_proxy_endpoints=`` value replaced."""
    return rewrite_env_value(text, ENDPOINTS_KEY, ",".join(endpoints))


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
        endpoints, credentials = fetch_proxy_list(url, args.timeout)
    except (urllib.error.URLError, OSError) as exc:
        print(f"ERROR: could not download proxy list: {exc}", file=sys.stderr)
        print("Existing endpoints and credentials left unchanged.", file=sys.stderr)
        return 1

    if not endpoints:
        print("ERROR: proxy list was empty; existing endpoints left unchanged.", file=sys.stderr)
        return 1

    with open(args.env_file, encoding="utf-8") as handle:
        text = handle.read()
    new_text = text
    changes = []

    old = [e for e in (env.get(ENDPOINTS_KEY) or "").split(",") if e]
    added = [e for e in endpoints if e not in set(old)]
    removed = [e for e in old if e not in set(endpoints)]
    print(f"Fetched {len(endpoints)} endpoints ({len(added)} new, {len(removed)} gone, {len(old)} previously listed).")
    for e in added:
        print(f"  + {e}")
    for e in removed:
        print(f"  - {e}")
    if endpoints != old:
        new_text = rewrite_endpoints_line(new_text, endpoints)
        changes.append(ENDPOINTS_KEY)

    # Credentials. Webshare rotates the shared username/password too, and a
    # stale pair makes every proxy answer HTTP 407 — which spiders that probe
    # the pool read as "all proxies unreachable" and silently go direct. Never
    # print the values; just say which changed.
    if len(credentials) == 1:
        user, password = credentials[0]
        if user != env.get(USERNAME_KEY):
            new_text = rewrite_env_value(new_text, USERNAME_KEY, user)
            changes.append(USERNAME_KEY)
        if password != env.get(PASSWORD_KEY):
            new_text = rewrite_env_value(new_text, PASSWORD_KEY, password)
            changes.append(PASSWORD_KEY)
        cred_changes = [k for k in changes if k != ENDPOINTS_KEY]
        print(f"Credentials: changed ({', '.join(cred_changes)})." if cred_changes else "Credentials: unchanged.")
    elif not credentials:
        print("Credentials: none in the proxy list; existing username/password left unchanged.")
    else:
        print(
            f"WARNING: proxy list has {len(credentials)} different username/password pairs; "
            "the env file holds one shared pair, so credentials were left unchanged.",
            file=sys.stderr,
        )

    if not changes:
        print("Endpoints and credentials already up to date; no change.")
        return 0

    if args.dry_run:
        print(f"--dry-run: not writing. Would update: {', '.join(changes)}.")
        return 0

    with open(args.env_file, "w", encoding="utf-8") as handle:
        handle.write(new_text)
    print(f"Updated {', '.join(changes)} in {args.env_file}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
