"""Optional multi-proxy egress pool for per-IP rate-limited hosts.

Some licensing sites (e.g. Maryland's ``checkccmd.org``) cap throughput per
egress IP — ~2 requests/min — so wall-clock scales only with the number of
distinct IPs, not concurrency. A pool of N proxies, each held to the single-IP
cadence on its own download slot, gives ~N x throughput.

This is strictly opt-in: with no pool configured a spider egresses from its one
host IP exactly as before. The pool object is built from a ``key=value`` env
file (same format as ``huggingface.env``) and/or explicit args, and is consumed
by ``ProxyPoolMiddleware``.

Assignment has two modes:

* **Sticky** (``for_key``): all requests sharing an affinity key — e.g. one
  county's ViewState-bound pagination chain — get the same proxy, so the session
  and its IP stay consistent. New keys are handed out round-robin for balance.
* **Rotating** (``next_rotating``): session-independent requests (detail GETs,
  PDFs) round-robin across the whole pool — where the throughput is won.

Proxy URLs carry credentials, so never log them; log the stable, cred-free slot
id (``proxy_id``) instead.
"""

import os
import re
from urllib.parse import quote, unquote, urlsplit, urlunsplit

# Scrapy signal fired with ``message`` and ``spider`` when the pool changes in a
# way an operator should hear about (a proxy switched away from, or removed).
proxy_pool_alert = object()


def load_env_file(path):
    """Parse a ``key=value`` env file into a dict; ``{}`` if it's absent.

    Blank lines and ``#`` comments are ignored; surrounding whitespace and a
    single layer of matching quotes are stripped from values. Mirrors the
    parser in ``scripts/upload_to_huggingface.py`` so both read the same format.
    """
    values = {}
    if not path or not os.path.exists(path):
        return values
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key.strip()] = value
    return values


def parse_endpoints(raw):
    """Split a comma/whitespace/newline-separated ``host:port`` list."""
    if not raw:
        return []
    return [p for p in re.split(r"[,\s]+", raw.strip()) if p]


def redact(proxy_url):
    """Return a proxy URL with any embedded credentials masked, for logging."""
    try:
        parts = urlsplit(proxy_url)
    except ValueError:
        return proxy_url
    if not parts.hostname:
        return proxy_url
    netloc = parts.hostname
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        netloc = f"***@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def proxy_host(proxy_url):
    """Return a proxy URL's credential-free ``host:port``, or ``"unknown"``."""
    try:
        parts = urlsplit(proxy_url or "")
        port = parts.port
    except ValueError:
        return "unknown"
    if not parts.hostname:
        return "unknown"
    return f"{parts.hostname}:{port}" if port else parts.hostname


def playwright_proxy(proxy_url):
    """Convert a ``http://user:pass@host:port`` proxy URL into the dict shape
    Playwright's ``browser.new_context(proxy=...)`` expects.

    Playwright wants the credentials split out of the server URL::

        {"server": "http://host:port", "username": "...", "password": "..."}

    Returns ``None`` if the URL has no host. Credentials are URL-decoded (they
    were percent-encoded into the URL by :func:`build_pool`).
    """
    try:
        parts = urlsplit(proxy_url)
    except ValueError:
        return None
    if not parts.hostname:
        return None
    server = f"{parts.scheme or 'http'}://{parts.hostname}"
    if parts.port:
        server = f"{server}:{parts.port}"
    proxy = {"server": server}
    if parts.username:
        proxy["username"] = unquote(parts.username)
    if parts.password:
        proxy["password"] = unquote(parts.password)
    return proxy


class ProxyPool:
    """An ordered set of proxy endpoints with sticky and rotating assignment."""

    def __init__(self, proxy_urls, proxy_ids=None):
        self._urls = list(proxy_urls)
        if not self._urls:
            raise ValueError("ProxyPool needs at least one proxy URL")
        self._ids = list(proxy_ids) if proxy_ids else [f"proxy-{i}" for i in range(len(self._urls))]
        if len(self._ids) != len(self._urls):
            raise ValueError("proxy_ids must match proxy_urls in length")
        # Plain counters, taken modulo the live size, so the pool can shrink.
        self._rotate = 0
        self._assign = 0
        self._affinity = {}

    def __len__(self):
        return len(self._urls)

    @property
    def ids(self):
        return list(self._ids)

    def entries(self):
        """Return ``(proxy_id, proxy_url)`` pairs in pool order.

        Handy for callers (e.g. the Rhode Island spider) that probe or launch
        each proxy individually rather than via the sticky/rotating assignment.
        """
        return list(zip(self._ids, self._urls, strict=True))

    def next_rotating(self):
        """Return ``(proxy_id, proxy_url)`` for the next proxy, round-robin."""
        i = self._rotate % len(self._ids)
        self._rotate += 1
        return self._ids[i], self._urls[i]

    def for_key(self, key):
        """Return ``(proxy_id, proxy_url)`` pinned to ``key``.

        Assigned once, unless the pinned proxy is later removed, in which case
        the key is re-pinned to a remaining proxy.
        """
        if key not in self._affinity:
            self._affinity[key] = self._ids[self._assign % len(self._ids)]
            self._assign += 1
        i = self._ids.index(self._affinity[key])
        return self._ids[i], self._urls[i]

    def remove(self, proxy_id):
        """Drop a proxy and release its pinned keys; return whether it was removed.

        The last remaining proxy is kept, so assignment always has an answer.
        """
        if proxy_id not in self._ids or len(self._ids) == 1:
            return False
        i = self._ids.index(proxy_id)
        del self._ids[i]
        del self._urls[i]
        self._affinity = {key: pid for key, pid in self._affinity.items() if pid != proxy_id}
        return True


def build_pool(endpoints, username=None, password=None, id_prefix="proxy"):
    """Build a :class:`ProxyPool` from ``host:port`` (or full-URL) endpoints.

    Bare ``host:port`` endpoints are turned into ``http://user:pass@host:port``
    when credentials are supplied. Returns ``None`` for an empty endpoint list
    (the signal for single-IP mode).
    """
    urls, ids = [], []
    for i, endpoint in enumerate(endpoints):
        endpoint = (endpoint or "").strip()
        if not endpoint:
            continue
        if "://" in endpoint:
            url = endpoint
        elif username and password:
            url = f"http://{quote(username, safe='')}:{quote(password, safe='')}@{endpoint}"
        else:
            url = f"http://{endpoint}"
        urls.append(url)
        ids.append(f"{id_prefix}-{i}")
    return ProxyPool(urls, ids) if urls else None


def load_pool(env_path=None, endpoints=None, username=None, password=None, id_prefix="proxy"):
    """Resolve pool config from an env file and/or explicit values.

    Explicit args win over env-file values. ``endpoints`` may be a raw string
    (comma/whitespace separated) or a list. Returns ``None`` when nothing is
    configured, so callers fall back to single-IP mode.
    """
    env = load_env_file(env_path) if env_path else {}
    user = username or env.get("webshare_proxy_username")
    pw = password or env.get("webshare_proxy_password")
    raw = endpoints if endpoints is not None else env.get("webshare_proxy_endpoints")
    endpoint_list = raw if isinstance(raw, (list, tuple)) else parse_endpoints(raw)
    return build_pool(endpoint_list, user, pw, id_prefix=id_prefix)
