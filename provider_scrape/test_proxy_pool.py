import pytest

from provider_scrape.proxy_pool import (
    ProxyPool,
    build_pool,
    load_env_file,
    load_pool,
    parse_endpoints,
    redact,
)


def test_parse_endpoints_splits_on_comma_and_whitespace():
    assert parse_endpoints("a:1, b:2\nc:3\t d:4") == ["a:1", "b:2", "c:3", "d:4"]
    assert parse_endpoints("") == []
    assert parse_endpoints(None) == []


def test_build_pool_composes_authed_urls_and_ids():
    pool = build_pool(["1.1.1.1:80", "2.2.2.2:81"], "user", "pass", id_prefix="ws")
    assert len(pool) == 2
    assert pool.ids == ["ws-0", "ws-1"]
    _id, url = pool.for_key("x")
    assert url.startswith("http://user:pass@")


def test_build_pool_url_encodes_credentials():
    pool = build_pool(["1.1.1.1:80"], "u@e", "p:w/d", id_prefix="ws")
    _id, url = pool.next_rotating()
    # ':' '@' '/' in creds must be percent-encoded so the URL parses correctly.
    assert "http://u%40e:p%3Aw%2Fd@1.1.1.1:80" == url


def test_build_pool_passes_through_full_urls():
    pool = build_pool(["http://user:pass@9.9.9.9:8080"])
    _id, url = pool.next_rotating()
    assert url == "http://user:pass@9.9.9.9:8080"


def test_build_pool_empty_returns_none():
    assert build_pool([], "u", "p") is None
    assert build_pool(["", "  "], "u", "p") is None


def test_proxy_pool_requires_at_least_one_url():
    with pytest.raises(ValueError):
        ProxyPool([])


def test_next_rotating_round_robins():
    pool = build_pool(["a:1", "b:2", "c:3"], "u", "p")
    ids = [pool.next_rotating()[0] for _ in range(7)]
    assert ids == ["proxy-0", "proxy-1", "proxy-2", "proxy-0", "proxy-1", "proxy-2", "proxy-0"]


def test_for_key_is_sticky():
    pool = build_pool(["a:1", "b:2", "c:3"], "u", "p")
    first = pool.for_key("Montgomery")
    assert pool.for_key("Montgomery") == first
    assert pool.for_key("Montgomery") == first


def test_for_key_assigns_new_keys_round_robin():
    pool = build_pool(["a:1", "b:2", "c:3"], "u", "p")
    # Each distinct, previously-unseen key advances the assignment cursor.
    got = [pool.for_key(k)[0] for k in ("A", "B", "C", "D")]
    assert got == ["proxy-0", "proxy-1", "proxy-2", "proxy-0"]


def test_rotating_and_sticky_use_independent_cursors():
    # A sticky assignment must not perturb the rotating sequence or vice versa.
    pool = build_pool(["a:1", "b:2"], "u", "p")
    pool.for_key("k")  # consumes one from the assign cursor only
    assert [pool.next_rotating()[0] for _ in range(2)] == ["proxy-0", "proxy-1"]


def test_redact_masks_credentials():
    assert redact("http://user:pass@1.2.3.4:80") == "http://***@1.2.3.4:80"
    # No credentials -> unchanged host:port.
    assert redact("http://1.2.3.4:80") == "http://1.2.3.4:80"


def test_load_env_file_parses_key_values(tmp_path):
    p = tmp_path / "webshare.env"
    p.write_text(
        "# comment\n\n"
        "webshare_proxy_username = user \n"
        'webshare_proxy_password="p@ss"\n'
        "webshare_proxy_endpoints=a:1,b:2\n"
    )
    env = load_env_file(str(p))
    assert env["webshare_proxy_username"] == "user"
    assert env["webshare_proxy_password"] == "p@ss"
    assert env["webshare_proxy_endpoints"] == "a:1,b:2"


def test_load_env_file_missing_returns_empty():
    assert load_env_file("/no/such/file.env") == {}
    assert load_env_file(None) == {}


def test_load_pool_from_env_file(tmp_path):
    p = tmp_path / "webshare.env"
    p.write_text(
        "webshare_proxy_username=u\n"
        "webshare_proxy_password=p\n"
        "webshare_proxy_endpoints=1.1.1.1:80, 2.2.2.2:81\n"
    )
    pool = load_pool(env_path=str(p), id_prefix="webshare")
    assert len(pool) == 2
    assert pool.ids == ["webshare-0", "webshare-1"]


def test_load_pool_explicit_endpoints_override_env(tmp_path):
    p = tmp_path / "webshare.env"
    p.write_text(
        "webshare_proxy_username=u\n"
        "webshare_proxy_password=p\n"
        "webshare_proxy_endpoints=1.1.1.1:80\n"
    )
    # Inline endpoints win; creds still resolve from the env file.
    pool = load_pool(env_path=str(p), endpoints="9.9.9.9:9000")
    assert len(pool) == 1
    _id, url = pool.next_rotating()
    assert url == "http://u:p@9.9.9.9:9000"


def test_load_pool_none_when_no_config(tmp_path):
    assert load_pool(env_path=str(tmp_path / "absent.env")) is None
