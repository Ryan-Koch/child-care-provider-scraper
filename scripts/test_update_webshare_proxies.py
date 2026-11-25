"""Tests for scripts/update_webshare_proxies.py (no network).

Run with the project virtualenv:
``.venv/bin/pytest scripts/test_update_webshare_proxies.py``.
"""

import urllib.error

import pytest
import update_webshare_proxies as uwp

ENV = """# comment stays
webshare_proxy_username=olduser
webshare_proxy_password=oldpass
webshare_proxy_endpoints=1.1.1.1:80,2.2.2.2:80
webshare_download_url=https://example.invalid/list
"""


@pytest.fixture
def env_file(tmp_path):
    path = tmp_path / "webshare.env"
    path.write_text(ENV, encoding="utf-8")
    return path


def _serve(monkeypatch, body):
    monkeypatch.setattr(uwp, "fetch_proxy_list", lambda url, timeout: uwp.parse_proxy_list(body))


def test_parse_proxy_list_endpoints_and_credentials():
    body = "1.1.1.1:80:u:p\n\n2.2.2.2:81:u:p\nbad\n3.3.3.3:82\n"
    endpoints, creds = uwp.parse_proxy_list(body)
    assert endpoints == ["1.1.1.1:80", "2.2.2.2:81", "3.3.3.3:82"]
    assert creds == [("u", "p")]


def test_parse_proxy_list_keeps_colon_in_password():
    _, creds = uwp.parse_proxy_list("1.1.1.1:80:user:pa:ss\n")
    assert creds == [("user", "pa:ss")]


def test_parse_proxy_list_never_echoes_malformed_line(capsys):
    uwp.parse_proxy_list("secretline\n")
    assert "secretline" not in capsys.readouterr().err


def test_rewrite_env_value_replaces_or_appends():
    assert uwp.rewrite_env_value("a=1\nb=2\n", "b", "3") == "a=1\nb=3\n"
    assert uwp.rewrite_env_value("a=1", "b", "3") == "a=1\nb=3"
    # A commented-out key is not the live one.
    assert uwp.rewrite_env_value("# b=old\n", "b", "new") == "# b=old\nb=new\n"


def test_rotated_credentials_are_rewritten_and_not_printed(monkeypatch, env_file, capsys):
    _serve(monkeypatch, "1.1.1.1:80:newuser:newsecret\n2.2.2.2:80:newuser:newsecret\n")
    assert uwp.main(["--env-file", str(env_file)]) == 0
    text = env_file.read_text(encoding="utf-8")
    assert "webshare_proxy_username=newuser\n" in text
    assert "webshare_proxy_password=newsecret\n" in text
    assert "webshare_proxy_endpoints=1.1.1.1:80,2.2.2.2:80\n" in text
    assert text.startswith("# comment stays\n")
    out = capsys.readouterr()
    assert "newsecret" not in out.out + out.err
    assert "oldpass" not in out.out + out.err


def test_endpoints_and_credentials_both_update(monkeypatch, env_file):
    _serve(monkeypatch, "9.9.9.9:90:newuser:oldpass\n")
    assert uwp.main(["--env-file", str(env_file)]) == 0
    text = env_file.read_text(encoding="utf-8")
    assert "webshare_proxy_endpoints=9.9.9.9:90\n" in text
    assert "webshare_proxy_username=newuser\n" in text
    assert "webshare_proxy_password=oldpass\n" in text


def test_up_to_date_leaves_file_untouched(monkeypatch, env_file, capsys):
    _serve(monkeypatch, "1.1.1.1:80:olduser:oldpass\n2.2.2.2:80:olduser:oldpass\n")
    assert uwp.main(["--env-file", str(env_file)]) == 0
    assert env_file.read_text(encoding="utf-8") == ENV
    assert "no change" in capsys.readouterr().out


def test_dry_run_does_not_write(monkeypatch, env_file):
    _serve(monkeypatch, "1.1.1.1:80:newuser:newpass\n")
    assert uwp.main(["--env-file", str(env_file), "--dry-run"]) == 0
    assert env_file.read_text(encoding="utf-8") == ENV


def test_conflicting_credentials_are_left_alone(monkeypatch, env_file, capsys):
    _serve(monkeypatch, "1.1.1.1:80:a:x\n2.2.2.2:80:b:y\n")
    assert uwp.main(["--env-file", str(env_file)]) == 0
    assert env_file.read_text(encoding="utf-8") == ENV
    assert "2 different username/password pairs" in capsys.readouterr().err


def test_list_without_credentials_keeps_existing(monkeypatch, env_file):
    _serve(monkeypatch, "3.3.3.3:80\n")
    assert uwp.main(["--env-file", str(env_file)]) == 0
    text = env_file.read_text(encoding="utf-8")
    assert "webshare_proxy_endpoints=3.3.3.3:80\n" in text
    assert "webshare_proxy_username=olduser\n" in text
    assert "webshare_proxy_password=oldpass\n" in text


def test_fetch_failure_leaves_file_untouched(monkeypatch, env_file):
    def boom(url, timeout):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(uwp, "fetch_proxy_list", boom)
    assert uwp.main(["--env-file", str(env_file)]) == 1
    assert env_file.read_text(encoding="utf-8") == ENV


def test_empty_list_leaves_file_untouched(monkeypatch, env_file):
    _serve(monkeypatch, "\n")
    assert uwp.main(["--env-file", str(env_file)]) == 1
    assert env_file.read_text(encoding="utf-8") == ENV
