"""Tests for scripts/upload_to_huggingface.py (no network).

Covers the README/config generation added so a JSON upload declares one Hugging
Face dataset configuration per state file (sidestepping the "all files must have
the same columns" load error), plus the frontmatter merge that preserves an
existing dataset card.

Run with the project virtualenv:
``.venv/bin/pytest scripts/test_upload_to_huggingface.py``.
"""

import os
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError

import pytest
import upload_to_huggingface as up
import yaml


# --------------------------------------------------------------------------- #
# config_name_for / build_configs
# --------------------------------------------------------------------------- #
def test_config_name_strips_dir_and_extension():
    assert up.config_name_for("state_output/alabama.json") == "alabama"
    assert up.config_name_for("out/new_jersey.csv") == "new_jersey"


def test_build_configs_one_entry_per_file_relative_to_root():
    files = ["out/alabama.json", "out/alaska.json"]
    assert up.build_configs(files, "") == [
        {"config_name": "alabama", "data_files": "alabama.json"},
        {"config_name": "alaska", "data_files": "alaska.json"},
    ]


def test_build_configs_includes_path_in_repo_prefix():
    configs = up.build_configs(["out/ohio.json"], "data/2026")
    assert configs == [
        {"config_name": "ohio", "data_files": "data/2026/ohio.json"},
    ]


def test_build_configs_drops_duplicate_config_names():
    # Same state from two dirs would collide; HF rejects repeated config names.
    files = ["a/texas.json", "b/texas.json"]
    assert up.build_configs(files, "") == [
        {"config_name": "texas", "data_files": "texas.json"},
    ]


# --------------------------------------------------------------------------- #
# split_frontmatter
# --------------------------------------------------------------------------- #
def test_split_frontmatter_no_fence_is_all_body():
    data, body = up.split_frontmatter("# Title\n\nsome prose")
    assert data == {}
    assert body == "# Title\n\nsome prose"


def test_split_frontmatter_parses_block_and_body():
    text = "---\nlicense: mit\ntags:\n- childcare\n---\n\n# Card body"
    data, body = up.split_frontmatter(text)
    assert data == {"license": "mit", "tags": ["childcare"]}
    assert body == "\n# Card body"


def test_split_frontmatter_unclosed_fence_is_body():
    # A leading '---' with no closing fence is a horizontal rule, not metadata.
    data, body = up.split_frontmatter("---\njust text, no close")
    assert data == {}
    assert body == "---\njust text, no close"


def test_split_frontmatter_invalid_yaml_raises():
    try:
        up.split_frontmatter("---\n: : bad\n\t- nope\n---\nbody")
    except ValueError:
        return
    raise AssertionError("expected ValueError on unparseable frontmatter")


# --------------------------------------------------------------------------- #
# render_readme
# --------------------------------------------------------------------------- #
def _frontmatter_of(text):
    assert text.startswith("---\n")
    fm = text.split("---\n", 2)[1]
    return yaml.safe_load(fm)


def test_render_readme_fresh_has_configs_and_default_body():
    configs = [{"config_name": "alabama", "data_files": "alabama.json"}]
    out = up.render_readme(None, configs)
    assert _frontmatter_of(out)["configs"] == configs
    assert "load_dataset" in out  # the default body was used


def test_render_readme_preserves_body_and_other_keys():
    existing = "---\nlicense: mit\npretty_name: US Child Care Providers\n---\n\n# My hand-written card\n\nKeep me.\n"
    configs = [{"config_name": "utah", "data_files": "utah.json"}]
    out = up.render_readme(existing, configs)

    fm = _frontmatter_of(out)
    assert fm["license"] == "mit"
    assert fm["pretty_name"] == "US Child Care Providers"
    assert fm["configs"] == configs
    # Body preserved verbatim, default body not injected.
    assert "# My hand-written card" in out
    assert "Keep me." in out
    assert "load_dataset" not in out


def test_render_readme_replaces_existing_configs():
    existing = "---\nconfigs:\n- config_name: stale\n  data_files: stale.json\n---\n\nbody\n"
    configs = [{"config_name": "ohio", "data_files": "ohio.json"}]
    out = up.render_readme(existing, configs)
    fm = _frontmatter_of(out)
    assert fm["configs"] == configs  # old entry fully replaced
    assert "stale" not in out


def test_render_readme_roundtrips_through_split():
    # What we emit must parse back cleanly (guards against fence/format drift).
    configs = up.build_configs(["out/alabama.json", "out/alaska.json"], "")
    out = up.render_readme(None, configs)
    data, body = up.split_frontmatter(out)
    assert data["configs"] == configs
    assert body.strip()


# --------------------------------------------------------------------------- #
# build_extra_operations
# --------------------------------------------------------------------------- #
def test_build_extra_operations_uploads_basename_at_root(tmp_path):
    src = tmp_path / "SOURCES.md"
    src.write_text("# sources", encoding="utf-8")
    ops = up.build_extra_operations([str(src)], "")
    assert len(ops) == 1
    assert ops[0].path_in_repo == "SOURCES.md"


def test_build_extra_operations_applies_path_in_repo_prefix(tmp_path):
    src = tmp_path / "SOURCES.md"
    src.write_text("# sources", encoding="utf-8")
    ops = up.build_extra_operations([str(src)], "data/2026")
    assert ops[0].path_in_repo == "data/2026/SOURCES.md"


def test_build_extra_operations_skips_missing_files(tmp_path):
    present = tmp_path / "SOURCES.md"
    present.write_text("# sources", encoding="utf-8")
    ops = up.build_extra_operations([str(present), str(tmp_path / "nope.md")], "")
    assert [o.path_in_repo for o in ops] == ["SOURCES.md"]


def test_validation_groups_empty_and_old_json_by_state(tmp_path):
    now = datetime(2026, 9, 26, tzinfo=UTC)
    old = tmp_path / "alabama.json"
    old.write_text("  [  ]\n", encoding="utf-8")
    old_time = (now - timedelta(days=6)).timestamp()
    os.utime(old, (old_time, old_time))
    zero = tmp_path / "alaska.json"
    zero.touch()
    cutoff = (now - timedelta(days=5)).timestamp()
    os.utime(zero, (cutoff, cutoff))
    issues = up.validate_inputs([str(tmp_path)], now=now)

    assert set(issues) == {"alabama", "alaska"}
    assert len(issues["alabama"]) == 2
    assert "empty JSON" in issues["alabama"][0]
    assert "older than 5 days" in issues["alabama"][1]
    assert "empty JSON" in issues["alaska"][0]
    assert len(issues["alaska"]) == 1
    assert "alabama:" in up.format_failures(issues)


def test_validation_clean_and_ignores_logs_and_nested_files(tmp_path):
    (tmp_path / "ohio.json").write_text('[{"name": "example"}]', encoding="utf-8")
    (tmp_path / "ohio.log").write_text(
        "2026-09-26 [scrapy.core.scraper] ERROR: Spider error processing request\n    'finish_reason': 'shutdown',\n",
        encoding="utf-8",
    )
    nested = tmp_path / "scratch"
    nested.mkdir()
    (nested / "bad.json").touch()

    assert up.validate_inputs([str(tmp_path)]) == {}


def test_validation_file_input_only_checks_selected_state(tmp_path):
    selected = tmp_path / "ohio.json"
    selected.write_text("[]", encoding="utf-8")
    (tmp_path / "texas.json").touch()

    assert list(up.validate_inputs([str(selected)])) == ["ohio"]
    assert len(up.validate_inputs([str(selected)])["ohio"]) == 1


def test_large_nonempty_json_does_not_need_full_parse(tmp_path):
    (tmp_path / "ohio.json").write_text(" " * 5000 + "[{}]", encoding="utf-8")
    assert up.validate_inputs([str(tmp_path)]) == {}


def test_discord_messages_include_each_state_without_exceeding_limit():
    failures = {f"state_{i:03}": ["JSON is older than 5 days: " + "X" * 150] for i in range(50)}

    messages = up.discord_messages(failures)

    assert len(messages) > 1
    assert all(len(message) <= 2000 for message in messages)
    assert all(f"state_{i:03}:" in "\n".join(messages) for i in range(50))


def test_discord_sends_json_with_confirmation_and_no_mentions(monkeypatch):
    requests = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return Response()

    monkeypatch.setattr(up, "urlopen", fake_urlopen)
    up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})

    request, timeout = requests[0]
    assert timeout == 10
    assert request.full_url.endswith("?wait=true")
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("User-agent") == up.DISCORD_USER_AGENT
    assert b'"allowed_mentions": {"parse": []}' in request.data
    assert b"ohio: empty JSON" in request.data


def test_discord_retries_429_after_retry_after(monkeypatch):
    calls = []
    waits = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    def fake_urlopen(request, timeout):
        calls.append(request)
        if len(calls) == 1:
            raise HTTPError(request.full_url, 429, "rate limited", {"Retry-After": "0.1"}, None)
        return Response()

    monkeypatch.setattr(up, "urlopen", fake_urlopen)
    monkeypatch.setattr(up.time, "sleep", waits.append)
    up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})
    assert len(calls) == 2
    assert waits == [0.1]


def test_discord_nonretryable_http_error_does_not_expose_webhook(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr(up, "urlopen", fake_urlopen)
    with pytest.raises(RuntimeError, match="Discord returned HTTP 404") as failure:
        up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})
    assert "secret" not in str(failure.value)


def test_discord_rate_limit_wait_is_bounded(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 429, "rate limited", {"Retry-After": "65"}, None)

    monkeypatch.setattr(up, "urlopen", fake_urlopen)
    monkeypatch.setattr(up.time, "sleep", lambda *_args: pytest.fail("should not sleep for a minute"))
    with pytest.raises(RuntimeError, match="Discord returned HTTP 429"):
        up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})


@pytest.mark.parametrize("format_name", ["json", "csv"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_failed_validation_alerts_and_prevents_upload(tmp_path, monkeypatch, format_name, dry_run):
    (tmp_path / "ohio.json").touch()
    if format_name == "csv":
        (tmp_path / "ohio.csv").write_text("name\nexample\n", encoding="utf-8")
    discord_env = tmp_path / "discord.env"
    discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")
    alerts = []
    monkeypatch.setattr(up, "send_discord_alert", lambda url, failures: alerts.append((url, failures)))
    monkeypatch.setattr(up, "HfApi", lambda **_kwargs: pytest.fail("Hugging Face must not be called"))

    args = ["-f", format_name, "--repo", "owner/data", "--token", "fake", "--discord-env-file", str(discord_env)]
    if dry_run:
        args.append("--dry-run")
    with pytest.raises(RuntimeError, match="empty JSON"):
        up.main([*args, str(tmp_path)])

    assert len(alerts) == 1
    assert alerts[0][1]["ohio"][0].endswith("empty JSON (0 bytes or [])")


def test_missing_or_failing_discord_does_not_allow_upload(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").touch()
    missing = tmp_path / "no-discord.env"
    with pytest.raises(RuntimeError, match="Discord alert not sent: missing webhook_url"):
        up.main(["--dry-run", "--discord-env-file", str(missing), str(tmp_path)])

    discord_env = tmp_path / "discord.env"
    discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")
    monkeypatch.setattr(up, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("secret")))
    with pytest.raises(RuntimeError, match="Could not connect to Discord") as failure:
        up.main(["--dry-run", "--discord-env-file", str(discord_env), str(tmp_path)])
    assert "ohio" in str(failure.value)
    assert "secret" not in str(failure.value)


def test_clean_dry_run_does_not_contact_discord_or_hugging_face(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    monkeypatch.setattr(up, "HfApi", lambda **_kwargs: pytest.fail("unexpected upload"))

    assert up.main(["--dry-run", "--repo", "owner/dataset", str(tmp_path)]) == 0


def test_clean_upload_commits_after_validation(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    calls = []

    class Api:
        def __init__(self, token):
            calls.append(("token", token))

        def create_commit(self, **kwargs):
            calls.append(("commit", kwargs))
            return type("Commit", (), {"commit_url": "https://huggingface.co/owner/data"})()

    monkeypatch.setattr(up, "HfApi", Api)
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    assert up.main(["--repo", "owner/data", "--token", "fake", "--no-readme", str(tmp_path)]) == 0
    assert calls[0] == ("token", "fake")
    assert calls[1][1]["operations"][0].path_in_repo == "ohio.json"
