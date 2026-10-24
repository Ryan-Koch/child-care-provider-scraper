"""Tests for scripts/upload_to_huggingface.py (no network).

Covers the README/config generation added so a JSON upload declares one Hugging
Face dataset configuration per state file (sidestepping the "all files must have
the same columns" load error), plus the frontmatter merge that preserves an
existing dataset card.

Run with the project virtualenv:
``.venv/bin/pytest scripts/test_upload_to_huggingface.py``.
"""

import json
import os
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError

import pytest
import upload_to_huggingface as up
import yaml

from provider_scrape import discord_notify


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


@pytest.mark.parametrize(
    "content",
    [
        '[{"name": "a"}, {"name": "b", "n": [1, 2.5, null, true]}]',
        '[\n{"name": "a"},\n{"name": "b"}\n]\n',
        "[1, 23, -4e5]",
        "  [ ]  ",
        '{"not": "an array"}',
        '"just a string"',
    ],
)
def test_json_problem_accepts_valid_json(tmp_path, content):
    path = tmp_path / "ohio.json"
    path.write_text(content, encoding="utf-8")
    assert up.json_problem(str(path)) is None


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        # A crawl killed mid-run: the exporter never wrote the closing bracket.
        ('[\n{"name": "a"},\n{"name": "b"}', "after 2 record(s): file ends before the closing ']'"),
        ('[\n{"name": "a"},\n{"name": "b', "after 1 record(s): Unterminated string"),
        ('[{"name": "a"} {"name": "b"}]', "expected ',' or ']'"),
        # Killed right after a record's separator (seen in a real Mississippi run).
        ('[\n{"name": "a"},\n', "after 1 record(s): file ends mid-record (truncated?)"),
        ('[{"name": "a"},]', "Expecting value (character 15)"),
        ('[{"name": "a"}]\n[{"name": "b"}]', "extra data"),
        ('[{"rating": NaN}]', "non-standard constant NaN"),
        ("<html>502 Bad Gateway</html>", "Expecting value (character 0)"),
    ],
)
def test_json_problem_reports_invalid_json(tmp_path, content, expected):
    path = tmp_path / "ohio.json"
    path.write_text(content, encoding="utf-8")
    problem = up.json_problem(str(path))
    assert problem.startswith("invalid JSON")
    assert expected in problem


def test_json_problem_not_utf8(tmp_path):
    path = tmp_path / "ohio.json"
    path.write_bytes(b'[{"name": "\xff"}]')
    assert "not UTF-8" in up.json_problem(str(path))


def test_json_problem_handles_values_spanning_reads(tmp_path, monkeypatch):
    # Tiny reads force records, strings and numbers to straddle buffer edges.
    monkeypatch.setattr(up, "JSON_CHUNK_CHARS", 3)
    records = [{"name": "x" * 50, "n": 1234567, "nested": {"list": [1, 2, 3]}}] * 20
    path = tmp_path / "ohio.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    assert up.json_problem(str(path)) is None

    path.write_text(json.dumps(records)[:-30], encoding="utf-8")
    assert "invalid JSON after 19 record(s)" in up.json_problem(str(path))


def test_json_problem_caps_lookahead_on_malformed_record(tmp_path, monkeypatch):
    monkeypatch.setattr(up, "JSON_CHUNK_CHARS", 4)
    monkeypatch.setattr(up, "JSON_MAX_RECORD_CHARS", 64)
    path = tmp_path / "ohio.json"
    path.write_text('[{"name": "unterminated' + "x" * 10_000 + "]", encoding="utf-8")
    reads = []
    real_read = up._JsonStream._read
    monkeypatch.setattr(up._JsonStream, "_read", lambda self, *a: reads.append(1) or real_read(self, *a))

    assert "Unterminated string" in up.json_problem(str(path))
    assert len(reads) < 20  # gave up near the cap instead of reading the whole file


def test_validation_flags_invalid_json_by_state(tmp_path):
    (tmp_path / "ohio.json").write_text('[{"name": "a"},\n{"name": "b"}', encoding="utf-8")
    (tmp_path / "utah.json").write_text('[{"name": "a"}]', encoding="utf-8")
    (tmp_path / "texas.json").touch()

    issues = up.validate_inputs([str(tmp_path)])

    assert set(issues) == {"ohio", "texas"}
    assert "invalid JSON after 2 record(s): file ends before the closing ']'" in issues["ohio"][0]
    # An empty file is reported once, as empty, not also as invalid JSON.
    assert len(issues["texas"]) == 1
    assert "empty JSON" in issues["texas"][0]


def test_invalid_json_alerts_discord_and_prevents_upload(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").write_text('[{"name": "a"},', encoding="utf-8")
    discord_env = tmp_path / "discord.env"
    discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")
    alerts = []
    monkeypatch.setattr(up, "send_discord_alert", lambda url, failures: alerts.append(failures))
    monkeypatch.setattr(up, "HfApi", lambda **_kwargs: pytest.fail("Hugging Face must not be called"))

    with pytest.raises(RuntimeError, match="invalid JSON"):
        up.main(["--repo", "owner/data", "--token", "fake", "--discord-env-file", str(discord_env), str(tmp_path)])

    assert len(alerts) == 1
    assert "invalid JSON" in alerts[0]["ohio"][0]


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

    monkeypatch.setattr(discord_notify, "urlopen", fake_urlopen)
    up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})

    request, timeout = requests[0]
    assert timeout == 10
    assert request.full_url.endswith("?wait=true")
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("User-agent") == discord_notify.DISCORD_USER_AGENT
    assert b'"allowed_mentions": {"parse": []}' in request.data
    assert b"ohio: empty JSON" in request.data


def test_discord_success_message_uses_same_transport(monkeypatch):
    requests = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    def fake_urlopen(request, timeout):
        requests.append(request)
        return Response()

    monkeypatch.setattr(discord_notify, "urlopen", fake_urlopen)
    message = "Uploaded 1 file(s) to owner/data (https://huggingface.co/owner/data)"
    up.send_discord_messages("https://discord.com/api/webhooks/123/secret", [message])

    assert len(requests) == 1
    assert json.loads(requests[0].data) == {"content": message, "allowed_mentions": {"parse": []}}
    assert requests[0].get_header("User-agent") == discord_notify.DISCORD_USER_AGENT


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

    monkeypatch.setattr(discord_notify, "urlopen", fake_urlopen)
    monkeypatch.setattr(discord_notify.time, "sleep", waits.append)
    up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})
    assert len(calls) == 2
    assert waits == [0.1]


def test_discord_nonretryable_http_error_does_not_expose_webhook(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setattr(discord_notify, "urlopen", fake_urlopen)
    with pytest.raises(RuntimeError, match="Discord returned HTTP 404") as failure:
        up.send_discord_alert("https://discord.com/api/webhooks/123/secret", {"ohio": ["empty JSON"]})
    assert "secret" not in str(failure.value)


def test_discord_rate_limit_wait_is_bounded(monkeypatch):
    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 429, "rate limited", {"Retry-After": "65"}, None)

    monkeypatch.setattr(discord_notify, "urlopen", fake_urlopen)
    monkeypatch.setattr(discord_notify.time, "sleep", lambda *_args: pytest.fail("should not sleep for a minute"))
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


@pytest.mark.parametrize("config", ["absent", "blank"])
def test_missing_webhook_skips_alert_but_still_blocks_invalid_json(tmp_path, monkeypatch, caplog, config):
    (tmp_path / "ohio.json").touch()
    discord_env = tmp_path / "discord.env"
    if config == "blank":
        discord_env.write_text("webhook_url=\n", encoding="utf-8")
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    monkeypatch.setattr(up, "HfApi", lambda **_kwargs: pytest.fail("Hugging Face must not be called"))

    with (
        caplog.at_level("INFO", logger="upload_to_huggingface"),
        pytest.raises(RuntimeError, match="empty JSON") as failure,
    ):
        up.main(["--repo", "owner/data", "--token", "fake", "--discord-env-file", str(discord_env), str(tmp_path)])
    assert "Discord alert not sent" not in str(failure.value)
    assert "No Discord webhook URL configured" in caplog.text


def test_failing_discord_still_blocks_invalid_json(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").touch()

    discord_env = tmp_path / "discord.env"
    discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")
    monkeypatch.setattr(discord_notify, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("secret")))
    with pytest.raises(RuntimeError, match="Could not connect to Discord") as failure:
        up.main(["--dry-run", "--discord-env-file", str(discord_env), str(tmp_path)])
    assert "ohio" in str(failure.value)
    assert "secret" not in str(failure.value)


def test_clean_dry_run_does_not_contact_discord_or_hugging_face(tmp_path, monkeypatch):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    monkeypatch.setattr(up, "send_discord_messages", lambda *_args: pytest.fail("unexpected success notification"))
    monkeypatch.setattr(up, "HfApi", lambda **_kwargs: pytest.fail("unexpected upload"))

    assert up.main(["--dry-run", "--repo", "owner/dataset", str(tmp_path)]) == 0


def test_clean_upload_notifies_discord_after_commit(tmp_path, monkeypatch, caplog):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    discord_env = tmp_path / "discord.env"
    discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")
    calls = []

    class Api:
        def __init__(self, token):
            calls.append(("token", token))

        def create_commit(self, **kwargs):
            calls.append(("commit", kwargs))
            return type("Commit", (), {"commit_url": "https://huggingface.co/owner/data"})()

    monkeypatch.setattr(up, "HfApi", Api)
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    monkeypatch.setattr(up, "send_discord_messages", lambda url, messages: calls.append(("discord", url, messages)))
    args = ["--repo", "owner/data", "--token", "fake", "--no-readme", "--discord-env-file", str(discord_env)]
    with caplog.at_level("INFO", logger="upload_to_huggingface"):
        assert up.main([*args, str(tmp_path)]) == 0
    assert calls[0] == ("token", "fake")
    assert calls[1][1]["operations"][0].path_in_repo == "ohio.json"
    message = "Uploaded 1 file(s) to owner/data (https://huggingface.co/owner/data)"
    assert calls[2] == ("discord", "https://discord.com/api/webhooks/123/secret", [message])
    assert message in caplog.text


def test_absent_discord_url_allows_clean_upload_without_notifications(tmp_path, monkeypatch, caplog):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    missing_env = tmp_path / "missing-discord.env"
    commits = []

    class Api:
        def __init__(self, token):
            assert token == "fake"

        def create_commit(self, **kwargs):
            commits.append(kwargs)
            return type("Commit", (), {"commit_url": "https://huggingface.co/owner/data"})()

    monkeypatch.setattr(up, "HfApi", Api)
    monkeypatch.setattr(up, "send_discord_alert", lambda *_args: pytest.fail("unexpected alert"))
    monkeypatch.setattr(up, "send_discord_messages", lambda *_args: pytest.fail("unexpected success notification"))

    args = ["--repo", "owner/data", "--token", "fake", "--no-readme", "--discord-env-file", str(missing_env)]
    with caplog.at_level("INFO", logger="upload_to_huggingface"):
        assert up.main([*args, str(tmp_path)]) == 0

    assert len(commits) == 1
    assert "No Discord webhook URL configured" in caplog.text
    assert "Uploaded 1 file(s) to owner/data (https://huggingface.co/owner/data)" in caplog.text


@pytest.mark.parametrize("config", ["absent", "blank", "delivery failed"])
def test_success_without_webhook_or_with_delivery_failure(tmp_path, monkeypatch, caplog, config):
    (tmp_path / "ohio.json").write_text("[{}]", encoding="utf-8")
    discord_env = tmp_path / "discord.env"
    if config == "blank":
        discord_env.write_text("webhook_url=\n", encoding="utf-8")
    elif config == "delivery failed":
        discord_env.write_text("webhook_url=https://discord.com/api/webhooks/123/secret\n", encoding="utf-8")

    class Api:
        def __init__(self, token):
            pass

        def create_commit(self, **kwargs):
            return type("Commit", (), {"commit_url": "https://huggingface.co/owner/data"})()

    monkeypatch.setattr(up, "HfApi", Api)
    if config == "delivery failed":

        def fail_delivery(_url, _messages):
            raise RuntimeError("Discord returned HTTP 403")

        monkeypatch.setattr(up, "send_discord_messages", fail_delivery)
    else:
        monkeypatch.setattr(up, "send_discord_messages", lambda *_args: pytest.fail("unexpected notification"))
    args = ["--repo", "owner/data", "--token", "fake", "--no-readme", "--discord-env-file", str(discord_env)]
    with caplog.at_level("INFO", logger="upload_to_huggingface"):
        assert up.main([*args, str(tmp_path)]) == 0
    if config == "delivery failed":
        assert "Upload succeeded, but Discord success notification was not sent" in caplog.text
    else:
        assert "No Discord webhook URL configured" in caplog.text
        assert "success notification was not sent" not in caplog.text
    assert "secret" not in caplog.text
