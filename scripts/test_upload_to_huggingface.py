"""Tests for scripts/upload_to_huggingface.py (no network).

Covers the README/config generation added so a JSON upload declares one Hugging
Face dataset configuration per state file (sidestepping the "all files must have
the same columns" load error), plus the frontmatter merge that preserves an
existing dataset card.

Run with the project virtualenv:
``.venv/bin/pytest scripts/test_upload_to_huggingface.py``.
"""
import yaml

import upload_to_huggingface as up


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
    existing = (
        "---\n"
        "license: mit\n"
        "pretty_name: US Child Care Providers\n"
        "---\n\n"
        "# My hand-written card\n\nKeep me.\n"
    )
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
    existing = (
        "---\n"
        "configs:\n"
        "- config_name: stale\n"
        "  data_files: stale.json\n"
        "---\n\nbody\n"
    )
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
    ops = up.build_extra_operations(
        [str(present), str(tmp_path / "nope.md")], "")
    assert [o.path_in_repo for o in ops] == ["SOURCES.md"]
