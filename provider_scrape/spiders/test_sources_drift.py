"""Drift-guard: the committed SOURCES.md must match a fresh generation, and
every spider must resolve to at least one source host.

Fix on failure: ``.venv/bin/python scripts/generate_sources.py`` and commit the
updated ``SOURCES.md``. If a newly added spider has no source, give it
``allowed_domains``/``start_urls`` or add it to ``SOURCE_OVERRIDES`` in
``scripts/generate_sources.py``.
"""
import importlib.util
import os

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GENERATOR = os.path.join(REPO_ROOT, "scripts", "generate_sources.py")
COMMITTED_DOC = os.path.join(REPO_ROOT, "SOURCES.md")


def _load_generator():
    spec = importlib.util.spec_from_file_location("generate_sources", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_doc_matches_fresh_generation():
    generator = _load_generator()
    fresh = generator.build_markdown()
    with open(COMMITTED_DOC, "r", encoding="utf-8") as handle:
        committed = handle.read()
    assert fresh == committed, (
        "SOURCES.md is out of date. Regenerate it with "
        "`.venv/bin/python scripts/generate_sources.py` and commit."
    )


def test_generation_is_deterministic():
    generator = _load_generator()
    assert generator.build_markdown() == generator.build_markdown()


def test_every_spider_has_a_source():
    generator = _load_generator()
    missing = [label for label, hosts in generator.collect_rows() if not hosts]
    assert not missing, (
        "These spiders resolve to no source host: %s. Add allowed_domains/"
        "start_urls or a SOURCE_OVERRIDES entry in generate_sources.py."
        % ", ".join(missing)
    )
