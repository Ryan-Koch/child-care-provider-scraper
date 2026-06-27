"""Drift-guard: the committed state_pipeline_details.md must match a fresh
generation. Fails if someone changes a mapping without regenerating the doc.

Fix on failure: ``.venv/bin/python scripts/generate_pipeline_details.py`` and
commit the updated ``state_pipeline_details.md``.
"""
import importlib.util
import os

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GENERATOR = os.path.join(REPO_ROOT, "scripts", "generate_pipeline_details.py")
COMMITTED_DOC = os.path.join(REPO_ROOT, "state_pipeline_details.md")


def _load_generator():
    spec = importlib.util.spec_from_file_location(
        "generate_pipeline_details", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_doc_matches_fresh_generation():
    generator = _load_generator()
    fresh = generator.build_markdown()
    with open(COMMITTED_DOC, "r", encoding="utf-8") as handle:
        committed = handle.read()
    assert fresh == committed, (
        "state_pipeline_details.md is out of date. Regenerate it with "
        "`.venv/bin/python scripts/generate_pipeline_details.py` and commit."
    )


def test_generation_is_deterministic():
    generator = _load_generator()
    assert generator.build_markdown() == generator.build_markdown()
