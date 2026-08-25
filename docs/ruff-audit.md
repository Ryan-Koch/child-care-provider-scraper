# Ruff Audit Report

**Date:** 2026-08-25
**Ruff version:** 0.16.4
**Config:** `pyproject.toml` — rules: E, W, F, I, UP, B, SIM, RUF

**Totals:** 472 lint findings across 110 project Python files, plus 103 files needing formatting.

---

## Summary by Rule

| Rule | Count | Auto-fixable | Description |
|------|------:|:---:|-------------|
| E501 | 120 | no | Line too long (>120 chars) |
| RUF012 | 84 | no | Mutable class default |
| W293 | 70 | safe | Blank line with whitespace |
| UP031 | 58 | unsafe | `%`-format string → f-string |
| I001 | 42 | safe | Unsorted imports |
| F401 | 22 | safe | Unused import |
| UP015 | 7 | safe | Redundant `open()` mode (`"r"`) |
| W292 | 7 | safe | Missing newline at end of file |
| F841 | 6 | unsafe | Unused variable |
| B905 | 5 | no | `zip()` without `strict=` |
| E722 | 5 | no | Bare `except` |
| RUF001 | 4 | no | Ambiguous unicode in string |
| RUF003 | 4 | no | Ambiguous unicode in comment |
| SIM105 | 4 | no | Use `contextlib.suppress` |
| SIM108 | 4 | no | Use ternary instead of if/else |
| RUF059 | 3 | no | Unused unpacked variable |
| SIM115 | 3 | no | Use context manager for `open()` |
| UP017 | 3 | safe | `datetime.timezone.utc` → `datetime.UTC` |
| B023 | 2 | no | Function uses loop variable |
| E701 | 2 | no | Multiple statements on one line |
| RUF002 | 2 | no | Ambiguous unicode in docstring |
| RUF100 | 2 | safe | Unused `noqa` |
| UP028 | 2 | no | `yield` in `for` → `yield from` |
| B007 | 1 | no | Unused loop control variable |
| B013 | 1 | safe | Redundant tuple in exception handler |
| B904 | 1 | no | `raise` without `from` inside `except` |
| E741 | 1 | no | Ambiguous variable name |
| F541 | 1 | safe | f-string missing placeholders |
| F811 | 1 | safe | Redefined while unused |
| RUF005 | 1 | no | List concatenation → spread |
| RUF046 | 1 | no | Unnecessary `int()` cast |
| SIM300 | 1 | safe | Yoda condition |
| UP008 | 1 | safe | `super(Cls, self)` → `super()` |
| W291 | 1 | safe | Trailing whitespace |

Plus **formatting**: `ruff format` would reformat 103 of 115 checked files (whitespace, string quoting, trailing commas, implicit string concatenation wrapping).

---

## Proposed Fix Strategy

### Phase 1 — Automated safe fixes (no behavior change)

These can all be applied with `ruff check --fix` and `ruff format`:

1. **`ruff format .`** — Reformat all 103 files. Handles whitespace (W291, W292, W293), string quoting, trailing commas, line wrapping.

2. **`ruff check --fix .`** — Auto-fix safe-fixable issues:
   - **I001** (42): Sort imports.
   - **F401** (22): Remove unused imports.
   - **UP015** (7): Remove redundant `open("file", "r")` → `open("file")`.
   - **UP017** (3): `datetime.timezone.utc` → `datetime.UTC`.
   - **RUF100** (2): Remove unused `# noqa` comments.
   - **UP028** (2): `for x in y: yield x` → `yield from y`.
   - **UP008** (1): `super(Cls, self)` → `super()`.
   - **B013** (1): Redundant tuple in exception handler.
   - **F541** (1): f-string with no placeholders → plain string.
   - **F811** (1): Redefined-while-unused import.
   - **SIM300** (1): Yoda condition `"literal" == var` → `var == "literal"`.

### Phase 2 — Manual fixes (mechanical but need human review)

#### RUF012: Mutable class default (84 findings) — NEEDS DECISION

Every Scrapy spider defines `allowed_domains = [...]` and `start_urls = [...]` as bare list literals on the class. Ruff flags these because a mutable class-level default is shared across instances. The idiomatic fix is to annotate them with `ClassVar`:

```python
from typing import ClassVar


class MySpider(scrapy.Spider):
    allowed_domains: ClassVar[list[str]] = ["example.com"]
    start_urls: ClassVar[list[str]] = ["https://example.com"]
```

This is correct — Scrapy never mutates these after class creation. Applies to ~40 spider files. A handful of non-Scrapy class attributes (e.g. `HEADERS` dicts in alaska.py, maryland.py) need the same treatment.

**Open question:** Do we want to annotate every spider with `ClassVar`, or suppress RUF012 globally for Scrapy spider files? Annotating is more correct; suppressing is less noisy. Recommendation: annotate, since it's a one-time mechanical change and keeps the rule active for non-Scrapy code.

#### UP031: `%`-format → f-string (58 findings) — NEEDS DECISION

Two distinct cases:

1. **Logger calls** (2 in middlewares.py): `logger.info("Spider opened: %s" % spider.name)`. These use eager `%` formatting. The best fix is lazy formatting: `logger.info("Spider opened: %s", spider.name)` (deferred evaluation), not an f-string. Only 2 cases.

2. **Regular string formatting** (56 across scripts/ and spiders/): `"pattern %s" % value`. These should become f-strings. Concentrated in `scripts/generate_pipeline_details.py` (19), `scripts/geocode_enrich.py` (15), and `scripts/upload_to_huggingface.py` (7). Can be auto-fixed with `ruff check --fix --unsafe-fixes --select UP031`, but needs review since it's marked unsafe — a few edge cases around `%` inside regex patterns (e.g., hawaii.py:134) could change behavior if the format string contains literal `%`.

**Open question:** Apply `--unsafe-fixes` for UP031 and review the diff, or convert manually? Recommendation: unsafe-fix + careful diff review. The hawaii.py regex pattern case will need manual attention.

#### E501: Line too long (120 findings)

Lines exceeding the 120-char limit. Most are in test files (long fixture strings, URLs, assertion messages) and a few spiders (long XPath expressions, URLs). `ruff format` will fix many of these by wrapping, but some are irreducible (single long strings/URLs).

**Fix:** Run `ruff format` first (Phase 1), then re-check. Remaining E501s will be:
- Long URLs/XPaths: add `# noqa: E501` or break across lines.
- Long test fixture strings: leave as-is with `# noqa: E501` (readability).

**Open question:** What's our tolerance? `# noqa: E501` on irreducible long strings, or raise the limit to 150? Recommendation: keep 120 limit, use targeted `# noqa: E501` for URLs/XPaths that can't reasonably wrap.

#### E722: Bare `except` (5 findings)

All in `arkansas.py` (4) and `virginia.py` (1). These catch Playwright timeouts/failures. Fix: `except Exception:` — narrow enough to still catch all runtime errors but excludes `KeyboardInterrupt`/`SystemExit`.

**Fix:** Replace `except:` with `except Exception:` in all 5 locations.

#### B023: Function uses loop variable (2 findings)

Both in `washington.py` — `cell_text()` defined inside a loop captures `tds` from the enclosing scope. Since `tds` is reassigned each iteration and `cell_text` is called immediately (not deferred), this is safe in practice, but Ruff can't prove that.

**Fix:** Add `tds` as a default argument: `def cell_text(idx, tds=tds):`.

#### F841: Unused variable (6 findings)

| File | Variable | Fix |
|------|----------|-----|
| new_mexico.py:93 | `last_count` | Remove assignment |
| ohio.py:92 | `data` | Remove assignment |
| test_ohio.py:139,175 | `provider_item_request` | Prefix with `_` |
| texas.py:42 | `wait_for_api` | Remove (dead code) |
| texas.py:123 | `providers` | Remove assignment |

#### B905: `zip()` without `strict=` (5 findings)

In iowa.py, test_minnesota.py, test_nevada.py. Adding `strict=True` ensures the iterables are the same length, which is a good safety net.

**Fix:** Add `strict=True` where lengths are expected to match (test files), `strict=False` where one side may be shorter (iowa.py `DAY_LABELS`/`DAY_KEYS` — verify lengths first).

#### SIM105: Use `contextlib.suppress` (4 findings)

The connecticut.py one is a deliberate try/except/pass for defensive logging. The others are similar patterns. `contextlib.suppress(Exception)` is equivalent but reads slightly different.

**Fix:** Convert to `contextlib.suppress` where the pattern is straightforward. Leave connecticut.py's as-is if the explicit try/except better conveys intent (the comment explains why) — add `# noqa: SIM105` with the existing comment.

#### SIM108: Use ternary (4 findings)

These suggest converting if/else blocks to ternary expressions. Some are readable as-is (alaska.py address assembly). Convert where the ternary is clearly simpler; leave complex ones with `# noqa: SIM108`.

#### Remaining minor fixes

| Rule | Count | Fix |
|------|------:|-----|
| RUF001/002/003 | 10 | **Intentional** unicode chars (en-dashes matching source data). Suppress with `# noqa` or add to `allowed-confusables` in config. |
| RUF059 | 3 | Prefix unused unpacked vars with `_` |
| SIM115 | 3 | Wrap `open()` in `with` statement (test_south_dakota.py) |
| E701 | 2 | Split `if val: return val` onto two lines (arkansas.py) |
| RUF005 | 1 | `[x] + list[1:]` → `[x, *list[1:]]` (geocode_enrich.py) |
| RUF046 | 1 | Remove unnecessary `int(round(...))` → `round(...)` (alaska.py) |
| B904 | 1 | Add `from exc` to re-raise (upload_to_huggingface.py) |
| E741 | 1 | Rename `l` to `lang` (colorado.py) |
| B007 | 1 | Prefix unused loop var with `_` |

---

## Open Questions Before Implementation

1. **RUF012 — ClassVar vs suppress?** Annotate all ~40 spider files with `ClassVar[list[str]]` for `allowed_domains`/`start_urls`, or add a per-file or project-wide suppression? (Recommendation: annotate.)

2. **UP031 — Unsafe auto-fix or manual?** Use `--unsafe-fixes` for the 58 `%`-format → f-string conversions and review the diff, or convert by hand? (Recommendation: auto-fix + review.)

3. **E501 — Handling irreducible long lines?** Keep 120-char limit with targeted `# noqa: E501`, or raise the limit? (Recommendation: keep 120, `noqa` the stragglers.)

4. **RUF001/002/003 — Intentional unicode?** The en-dashes in normalization.py match source data and the comments document this. Add them to `allowed-confusables` in pyproject.toml, or suppress inline? (Recommendation: `allowed-confusables` in config.)

5. **Formatting scope?** `ruff format` will touch 103 files. This is a large diff. Do we want to do this in a single formatting commit (easier to review as "whitespace only"), or interleave with lint fixes? (Recommendation: single formatting commit first, then lint fixes.)
