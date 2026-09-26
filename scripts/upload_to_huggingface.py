"""Upload scraped state output to a Hugging Face dataset repository.

Gathers the data files (``.json`` by default, ``.csv`` with ``--format csv``)
from one or more directories/files and pushes them to a Hugging Face *dataset*
repo in a single commit. Log files are never uploaded.

Auth and target repo are read from ``huggingface.env`` at the repo root (a
git-ignored ``key=value`` file):

    hugging_face_token=hf_xxxxxxxx
    hugging_face_repo=owner/dataset-name

Both can be overridden on the command line (``--repo``, ``--token``, or a
different ``--env-file``). The token is passed directly to the Hugging Face API
rather than through a global ``login()`` so nothing is written to disk and
scheduled runs stay self-contained.

The target repo must already exist; the script does not create it. If it is
missing (or the token can't see it) the run fails with a clear message.

Because each state file has its own set of columns, a single-table load fails
Hugging Face's "all files must have the same columns" check. So a JSON upload
also writes a ``README.md`` whose YAML frontmatter declares one dataset
*configuration* per state file (``config_name`` = the file's stem). Each state
is then parsed independently, which sidesteps the column/type/nested-struct
mismatches entirely. Any existing hand-written card body and other frontmatter
keys are preserved; only the ``configs`` key is regenerated. Disable with
``--no-readme`` (the default is on for JSON, off for CSV).

Extra non-data files (e.g. a generated ``SOURCES.md`` provenance table) can ride
along in the same commit via one or more ``--extra-file`` flags.

Usage:
    .venv/bin/python scripts/upload_to_huggingface.py state_output_normal_run/
    .venv/bin/python scripts/upload_to_huggingface.py --dry-run state_output/
    .venv/bin/python scripts/upload_to_huggingface.py -f csv --repo me/data out/
    .venv/bin/python scripts/upload_to_huggingface.py --extra-file SOURCES.md out/

Invoked automatically at the end of a run by ``run_spiders.sh -u``.
Before uploading, validates the input directory's JSON files.
If any fail, no data is uploaded and a report is sent to the incoming webhook
configured as ``webhook_url`` in the repo-root ``discord.env``.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import yaml
from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import (
    EntryNotFoundError,
    HfHubHTTPError,
    RepositoryNotFoundError,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ENV_FILE = os.path.join(REPO_ROOT, "huggingface.env")
DEFAULT_DISCORD_ENV_FILE = os.path.join(REPO_ROOT, "discord.env")
TOKEN_KEY = "hugging_face_token"
REPO_KEY = "hugging_face_repo"
WEBHOOK_KEY = "webhook_url"
MAX_AGE = timedelta(days=5)
DISCORD_CONTENT_LIMIT = 2000
DISCORD_USER_AGENT = "Mozilla/5.0 (compatible; ChildCareProviderScraper/1.0)"

# The dataset card lives at the repo root (HF only reads config metadata there).
README_FILENAME = "README.md"
# Body used only when the repo has no README yet; an existing card's body is
# preserved untouched.
DEFAULT_README_BODY = """# US Child Care Providers

Per-state child care provider licensing data. Each U.S. state is exposed as a
separate dataset configuration: pick a state in the dataset viewer, or pass its
name as the config argument, e.g.
``load_dataset("<repo>", "alabama")``.

The `configs` metadata above is generated automatically by the upload script,
one entry per state data file."""

logger = logging.getLogger("upload_to_huggingface")


def load_env_file(path):
    """Parse a ``key=value`` env file into a dict; return {} if it's absent.

    Blank lines and ``#`` comments are ignored; surrounding whitespace and a
    single layer of matching quotes are stripped from values.
    """
    values = {}
    if not os.path.exists(path):
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


def collect_files(inputs, fmt):
    """Return the sorted data files to upload from the given files/dirs.

    A directory contributes its top-level ``*.<fmt>`` files (non-recursive, so
    logs and any nested scratch never sneak in); a file is taken as-is.
    """
    suffix = "." + fmt
    files = []
    for item in inputs:
        if os.path.isdir(item):
            for name in sorted(os.listdir(item)):
                full = os.path.join(item, name)
                if os.path.isfile(full) and name.endswith(suffix):
                    files.append(full)
        elif os.path.isfile(item):
            files.append(item)
        else:
            logger.warning("Skipping %s: not a file or directory", item)
    # De-dupe while preserving order (a file may also match via its directory).
    seen = set()
    unique = []
    for path in files:
        real = os.path.realpath(path)
        if real not in seen:
            seen.add(real)
            unique.append(path)
    return unique


def validation_paths(inputs):
    """Collect top-level JSON files from directories and explicitly selected files."""
    json_files = set()
    for item in inputs:
        if os.path.isdir(item):
            for name in os.listdir(item):
                path = os.path.join(item, name)
                if os.path.isfile(path) and name.endswith(".json"):
                    json_files.add(os.path.realpath(path))
        elif os.path.isfile(item) and item.endswith(".json"):
            json_files.add(os.path.realpath(item))
    return sorted(json_files)


def is_empty_json(path):
    """Only 0-byte files and whitespace-wrapped [] count as empty."""
    if os.path.getsize(path) == 0:
        return True
    with open(path, "rb") as handle:
        # Avoid loading potentially huge scrape output into memory.
        content = b""
        for chunk in iter(lambda: handle.read(4096), b""):
            content += b"".join(chunk.split())
            if len(content) > 2:
                return False
        return content == b"[]"


def validate_inputs(inputs, now=None):
    """Return {state: [issues]} for all failures in the selected directories."""
    now = now or datetime.now(UTC)
    failures = {}
    for path in validation_paths(inputs):
        state = os.path.splitext(os.path.basename(path))[0]
        issues = []
        if is_empty_json(path):
            issues.append(f"{path}: empty JSON (0 bytes or [])")
        modified = datetime.fromtimestamp(os.path.getmtime(path), UTC)
        if now - modified > MAX_AGE:
            issues.append(f"{path}: JSON is older than 5 days (modified {modified:%Y-%m-%d %H:%M UTC})")
        if issues:
            failures.setdefault(state, []).extend(issues)
    return failures


def format_failures(failures):
    """Human-readable, state-grouped report for stderr and Discord."""
    lines = ["Hugging Face upload blocked by state output validation:"]
    for state, issues in sorted(failures.items()):
        lines.append(f"{state}:")
        lines.extend(f"  - {issue}" for issue in issues)
    return "\n".join(lines)


def discord_messages(failures):
    """Split the full report into messages under Discord's content limit."""
    heading = "Hugging Face upload blocked by state output validation:"
    messages = []
    current = heading
    for state, issues in sorted(failures.items()):
        for issue in issues:
            # Each issue gets its state label, even across message boundaries.
            line = f"{state}: {issue}"
            if len(line) > DISCORD_CONTENT_LIMIT - len(heading) - 2:
                line = line[: DISCORD_CONTENT_LIMIT - len(heading) - 5] + "..."
            if len(current) + len(line) + 1 > DISCORD_CONTENT_LIMIT:
                messages.append(current)
                current = heading
            current += "\n" + line
    messages.append(current)
    return messages


def send_discord_alert(webhook_url, failures):
    """Send confirmed Discord messages, never including the secret in errors."""
    try:
        parts = urlsplit(webhook_url)
        valid_host = parts.hostname in {"discord.com", "discordapp.com"} and parts.port is None
    except ValueError:
        raise ValueError("discord.env has no valid Discord incoming webhook URL") from None
    if (
        parts.scheme != "https"
        or not valid_host
        or parts.username is not None
        or not re.fullmatch(r"/api(?:/v\d+)?/webhooks/\d+/[^/]+", parts.path)
    ):
        raise ValueError("discord.env has no valid Discord incoming webhook URL")
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query) if key != "wait"] + [("wait", "true")])
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))
    for content in discord_messages(failures):
        payload = json.dumps({"content": content, "allowed_mentions": {"parse": []}}).encode("utf-8")
        # Discord's Cloudflare layer rejects urllib's default Python-urllib UA (403 / 1010).
        request = Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": DISCORD_USER_AGENT},
            method="POST",
        )
        for attempt in range(3):
            try:
                with urlopen(request, timeout=10) as response:
                    if response.status != 200:
                        raise RuntimeError(f"Discord returned HTTP {response.status}")
                break
            except HTTPError as error:
                if error.code == 429 and attempt < 2:
                    try:
                        delay = float((error.headers or {}).get("Retry-After", ""))
                    except (TypeError, ValueError):
                        delay = 0
                    if 0 < delay <= 5:
                        time.sleep(delay)
                        continue
                raise RuntimeError(f"Discord returned HTTP {error.code}") from None
            except (URLError, OSError):
                raise RuntimeError("Could not connect to Discord") from None


def build_operations(files, path_in_repo):
    """Map local files to flat CommitOperationAdd entries under path_in_repo."""
    prefix = path_in_repo.strip("/")
    operations = []
    for path in files:
        name = os.path.basename(path)
        target = f"{prefix}/{name}" if prefix else name
        operations.append(CommitOperationAdd(path_in_repo=target, path_or_fileobj=path))
    return operations


def build_extra_operations(extra_files, path_in_repo):
    """CommitOperationAdd entries for explicit extra files (e.g. SOURCES.md).

    Each file is uploaded under path_in_repo using its basename, alongside the
    data files. A missing file is skipped with a warning so a best-effort upload
    still proceeds.
    """
    prefix = path_in_repo.strip("/")
    operations = []
    for path in extra_files:
        if not os.path.isfile(path):
            logger.warning("Skipping extra file %s: not found", path)
            continue
        name = os.path.basename(path)
        target = f"{prefix}/{name}" if prefix else name
        operations.append(CommitOperationAdd(path_in_repo=target, path_or_fileobj=path))
    return operations


def config_name_for(path):
    """Derive a Hugging Face config name from a data file (its basename stem)."""
    return os.path.splitext(os.path.basename(path))[0]


def build_configs(files, path_in_repo):
    """Build the ``configs`` list for the dataset card: one entry per file.

    ``data_files`` is the file's path relative to the repo root (so it includes
    any ``--path-in-repo`` prefix, matching what ``build_operations`` uploads).
    Duplicate config names (same basename stem from two inputs) are dropped with
    a warning, since Hugging Face rejects repeated config names.
    """
    prefix = path_in_repo.strip("/")
    configs = []
    seen = set()
    for path in files:
        name = os.path.basename(path)
        cfg = config_name_for(path)
        if cfg in seen:
            logger.warning("Duplicate config name %r (%s); keeping the first.", cfg, path)
            continue
        seen.add(cfg)
        data_file = f"{prefix}/{name}" if prefix else name
        configs.append({"config_name": cfg, "data_files": data_file})
    return configs


def split_frontmatter(text):
    """Split a Markdown doc into ``(frontmatter_dict, body_str)``.

    Handles the ``---`` YAML frontmatter block Hugging Face dataset cards use.
    Returns an empty dict and the whole text as the body when there's no
    frontmatter. Raises ``ValueError`` when a frontmatter block is present but
    isn't valid YAML (or isn't a mapping), so callers can decline to overwrite a
    card they can't safely edit.
    """
    if not text:
        return {}, ""
    stripped = text.lstrip("\ufeff")  # strip a leading BOM if present
    lines = stripped.split("\n")
    if lines[0].strip() != "---":
        return {}, text
    end = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end = i
            break
    if end is None:
        # Opening fence with no close: not real frontmatter, treat as body.
        return {}, text
    fm_text = "\n".join(lines[1:end])
    body = "\n".join(lines[end + 1 :])
    try:
        data = yaml.safe_load(fm_text)
    except yaml.YAMLError as exc:
        raise ValueError(str(exc)) from exc
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError("frontmatter is not a mapping")
    return data, body


def render_readme(existing_text, configs):
    """Return README text with ``configs`` set, preserving everything else.

    The existing card's body and any other frontmatter keys are kept as-is; only
    the ``configs`` key is replaced. A fresh card gets a default body.
    """
    data, body = split_frontmatter(existing_text)
    data = dict(data)
    data["configs"] = configs
    frontmatter = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False).rstrip("\n")
    body = body.strip("\n")
    if not body:
        body = DEFAULT_README_BODY.strip("\n")
    return f"---\n{frontmatter}\n---\n\n{body}\n"


def fetch_existing_readme(api, repo):
    """Return the repo's current README text, or None if it has none."""
    try:
        local = api.hf_hub_download(repo_id=repo, repo_type="dataset", filename=README_FILENAME)
    except EntryNotFoundError:
        return None
    except (RepositoryNotFoundError, HfHubHTTPError) as exc:
        logger.warning("Couldn't fetch existing %s (%s); generating a fresh one.", README_FILENAME, exc)
        return None
    with open(local, encoding="utf-8") as handle:
        return handle.read()


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("inputs", nargs="+", help="directories and/or files to upload")
    parser.add_argument(
        "-f",
        "--format",
        default="json",
        choices=["json", "csv"],
        help="data file extension to collect from directories (default: %(default)s)",
    )
    parser.add_argument("--repo", help=f"dataset repo id (default: {REPO_KEY} in the env file)")
    parser.add_argument("--token", help=f"write token (default: {TOKEN_KEY} in the env file)")
    parser.add_argument(
        "--env-file", default=DEFAULT_ENV_FILE, help="key=value file with token/repo (default: %(default)s)"
    )
    parser.add_argument(
        "--discord-env-file",
        default=DEFAULT_DISCORD_ENV_FILE,
        help="key=value file with webhook_url (default: %(default)s)",
    )
    parser.add_argument(
        "--path-in-repo", default="", help="subdirectory in the repo to upload into (default: repo root)"
    )
    parser.add_argument(
        "--extra-file",
        dest="extra_files",
        action="append",
        default=[],
        metavar="PATH",
        help="additional file to include in the commit, as-is, "
        "under --path-in-repo (repeatable); e.g. a "
        "generated SOURCES.md. Missing files are skipped.",
    )
    parser.add_argument("--commit-message", help="commit message (default: a timestamped message)")
    parser.add_argument(
        "--readme",
        dest="readme",
        action="store_true",
        default=None,
        help="also write a README.md declaring one dataset config per state file (default: on for json)",
    )
    parser.add_argument("--no-readme", dest="readme", action="store_false", help="don't touch the dataset README.md")
    parser.add_argument(
        "--dry-run", action="store_true", help="list what would be uploaded; no Hugging Face push (alerts still send)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    failures = validate_inputs(args.inputs)
    if failures:
        report = format_failures(failures)
        webhook_url = load_env_file(args.discord_env_file).get(WEBHOOK_KEY)
        if not webhook_url:
            raise RuntimeError(f"{report}\nDiscord alert not sent: missing {WEBHOOK_KEY} in {args.discord_env_file}")
        try:
            send_discord_alert(webhook_url, failures)
        except (ValueError, RuntimeError) as error:
            raise RuntimeError(f"{report}\nDiscord alert not sent: {error}") from None
        raise RuntimeError(report)

    env = load_env_file(args.env_file)
    token = args.token or env.get(TOKEN_KEY)
    repo = args.repo or env.get(REPO_KEY)

    if not repo:
        logger.error("No dataset repo id: pass --repo or set %s in %s", REPO_KEY, args.env_file)
        return 2
    if not token and not args.dry_run:
        logger.error("No token: pass --token or set %s in %s", TOKEN_KEY, args.env_file)
        return 2

    files = collect_files(args.inputs, args.format)
    if not files:
        logger.error("No .%s files found in: %s", args.format, ", ".join(args.inputs))
        return 1

    total_bytes = sum(os.path.getsize(f) for f in files)
    logger.info(
        "Found %d .%s file(s) totaling %.1f MB to upload to %s",
        len(files),
        args.format,
        total_bytes / 1_048_576.0,
        repo,
    )
    for path in files:
        logger.info("  %s (%.1f MB)", path, os.path.getsize(path) / 1_048_576.0)

    # Default: manage the README for JSON (the format HF parses into the
    # per-state dataset), leave it alone for CSV. --readme/--no-readme override.
    generate_readme = args.readme if args.readme is not None else (args.format == "json")

    if args.dry_run:
        if generate_readme:
            configs = build_configs(files, args.path_in_repo)
            logger.info(
                "Would write %s with %d per-state config(s): %s",
                README_FILENAME,
                len(configs),
                ", ".join(c["config_name"] for c in configs),
            )
        for path in args.extra_files:
            if os.path.isfile(path):
                logger.info("Would include extra file %s", path)
            else:
                logger.warning("Extra file %s not found; would be skipped", path)
        logger.info("Dry run: nothing uploaded.")
        return 0

    message = args.commit_message or (f"Scheduled data upload {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    api = HfApi(token=token)
    operations = build_operations(files, args.path_in_repo)
    operations.extend(build_extra_operations(args.extra_files, args.path_in_repo))

    if generate_readme:
        configs = build_configs(files, args.path_in_repo)
        try:
            readme_text = render_readme(fetch_existing_readme(api, repo), configs)
        except ValueError as exc:
            logger.warning(
                "Existing %s frontmatter couldn't be parsed (%s); skipping the "
                "README config update. Fix or delete it to let this script "
                "regenerate it.",
                README_FILENAME,
                exc,
            )
        else:
            operations.insert(
                0, CommitOperationAdd(path_in_repo=README_FILENAME, path_or_fileobj=readme_text.encode("utf-8"))
            )
            logger.info("Including auto-generated %s with %d per-state config(s).", README_FILENAME, len(configs))
    try:
        commit = api.create_commit(
            repo_id=repo,
            repo_type="dataset",
            operations=operations,
            commit_message=message,
        )
    except RepositoryNotFoundError:
        logger.error(
            "Dataset repo %s not found (or the token lacks access). Create it "
            "on the Hugging Face website and give the token write scope; this "
            "script does not create repos.",
            repo,
        )
        return 1
    except HfHubHTTPError as error:
        logger.error("Upload to %s failed: %s", repo, error)
        return 1

    commit_url = getattr(commit, "commit_url", None) or repo
    logger.info("Uploaded %d file(s) to %s (%s)", len(operations), repo, commit_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
