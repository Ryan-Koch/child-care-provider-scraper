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

Usage:
    .venv/bin/python scripts/upload_to_huggingface.py state_output_normal_run/
    .venv/bin/python scripts/upload_to_huggingface.py --dry-run state_output/
    .venv/bin/python scripts/upload_to_huggingface.py -f csv --repo me/data out/

Invoked automatically at the end of a run by ``run_spiders.sh -u``.
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ENV_FILE = os.path.join(REPO_ROOT, "huggingface.env")
TOKEN_KEY = "hugging_face_token"
REPO_KEY = "hugging_face_repo"

logger = logging.getLogger("upload_to_huggingface")


def load_env_file(path):
    """Parse a ``key=value`` env file into a dict; return {} if it's absent.

    Blank lines and ``#`` comments are ignored; surrounding whitespace and a
    single layer of matching quotes are stripped from values.
    """
    values = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as handle:
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


def build_operations(files, path_in_repo):
    """Map local files to flat CommitOperationAdd entries under path_in_repo."""
    prefix = path_in_repo.strip("/")
    operations = []
    for path in files:
        name = os.path.basename(path)
        target = "%s/%s" % (prefix, name) if prefix else name
        operations.append(
            CommitOperationAdd(path_in_repo=target, path_or_fileobj=path))
    return operations


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("inputs", nargs="+",
                        help="directories and/or files to upload")
    parser.add_argument("-f", "--format", default="json",
                        choices=["json", "csv"],
                        help="data file extension to collect from directories "
                             "(default: %(default)s)")
    parser.add_argument("--repo",
                        help="dataset repo id (default: %s in the env file)"
                             % REPO_KEY)
    parser.add_argument("--token",
                        help="write token (default: %s in the env file)"
                             % TOKEN_KEY)
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE,
                        help="key=value file with token/repo "
                             "(default: %(default)s)")
    parser.add_argument("--path-in-repo", default="",
                        help="subdirectory in the repo to upload into "
                             "(default: repo root)")
    parser.add_argument("--commit-message",
                        help="commit message (default: a timestamped message)")
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would be uploaded; no network, no push")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="debug-level logging")
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    env = load_env_file(args.env_file)
    token = args.token or env.get(TOKEN_KEY)
    repo = args.repo or env.get(REPO_KEY)

    if not repo:
        logger.error("No dataset repo id: pass --repo or set %s in %s",
                     REPO_KEY, args.env_file)
        return 2
    if not token and not args.dry_run:
        logger.error("No token: pass --token or set %s in %s",
                     TOKEN_KEY, args.env_file)
        return 2

    files = collect_files(args.inputs, args.format)
    if not files:
        logger.error("No .%s files found in: %s",
                     args.format, ", ".join(args.inputs))
        return 1

    total_bytes = sum(os.path.getsize(f) for f in files)
    logger.info("Found %d .%s file(s) totaling %.1f MB to upload to %s",
                len(files), args.format, total_bytes / 1_048_576.0, repo)
    for path in files:
        logger.info("  %s (%.1f MB)", path, os.path.getsize(path) / 1_048_576.0)

    if args.dry_run:
        logger.info("Dry run: nothing uploaded.")
        return 0

    message = args.commit_message or (
        "Scheduled data upload %s"
        % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    operations = build_operations(files, args.path_in_repo)
    api = HfApi(token=token)
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
            "script does not create repos.", repo)
        return 1
    except HfHubHTTPError as error:
        logger.error("Upload to %s failed: %s", repo, error)
        return 1

    commit_url = getattr(commit, "commit_url", None) or repo
    logger.info("Uploaded %d file(s) to %s (%s)", len(files), repo, commit_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
