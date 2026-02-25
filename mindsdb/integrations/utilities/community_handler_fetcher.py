"""
On-demand fetcher for community handlers from GitHub.

Environment variables:
    COMMUNITY_HANDLERS_REPO    GitHub owner/repo (default: mindsdb/mindsdb-community-handlers)
    COMMUNITY_HANDLERS_BRANCH  Branch/ref to fetch from (default: main)
    COMMUNITY_HANDLERS_PATH    Path prefix inside repo (default: community_handlers)
    GITHUB_TOKEN               Optional PAT for higher rate limits (5000 req/hr vs 60)
"""

import json
import os
import shutil
import threading
from pathlib import Path
from typing import Optional

import requests

from mindsdb.utilities import log

logger = log.getLogger(__name__)

_GITHUB_API_BASE = "https://api.github.com"
_DEFAULT_REPO = "mindsdb/mindsdb-community-handlers"
_DEFAULT_BRANCH = "main"
_DEFAULT_PATH_PREFIX = "community_handlers"

# Per-handler locks prevent concurrent duplicate fetches of the same handler
_fetch_locks: dict = {}
_fetch_locks_lock = threading.Lock()


def _get_fetch_lock(handler_dir_name: str) -> threading.Lock:
    with _fetch_locks_lock:
        if handler_dir_name not in _fetch_locks:
            _fetch_locks[handler_dir_name] = threading.Lock()
        return _fetch_locks[handler_dir_name]


def _github_headers() -> dict:
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _get_repo_config() -> tuple:
    """Returns (repo, branch, path_prefix)."""
    repo = os.environ.get("COMMUNITY_HANDLERS_REPO", _DEFAULT_REPO)
    branch = os.environ.get("COMMUNITY_HANDLERS_BRANCH", _DEFAULT_BRANCH)
    path_prefix = os.environ.get("COMMUNITY_HANDLERS_PATH", _DEFAULT_PATH_PREFIX)
    return repo, branch, path_prefix


def fetch_handler(handler_dir_name: str, storage_dir: Path) -> Optional[Path]:
    """
    Fetch a single community handler directory from GitHub into storage_dir.

    Downloads only the files for the specific requested handler using the
    GitHub Contents API. Subsequent calls for the same handler are no-ops
    (the directory already exists on disk).

    Args:
        handler_dir_name: The directory name of the handler (e.g. "github_handler")
        storage_dir: Root directory where community handlers are stored

    Returns:
        Path to the fetched handler directory, or None if the handler does not
        exist in the remote repository.

    Raises:
        RuntimeError: On network errors or unexpected GitHub API responses.
    """
    lock = _get_fetch_lock(handler_dir_name)
    with lock:
        dest_dir = storage_dir / handler_dir_name

        # Idempotent: already fetched
        if dest_dir.is_dir() and (dest_dir / "__init__.py").exists():
            logger.debug(
                "Community handler '%s' already on disk at %s",
                handler_dir_name, dest_dir
            )
            return dest_dir

        repo, branch, path_prefix = _get_repo_config()
        api_url = (
            f"{_GITHUB_API_BASE}/repos/{repo}/contents"
            f"/{path_prefix}/{handler_dir_name}"
        )
        params = {"ref": branch}
        headers = _github_headers()

        logger.info(
            "Fetching community handler '%s' from %s@%s",
            handler_dir_name, repo, branch
        )

        try:
            resp = requests.get(api_url, params=params, headers=headers, timeout=30)
        except requests.RequestException as e:
            raise RuntimeError(
                f"Network error fetching handler '{handler_dir_name}': {e}"
            ) from e

        if resp.status_code == 404:
            logger.debug(
                "Community handler '%s' not found in repo '%s'",
                handler_dir_name, repo
            )
            return None

        if resp.status_code != 200:
            raise RuntimeError(
                f"GitHub API error for '{handler_dir_name}': "
                f"HTTP {resp.status_code} — {resp.text[:300]}"
            )

        try:
            file_entries = resp.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON from GitHub API for '{handler_dir_name}': {e}"
            ) from e

        if not isinstance(file_entries, list):
            logger.debug(
                "Expected a directory listing for '%s', got non-list response",
                handler_dir_name
            )
            return None

        # Download into a temp directory first, then atomically rename to dest_dir
        tmp_dir = storage_dir / f".tmp_{handler_dir_name}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            for entry in file_entries:
                if entry.get("type") != "file":
                    # Skip subdirectories (uncommon in handlers but possible)
                    continue
                file_name = entry["name"]
                download_url = entry.get("download_url")
                if not download_url:
                    continue

                try:
                    file_resp = requests.get(
                        download_url, headers=headers, timeout=30
                    )
                    file_resp.raise_for_status()
                except requests.RequestException as e:
                    raise RuntimeError(
                        f"Failed to download '{file_name}' for handler "
                        f"'{handler_dir_name}': {e}"
                    ) from e

                (tmp_dir / file_name).write_bytes(file_resp.content)

            # Atomic rename
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            tmp_dir.rename(dest_dir)

        except Exception:
            # Clean up temp dir on any failure to avoid leaving partial state
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            raise

        logger.info(
            "Community handler '%s' fetched successfully to %s",
            handler_dir_name, dest_dir
        )
        return dest_dir


def get_community_handlers_storage_dir(storage_root: Path) -> Path:
    """Returns (and creates if needed) the community handlers storage directory."""
    community_dir = storage_root / "community_handlers"
    community_dir.mkdir(parents=True, exist_ok=True)
    return community_dir
