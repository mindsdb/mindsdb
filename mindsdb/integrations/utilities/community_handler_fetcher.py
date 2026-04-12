import base64
import json
import os
import shutil
import threading
from pathlib import Path
from typing import Optional

import requests

from mindsdb.utilities import log

logger = log.getLogger(__name__)

# GitHub API configuration
# It can be replaced later with making the repo public.
GITHUB_API_BASE = "https://api.github.com"
DEFAULT_REPO = "mindsdb/mindsdb-community-handlers"
DEFAULT_BRANCH = "main"
DEFAULT_PATH_PREFIX = "community_handlers"
_fetch_locks: dict = {}
_fetch_locks_lock = threading.Lock()


def _get_fetch_lock(handler_dir_name: str) -> threading.Lock:
    """
    Get and create if needed a threading.
    Lock for the given handler directory.
    This ensures that concurrent fetches for the same handler_dir_name are
    serializedlly, preventing race conditions on disk.
    """
    with _fetch_locks_lock:
        if handler_dir_name not in _fetch_locks:
            _fetch_locks[handler_dir_name] = threading.Lock()
        return _fetch_locks[handler_dir_name]


def _github_headers() -> dict:
    """
    Return headers for GitHub API requests, including optional auth if GITHUB_TOKEN is set in the environment.
    TODO: Remove this after repository is set to public.
    """
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


# It can be removed later with making the repo public. TBD
def _get_repo_config() -> tuple:
    """Returns (repo, branch, path_prefix)."""
    repo = os.environ.get("COMMUNITY_HANDLERS_REPO", DEFAULT_REPO)
    branch = os.environ.get("COMMUNITY_HANDLERS_BRANCH", DEFAULT_BRANCH)
    path_prefix = os.environ.get("COMMUNITY_HANDLERS_PATH", DEFAULT_PATH_PREFIX)
    return repo, branch, path_prefix


def _resolve_tree_sha(repo: str, branch: str, dir_path: str, headers: dict) -> Optional[str]:
    """Return the Git tree SHA for dir_path by inspecting its parent directory listing.

    Calls the Contents API on the parent of dir_path, then finds the matching
    directory entry and returns its SHA.  Returns None if the path does not exist
    (404) or if the directory name is not found in the parent listing.

    Raises:
        RuntimeError: On network errors or unexpected GitHub API responses.
    """
    parent_path, _, dir_name = dir_path.rstrip("/").rpartition("/")
    api_url = f"{GITHUB_API_BASE}/repos/{repo}/contents/{parent_path}"
    params = {"ref": branch}
    try:
        resp = requests.get(api_url, params=params, headers=headers, timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error resolving tree SHA for '{dir_path}': {e}") from e
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(
            f"GitHub API error resolving tree SHA for '{dir_path}': HTTP {resp.status_code} — {resp.text[:300]}"
        )
    try:
        entries = resp.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON resolving tree SHA for '{dir_path}': {e}") from e
    for entry in entries:
        if entry.get("name") == dir_name and entry.get("type") == "dir":
            return entry.get("sha")
    return None


def _fetch_tree_recursive(
    repo: str,
    branch: str,
    tree_sha: str,
    remote_prefix: str,
    dest_dir: Path,
    headers: dict,
    max_depth: int = 4,
) -> int:
    """Fetch all files in a Git tree recursively, preserving directory structure.

    Uses the Git Trees API with ?recursive=1 to obtain the full file listing in
    a single API call, then downloads each blob from raw.githubusercontent.com.

    Args:
        repo: GitHub repository in "owner/repo" format.
        branch: Branch or ref name used to build raw download URLs.
        tree_sha: SHA of the Git tree to fetch.
        remote_prefix: Path within the repo to the handler directory
            (e.g. "community_handlers/elasticsearch_handler").  Used to
            construct raw download URLs.
        dest_dir: Local directory where files will be written.
        headers: HTTP headers (auth, Accept) for GitHub API requests.
        max_depth: Maximum allowed directory nesting depth.  Entries whose
            relative path contains >= max_depth slashes are skipped.

    Returns:
        Number of files downloaded.

    Raises:
        RuntimeError: On network errors or unexpected API responses.
    """
    api_url = f"{GITHUB_API_BASE}/repos/{repo}/git/trees/{tree_sha}"
    params = {"recursive": "1"}
    try:
        resp = requests.get(api_url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Network error fetching tree '{tree_sha}': {e}") from e
    try:
        tree_data = resp.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON from Git Trees API for tree '{tree_sha}': {e}") from e

    if tree_data.get("truncated"):
        logger.warning("Tree for handler '%s' was truncated; some files may be missing", remote_prefix)

    file_count = 0
    for entry in tree_data.get("tree", []):
        if entry.get("type") != "blob":
            continue
        path = entry["path"]
        if path.count("/") >= max_depth:
            logger.debug("Skipping deeply nested path '%s' (max_depth=%d)", path, max_depth)
            continue
        local_path = dest_dir / path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{remote_prefix}/{path}"
        try:
            file_resp = requests.get(raw_url, headers=headers, timeout=30)
            file_resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to download '{path}' for handler '{remote_prefix}': {e}") from e
        local_path.write_bytes(file_resp.content)
        logger.debug("Downloaded %s (%d bytes)", path, entry.get("size", 0))
        file_count += 1
    return file_count


def fetch_handler(handler_dir_name: str, storage_dir: Path) -> Optional[Path]:
    """
    Fetch a single community handler directory from GitHub into storage_dir.

    Downloads the full directory tree for the requested handler using the
    GitHub Git Trees API, preserving subdirectory structure.

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

        if dest_dir.is_dir() and (dest_dir / "__init__.py").exists():
            logger.debug("Community handler '%s' already on disk at %s", handler_dir_name, dest_dir)
            return dest_dir

        repo, branch, path_prefix = _get_repo_config()
        headers = _github_headers()
        remote_prefix = f"{path_prefix}/{handler_dir_name}"

        logger.debug("Fetching community handler '%s' from %s@%s", handler_dir_name, repo, branch)

        tree_sha = _resolve_tree_sha(repo, branch, remote_prefix, headers)
        if tree_sha is None:
            logger.error("Community handler '%s' not found in repo '%s'", handler_dir_name, repo)
            return None

        # Use a temporary directory for downloading files before moving to the final location.
        # This prevents leaving a partially downloaded handler on disk if something goes wrong.
        # As a fail-safe measure, we remove any existing temp directory before starting, and ensure cleanup on exceptions.
        tmp_dir = storage_dir / f".tmp_{handler_dir_name}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            file_count = _fetch_tree_recursive(repo, branch, tree_sha, remote_prefix, tmp_dir, headers)
            logger.debug("Fetched %d files for handler '%s'", file_count, handler_dir_name)

            # Atomic rename.
            # If dest_dir already exists, remove it first.
            # This ensures that we don't end up with a mix of old and new files if the handler is updated.
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            tmp_dir.rename(dest_dir)

        except Exception:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            raise

        logger.debug("Community handler '%s' fetched successfully to %s", handler_dir_name, dest_dir)
        return dest_dir


def community_handlers_enabled() -> bool:
    """Returns True if community handlers are enabled via env var.

    Set MINDSDB_COMMUNITY_HANDLERS=true to opt in.
    Community handlers are disabled by default.
    """
    val = os.environ.get("MINDSDB_COMMUNITY_HANDLERS", "false").lower()
    return val in ("1", "true", "yes", "enabled")


def get_community_handlers_storage_dir(storage_root: Path) -> Path:
    """Returns (and creates if needed) the community handlers storage directory."""
    community_dir = storage_root / "community_handlers"
    # Creating the directory, maybe can be done on init?
    community_dir.mkdir(parents=True, exist_ok=True)
    return community_dir


def list_available_handlers() -> list:
    """
    Return handler metadata from the community index.json.

    Each dict has keys: name, title, folder, type, support_level,
    icon_path, description.
    """
    repo, branch, _ = _get_repo_config()
    api_url = f"{GITHUB_API_BASE}/repos/{repo}/contents/index.json"
    params = {"ref": branch}

    try:
        logger.debug("Fetching community handlers index from GitHub: %s", api_url)
        resp = requests.get(api_url, params=params, headers=_github_headers(), timeout=30)
        if resp.status_code == 200:
            entry = resp.json()
            raw = base64.b64decode(entry["content"]).decode("utf-8")
            data = json.loads(raw)
            return data.get("handlers", [])
        logger.warning("Could not fetch community index: HTTP %s", resp.status_code)
    except Exception as e:
        logger.warning("Could not fetch community handlers index: %s", e)
    return []
