"""Fetch information about any current git repo."""

import functools
import logging
import subprocess
from typing import List, Optional, TypeVar

from typing_extensions import TypedDict

logger = logging.getLogger(__name__)

T = TypeVar("T")


def exec_git(command: List[str]) -> Optional[str]:
    try:
        return subprocess.check_output(
            ["git"] + command, encoding="utf-8", stderr=subprocess.DEVNULL
        ).strip()
    except BaseException:
        return None


class GitInfo(TypedDict, total=False):
    repo_name: Optional[str]
    remote_url: Optional[str]
    commit: Optional[str]
    branch: Optional[str]
    author_name: Optional[str]
    author_email: Optional[str]
    commit_time: Optional[str]
    dirty: Optional[bool]
    tags: Optional[str]


@functools.lru_cache(maxsize=1)
def get_git_info(remote: str = "origin") -> GitInfo:
    """Get information about the git repository."""
    if not exec_git(["rev-parse", "--is-inside-work-tree"]):
        return GitInfo(
            remote_url=None,
            commit=None,
            branch=None,
            author_name=None,
            author_email=None,
            commit_time=None,
            dirty=None,
            tags=None,
            repo_name=None,
        )

    return {
        "remote_url": exec_git(["remote", "get-url", remote]),
        "commit": exec_git(["rev-parse", "HEAD"]),
        "commit_time": exec_git(["log", "-1", "--format=%ct"]),
        "branch": exec_git(["rev-parse", "--abbrev-ref", "HEAD"]),
        "tags": exec_git(
            ["describe", "--tags", "--exact-match", "--always", "--dirty"]
        ),
        "dirty": exec_git(["status", "--porcelain"]) != "",
        "author_name": exec_git(["log", "-1", "--format=%an"]),
        "author_email": exec_git(["log", "-1", "--format=%ae"]),
        "repo_name": (exec_git(["rev-parse", "--show-toplevel"]) or "").split("/")[-1],
    }
