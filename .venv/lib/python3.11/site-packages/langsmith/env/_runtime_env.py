"""Environment information."""

import functools
import logging
import os
import platform
import subprocess
from typing import Dict, List, Optional, Union

from langsmith.utils import get_docker_compose_command
from langsmith.env._git import exec_git

try:
    # psutil is an optional dependency
    import psutil

    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False
logger = logging.getLogger(__name__)


def get_runtime_and_metrics() -> dict:
    """Get the runtime information as well as metrics."""
    return {**get_runtime_environment(), **get_system_metrics()}


def get_system_metrics() -> Dict[str, Union[float, dict]]:
    """Get CPU and other performance metrics."""
    global _PSUTIL_AVAILABLE
    if not _PSUTIL_AVAILABLE:
        return {}
    try:
        process = psutil.Process(os.getpid())
        metrics: Dict[str, Union[float, dict]] = {}

        with process.oneshot():
            mem_info = process.memory_info()
            metrics["thread_count"] = float(process.num_threads())
            metrics["mem"] = {
                "rss": float(mem_info.rss),
            }
            ctx_switches = process.num_ctx_switches()
            cpu_times = process.cpu_times()
            metrics["cpu"] = {
                "time": {
                    "sys": cpu_times.system,
                    "user": cpu_times.user,
                },
                "ctx_switches": {
                    "voluntary": float(ctx_switches.voluntary),
                    "involuntary": float(ctx_switches.involuntary),
                },
                "percent": process.cpu_percent(),
            }
        return metrics
    except Exception as e:
        # If psutil is installed but not compatible with the build,
        # we'll just cease further attempts to use it.
        _PSUTIL_AVAILABLE = False
        logger.debug("Failed to get system metrics: %s", e)
        return {}


@functools.lru_cache(maxsize=1)
def get_runtime_environment() -> dict:
    """Get information about the environment."""
    # Lazy import to avoid circular imports
    from langsmith import __version__

    shas = get_release_shas()
    return {
        "sdk": "langsmith-py",
        "sdk_version": __version__,
        "library": "langsmith",
        "platform": platform.platform(),
        "runtime": "python",
        "py_implementation": platform.python_implementation(),
        "runtime_version": platform.python_version(),
        "langchain_version": get_langchain_environment(),
        "langchain_core_version": get_langchain_core_version(),
        **shas,
    }


@functools.lru_cache(maxsize=1)
def get_langchain_environment() -> Optional[str]:
    try:
        import langchain  # type: ignore

        return langchain.__version__
    except:  # noqa
        return None


@functools.lru_cache(maxsize=1)
def get_langchain_core_version() -> Optional[str]:
    try:
        import langchain_core  # type: ignore

        return langchain_core.__version__
    except ImportError:
        return None


@functools.lru_cache(maxsize=1)
def get_docker_version() -> Optional[str]:
    import subprocess

    try:
        docker_version = (
            subprocess.check_output(["docker", "--version"]).decode("utf-8").strip()
        )
    except FileNotFoundError:
        docker_version = "unknown"
    except:  # noqa
        return None
    return docker_version


@functools.lru_cache(maxsize=1)
def get_docker_compose_version() -> Optional[str]:
    try:
        docker_compose_version = (
            subprocess.check_output(["docker-compose", "--version"])
            .decode("utf-8")
            .strip()
        )
    except FileNotFoundError:
        docker_compose_version = "unknown"
    except:  # noqa
        return None
    return docker_compose_version


@functools.lru_cache(maxsize=1)
def _get_compose_command() -> Optional[List[str]]:
    try:
        compose_command = get_docker_compose_command()
    except ValueError as e:
        compose_command = [f"NOT INSTALLED: {e}"]
    except:  # noqa
        return None
    return compose_command


@functools.lru_cache(maxsize=1)
def get_docker_environment() -> dict:
    """Get information about the environment."""
    compose_command = _get_compose_command()
    return {
        "docker_version": get_docker_version(),
        "docker_compose_command": (
            " ".join(compose_command) if compose_command is not None else None
        ),
        "docker_compose_version": get_docker_compose_version(),
    }


def get_langchain_env_vars() -> dict:
    """Retrieve the langchain environment variables."""
    env_vars = {k: v for k, v in os.environ.items() if k.startswith("LANGCHAIN_")}
    for key in list(env_vars):
        if "key" in key.lower():
            v = env_vars[key]
            env_vars[key] = v[:2] + "*" * (len(v) - 4) + v[-2:]
    return env_vars


@functools.lru_cache(maxsize=1)
def get_langchain_env_var_metadata() -> dict:
    """Retrieve the langchain environment variables."""
    excluded = {
        "LANGCHAIN_API_KEY",
        "LANGCHAIN_ENDPOINT",
        "LANGCHAIN_TRACING_V2",
        "LANGCHAIN_PROJECT",
        "LANGCHAIN_SESSION",
        "LANGSMITH_RUNS_ENDPOINTS",
    }
    langchain_metadata = {
        k: v
        for k, v in os.environ.items()
        if (k.startswith("LANGCHAIN_") or k.startswith("LANGSMITH_"))
        and k not in excluded
        and "key" not in k.lower()
        and "secret" not in k.lower()
        and "token" not in k.lower()
    }
    env_revision_id = langchain_metadata.pop("LANGCHAIN_REVISION_ID", None)
    if env_revision_id:
        langchain_metadata["revision_id"] = env_revision_id
    elif default_revision_id := _get_default_revision_id():
        langchain_metadata["revision_id"] = default_revision_id

    return langchain_metadata


@functools.lru_cache(maxsize=1)
def _get_default_revision_id() -> Optional[str]:
    """Get the default revision ID based on `git describe`."""
    try:
        return exec_git(["describe", "--tags", "--always", "--dirty"])
    except BaseException:
        return None


@functools.lru_cache(maxsize=1)
def get_release_shas() -> Dict[str, str]:
    common_release_envs = [
        "VERCEL_GIT_COMMIT_SHA",
        "NEXT_PUBLIC_VERCEL_GIT_COMMIT_SHA",
        "COMMIT_REF",
        "RENDER_GIT_COMMIT",
        "CI_COMMIT_SHA",
        "CIRCLE_SHA1",
        "CF_PAGES_COMMIT_SHA",
        "REACT_APP_GIT_SHA",
        "SOURCE_VERSION",
        "GITHUB_SHA",
        "TRAVIS_COMMIT",
        "GIT_COMMIT",
        "BUILD_VCS_NUMBER",
        "bamboo_planRepository_revision",
        "Build.SourceVersion",
        "BITBUCKET_COMMIT",
        "DRONE_COMMIT_SHA",
        "SEMAPHORE_GIT_SHA",
        "BUILDKITE_COMMIT",
    ]
    shas = {}
    for env in common_release_envs:
        env_var = os.environ.get(env)
        if env_var is not None:
            shas[env] = env_var
    return shas
