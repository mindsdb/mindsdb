import os
import sys
import subprocess
from typing import List


def parse_handlers_env(value: str | None) -> List[str]:
    """Parse newline-separated handler names from an env var."""
    if not value:
        return []
    handlers: List[str] = []
    for line in value.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        handlers.append(stripped)
    return handlers


def run(cmd: list[str], capture: bool = False) -> subprocess.CompletedProcess:
    print("[coverage]", " ".join(cmd))
    kwargs: dict = {"text": True}
    if capture:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.STDOUT
    return subprocess.run(cmd, **kwargs)


def run_pytest_with_coverage(handlers: list[str]) -> None:
    """Run pytest with coverage only for the selected handlers."""
    cov_args: list[str] = [f"--cov=mindsdb/integrations/handlers/{h}_handler.py" for h in handlers]

    pytest_cmd = [
        "pytest",
        "--junitxml=pytest.xml",
        "--cov-report=term-missing:skip-covered",
        "--cov-report=xml",
        *cov_args,
        "tests/unit/handlers",
    ]

    proc = run(pytest_cmd, capture=True)

    # Save output for the GitHub coverage comment action
    output = proc.stdout or ""
    with open("pytest-coverage.txt", "w", encoding="utf-8") as fh:
        fh.write(output)
    print(output, end="")

    if proc.returncode != 0:
        print(f"[coverage] pytest failed with exit code {proc.returncode}", file=sys.stderr)
        sys.exit(proc.returncode)


def enforce_per_handler_threshold(handlers: list[str], threshold: float) -> None:
    """Run `coverage report` per handler file and fail if any is below the threshold."""
    failed = False

    for h in handlers:
        target = f"mindsdb/integrations/handlers/{h}_handler.py"
        print(f"[coverage] Checking {target} >= {threshold:.2f}%")

        cmd = [
            "coverage",
            "report",
            f"--fail-under={threshold}",
            target,
        ]
        result = run(cmd)
        if result.returncode != 0:
            failed = True

    if failed:
        print("[coverage] One or more handlers are below the coverage threshold.", file=sys.stderr)
        sys.exit(1)

    print("[coverage] All handlers meet the coverage threshold.")


def main() -> int:
    # Prefer HANDLERS_TO_VERIFY, fallback to HANDLERS_TO_INSTALL
    handlers_env = os.environ.get("HANDLERS_TO_VERIFY") or os.environ.get("HANDLERS_TO_INSTALL", "")
    handlers = parse_handlers_env(handlers_env)

    if not handlers:
        print(
            "[coverage] No handlers configured in HANDLERS_TO_VERIFY or HANDLERS_TO_INSTALL; failing.",
            file=sys.stderr,
        )
        return 1

    threshold_str = os.environ.get("COVERAGE_FAIL_UNDER", "80")
    try:
        threshold = float(threshold_str)
    except ValueError:
        print(f"[coverage] Invalid COVERAGE_FAIL_UNDER={threshold_str!r}", file=sys.stderr)
        return 1

    run_pytest_with_coverage(handlers)
    enforce_per_handler_threshold(handlers, threshold)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
