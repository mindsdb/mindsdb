import json
import os
import sys
import subprocess
from typing import Dict, List


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


def build_handler_metrics(handlers: list[str]) -> Dict[str, Dict[str, float]]:
    """Generate coverage metrics per handler directory."""
    # `coverage json` reads the latest .coverage data created by pytest
    result = run(["coverage", "json", "-o", "coverage.json"])
    if result.returncode != 0:
        print("[coverage] Failed to produce coverage.json", file=sys.stderr)
        sys.exit(result.returncode)

    try:
        with open("coverage.json", "r", encoding="utf-8") as fh:
            coverage_data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[coverage] Unable to read coverage.json: {exc}", file=sys.stderr)
        sys.exit(1)

    files: Dict[str, dict] = coverage_data.get("files", {})
    metrics: Dict[str, Dict[str, float]] = {}

    for handler in handlers:
        prefix = f"mindsdb/integrations/handlers/{handler}_handler"
        statements = 0
        missing = 0

        for path, info in files.items():
            if not (path == f"{prefix}.py" or path.startswith(f"{prefix}/")):
                continue
            summary = info.get("summary", {})
            statements += summary.get("num_statements", 0)
            missing += summary.get("missing_lines", 0)

        metrics[handler] = {
            "statements": statements,
            "missing": missing,
            "coverage": 0.0 if statements == 0 else (1 - missing / statements) * 100,
        }

    return metrics


def enforce_per_handler_directory_threshold(metrics: Dict[str, Dict[str, float]], threshold: float) -> None:
    """Ensure each handler directory meets the threshold when all its files are considered together."""
    failed = False

    for handler, info in metrics.items():
        statements = info["statements"]
        coverage_pct = info["coverage"]
        if statements == 0:
            print(
                f"[coverage] No executable statements detected for handler '{handler}'. "
                "Ensure tests import the handler package.",
                file=sys.stderr,
            )
            failed = True
            continue

        print(
            f"[coverage] Handler '{handler}' coverage: {coverage_pct:.2f}% "
            f"({statements - info['missing']}/{statements} statements)"
        )
        if coverage_pct < threshold:
            print(
                f"[coverage] Handler '{handler}' coverage below threshold {threshold:.2f}%.",
                file=sys.stderr,
            )
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
    metrics = build_handler_metrics(handlers)
    enforce_per_handler_directory_threshold(metrics, threshold)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
