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


def run(cmd: list[str], capture: bool = False, env: dict | None = None) -> subprocess.CompletedProcess:
    print("[coverage]", " ".join(cmd))
    kwargs: dict = {"text": True}
    if env is not None:
        kwargs["env"] = env
    if capture:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.STDOUT
    return subprocess.run(cmd, **kwargs)


def build_handler_metrics(handlers: list[str], coverage_file: str) -> Dict[str, Dict[str, float]]:
    """Generate coverage metrics per handler directory from an existing coverage data file."""
    env = os.environ.copy()
    env["COVERAGE_FILE"] = coverage_file

    # `coverage json` reads the .coverage data created by pytest
    result = run(["coverage", "json", "-o", "coverage.json"], env=env)
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

    print("[coverage] Verifying configured handlers only (not full-suite coverage).")

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

    coverage_file = os.environ.get("COVERAGE_FILE") or ".coverage"
    if not os.path.exists(coverage_file):
        print(
            f"[coverage] Coverage data file {coverage_file!r} not found. "
            "Run pytest with coverage before executing this script.",
            file=sys.stderr,
        )
        return 1

    metrics = build_handler_metrics(handlers, coverage_file)
    enforce_per_handler_directory_threshold(metrics, threshold)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
