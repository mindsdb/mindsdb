#!/usr/bin/env python3
"""Dependency checks driven by ``pyproject.toml`` and ``deptry``.

deptry loads ``pyproject.toml`` (run from the repo root). PEP 621
optional groups are classified with ``--optional-dependencies-dev-groups``:

- **Core** — optional groups except ``agents``, ``kb``, ``opentelemetry``, and
  ``langfuse`` are marked dev-only so they're ignored by deptry;
  regular deps are ``[project.dependencies]`` plus
  those four extras. Scan ``.`` with the handlers tree excluded.

- **Per-handler extra** — regular deps are main + ``agents`` + ``kb`` +
  ``opentelemetry`` + ``langfuse`` + the handler extra under test; every other
  optional group is marked dev-only. Scan
  ``mindsdb/integrations/handlers/<extra_name>_handler/``.

Handler runs pass ``--per-rule-ignores`` built from ``[tool.deptry.per_rule_ignores]``,
with DEP002 extended by the set of declared package names for that scan (main +
core extras + handler extra) so unused-dependency noise is suppressed inside a
single handler directory.

Run::

    uv run --extra dev python tests/scripts/check_pyproject_dependencies.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[2]
HANDLERS_ROOT = REPO_ROOT / "mindsdb" / "integrations" / "handlers"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

_PKG_NAME_RE = re.compile(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)")

success = True

# Always treated as non-dev (regular) optional extras for deptry — core product stack.
CORE_OPTIONAL_EXTRAS = frozenset({"agents", "kb", "opentelemetry", "langfuse"})


def extract_package_name(dep_string: str) -> str:
    m = _PKG_NAME_RE.match(dep_string.strip())
    return m.group(1) if m else ""


def load_pyproject() -> dict:
    with open(PYPROJECT_PATH, "rb") as f:
        return tomllib.load(f)


def all_optional_group_names(pyproject: dict) -> list[str]:
    return sorted(pyproject.get("project", {}).get("optional-dependencies", {}).keys())


def regular_packages_for_handler_scan(pyproject: dict, handler_extra: str) -> set[str]:
    """Declared regular package names: main + CORE_OPTIONAL_EXTRAS + ``handler_extra``."""
    names: set[str] = set()
    for dep in pyproject["project"].get("dependencies", []):
        p = extract_package_name(dep)
        if p:
            names.add(p.lower())
    groups = CORE_OPTIONAL_EXTRAS | {handler_extra}
    for g in groups:
        for dep in pyproject["project"].get("optional-dependencies", {}).get(g, []):
            s = dep.strip()
            if not s or s.startswith("#"):
                continue
            p = extract_package_name(dep)
            if p:
                names.add(p.lower())
    return names


def check_main_vs_extra_overlap(pyproject: dict) -> bool:
    global success
    ok = True
    main = {extract_package_name(d).lower() for d in pyproject["project"]["dependencies"] if extract_package_name(d)}
    for extra, deps in pyproject["project"].get("optional-dependencies", {}).items():
        extra_pkgs = {
            extract_package_name(d).lower()
            for d in deps
            if d.strip() and not d.strip().startswith("#") and extract_package_name(d)
        }
        overlap = sorted(main & extra_pkgs)
        if overlap:
            ok = False
            success = False
            print(f"- Overlap: packages in [project.dependencies] and extra [{extra}]: {', '.join(overlap)}")
    return ok


def _fmt_per_rule_ignores(rules: dict[str, list[str]]) -> str:
    return ",".join(f"{rule}={'|'.join(sorted(set(pkgs)))}" for rule, pkgs in sorted(rules.items()) if pkgs)


def _run_deptry(
    args: list[str],
    *,
    cwd: Path,
) -> list[str]:
    errors: list[str] = []
    output_file = REPO_ROOT / "deptry.json"
    cmd = [sys.executable, "-m", "deptry", "--no-ansi", "-o", str(output_file), *args]
    try:
        subprocess.run(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not output_file.exists():
            errors.append("deptry did not write JSON output (see stderr if any).")
            return errors
        with open(output_file) as f:
            data = json.load(f)
        for r in data:
            loc = r["location"]
            err = r["error"]
            file_ = loc.get("file", "?")
            errors.append(f"{file_}:{loc['line']}:{loc['column']}: {err['code']} {err['message']}")
    finally:
        if output_file.exists():
            output_file.unlink()
    return errors


def _per_rule_ignores_for_handler_extra(pyproject: dict, extra_name: str) -> str:
    tool = pyproject.get("tool", {}).get("deptry", {})
    pri = tool.get("per_rule_ignores", {})
    regular = regular_packages_for_handler_scan(pyproject, extra_name)
    dep001 = sorted({p.lower() for p in pri.get("DEP001", [])})
    dep002 = sorted(regular | {p.lower() for p in pri.get("DEP002", [])})
    dep003 = sorted({p.lower() for p in pri.get("DEP003", [])})
    return _fmt_per_rule_ignores({"DEP001": dep001, "DEP002": dep002, "DEP003": dep003})


def check_core_deptry(all_optional_groups: list[str]) -> bool:
    """Regular deps = main + CORE_OPTIONAL_EXTRAS; all other optional groups are dev-only."""
    global success
    dev_groups = [g for g in all_optional_groups if g not in CORE_OPTIONAL_EXTRAS]
    args: list[str] = []
    if dev_groups:
        args.extend(["--optional-dependencies-dev-groups", ",".join(dev_groups)])
    args.extend(
        [
            "--extend-exclude",
            "mindsdb/integrations/handlers",
            ".",
        ]
    )
    errors = _run_deptry(args, cwd=REPO_ROOT)
    if errors:
        success = False
        print(
            "- Core deptry (main + agents, kb, opentelemetry, langfuse vs non-handler tree; "
            "other optional groups as dev)"
        )
        for line in errors:
            print(f"    {line}")
        return False
    return True


def check_handler_extra_deptry(pyproject: dict, all_optional_groups: list[str], extra_name: str) -> bool:
    global success
    handler_dir = HANDLERS_ROOT / f"{extra_name}_handler"
    if not handler_dir.is_dir():
        return True

    regular_groups = CORE_OPTIONAL_EXTRAS | {extra_name}
    dev_groups = [g for g in all_optional_groups if g not in regular_groups]
    pri = _per_rule_ignores_for_handler_extra(pyproject, extra_name)
    rel = handler_dir.relative_to(REPO_ROOT)
    args: list[str] = []
    if dev_groups:
        args.extend(["--optional-dependencies-dev-groups", ",".join(dev_groups)])
    args.extend(
        [
            "--per-rule-ignores",
            pri,
            str(rel),
        ]
    )
    errors = _run_deptry(args, cwd=REPO_ROOT)

    if errors:
        success = False
        print(f"- Handler extra [{extra_name}] → {rel}")
        for line in errors:
            print(f"    {line}")
        return False
    return True


def main() -> None:
    pyproject = load_pyproject()
    all_optional_groups = all_optional_group_names(pyproject)

    print("=== 1. Main vs optional-extra overlap ===\n")
    overlap_ok = check_main_vs_extra_overlap(pyproject)
    if overlap_ok:
        print("OK — no package appears in both main and an extra.\n")
    else:
        print()

    print("=== 2. Core deptry (excluding mindsdb/integrations/handlers/) ===\n")
    core_ok = check_core_deptry(all_optional_groups)
    if core_ok:
        print("OK\n")
    else:
        print()

    print("=== 3. Handler extras (main + extra vs mindsdb/integrations/handlers/<extra>_handler/) ===\n")
    any_handler_failed = False
    for name in all_optional_groups:
        if not check_handler_extra_deptry(pyproject, all_optional_groups, name):
            any_handler_failed = True
    if not any_handler_failed:
        print("OK — all handler extra deptry checks passed.\n")
    else:
        print("One or more handler extra deptry checks failed.\n")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
