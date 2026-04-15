#!/usr/bin/env python3
"""Check dependency hygiene for MindsDB.

Checks
------
1. Core deptry   – run deptry on the main codebase (handler dirs excluded)
2. Handler deptry – run deptry on each *supported* handler individually
3. Cross-handler imports – verify inter-handler deps are declared in requirements.txt

Supported handlers are derived from ``[project.optional-dependencies]`` in
``pyproject.toml`` by matching extras that have a corresponding
``mindsdb/integrations/handlers/{name}_handler/`` directory on disk.
"""

import glob
import json
import re
import subprocess
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # backport; present in requirements-dev.txt

REPO_ROOT = Path(__file__).resolve().parents[2]
HANDLERS_DIR = REPO_ROOT / "mindsdb" / "integrations" / "handlers"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

NON_HANDLER_EXTRAS = frozenset(
    {
        "agents",
        "kb",
        "opentelemetry",
        "langfuse",
        "dev",
        "test",
    }
)

# Packages that are used indirectly by specific handlers and should never be
# flagged as DEP002 during handler scans.
HANDLER_DEP002_INDIRECT = [
    "pyarrow",  # BYOM – loaded at runtime via string
    "scikit-learn",  # BYOM
    "thrift-sasl",  # Hive – required by pyhive transport
    "gcsfs",  # GCS – filesystem backend
    "protobuf",  # Lindorm
    "torch",  # Huggingface – optional GPU backend
    "sentence-transformers",  # RAG
    "sqlalchemy-solr",  # Solr
    "tiktoken",  # OpenAI – tokenizer loaded at runtime
    "onnxruntime",  # ChromaDB – embedding backend
    "python-freshdesk",  # Freshdesk – non-standard import name
]

HANDLER_DEP001_INDIRECT = [
    "tests",  # mindsdb tests folder on sys.path
    "pyarrow",  # snowflake handler optional
    "IfxPyDbi",  # Informix – non-standard import
    "ingres_sa_dialect",  # Ingres – non-standard import
    "pyodbc",  # several handlers, optional
    "freshdesk",  # Freshdesk – non-standard import
]

HANDLER_DEP003_INDIRECT = [
    "pyarrow",  # snowflake handler – transitive via connector
]

success = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pyproject() -> dict:
    with open(PYPROJECT_PATH, "rb") as f:
        return tomllib.load(f)


_PKG_NAME_RE = re.compile(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)")


def extract_package_name(dep_string: str) -> str:
    """Return the package name from a PEP 508 dependency string.

    >>> extract_package_name("psycopg[binary] >= 2.0")
    'psycopg'
    """
    m = _PKG_NAME_RE.match(dep_string.strip())
    return m.group(1) if m else ""


def get_all_pyproject_packages(pyproject: dict) -> set[str]:
    """Every package name declared anywhere in pyproject.toml."""
    names: set[str] = set()
    for dep in pyproject.get("project", {}).get("dependencies", []):
        pkg = extract_package_name(dep)
        if pkg:
            names.add(pkg)
    for deps in pyproject.get("project", {}).get("optional-dependencies", {}).values():
        for dep in deps:
            pkg = extract_package_name(dep)
            if pkg:
                names.add(pkg)
    return names


def get_supported_handlers(pyproject: dict) -> list[str]:
    """Handler names whose extra exists in pyproject.toml AND whose dir is on disk."""
    extras = pyproject.get("project", {}).get("optional-dependencies", {})
    return sorted(
        name for name in extras if name not in NON_HANDLER_EXTRAS and (HANDLERS_DIR / f"{name}_handler").is_dir()
    )


def print_errors(label: str, errors: list[str]) -> None:
    global success
    if errors:
        success = False
        print(f"- {label}")
        for line in errors:
            print(f"    {line}")
        print()


def _fmt_per_rule_ignores(rules: dict[str, list[str]]) -> str:
    """``DEP001=a|b,DEP002=c|d`` format expected by ``deptry --per-rule-ignores``."""
    return ",".join(f"{rule}={'|'.join(sorted(set(pkgs)))}" for rule, pkgs in sorted(rules.items()) if pkgs)


def _run_deptry(args: list[str]) -> list[str]:
    """Execute deptry with *args*; return human-readable error strings."""
    errors: list[str] = []
    output_file = REPO_ROOT / "deptry.json"
    cmd = [sys.executable, "-m", "deptry", "--no-ansi", "-o", str(output_file), *args]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not output_file.exists():
            stderr = result.stderr.decode("utf-8", errors="replace")
            if result.returncode != 0:
                errors.append(f"deptry error: {stderr}")
            else:
                errors.append("deptry error: output file was not generated.")
            return errors

        with open(output_file) as f:
            for r in json.load(f):
                loc = r["location"]
                err = r["error"]
                errors.append(f"{loc.get('file', '?')}:{loc['line']}:{loc['column']}: {err['code']} {err['message']}")
    finally:
        if output_file.exists():
            output_file.unlink()
    return errors


# ---------------------------------------------------------------------------
# Check 1 – core codebase
# ---------------------------------------------------------------------------


def check_core_deptry() -> None:
    """Run deptry on the core codebase using pyproject.toml config.

    deptry auto-discovers ``pyproject.toml`` in cwd and reads all
    ``[tool.deptry]`` settings (extend_exclude, per_rule_ignores,
    package_module_name_map, etc.) natively.  We avoid ``--config``
    because an explicit path breaks exclude-pattern matching in 0.25.
    """
    errors = _run_deptry(["."])
    print_errors("pyproject.toml", errors)


# ---------------------------------------------------------------------------
# Check 2 – supported handlers
# ---------------------------------------------------------------------------


def _dep002_exceptions_from_file(path: Path) -> list[str]:
    """Packages annotated with Snyk-pin or ignore-DEP002 in a requirements file."""
    out: list[str] = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "pinned by Snyk to avoid a vulnerability" in stripped or "ignore-DEP002" in stripped:
                m = _PKG_NAME_RE.match(stripped)
                if m:
                    out.append(m.group(1))
    return out


def check_handler_deptry(pyproject: dict, handler_names: list[str]) -> None:
    """Run deptry on each supported handler that has a requirements.txt.

    ``package_module_name_map`` is intentionally NOT passed via CLI so
    that deptry reads it from the auto-discovered ``pyproject.toml``.
    All paths are relative to REPO_ROOT (the cwd for deptry).
    """
    all_pyproject_pkgs = sorted(get_all_pyproject_packages(pyproject))
    non_handler_reqs = sorted(glob.glob(str(REPO_ROOT / "requirements" / "requirements*.txt")))

    base_dep002 = sorted(set(all_pyproject_pkgs + HANDLER_DEP002_INDIRECT))

    for name in handler_names:
        handler_dir = HANDLERS_DIR / f"{name}_handler"
        reqs_file = handler_dir / "requirements.txt"
        if not reqs_file.exists():
            continue

        dep002 = list(base_dep002)
        dep002.extend(_dep002_exceptions_from_file(reqs_file))

        per_rule = {
            "DEP001": list(HANDLER_DEP001_INDIRECT),
            "DEP002": dep002,
            "DEP003": list(HANDLER_DEP003_INDIRECT),
        }

        handler_dir_rel = handler_dir.relative_to(REPO_ROOT)
        reqs_file_rel = reqs_file.relative_to(REPO_ROOT)
        all_reqs = [str(reqs_file_rel)] + [str(Path(r).relative_to(REPO_ROOT)) for r in non_handler_reqs]

        errors = _run_deptry(
            [
                "--known-first-party",
                "mindsdb",
                "--requirements-files",
                ",".join(all_reqs),
                "--per-rule-ignores",
                _fmt_per_rule_ignores(per_rule),
                str(handler_dir_rel),
            ]
        )
        print_errors(str(reqs_file), errors)


# ---------------------------------------------------------------------------
# Check 3 – cross-handler imports
# ---------------------------------------------------------------------------

_RELATIVE_IMPORT_RE = re.compile(r"(?:\s|^)(?:from|import) \.\.\w+_handler")


def _handler_req_refs(handler_dir: Path) -> dict[str, str]:
    """Return ``{other_handler_name: '-r ...' line}`` from requirements files."""
    entries: dict[str, str] = {}
    for req_file in handler_dir.glob("requirements*.txt"):
        with open(req_file) as f:
            for line in f:
                low = line.lower().strip()
                if low.startswith("-r mindsdb/integrations/handlers/"):
                    ref_handler = low.split("mindsdb/integrations/handlers/")[1].split("/")[0]
                    entries[ref_handler] = low
    return entries


def check_cross_handler_imports(handler_names: list[str]) -> None:
    """Verify that cross-handler imports are absolute and declared in requirements."""
    for name in handler_names:
        handler_dir = HANDLERS_DIR / f"{name}_handler"
        if not handler_dir.is_dir():
            continue

        abs_import_re = re.compile(
            rf"(?:\s|^)(?:from|import) mindsdb\.integrations\.handlers\.(?!{name}_handler)\w+_handler"
        )

        required_handlers = _handler_req_refs(handler_dir)
        all_imported: list[str] = []

        for py_file in sorted(handler_dir.rglob("*.py")):
            errors: list[str] = []
            content = py_file.read_text()

            for match in _RELATIVE_IMPORT_RE.findall(content):
                errors.append(f"{match.strip()} <- Relative import of handler. Use absolute import instead")

            imported: dict[str, str] = {}
            for match in abs_import_re.findall(content):
                ref = match.strip().split("_handler")[0].split(".")[-1] + "_handler"
                imported[match.strip()] = ref
            all_imported.extend(imported.values())

            for line, imp_handler in imported.items():
                imp_reqs = HANDLERS_DIR / imp_handler / "requirements.txt"
                if imp_reqs.exists() and imp_handler not in required_handlers:
                    errors.append(
                        f"{line} <- {imp_handler} not in handler requirements.txt. "
                        f'Add: "-r mindsdb/integrations/handlers/{imp_handler}/requirements.txt"'
                    )

            print_errors(str(py_file), errors)

        unused = [f"{h} in requirements.txt but not used in code" for h in required_handlers if h not in all_imported]
        print_errors(str(handler_dir), unused)

        broken = []
        for _h, ref_line in required_handlers.items():
            ref_path = ref_line.split("-r ")[1]
            if not (REPO_ROOT / ref_path).exists():
                broken.append(f"{ref_line} <- this requirements file doesn't exist.")
        print_errors(str(handler_dir), broken)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _ensure_pyproject() -> None:
    """Restore pyproject.toml from git if a previous CI step deleted it."""
    if not PYPROJECT_PATH.exists():
        subprocess.run(
            ["git", "checkout", "--", str(PYPROJECT_PATH.name)],
            cwd=str(REPO_ROOT),
            check=True,
        )


def main() -> None:
    _ensure_pyproject()
    pyproject = load_pyproject()
    handler_names = get_supported_handlers(pyproject)

    print(f"Supported handlers: {', '.join(handler_names)}\n")

    print("--- Checking core dependencies ---")
    check_core_deptry()
    print()

    print("--- Checking handler dependencies ---")
    check_handler_deptry(pyproject, handler_names)
    print()

    print("--- Checking handlers that require other handlers ---")
    check_cross_handler_imports(handler_names)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
