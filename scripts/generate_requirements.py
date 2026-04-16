#!/usr/bin/env python3
import re
import pathlib

try:
    import tomllib
except ImportError:
    import tomli as tomllib

root = pathlib.Path(__file__).parent.parent
handlers_dir = root / "mindsdb" / "integrations" / "handlers"
req_dir = root / "requirements"

# Regex to split a dep string at the first version specifier, extra bracket, or marker
_PKG_NAME_RE = re.compile(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)")


def _normalize(name: str) -> str:
    """Normalize a package name to the canonical form used in uv.lock."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _load_lock_versions(lock_path: pathlib.Path) -> dict:
    """
    Parse uv.lock and return {normalized_name: version}.
    When a package appears with multiple versions (rare forks), the last entry wins.
    """
    with open(lock_path, "rb") as f:
        lock = tomllib.load(f)
    return {_normalize(p["name"]): p["version"] for p in lock.get("package", []) if "version" in p}


def _pin(dep_str: str, lock_versions: dict) -> str:
    """
    Given a raw dependency string from pyproject.toml, return it pinned to the
    lock-resolved version.  Preserves environment markers (e.g. ; sys_platform).
    Falls back to the original string if the package is not in the lock file.
    """
    dep_str = dep_str.split("#")[0].strip()
    if not dep_str:
        return dep_str

    marker = ""
    if ";" in dep_str:
        pkg_part, marker = dep_str.split(";", 1)
        marker = " ; " + marker.strip()
    else:
        pkg_part = dep_str

    m = _PKG_NAME_RE.match(pkg_part.strip())
    if not m:
        return dep_str

    raw_name = m.group(1)
    resolved = lock_versions.get(_normalize(raw_name))
    if resolved is None:
        return dep_str

    return f"{raw_name}=={resolved}{marker}"


def write_if_changed(path: pathlib.Path, lines: list) -> bool:
    content = "\n".join(lines) + "\n" if lines else ""
    if path.exists() and path.read_text() == content:
        return False
    path.write_text(content)
    return True


def main() -> None:
    with open(root / "pyproject.toml", "rb") as f:
        data = tomllib.load(f)

    lock_path = root / "uv.lock"
    if lock_path.exists():
        lock_versions = _load_lock_versions(lock_path)
    else:
        print("warning: uv.lock not found — writing loose constraints from pyproject.toml")
        lock_versions = {}

    extras = data["project"]["optional-dependencies"]
    groups = data.get("dependency-groups", {})

    changed = 0

    # Optional extras → handler dirs or requirements/
    for name, raw_deps in extras.items():
        pinned = [_pin(d, lock_versions) for d in raw_deps if isinstance(d, str)]

        handler_dir = handlers_dir / f"{name}_handler"
        dest = handler_dir / "requirements.txt" if handler_dir.is_dir() else req_dir / f"requirements-{name}.txt"

        if write_if_changed(dest, pinned):
            print(f"  wrote {dest.relative_to(root)}")
            changed += 1

    # Dependency groups (PEP 735) → requirements/
    for name, entries in groups.items():
        # PEP 735 entries can be strings or include-group dicts
        pinned = [_pin(e, lock_versions) for e in entries if isinstance(e, str)]
        dest = req_dir / f"requirements-{name}.txt"

        if write_if_changed(dest, pinned):
            print(f"  wrote {dest.relative_to(root)}")
            changed += 1

    print(f"\n{changed} file(s) updated.")


if __name__ == "__main__":
    main()
