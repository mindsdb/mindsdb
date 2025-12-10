"""
Validate per-handler coverage thresholds using coverage XML output.
"""

from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET


def _load_handlers_from_env() -> list[str]:
    handlers_blob = os.environ.get("VERIFIED_HANDLER_LIST", "")
    handlers = [line.strip() for line in handlers_blob.splitlines() if line.strip()]
    return handlers


def _load_threshold_from_env() -> float:
    raw_threshold = os.environ.get("COVERAGE_FAIL_UNDER", "80")
    try:
        return float(raw_threshold)
    except ValueError as exc:  # pragma: no cover - defensive path
        raise ValueError(f"Invalid COVERAGE_FAIL_UNDER value: {raw_threshold}") from exc


def _load_coverage(xml_path: str) -> dict[str, float]:
    tree = ET.parse(xml_path)
    coverage_by_file = {}
    for cls in tree.findall(".//class"):
        filename = cls.get("filename")
        line_rate = cls.get("line-rate")
        if not filename or line_rate is None:
            continue
        coverage_by_file[filename] = float(line_rate) * 100
    return coverage_by_file


def main() -> int:
    handlers = _load_handlers_from_env()
    if not handlers:
        print("[coverage] No handlers configured; nothing to check.")
        return 1

    threshold = _load_threshold_from_env()
    xml_path = os.environ.get("COVERAGE_XML_PATH", "coverage.xml")
    if not os.path.exists(xml_path):
        print(f"[coverage] Coverage xml not found at {xml_path}")
        return 1

    coverage_by_file = _load_coverage(xml_path)
    failed = False

    for handler in handlers:
        filename = f"mindsdb/integrations/handlers/{handler}_handler.py"
        rate = coverage_by_file.get(filename)
        if rate is None:
            print(f"[coverage] Missing data for {filename}")
            failed = True
            continue
        if rate < threshold:
            print(f"[coverage] {filename} {rate:.2f}% < {threshold}%")
            failed = True
        else:
            print(f"[coverage] {filename} {rate:.2f}% >= {threshold}%")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
