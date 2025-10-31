"""Repository guard checks for UI contract compliance."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def check_profile_strings(errors: List[str]) -> None:
    from ui.strings import ProfileStrings as PS

    expected = {
        "RUN_BUTTON": "▶️ Run Profile",
        "SUGGEST_BUTTON": "✨ Suggest DQ Config",
        "SAVE_TOGGLE": "💾 Save Profile",
        "GRID_COLUMN_SELECT": "Select",
        "GRID_COLUMN_COLUMN": "Column",
        "GRID_COLUMN_PHYSICAL_TYPE": "Physical Type",
        "GRID_COLUMN_NULLS": "Nulls",
        "GRID_COLUMN_DISTINCT": "Distinct",
        "GRID_COLUMN_AVG_LENGTH": "Avg Length",
        "GRID_COLUMN_MIN_VALUE": "Min Value",
        "GRID_COLUMN_MAX_VALUE": "Max Value",
        "GRID_COLUMN_WHITESPACE": "Whitespace %",
        "GRID_COLUMN_GUESSED_TYPE": "Guessed Type",
        "GRID_COLUMN_CONFIDENCE": "Confidence",
        "GRID_COLUMN_NOTE": "Note",
        "SUGGEST_BUTTON": "✨ Suggest DQ Config",
    }
    for attr, expected_value in expected.items():
        actual = getattr(PS, attr, None)
        if actual != expected_value:
            errors.append(
                f"ProfileStrings.{attr} expected '{expected_value}' but found '{actual}'."
            )


def check_docs_strings(errors: List[str]) -> None:
    from ui.strings import DocsStrings

    expected_tabs = (
        "User Guide",
        "Profiling",
        "DQ Framework",
        "Technical Overview",
        "Data Governance",
        "Version History",
    )
    if tuple(DocsStrings.TABS) != expected_tabs:
        errors.append(
            "DocsStrings.TABS must remain the six default documentation tabs."
        )


def main() -> int:
    errors: List[str] = []
    try:
        check_profile_strings(errors)
        check_docs_strings(errors)
    except Exception as exc:  # pragma: no cover - guard failure path
        errors.append(f"Guard check execution error: {exc}")

    if errors:
        print("UI contract guard check failed:\n", file=sys.stderr)
        for error in errors:
            print(f" - {error}", file=sys.stderr)
        print(
            "Refer to docs/ui_contract_docs.md before modifying UI labels or layout.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
