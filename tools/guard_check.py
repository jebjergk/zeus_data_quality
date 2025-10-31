"""UI contract guard checks for critical views."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_VIEW_PATH = PROJECT_ROOT / "views" / "profile_view.py"
CONFIG_VIEW_PATH = PROJECT_ROOT / "views" / "config_editor.py"
DOCS_VIEW_PATH = PROJECT_ROOT / "views" / "docs_view.py"
CONTRACT_DOCS_URL = "docs/ui_change_guidance.md"


class GuardError(Exception):
    """Raised when a guard check fails."""


def _load_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:  # pragma: no cover - should not happen in CI
        raise GuardError(f"Required file missing: {path}") from exc


def _check_tokens(path: Path, tokens: Iterable[str], description: str) -> None:
    content = _load_text(path)
    missing: List[str] = [token for token in tokens if token not in content]
    if missing:
        formatted = ", ".join(sorted(missing))
        raise GuardError(
            f"{description} is missing required tokens: {formatted}. See {CONTRACT_DOCS_URL}."
        )


def run_guard_checks() -> None:
    errors: List[str] = []

    try:
        from ui import keys as ui_keys
        from ui import strings as ui_strings
    except Exception as exc:  # pragma: no cover - import errors should fail CI
        raise GuardError(f"Unable to import UI constants: {exc}") from exc

    expected_grid_columns = [
        "Select",
        "Column",
        "Physical Type",
        "Nulls",
        "Distinct",
        "Avg Length",
        "Min Value",
        "Max Value",
        "Whitespace %",
        "Guessed Type",
        "Confidence",
        "Note",
    ]
    if list(ui_strings.PROFILE_GRID_COLUMNS) != expected_grid_columns:
        errors.append(
            "PROFILE_GRID_COLUMNS must remain: "
            + " | ".join(expected_grid_columns)
        )

    if ui_strings.PROFILE_SUGGEST_BUTTON_LABEL != "✨ Suggest DQ Config":
        errors.append(
            "PROFILE_SUGGEST_BUTTON_LABEL must stay '✨ Suggest DQ Config'."
        )

    if ui_keys.PROFILE_SELECTION_EDITOR != "profile_results_selection":
        errors.append(
            "PROFILE_SELECTION_EDITOR key must remain 'profile_results_selection'."
        )

    if errors:
        error_text = "\n".join(errors)
        raise GuardError(
            f"UI contract constants changed unexpectedly:\n{error_text}\nSee {CONTRACT_DOCS_URL}."
        )

    _check_tokens(
        PROFILE_VIEW_PATH,
        [
            "UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION",
            "st.data_editor",
            "st.dataframe",
            ui_keys.PROFILE_SELECTION_EDITOR,
        ],
        "Profile view",
    )

    _check_tokens(
        CONFIG_VIEW_PATH,
        ["render_row_count_preview", "UI contract violation"],
        "Config editor view",
    )

    _check_tokens(
        DOCS_VIEW_PATH,
        ["st.tabs", "Version History"],
        "Documentation view",
    )


def main() -> int:
    try:
        run_guard_checks()
    except GuardError as exc:
        print(f"guard_check failure: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
