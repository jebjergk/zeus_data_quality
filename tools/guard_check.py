"""Lightweight guard checks for UI contracts."""

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
    _check_tokens(
        PROFILE_VIEW_PATH,
        [
            "PROFILE_V2_HEADER_TITLE",
            "PROFILE_V2_HEADER_CAPTION",
            "PROFILE_V2_METADATA_NOTE",
            "PROFILE_V2_RUN_BUTTON",
            "PROFILE_V2_REFRESH_BUTTON",
            "PROFILE_V2_TAB_FEATURES",
            "PROFILE_V2_TAB_CLASSIFICATION",
            "PROFILE_V2_TAB_SUGGESTIONS",
            "PROFILE_V2_TAB_RUNS",
            "PROFILE_V2_DEBUG_EXPANDER",
        ],
        "Profile view",
    )

    _check_tokens(
        CONFIG_VIEW_PATH,
        [
            "render_row_count_preview",
            "CONFIG_PREVIEW_CONTRACT_VIOLATION_FRAME",
        ],
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
