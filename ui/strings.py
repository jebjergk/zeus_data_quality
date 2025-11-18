"""Centralised user-facing strings for Streamlit views."""

from __future__ import annotations

# Profiling v2 strings
PROFILE_V2_HEADER_TITLE = "🧪 Profiling"
PROFILE_V2_HEADER_CAPTION = (
    "Run deterministic metadata-driven profiling backed by ZEUS_ANALYTICS_SIMU.DISCOVERY."
)
PROFILE_V2_METADATA_NOTE = "Profiling metadata source: {namespace}."
PROFILE_V2_SESSION_WARNING = (
    "Connect to Snowflake to select a table and run profiling."
)
PROFILE_V2_TARGET_CAPTION = "Profiling target: {table}"
PROFILE_V2_NO_TARGET = (
    "Use the database, schema, and table selectors above to choose a target table."
)
PROFILE_V2_RUN_BUTTON = "▶️ Run profiling"
PROFILE_V2_REFRESH_BUTTON = "🔄 Refresh results"
PROFILE_V2_REFRESH_MESSAGE = "Reloaded profiling metadata."
PROFILE_V2_LOAD_SPINNER = "Loading profiling metadata..."
PROFILE_V2_RUN_SPINNER = "Running DQ_PROFILE_FULL for {table}..."
PROFILE_V2_RUN_ERROR = "Failed to profile table: {error}"
PROFILE_V2_RUN_SUCCESS = "Profiling completed for {table}."
PROFILE_V2_METADATA_ERROR = "Failed to load profiling metadata: {error}"
PROFILE_V2_STATUS_SUBHEADER = "Last profiling run"
PROFILE_V2_STATUS_EMPTY = "No profiling runs recorded for {table}. Run profiling to populate results."
PROFILE_V2_STATUS_MESSAGE = (
    "Last run **{status}** at {timestamp} (run ID {run_id}, duration {duration})."
)
PROFILE_V2_STATUS_DETAILS = "Details: {details}"
PROFILE_V2_COLUMNS_SUBHEADER = "Columns"
PROFILE_V2_COLUMNS_EMPTY = "Run profiling to load column-level metrics."
PROFILE_V2_SUGGESTIONS_SUBHEADER = "Suggested data quality checks"
PROFILE_V2_SUGGESTIONS_EMPTY = "Run profiling to see suggested checks for this table."
PROFILE_V2_VALUE_UNKNOWN = "—"

# Config preview strings
CONFIG_PREVIEW_CONTRACT_VIOLATION_FRAME = (
    "UI contract violation in config preview: expected a pandas DataFrame."
)
CONFIG_PREVIEW_CONTRACT_VIOLATION_COLUMNS = (
    "UI contract violation in config preview: expected columns {expected} but found {actual}."
)
CONFIG_PREVIEW_EXPECTED_COLUMNS = ("day", "cnt")
CONFIG_PREVIEW_HEIGHT = 320
CONFIG_PREVIEW_CONTRACT_NAME = "config preview"

# Documentation view strings
DOCS_HEADER = "Zeus Data Quality documentation"
DOCS_EXPECTED_TABS = [
    "User Guide",
    "Profiling",
    "DQ Framework",
    "Technical Overview",
    "Data Governance",
    "Version History",
]
DOCS_CONTRACT_TAB_COUNT = "UI contract violation in documentation view: expected 6 tabs."

# README governance section strings
README_GOVERNANCE_HEADING = "## Governance"
README_GOVERNANCE_LINES = [
    "- [CONTRIBUTING](CONTRIBUTING.md)",
    "- [ARCHITECTURE](ARCHITECTURE.md)",
    "- [UI Contracts](docs/ui_change_guidance.md)",
    "- [PR Template](.github/pull_request_template.md)",
    "- No unsolicited UI/UX refactors; gate experiments behind flags.",
    "- Snapshot updates require explicit intent; set `UPDATE_SNAPSHOTS=1` when regenerating.",
]
