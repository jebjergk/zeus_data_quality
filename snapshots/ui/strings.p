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
PROFILE_V2_PLACEHOLDER_MESSAGE = "Profiling v2 is under construction. Check back soon."
PROFILE_V2_PICKER_SUBHEADER = "Select a table"
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
PROFILE_V2_SUMMARY_SUBHEADER = "Latest profile summary"
PROFILE_V2_SUMMARY_EMPTY = "No profiling summary available for this table yet."
PROFILE_V2_SUMMARY_ROWS = "Rows profiled"
PROFILE_V2_SUMMARY_SAMPLE = "Sample"
PROFILE_V2_SUMMARY_DURATION = "Duration"
PROFILE_V2_SUMMARY_TIMESTAMP = "Last profiled at {timestamp}"
PROFILE_V2_TAB_FEATURES = "Column features"
PROFILE_V2_TAB_CLASSIFICATION = "Semantic tags"
PROFILE_V2_TAB_SUGGESTIONS = "Suggested checks"
PROFILE_V2_TAB_RUNS = "Run history"
PROFILE_V2_FEATURES_SUBHEADER = "Column statistics"
PROFILE_V2_FEATURES_EMPTY = "No column metrics found. Run profiling to populate statistics."
PROFILE_V2_CLASSIFICATION_SUBHEADER = "Semantic classification"
PROFILE_V2_CLASSIFICATION_EMPTY = "No semantic classifications recorded for this table."
PROFILE_V2_SUGGESTIONS_SUBHEADER = "Recommended data quality checks"
PROFILE_V2_SUGGESTIONS_EMPTY = "No suggested checks available for the current profile."
PROFILE_V2_RUNS_SUBHEADER = "Recent profiling runs"
PROFILE_V2_RUNS_EMPTY = "No profiling runs have been logged yet."
PROFILE_V2_VALUE_UNKNOWN = "—"
PROFILE_V2_DEBUG_EXPANDER = "Debug · Profiling payload"

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
