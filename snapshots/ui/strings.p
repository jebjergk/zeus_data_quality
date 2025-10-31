"""Centralised user-facing strings for Streamlit views."""

from __future__ import annotations

PROFILE_HEADER_TITLE = "🧪 Profile Table"
PROFILE_HEADER_CAPTION = (
    "Profile a table to explore null rates, distinct counts, ranges, and common values "
    "before defining data quality checks."
)
PROFILE_SELECTION_STATUS = "Suggested: {selected} of {total} columns selected"
PROFILE_RUN_BUTTON_LABEL = "▶️ Run Profile"
PROFILE_SUGGEST_BUTTON_LABEL = "✨ Suggest DQ Config"
PROFILE_CLEAR_LOADED_INFO = "Viewing a saved profile."
PROFILE_CLEAR_LOADED_BUTTON = "Clear loaded profile"
PROFILE_LOAD_BUTTON_LABEL = "Load"
PROFILE_LOAD_SELECT_LABEL = "Load saved profile"
PROFILE_LOAD_SELECT_PLACEHOLDER = "— Select a saved run —"
PROFILE_LOAD_SELECT_EMPTY = "— No saved profiles —"
PROFILE_LOAD_CAPTION_DISABLED = (
    "Connect to Snowflake and configure metadata targets to enable loading saved profiles."
)
PROFILE_LOAD_CAPTION_EMPTY = "No saved profiles found in metadata tables yet."
PROFILE_LOAD_WARNING_NO_SESSION = (
    "Loading profiles requires a Snowflake connection and metadata configuration."
)
PROFILE_LOAD_WARNING_NO_SELECTION = "Select a saved run to load."
PROFILE_RUN_ERROR_NO_SESSION = "No active Snowpark session — unable to profile tables."
PROFILE_RUN_WARNING_NO_TABLE = "Select a database, schema, and table to profile."
PROFILE_RUN_SPINNER = "Profiling table..."
PROFILE_RUN_ERROR_GENERIC = "Failed to profile table: {error}"
PROFILE_LOAD_ERROR_GENERIC = "Failed to load saved profile: {error}"
PROFILE_LOAD_WARNING_MISSING = "Saved profile was not found or is empty."
PROFILE_LOAD_SUCCESS = "Loaded saved profile {run_id}."
PROFILE_SAVE_LABEL = "💾 Save Profile"
PROFILE_SAVE_HELP_ENABLED = "Persist the current profile results to metadata tables."
PROFILE_SAVE_HELP_DISABLED = (
    "Connect to Snowflake and select metadata targets to enable saving."
)
PROFILE_SAVE_ERROR = "Failed to save profile: {error}"
PROFILE_SAVE_SUCCESS = "Saved profile run {run_id} to metadata."
PROFILE_SUGGEST_WARNING_EMPTY = "Select at least one column before generating DQ suggestions."
PROFILE_SUGGEST_SUCCESS = "Loaded profile suggestion into the configuration editor."
PROFILE_SUGGEST_EMPTY = "No suggestions available for the current profile."
PROFILE_SELECTION_EDITOR_HELP = (
    "Toggle to include the column in downstream DQ suggestions."
)
PROFILE_SELECTION_SECONDARY_SELECTOR = (
    "UI contract violation: secondary selector state detected ({keys}). Skipping selector."
)
PROFILE_INLINE_SELECT_DISABLED = (
    "UI contract violation: inline selection must remain enabled."
)
PROFILE_GRID_NAME = "profile results grid"
PROFILE_SELECTION_EDITOR_NAME = "profile selection editor"
PROFILE_GRID_COLUMNS = [
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
PROFILE_SELECTION_EDITOR_COLUMNS = ["Select", "Column"]
PROFILE_METRIC_ROWS = "Rows profiled"
PROFILE_METRIC_SAMPLING = "Sampling"
PROFILE_METRIC_DURATION = "Duration"
PROFILE_METRIC_DURATION_FORMAT = "{duration:.2f}s"
PROFILE_SAMPLE_FULL_SCAN_LABEL = "Full scan"
PROFILE_SAMPLE_LABEL = "Sample %"
PROFILE_SAMPLE_HELP_BASE = (
    "The suggested value relies on Snowflake metadata only, so it doesn't trigger an extra table scan."
)
PROFILE_SAMPLE_HELP_FULL_SCAN = "Enter 0 for a full table scan."
PROFILE_TOP_N_LABEL = "Top N values"
PROFILE_TOP_N_HELP = "Collect up to {max_top_n} of the most common values per column."
PROFILE_SAVED_PROFILE_CAPTION = "Target Table: {value}"
PROFILE_FULL_SCAN_WARNING = (
    "Full table scan processed {rows:,} rows. Consider sampling to improve performance."
)
PROFILE_FILTERS_SUBHEADER = "Filters"
PROFILE_FILTER_HIGH_NULL = "High null % (>20%)"
PROFILE_FILTER_UNIQUE = "Unique candidates"
PROFILE_FILTER_LOW_CARD = "Low cardinality"
PROFILE_FILTER_WHITESPACE = "Whitespace risk"
PROFILE_FILTER_SEMANTIC_LABEL = "Semantic tags"
PROFILE_FILTER_SEMANTIC_IDENTIFIERS = "Identifiers"
PROFILE_FILTER_SEMANTIC_FINANCIAL = "Financial"
PROFILE_FILTER_SEMANTIC_INSTRUMENT = "Instrument"
PROFILE_FILTER_SEMANTIC_GEO = "Geo"
PROFILE_FILTER_SEMANTIC_CONTACT = "Contact"
PROFILE_FILTER_SEMANTIC_DATE_TEXT = "Date (Text)"
PROFILE_FILTER_SEMANTIC_REFERENCE = "Reference Codes"
PROFILE_TOP_VALUES_SUBHEADER = "Top values by column"
PROFILE_TOP_VALUES_INFO_NO_NON_NULL = "No non-null values to display."
PROFILE_TOP_VALUES_INFO_EMPTY = "No top values available."
PROFILE_TOP_VALUES_EMPTY_NOTICE = "No columns matched the selected filters."
PROFILE_SAVED_PROFILE_INFO = "Version history will be documented here once releases are tracked."
PROFILE_WARNING_SAVED_MISSING = "Saved profile was not found or is empty."
PROFILE_WARNING_SECONDARY_SELECTOR = "UI contract violation: secondary selector state detected ({keys}). Skipping selector."
PROFILE_SELECTION_COUNTS_STATE = "profile_selection_counts"
PROFILE_EMPTY_SELECTION_NOTICE = "No columns matched the selected filters."
PROFILE_ERROR_LOAD_SAVED = "Failed to load saved profile: {error}"
PROFILE_WARNING_SAVED_EMPTY = "Saved profile was not found or is empty."
PROFILE_SUCCESS_LOAD_SAVED = "Loaded saved profile {run_id}."
PROFILE_INFO_SAVED_VIEWING = "Viewing a saved profile."
PROFILE_INFO_NO_FILTER_RESULTS = "No columns matched the selected filters."
PROFILE_INFO_NO_SELECTION = "No columns matched the selected filters."
PROFILE_INFO_NO_SUGGESTIONS = "No suggestions available for the current profile."
PROFILE_INFO_NO_VALUES = "No top values available."
PROFILE_INFO_NO_NON_NULL_VALUES = "No non-null values to display."
PROFILE_INFO_NO_COLUMNS_FILTER = "No columns matched the selected filters."
PROFILE_INFO_SAVE_DISABLED = "Connect to Snowflake and select metadata targets to enable saving."
PROFILE_INFO_SAVE_ENABLED = "Persist the current profile results to metadata tables."
PROFILE_CONFIDENCE_LEGEND_HTML = """
<div style="display:flex; gap:12px; align-items:center; font-size:0.85rem; margin:0.5rem 0;">
    <span style="display:flex; align-items:center; gap:4px;">
        <span style="width:12px; height:12px; border-radius:2px; background-color:#2e7d32; display:inline-block;"></span>
        <span>Green ≥90% (High)</span>
    </span>
    <span style="display:flex; align-items:center; gap:4px;">
        <span style="width:12px; height:12px; border-radius:2px; background-color:#f9a825; display:inline-block;"></span>
        <span>Amber 75–89% (Medium)</span>
    </span>
    <span style="display:flex; align-items:center; gap:4px;">
        <span style="width:12px; height:12px; border-radius:2px; background-color:#9e9e9e; display:inline-block;"></span>
        <span>Grey &lt;75% (Low/Unknown)</span>
    </span>
</div>
"""
PROFILE_DEMO_LOCK_MESSAGE = (
    "Demo safety mode active: experimental controls are hidden and UI contract issues are treated as errors."
)
PROFILE_SUGGEST_BUTTON_DISABLED_MESSAGE = "Select at least one column before generating DQ suggestions."
PROFILE_DEBUG_PAYLOAD_TITLE = "Debug: Profile payload"
PROFILE_DEBUG_INCLUDE_MAP_TITLE = "Debug: Include map"

CONFIG_PREVIEW_CONTRACT_VIOLATION_FRAME = (
    "UI contract violation in config preview: expected a pandas DataFrame."
)
CONFIG_PREVIEW_CONTRACT_VIOLATION_COLUMNS = (
    "UI contract violation in config preview: expected columns {expected} but found {actual}."
)
CONFIG_PREVIEW_EXPECTED_COLUMNS = ("day", "cnt")
CONFIG_PREVIEW_HEIGHT = 320
CONFIG_PREVIEW_CONTRACT_NAME = "config preview"

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

README_GOVERNANCE_HEADING = "## Governance"
README_GOVERNANCE_LINES = [
    "- [CONTRIBUTING](CONTRIBUTING.md)",
    "- [ARCHITECTURE](ARCHITECTURE.md)",
    "- [UI Contracts](docs/ui_change_guidance.md)",
    "- [PR Template](.github/pull_request_template.md)",
    "- No unsolicited UI/UX refactors; gate experiments behind flags.",
    "- Snapshot updates require explicit intent; set `UPDATE_SNAPSHOTS=1` when regenerating.",
]
