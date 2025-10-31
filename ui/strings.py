"""Centralised UI strings for Streamlit views."""

from __future__ import annotations

from typing import Final, Tuple


class ProfileStrings:
    HEADER: Final[str] = "🧪 Profile Table"
    CAPTION: Final[str] = (
        "Profile a table to explore null rates, distinct counts, ranges, and common values before defining data quality checks."
    )
    RUN_BUTTON: Final[str] = "▶️ Run Profile"
    SUGGEST_BUTTON: Final[str] = "✨ Suggest DQ Config"
    CLEAR_LOADED_BUTTON: Final[str] = "Clear loaded profile"
    SAVED_PROFILE_INFO: Final[str] = "Viewing a saved profile."
    SAVE_TOGGLE: Final[str] = "💾 Save Profile"
    NO_COLUMNS_MATCHED: Final[str] = "No columns matched the selected filters."
    TOP_VALUES_SUBHEADER: Final[str] = "Top values by column"
    NO_NON_NULL_VALUES: Final[str] = "No non-null values to display."
    NO_TOP_VALUES: Final[str] = "No top values available."
    SAMPLE_LABEL: Final[str] = "Sample %"
    TOP_N_LABEL: Final[str] = "Top N values"
    TOP_N_HELP_TEMPLATE: Final[str] = "Collect up to {max_top} of the most common values per column."
    LOAD_SAVED_LABEL: Final[str] = "Load saved profile"
    LOAD_SAVED_EMPTY: Final[str] = "— No saved profiles —"
    LOAD_SAVED_PLACEHOLDER: Final[str] = "— Select a saved run —"
    LOAD_BUTTON: Final[str] = "Load"
    LOAD_WARNING_CONNECTION: Final[str] = "Loading profiles requires a Snowflake connection and metadata configuration."
    LOAD_WARNING_SELECT_RUN: Final[str] = "Select a saved run to load."
    LOAD_ERROR: Final[str] = "Failed to load saved profile: {error}"
    LOAD_WARNING_NOT_FOUND: Final[str] = "Saved profile was not found or is empty."
    LOAD_SUCCESS: Final[str] = "Loaded saved profile {run_id}."
    LOAD_DISABLED_CAPTION: Final[str] = "Connect to Snowflake and select metadata targets to enable saving."
    LOAD_ENABLED_CAPTION: Final[str] = "Persist the current profile results to metadata tables."
    CLEAR_PROFILE_SUCCESS: Final[str] = "Saved profile run {run_id} to metadata."
    CLEAR_PROFILE_ERROR: Final[str] = "Failed to save profile: {error}"
    NO_SUGGESTIONS: Final[str] = "No suggestions available for the current profile."
    SAMPLE_REASON_METADATA: Final[str] = (
        "The suggested value relies on Snowflake metadata only, so it doesn't trigger an extra table scan."
    )
    SAMPLE_REASON_FULL_SCAN: Final[str] = "Enter 0 for a full table scan."
    SAMPLE_REASON_TABLE_ROWS: Final[str] = "Table metadata reports approximately {rows:,} rows."
    SAMPLE_REASON_APPROX_ROWS: Final[str] = "This sample size profiles about {rows:,} rows."
    SAMPLE_DEFAULT_REASON: Final[str] = "Defaulting to a 10% sample. Enter 0 for a full table scan."
    SAMPLE_HELP_METADATA: Final[str] = SAMPLE_REASON_METADATA
    SAMPLE_HELP_FULL_SCAN: Final[str] = SAMPLE_REASON_FULL_SCAN
    SAMPLE_CAPTION: Final[str] = "Suggested: {selected} of {total} columns selected"
    SAMPLE_SPINNER: Final[str] = "Profiling table..."
    PROFILE_ERROR: Final[str] = "Failed to profile table: {error}"
    PROFILE_WARNING_NO_SESSION: Final[str] = "No active Snowpark session — unable to profile tables."
    PROFILE_WARNING_SELECT_TABLE: Final[str] = "Select a database, schema, and table to profile."
    PROFILE_SUCCESS_SAVE: Final[str] = "Saved profile run {run_id} to metadata."
    PROFILE_WARNING_SELECT_COLUMNS: Final[str] = "Select at least one column before generating DQ suggestions."
    PROFILE_SUCCESS_SUGGESTION: Final[str] = "Loaded profile suggestion into the configuration editor."
    SAMPLE_WARNING_FULL_SCAN: Final[str] = (
        "Full table scan processed {rows:,} rows. Consider sampling to improve performance."
    )
    METRIC_ROWS_PROFILED: Final[str] = "Rows profiled"
    METRIC_SAMPLING: Final[str] = "Sampling"
    METRIC_DURATION: Final[str] = "Duration"
    METRIC_VIEWING_SAVED: Final[str] = SAVED_PROFILE_INFO
    SAMPLE_LABEL_FULL_SCAN: Final[str] = "Full scan"
    FILTERS_SUBHEADER: Final[str] = "Filters"
    FILTER_HIGH_NULL: Final[str] = "High null % (>20%)"
    FILTER_UNIQUE: Final[str] = "Unique candidates"
    FILTER_LOW_CARDINALITY: Final[str] = "Low cardinality"
    FILTER_WHITESPACE: Final[str] = "Whitespace risk"
    FILTER_SEMANTIC_LABEL: Final[str] = "Semantic tags"
    FILTER_SEMANTIC_OPTIONS: Final[Tuple[str, ...]] = (
        "Identifiers",
        "Financial",
        "Instrument",
        "Geo",
        "Contact",
        "Date (Text)",
        "Reference Codes",
    )
    SAMPLE_PCT_REASON_METADATA: Final[str] = SAMPLE_REASON_METADATA
    SAVE_DISABLED_MESSAGE: Final[str] = LOAD_DISABLED_CAPTION
    SAVE_ENABLED_MESSAGE: Final[str] = LOAD_ENABLED_CAPTION
    SAVED_PROFILE_INFO_BOX: Final[str] = SAVED_PROFILE_INFO
    GRID_COLUMN_SELECT: Final[str] = "Select"
    GRID_COLUMN_COLUMN: Final[str] = "Column"
    GRID_COLUMN_PHYSICAL_TYPE: Final[str] = "Physical Type"
    GRID_COLUMN_NULLS: Final[str] = "Nulls"
    GRID_COLUMN_DISTINCT: Final[str] = "Distinct"
    GRID_COLUMN_AVG_LENGTH: Final[str] = "Avg Length"
    GRID_COLUMN_MIN_VALUE: Final[str] = "Min Value"
    GRID_COLUMN_MAX_VALUE: Final[str] = "Max Value"
    GRID_COLUMN_WHITESPACE: Final[str] = "Whitespace %"
    GRID_COLUMN_GUESSED_TYPE: Final[str] = "Guessed Type"
    GRID_COLUMN_CONFIDENCE: Final[str] = "Confidence"
    GRID_COLUMN_NOTE: Final[str] = "Note"
    GRID_CHECKBOX_HELP: Final[str] = "Toggle to include the column in downstream DQ suggestions."
    GRID_EMPTY_INFO: Final[str] = "No columns matched the selected filters."
    LOAD_CAPTION_NO_SAVED: Final[str] = "No saved profiles found in metadata tables yet."
    LOAD_CAPTION_NEEDS_CONNECTION: Final[str] = (
        "Connect to Snowflake and configure metadata targets to enable loading saved profiles."
    )
    PROFILE_SUCCESS_SAVED_PROFILE: Final[str] = "Saved profile run {run_id} to metadata."
    PROFILE_SUMMARY_SAVED_PROFILE: Final[str] = "Viewing a saved profile."
    BUTTON_CLEAR_PROFILE: Final[str] = CLEAR_LOADED_BUTTON
    INFO_VIEWING_SAVED: Final[str] = SAVED_PROFILE_INFO
    INFO_NO_SAVED_PROFILE: Final[str] = "Saved profile was not found or is empty."
    SUCCESS_LOADED_PROFILE: Final[str] = "Loaded saved profile {run_id}."
    SUCCESS_PROFILE_SUGGESTION: Final[str] = PROFILE_SUCCESS_SUGGESTION
    SUCCESS_PROFILE_SAVE: Final[str] = PROFILE_SUCCESS_SAVE
    WARNING_SELECT_PROFILE: Final[str] = LOAD_WARNING_SELECT_RUN
    WARNING_NO_CONNECTION: Final[str] = LOAD_WARNING_CONNECTION
    WARNING_SELECT_COLUMNS: Final[str] = PROFILE_WARNING_SELECT_COLUMNS
    WARNING_NO_COLUMNS: Final[str] = NO_COLUMNS_MATCHED
    INFO_SAVED_PROFILE: Final[str] = SAVED_PROFILE_INFO
    INFO_NO_VALUES: Final[str] = NO_TOP_VALUES
    INFO_NO_NON_NULL: Final[str] = NO_NON_NULL_VALUES
    INFO_SAVED_PROFILE_VIEW: Final[str] = SAVED_PROFILE_INFO
    INFO_NO_COLUMNS: Final[str] = NO_COLUMNS_MATCHED
    TOP_VALUES_VALUE_HEADER: Final[str] = "Value"
    TOP_VALUES_COUNT_HEADER: Final[str] = "Count"
    INLINE_SELECT_DISABLED: Final[str] = (
        "UI contract violation: inline selection must remain enabled. Set PROFILE_INLINE_SELECT=1."
    )
    DEBUG_PROFILE_PAYLOAD: Final[str] = "Debug: profile payload"
    DEBUG_SELECTION_STATE: Final[str] = "Debug: include map"


class ConfigEditorStrings:
    CONTRACT_DATAFRAME_EXPECTED: Final[str] = (
        "UI contract violation in config preview: expected a pandas DataFrame."
    )
    CONTRACT_COLUMNS_MISMATCH: Final[str] = (
        "UI contract violation in config preview: expected columns {expected} but found {actual}."
    )


class DocsStrings:
    HEADER: Final[str] = "Zeus Data Quality documentation"
    TABS: Final[Tuple[str, ...]] = (
        "User Guide",
        "Profiling",
        "DQ Framework",
        "Technical Overview",
        "Data Governance",
        "Version History",
    )
    CONTRACT_TABS_MISMATCH: Final[str] = (
        "UI contract violation in documentation view: expected 6 tabs."
    )
    USER_GUIDE_SUBHEADER: Final[str] = "User Guide"
    PROFILING_SUBHEADER: Final[str] = "Profiling"
    FRAMEWORK_SUBHEADER: Final[str] = "Snowflake-native data quality framework"
    TECHNICAL_SUBHEADER: Final[str] = "Technical Overview"
    DATA_GOVERNANCE_SUBHEADER: Final[str] = "Data Governance and Security"
    VERSION_HISTORY_SUBHEADER: Final[str] = "Version History"
    VERSION_HISTORY_INFO: Final[str] = (
        "Version history will be documented here once releases are tracked."
    )
    USER_GUIDE_CONTENT: Final[str] = (
        """
#### What the app delivers
- **Purpose**: Monitor critical Snowflake tables so issues surface before reporting deadlines.
- **Audience**: Data owners, analysts, and operations leads who prefer guided workflows over ad-hoc SQL.

#### Create a configuration
1. **Select a source** – choose the database, schema, and table to protect.
2. **Name the setup** – use a business-friendly title so teams recognise the coverage.
3. **Pick columns** – decide which checks apply to each column based on risk.
4. **Review table options** – confirm the timestamp and warehouse before saving.
5. **Save and apply** – the configuration is stored and the daily 08:00 (Europe/Berlin) task is scheduled.

> **Tip:** Use **Save** to draft changes and **Save and apply** once the table is ready for monitoring.

#### Checks in plain language
- **Uniqueness** identifies duplicate business keys.
- **Null count** tracks missing information.
- **Minimum/maximum** keeps numeric ranges within agreed limits.
- **Whitespace** flags leading or trailing spaces that break joins.
- **Format distribution** watches identifier patterns such as IBAN, ISIN, or email.
- **Value distribution** monitors the mix of categories and highlights unusual shifts.
- **Freshness** confirms data arrives on time based on the chosen timestamp column.
- **Row count anomaly** spots sudden volume changes relative to recent history.
- **Aggregate checks** support custom totals or ratios when business validation requires them.

#### Run and monitor results
- **Run now** performs an immediate evaluation for spot checks.
- **Daily tasks** operate automatically once a schedule is enabled.
- **Monitor** surfaces outcomes with filters by table, check type, or status; results can be downloaded for follow-up.

#### Troubleshooting essentials
- Verify the Snowflake warehouse is running and sized for the workload.
- Confirm the active role has access to the source objects and metadata schema.
- Review stored procedure messages in the Monitor tab or execute the procedure manually for detailed logs.
"""
    )
    PROFILING_CONTENT: Final[str] = (
        """
#### Why profile first
Profiling runs lightweight column statistics so you understand data shape and completeness before finalising monitoring rules. The output points to high-risk fields, validates business keys, and surfaces unexpected formats.

#### Metrics collected per column
- **Null percentage and count** highlight missing data hotspots.
- **Distinct percentage and count** confirm uniqueness or signal categorical fields.
- **Minimum and maximum** verify numeric and timestamp ranges.
- **Average length** spots truncated strings or inconsistent identifiers.
- **Whitespace percentage** exposes formatting issues that can break joins.
- **Top values** display the most frequent categories or potential outliers.

#### Semantic insights and confidence
- Columns receive semantic suggestions such as IBAN, ISIN, email, account ID, country, or timestamp based on detected patterns.
- Confidence levels appear as High, Medium, Low, or Unknown with colour coding in the grid.
- Hover the **Confidence rationale** tooltip to see why a tag was chosen (for example, "Regex match on 92% of rows").

#### Filters that guide design
- Apply filters for high null percentage, unique candidates, low cardinality, or whitespace risk to shortlist columns.
- Use semantic tag filters (Identifiers, Financial, Instrument, Geography, Contact) to concentrate on sensitive fields.
- Combine these insights to select uniqueness, null, pattern, or distribution checks inside the configuration editor.

#### Performance and accuracy notes
- Sampling defaults to a recommended percentage based on table size; set the value to `0` when a full scan is required.
- Large tables automatically switch distinct counts to `APPROX_COUNT_DISTINCT`, balancing accuracy (within ~1%) and speed.
- The summary banner reports rows profiled, sampling choice, and runtime so you can judge cost before rerunning.

#### Persisting and reusing results
- Enable **Save profile** once metadata targets are configured to store runs for auditing or historical comparison.
- Use **Suggest DQ config** after profiling to pre-populate the configuration editor with recommended checks.
"""
    )
    FRAMEWORK_CONTENT: Final[str] = (
        """
### Data Monitoring Framework (DMF) checks
- Each row-level rule becomes a DMF view that runs inside Snowflake.
- Failing rows surface in `DQ_<CONFIG_ID>_<CHECK_ID>_FAILS` within `{metadata_db}`.`{metadata_schema}`.
- Views follow the pattern `SELECT * FROM <source> WHERE NOT (<rule_predicate>)`, using generated identifiers to avoid collisions.
- Save and apply creates or refreshes the views and grants access. Removing a config cleans up unused artefacts.

### Aggregate table-level checks
- Freshness and row-count anomaly checks execute as aggregate SQL directly against the source table.
- Because they summarise the entire table, they store only results in `{run_results_table}` and do not create DMF views.
- Freshness compares the latest timestamp in the monitored column; row-count anomaly looks at recent history for spikes or droughts.

### Stored procedures orchestrating runs
- `{metadata_db}.{metadata_schema}.DQ_RUN_CONFIG` (Snowpark Python) receives a configuration ID, evaluates every check, and records outcomes in `{run_results_table}`.
- `{metadata_db}.{metadata_schema}.SP_DQ_MANAGE_TASK` manages task lifecycle with `EXECUTE AS CALLER`, ensuring warehouse and privilege alignment.

### Tasks per configuration
- Each configuration owns a Snowflake task `DQ_TASK_<CONFIG_ID>` in `{metadata_db}.{metadata_schema}`.
- The task runs `CALL {proc_name}('<CONFIG_ID>')`, inheriting the caller's warehouse by default.
- Cron syntax with IANA time zones supports 08:00 Europe/Berlin schedules and any overrides defined by administrators.

### Roles, context, and required grants
- Procedures execute as caller so data access policies remain intact.
- The app expects USAGE/MONITOR on the warehouse, USAGE on `{metadata_db}` and `{metadata_db}.{metadata_schema}`, CREATE TASK in the metadata schema, and EXECUTE on both procedures.
- Streamlit in Snowflake keeps ownership with the application role, simplifying audits and change tracking.

### Why Snowflake for data quality
- In-database compute keeps checks near the data—no egress or shadow copies.
- Snowpark Python combines orchestration logic with native SQL execution.
- Streamlit in Snowflake provides the UI where teams already work.
- INFORMATION_SCHEMA and Account Usage power metadata-driven discovery and monitoring.
- Governed sharing and roles ensure DMF views, tasks, and procedures respect enterprise security boundaries.
"""
    )
    FRAMEWORK_ENTITY_GRAPH: Final[str] = (
        """
digraph G {{
    graph [rankdir=LR, fontname="Helvetica", fontsize=11, bgcolor="white", pad=0.4, nodesep=0.9, ranksep=1.1, splines=true];
    node [shape=rect, style="rounded,filled", fontname="Helvetica", fontsize=11, fillcolor="#f4f6fb", color="#d5dbed", penwidth=1.2];
    edge [color="#4f46e5", fontname="Helvetica", fontsize=10, arrowsize=0.8];

    subgraph cluster_metadata {{
        label="Metadata Schema";
        fontname="Helvetica";
        fontsize=11;
        color="#c7d2fe";
        style="rounded";
        {cfg_tbl_node} [label="{cfg_tbl_label}", fillcolor="#eef2ff", color="#c7d2fe"];
        {chk_tbl_node} [label="{chk_tbl_label}", fillcolor="#eef2ff", color="#c7d2fe"];
        {run_tbl_node} [label="{run_tbl_label}", fillcolor="#eef2ff", color="#c7d2fe"];
        {profile_run_node} [label="{profile_run_label}\\n(optional)", style="rounded,dashed,filled", fillcolor="#f8fafc", color="#d5dbed"];
        {profile_col_node} [label="{profile_col_label}\\n(optional)", style="rounded,dashed,filled", fillcolor="#f8fafc", color="#d5dbed"];
        dmfv [label="DMF_FAIL views\\n(per active check)", shape=folder, fillcolor="#fdf2f8", color="#fbcfe8", fontcolor="#831843"];
    }}

    app [label="Streamlit App", shape=rect, fillcolor="#ecfdf5", color="#bbf7d0", fontcolor="#047857"];
    {proc_node} [label="{proc_label}", shape=rect, fillcolor="#ede9fe", color="#c4b5fd", fontcolor="#5b21b6"];
    task [label="Snowflake Task\\nper config", shape=rect, fillcolor="#fef3c7", color="#fcd34d", fontcolor="#92400e"];

    app -> {cfg_tbl_node} [label="creates / edits"];
    app -> {chk_tbl_node} [label="creates / edits"];
    app -> dmfv [label="renders"];
    app -> {profile_run_node} [label="captures profiles"];
    app -> {profile_col_node} [label="renders"];
    {cfg_tbl_node} -> {chk_tbl_node} [label="defines"];
    {cfg_tbl_node} -> task [label="schedules"];
    task -> {proc_node} [label="calls"];
    {proc_node} -> {run_tbl_node} [label="logs to"];
    {proc_node} -> dmfv [label="populates"];
    {proc_node} -> {profile_run_node} [label="logs to", style=dashed, color="#6b7280", fontcolor="#6b7280"];
    {profile_run_node} -> {profile_col_node} [label="summarises"];
    {run_tbl_node} -> app [label="renders"];
    dmfv -> app [label="renders", style=dashed, color="#6b7280", fontcolor="#6b7280"];
}}
"""
    )
    FRAMEWORK_WORKFLOW_GRAPH: Final[str] = (
        """
digraph W {
    graph [rankdir=LR, fontname="Helvetica", fontsize=11, bgcolor="white", pad=0.5, nodesep=0.9, ranksep=1.1, splines=ortho];
    node [shape=rect, style="rounded,filled", fontname="Helvetica", fontsize=11, width=2.2, height=0.8];
    edge [color="#6366f1", fontname="Helvetica", fontsize=10, arrowsize=0.85];

    step1 [label="User edits\\nconfig", fillcolor="#eef2ff", color="#c7d2fe", fontcolor="#312e81"];
    step2 [label="Save & Apply", fillcolor="#e0f2fe", color="#bae6fd", fontcolor="#0c4a6e"];
    step3 [label="Attach DMF\\nviews", fillcolor="#dcfce7", color="#bbf7d0", fontcolor="#065f46"];
    step4 [label="Task schedule", fillcolor="#fef3c7", color="#fde68a", fontcolor="#92400e"];
    step5 [label="Daily run", fillcolor="#fee2e2", color="#fecaca", fontcolor="#991b1b"];
    step6 [label="Log results", fillcolor="#ede9fe", color="#ddd6fe", fontcolor="#5b21b6"];
    step7 [label="Monitor", fillcolor="#f5f3ff", color="#c4b5fd", fontcolor="#4c1d95"];

    step1 -> step2 [label="commit changes"];
    step2 -> step3 [label="provision checks"];
    step3 -> step4 [label="enable task"];
    step4 -> step5 [label="cron trigger"];
    step5 -> step6 [label="stored procedure"];
    step6 -> step7 [label="Surface in app"];
}
"""
    )
    TECHNICAL_CONTENT: Final[str] = (
        """
#### Runtime
- Streamlit in Snowflake using Snowpark Python.

#### Metadata and results
- Configs: `{cfg_tbl_display}`
- Checks: `{chk_tbl_display}`
- Results: `{run_results_table}`

#### Procedures
- Runner: `{metadata_db}.{metadata_schema}.{proc_name}(VARCHAR)` evaluates checks and logs to the results table.
- Task manager: `{metadata_db}.{metadata_schema}.SP_DQ_MANAGE_TASK(STRING, STRING, STRING, STRING, STRING, STRING, STRING, BOOLEAN)` creates or updates tasks and runs as caller.

#### Tasks
- One per configuration: `DQ_TASK_<CONFIG_ID>` in `{metadata_db}.{metadata_schema}` with body `CALL {proc_name}('<CONFIG_ID>')`.

#### Warehouses
- Schedules run on the default application warehouse (for example, `DQ_WH`).
"""
    )
    TECHNICAL_PRIVILEGES_HEADING: Final[str] = "#### Required privileges for the caller role"
    TECHNICAL_PRIVILEGES_SNIPPET: Final[str] = (
        """
USAGE ON WAREHOUSE DQ_WH
USAGE ON DATABASE {metadata_db}
USAGE ON SCHEMA {metadata_db}.{metadata_schema}
CREATE TASK ON SCHEMA {metadata_db}.{metadata_schema}
EXECUTE ON PROCEDURE {metadata_db}.{metadata_schema}.{proc_name}(VARCHAR)
"""
    )
    DATA_GOVERNANCE_CONTENT: Final[str] = (
        """
#### Roles and isolation
- The application runs with a defined caller role and relies on `EXECUTE AS CALLER` when managing tasks.
- Configuration and results tables live in a dedicated metadata schema to control privileges.

#### Traceability
- `DQ_RUN_RESULTS` captures timestamps, check identifiers, failure counts, success flags, and error messages.
- Each configuration owns a Snowflake task, which is auditable through ACCOUNT USAGE views.

#### Access patterns
- Source tables are read-only; writes are limited to metadata objects for configs, checks, and results.
- DMF failing-row views remain in the metadata schema so investigators avoid querying production tables directly.

#### Handling sensitive data
- Prefer checks that avoid materialising personal data in logs; views expose only what investigators need.
- Apply column masking to metadata views when sensitive attributes require additional protection.
"""
    )
