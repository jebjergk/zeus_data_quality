"""Documentation view rendering helpers."""

from __future__ import annotations

from typing import Tuple

import streamlit as st

from utils.meta import _q


def _safe_quote(identifier: str) -> str:
    """Return a safely quoted identifier for display purposes."""
    try:
        return _q(identifier)
    except Exception:
        return identifier


def _sanitize_graph_label(label: str) -> Tuple[str, str]:
    """Sanitize a fully qualified name for use in future graph visuals."""
    node_name = "".join(ch if ch.isalnum() else "_" for ch in label)
    if not node_name:
        node_name = "node"
    display = _safe_quote(label).replace('"', '\\"')
    return node_name, display


def render_docs(
    metadata_db: str,
    metadata_schema: str,
    proc_name: str,
    configs_table: str,
    checks_table: str,
    run_results_table: str,
) -> None:
    """Render the documentation tabs for the Streamlit application."""
    st.header("📘 Zeus Data Quality – Documentation")

    tabs = st.tabs(
        [
            "User Guide",
            "Profiling",
            "DQ Framework",
            "Technical Overview",
            "Data Governance",
            "Version History",
        ]
    )

    cfg_tbl_display = _safe_quote(configs_table)
    chk_tbl_display = _safe_quote(checks_table)

    (
        cfg_tbl_node,
        cfg_tbl_label,
    ) = _sanitize_graph_label(configs_table)
    (
        chk_tbl_node,
        chk_tbl_label,
    ) = _sanitize_graph_label(checks_table)
    (
        run_tbl_node,
        run_tbl_label,
    ) = _sanitize_graph_label(run_results_table)
    (
        proc_node,
        proc_label,
    ) = _sanitize_graph_label(proc_name)

    profile_run_node, profile_run_label = _sanitize_graph_label(
        f"{metadata_db}.{metadata_schema}.DQ_PROFILE_RUN"
    )
    profile_col_node, profile_col_label = _sanitize_graph_label(
        f"{metadata_db}.{metadata_schema}.DQ_PROFILE_COLUMN"
    )

    with tabs[0]:
        st.subheader("User Guide")
        st.markdown(
            """
### What this app does
- **Purpose**: Keeps critical tables under watch so data issues are caught before they reach reporting.
- **Audience**: Data owners, analysts, and ops leads who need a quick health summary without writing SQL.

### Create a configuration
1. **Select a source** – choose the database, schema, and table you care about.
2. **Name the setup** – give the configuration a business-friendly name so others recognise it.
3. **Pick columns** – for each column decide which checks should guard it.
4. **Review table-level options** – confirm the timestamp used for freshness and volume tracking.
5. **Save & Apply** – the app stores the rules and schedules the daily run (08:00 Europe/Berlin by default).

### Checks in plain language
- **Uniqueness**: Flags duplicate values where every row should be distinct (for example, order IDs).
- **Null Count**: Watches how many blanks appear so missing information is caught quickly.
- **Minimum / Maximum**: Ensures numbers stay within an acceptable range, highlighting outliers.
- **Whitespace**: Spots accidental leading or trailing spaces that can break joins or filters.
- **Format Distribution**: Monitors standard patterns such as IBAN, ISIN, or email formats and alerts when the mix changes.
- **Value Distribution**: Tracks the share of categories (e.g., product types) and calls out unusual shifts.
- **Freshness**: Confirms new records keep arriving on time based on the chosen timestamp column.
- **Row Count Anomaly**: Detects sudden spikes or drops in total rows compared with recent history.
- **Aggregate (AGG) Checks**: Custom business rules that summarise data (for example, totals or ratios) to confirm aggregated results still look right.

### Run & monitor results
- **Run Now** triggers an immediate evaluation when you want to double-check a change.
- **Daily task** executes automatically using the saved schedule so you get continuous coverage.
- **Results** appear on the Monitor page where you can filter by table, check type, or status and download issue details.

### Troubleshooting basics
- **Warehouse**: Make sure the designated compute warehouse is running and has capacity.
- **Role**: Use the business role granted access to the monitored tables and metadata schema.
- **Procedure**: If runs fail, review the latest procedure message in the Monitor tab or rerun the stored procedure from Snowflake with the configuration name.
            """
        )

    with tabs[1]:
        st.subheader("Profiling")
        st.markdown(
            """
### Why profile first?
Profiling runs lightweight column statistics so you understand shape, completeness, and content **before** locking a data quality policy. It highlights high-risk fields, confirms business keys, and surfaces unexpected formats that deserve a rule.

### Metrics collected per column
- **Null % / Null count** – identify missing data hotspots.
- **Distinct % / Distinct count** – confirm uniqueness or spot categorical fields.
- **Min / Max** – verify ranges for numbers and timestamps.
- **Average length** – catch truncated strings or atypical ID lengths.
- **Whitespace %** – flags leading/trailing spaces that break joins.
- **Top values** – show the most common values (configurable Top N) to reveal dominant categories or odd outliers.

### Guessed content & confidence
- Each column receives semantic badges such as **IBAN**, **ISIN**, **EMAIL**, **ACCOUNT_ID**, **ORDER_ID**, **PRICE**, **CURRENCY**, **COUNTRY**, **TIMESTAMP**, **ENUM**, and more depending on detected patterns.
- Confidence badges are colour coded: **High** (green), **Medium** (amber), **Low** (grey), and **Unknown** (neutral) when the profiler has insufficient evidence.
- Hover the *Confidence Rationale* tooltip in the grid for a short explanation (e.g., "Regex match on 92% of rows" or "Length variance too high").

### Filters that guide DQ design
- Toggle filters for **High null %**, **Unique candidates**, **Low cardinality**, and **Whitespace risk** to find columns that deserve specific checks.
- Use semantic tag filters (Identifiers, Financial, Instrument, Geo, Contact) to focus on IBAN/ISIN/email style fields when planning format, uniqueness, or reference validations.
- Combine the insights to decide which fields need **uniqueness**, **null bounds**, **pattern** or **distribution** checks inside the Configurations editor.

### Performance & accuracy notes
- Sampling defaults to the recommended percentage (typically 10%) based on table size. Set the input to **0** for a full scan when accuracy matters more than speed.
- Distinct counts switch to Snowflake `APPROX_COUNT_DISTINCT` automatically when the profiler touches large row volumes, trading tiny error (<1%) for faster feedback.
- The summary banner reports rows profiled, sampling choice, and runtime so you can judge cost before rerunning.

### Persisting and using results
- Enable **💾 Save Profile** (once a Snowflake session and metadata targets are configured) to store the run in metadata for auditing or to compare over time.
- Click **✨ Suggest DQ Config** after a run to pre-fill the configuration editor with recommended column checks based on the discovered metrics, speeding up the creation of a new monitoring setup.
            """
        )

    with tabs[2]:
        st.subheader("Snowflake-native DQ Framework narrative")
        st.markdown(
            f"""
### Data Monitoring Framework (DMF) checks
* **Purpose**: Every row-level rule becomes a Data Monitoring Framework (DMF) check that Snowflake can execute in-database.
* **Failing-row views**: For each active check we create `DQ_<CONFIG_ID>_<CHECK_ID>_FAILS` inside `{_safe_quote(metadata_db)}.{_safe_quote(metadata_schema)}`.
  * View body: `SELECT * FROM <source> WHERE NOT (<rule_predicate>)` so investigators can explore bad rows without copying data.
  * **Safety**: Names are generated from UUID-style identifiers to avoid collisions, and views are created with `CREATE OR REPLACE` to prevent residual state.
* **Attach / detach lifecycle**:
  * On **Save & Apply**, DMF checks are created or refreshed and granted to the application role as needed.
  * On delete or when a config detaches from a table, the app drops only the unused views (skipping shared tables) to keep the metadata schema clean.

### Aggregate (AGG) table-level checks
* Freshness and Row Count Anomaly run as **aggregate SQL queries** directly against the source table.
* Because they summarise the whole table (no row payload), they do **not** materialise DMF views—results are stored only in `{run_results_table}`.
* Freshness compares the latest timestamp in the chosen column, while Row Count Anomaly uses robust statistics over recent daily totals to spot spikes or droughts.

### Stored procedures orchestrating runs
* `{metadata_db}.{metadata_schema}.DQ_RUN_CONFIG` is a Snowpark Python stored procedure.
  * Accepts a configuration ID, executes every check (DMF and AGG), captures failure counts, and records outcomes in `{run_results_table}`.
* `{metadata_db}.{metadata_schema}.SP_DQ_MANAGE_TASK` manages scheduling via **EXECUTE AS CALLER** so Snowflake authorisation stays with the business role.
  * Handles create/update for tasks, enforces warehouse selection, and flips enablement flags without leaving the platform.

### Tasks per configuration
* Each config is paired with a dedicated task: `DQ_TASK_<CONFIG_ID>` within `{metadata_db}.{metadata_schema}`.
* The task body runs `CALL {proc_name}('<CONFIG_ID>')` and inherits the caller’s warehouse (the app defaults to an internal DQ warehouse unless you override it).
* Scheduling uses Snowflake cron syntax with IANA time zones, so `0 8 * * * Europe/Berlin` means 08:00 local time every day.
* Enabling/disabling simply toggles the Snowflake task state—no need for external schedulers.

### Roles, context, and required grants
* Procedures execute **AS CALLER**, ensuring the running role’s data access policies are honoured.
* The application expects USAGE/MONITOR on the warehouse, USAGE on `{metadata_db}` and `{metadata_db}.{metadata_schema}`, CREATE TASK in the metadata schema, and EXECUTE on both stored procedures.
* When deployed as a Streamlit-in-Snowflake app, ownership of metadata objects stays with the application role so auditors can trace every change.

### Why Snowflake for data quality?
* **In-database compute** keeps checks close to the data—no egress, no shadow copies, just Snowflake warehouses doing the work.
* **Snowpark Python** powers the runner procedure, letting us blend Python orchestration with native SQL performance.
* **Streamlit in Snowflake** delivers the UI right where the data lives, eliminating context switching for data stewards.
* **INFORMATION_SCHEMA & Account Usage** supply rich metadata for monitoring configurations, tasks, and run history.
* **Governed sharing & roles** ensure DMF views, tasks, and procedures respect enterprise security while still being explorable when incidents occur.
            """
        )

        st.divider()
        st.subheader("Entity Diagram")
        entity_graph = f"""
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
        {profile_run_node} [label="{profile_run_label}\n(optional)", style="rounded,dashed,filled", fillcolor="#f8fafc", color="#d5dbed"];
        {profile_col_node} [label="{profile_col_label}\n(optional)", style="rounded,dashed,filled", fillcolor="#f8fafc", color="#d5dbed"];
        dmfv [label="DMF_FAIL views\n(per active check)", shape=folder, fillcolor="#fdf2f8", color="#fbcfe8", fontcolor="#831843"];
    }}

    app [label="Streamlit App", shape=rect, fillcolor="#ecfdf5", color="#bbf7d0", fontcolor="#047857"];
    {proc_node} [label="{proc_label}", shape=rect, fillcolor="#ede9fe", color="#c4b5fd", fontcolor="#5b21b6"];
    task [label="Snowflake Task\nper config", shape=rect, fillcolor="#fef3c7", color="#fcd34d", fontcolor="#92400e"];

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

        st.graphviz_chart(entity_graph, use_container_width=True)

        st.subheader("Workflow Diagram")
        workflow_graph = """
digraph W {
    graph [rankdir=LR, fontname="Helvetica", fontsize=11, bgcolor="white", pad=0.5, nodesep=0.9, ranksep=1.1, splines=ortho];
    node [shape=rect, style="rounded,filled", fontname="Helvetica", fontsize=11, width=2.2, height=0.8];
    edge [color="#6366f1", fontname="Helvetica", fontsize=10, arrowsize=0.85];

    step1 [label="User edits\nconfig", fillcolor="#eef2ff", color="#c7d2fe", fontcolor="#312e81"];
    step2 [label="Save & Apply", fillcolor="#e0f2fe", color="#bae6fd", fontcolor="#0c4a6e"];
    step3 [label="Attach DMF\nviews", fillcolor="#dcfce7", color="#bbf7d0", fontcolor="#065f46"];
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

        st.graphviz_chart(workflow_graph, use_container_width=True)

    with tabs[3]:
        st.subheader("Technical Overview")
        st.markdown(
            f"""
**Runtime**
- Streamlit (in Snowflake) using Snowpark Python.

**Metadata & Results**
- Configs: `{cfg_tbl_display}`
- Checks:  `{chk_tbl_display}`
- Results: `{run_results_table}`

**Procedures**
- Runner: `{metadata_db}.{metadata_schema}.{proc_name}(VARCHAR)` – evaluates checks and logs into results.
- Task Manager: `{metadata_db}.{metadata_schema}.SP_DQ_MANAGE_TASK(STRING, STRING, STRING, STRING, STRING, STRING, STRING, BOOLEAN)` – creates/updates task. **EXECUTE AS CALLER**.

**Tasks**
- One per config: `DQ_TASK_<CONFIG_ID>` in `{metadata_db}.{metadata_schema}`; body: `CALL {proc_name}('<CONFIG_ID>')`.

**Warehouses**
- Schedules run on default app WH (e.g., `DQ_WH`).
            """
        )

        st.markdown("**Required Privileges (caller role)**")
        st.code(
            f"""
USAGE ON WAREHOUSE DQ_WH
USAGE ON DATABASE {metadata_db}
USAGE ON SCHEMA {metadata_db}.{metadata_schema}
CREATE TASK ON SCHEMA {metadata_db}.{metadata_schema}
EXECUTE ON PROCEDURE {metadata_db}.{metadata_schema}.{proc_name}(VARCHAR)
            """,
            language="text",
        )

    with tabs[4]:
        st.subheader("Data Governance & Security")
        st.markdown(
            """
**Roles & Isolation**
- App runs with a specific **caller role** and uses **EXECUTE AS CALLER** for task management.
- Config/results live in a dedicated metadata schema to isolate privileges.

**Traceability**
- `DQ_RUN_RESULTS` logs: run timestamp, check id/type, failures, `OK` flag, and error messages if any.
- Tasks: one per config, auditable in ACCOUNT usage views.

**Access Patterns**
- Read-only access to source tables for checks.
- Controlled write access only to metadata objects (config/checks/results).
- DMF failing-row views live in metadata schema (no writes to source).

**PII / Sensitive Data**
- Prefer checks that don’t materialize sensitive columns in logs. Views expose only what investigators need.
- If required, add column masking on sensitive attributes in metadata views.
            """
        )

    with tabs[5]:
        st.subheader("Version History")
        st.info("Version history will be documented here once releases are tracked.")
