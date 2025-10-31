"""UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Layout requirements:
1. Page header "Zeus Data Quality documentation" appears once at the top.
2. A fixed `st.tabs` call defines six tabs in this exact order: "User Guide", "Profiling", "DQ Framework", "Technical Overview", "Data Governance", "Version History".  Tab labels and count must not change.
3. Tab content expectations:
   • "User Guide" tab renders the subheader "User Guide" followed by the existing markdown sections (What the app delivers, Create a configuration, Checks in plain language, Run and monitor results, Troubleshooting essentials).  No interactive widgets belong in this tab.
   • "Profiling" tab renders the subheader "Profiling" and the markdown sections covering why to profile, metrics, semantic insights, filters, performance notes, and persistence guidance exactly as shipped.  This tab remains markdown-only.
   • "DQ Framework" tab renders the subheader "Snowflake-native data quality framework" with descriptive markdown, then a divider, then subheader "Entity Diagram" with a Graphviz diagram (rendered via `st.graphviz_chart`) describing metadata objects, followed by subheader "Workflow Diagram" with its Graphviz diagram.  Chart ordering and titles must remain unchanged.
   • "Technical Overview" tab renders the subheader "Technical Overview", markdown with runtime/metadata/procedure details, then a `st.markdown` heading "#### Required privileges for the caller role" and a single `st.code` block listing privilege statements.  No additional controls may be added.
   • "Data Governance" tab renders the subheader "Data Governance and Security" plus the markdown sections on roles, traceability, access, and sensitive data handling.
   • "Version History" tab renders the subheader "Version History" followed by the info message "Version history will be documented here once releases are tracked."  No other content is permitted.

Forbidden patterns:
• Do not change the tab order, labels, or count.
• Do not replace markdown content with inputs, tables, or dataframes.
• Do not remove or reposition the Graphviz charts within the "DQ Framework" tab.
• Do not add new alerts, buttons, or accordions to any tab without explicit approval.
"""

"""Documentation view rendering helpers."""

from __future__ import annotations

from typing import Tuple
import os

import streamlit as st

from utils.meta import _q

_CONTRACT_ENV_FLAG = "UI_CONTRACT_STRICT"


def _contract_message(message: str) -> None:
    """Display contract feedback as warning or error based on strict mode."""

    strict = os.getenv(_CONTRACT_ENV_FLAG, "0") == "1"
    if strict:
        st.error(message)
    else:
        st.warning(message)


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
    st.header("Zeus Data Quality documentation")

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

    if len(tabs) != 6:
        _contract_message(
            "UI contract violation in documentation view: expected 6 tabs."
        )
        return

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

    with tabs[1]:
        st.subheader("Profiling")
        st.markdown(
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

    with tabs[2]:
        st.subheader("Snowflake-native data quality framework")
        st.markdown(
            f"""
### Data Monitoring Framework (DMF) checks
- Each row-level rule becomes a DMF view that runs inside Snowflake.
- Failing rows surface in `DQ_<CONFIG_ID>_<CHECK_ID>_FAILS` within `{_safe_quote(metadata_db)}.{_safe_quote(metadata_schema)}`.
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

        st.markdown("#### Required privileges for the caller role")
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
        st.subheader("Data Governance and Security")
        st.markdown(
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

    with tabs[5]:
        st.subheader("Version History")
        st.info("Version history will be documented here once releases are tracked.")
