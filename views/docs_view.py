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
    node_name = label.replace('"', "")
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

    _, cfg_tbl_label = _sanitize_graph_label(configs_table)
    _, chk_tbl_label = _sanitize_graph_label(checks_table)
    _, run_tbl_label = _sanitize_graph_label(run_results_table)
    _, proc_label = _sanitize_graph_label(proc_name)

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
        st.info("Profiling documentation coming soon – this tab will cover profiling workflows and examples.")

    with tabs[2]:
        st.subheader("Snowflake Data Quality Framework (DMF) usage")
        st.markdown(
            """
**Failing-row Views**
- For row-level checks, the app creates views per check in the **metadata schema**:
  - `DQ_<CONFIG_ID>_<CHECK_ID>_FAILS`
- Each view is `SELECT * FROM <source_table> WHERE NOT (<predicate>)`.

**Attach/Detach**
- On **Save & Apply**: create/replace the needed FAIL views (skips AGG checks).
- On delete or when a table is no longer monitored: drop views if no other active config shares that table.

**Why DMF-style views?**
- Zero-copy investigation of bad records
- Stable, re-usable object per check
            """
        )

        st.divider()
        st.subheader("Anomaly Detection")
        st.markdown(
            """
**Current Implementation (Robust Z-Score)**
- Build daily counts from `timestamp_column` over `lookback_days` (default 28).
- Compute **median** and **MAD** over history (excluding today).
- Today is **OK** iff:
  1) `history_days >= min_history_days` (default 7), and
  2) `|today - median| / (1.4826 * MAD) <= sensitivity` (default 3.0).

**Why this approach?**
- Pure SQL, robust to outliers, no external model.

**Planned Cortex Path (optional)**
- Replace the MAD step with **Snowflake Cortex time-series anomaly** over (day, count).
- Parameters map roughly as:
  - `lookback_days` → training window
  - `sensitivity` → anomaly score threshold
  - `min_history_days` → gating before scoring

> Note: Your current `RULE_EXPR` for ROW_COUNT_ANOMALY is an `AGG:` SQL using the robust MAD method.
            """
        )

        st.divider()
        st.subheader("Entity Diagram")
        st.caption(
            "Entity relationship diagram placeholder – will visualize metadata objects such as "
            f"{cfg_tbl_label}, {chk_tbl_label}, {run_tbl_label}, and {proc_label}."
        )

        st.subheader("Workflow Diagram")
        st.caption(
            "Workflow diagram placeholder – will illustrate how configs feed checks, tasks, and the runner procedure."
        )

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
