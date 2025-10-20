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
**What you can do**
1. **Create/Edit Configs**: pick a table, choose columns, enable checks.
2. **Save & Apply**: attaches failing-row views (DMF) and creates a daily task (08:00 Europe/Berlin).
3. **Run Now**: ad-hoc evaluate all checks; results appear on **Monitor**.
4. **Monitor**: filter, trend, inspect failures and anomalies.

**Checks**
- **Column**: UNIQUE, NULL_COUNT, MIN_MAX, WHITESPACE, FORMAT_DISTRIBUTION, VALUE_DISTRIBUTION
- **Table** (always included): FRESHNESS, ROW_COUNT_ANOMALY

**Tips**
- Use a stable timestamp column (e.g., `LOAD_TIMESTAMP`) for table checks.
- Start with sensitivity=3.0; adjust if you see false positives.
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
