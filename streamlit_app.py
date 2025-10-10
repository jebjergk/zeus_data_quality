import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import uuid4

import altair as alt
import pandas as pd
import streamlit as st

# --- Snowpark session (works in Snowsight; safe locally) ---
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    session = None

# --- App imports (our modules) ---
from utils.meta import (
    DQConfig, DQCheck,
    list_configs, get_config, get_checks,
    list_databases, list_schemas, list_tables, list_columns,
    fq_table, fetch_run_results, fetch_timeseries_daily, fetch_config_map,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.state import get_state, set_state
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

# ---------- Styling (simple Snowflake-ish) ----------
st.markdown("""
<style>
.sf-hr { height:1px; background:#e7ebf3; border:0; margin=.6rem 0 1rem 0; }
.badge { display:inline-block; padding=.15rem .55rem; border-radius:999px; font-size=.75rem; font-weight:600;
 background:#e5f6fd; color:#055e86; border:1px solid #cbeefb; }
.badge-green { background:#eafaf0; border-color:#d4f2df; color:#0a5c2b; }
.card { border:1px solid #e7ebf3; border-radius:12px; padding=.9rem 1rem; background:#fff; box-shadow:0 1px 2px rgba(12,18,28,.04); }
.small { font-size:.85rem; color:#6b7280; }
.kv { color:#111827; font-weight:600; }
section[data-testid="stSidebar"] .stButton>button {
 width:100%;
 border-radius:10px;
}
section[data-testid="stSidebar"] .stButton {
 margin-bottom:.4rem;
}
</style>
""", unsafe_allow_html=True)

# ---------- Helpers ----------
def _keyify(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s).lower()


def _sanitize_id(s: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "_", (s or "").upper())


def navigate_to(page: str) -> None:
    """Update the current page selection in session state."""
    st.session_state["page"] = page
    if page == "home":
        st.session_state["cfg_mode"] = "list"


def _quote_ident(value: str) -> str:
    """Return a Snowflake-quoted identifier."""
    safe = value.replace('"', '""')
    return f'"{safe}"'


def _task_fqn_for_config(cfg: DQConfig) -> str:
    """Build the fully qualified task name for the given configuration."""
    target_fqn = (cfg.target_table_fqn or "").strip()
    if not target_fqn:
        raise ValueError("Target table FQN missing database or schema")

    parts = [p.strip().strip('"') for p in target_fqn.split(".") if p.strip()]
    if len(parts) < 2:
        raise ValueError("Target table FQN missing database or schema")

    db_name, schema_name = parts[0], parts[1]
    cfg_id = str(cfg.config_id).replace('"', '')
    task_identifier = f"DQ_TASK_{cfg_id}"
    return ".".join([
        _quote_ident(db_name),
        _quote_ident(schema_name),
        _quote_ident(task_identifier),
    ])

def stateless_table_picker(preselect_fqn: Optional[str]):
    """Simple, stateless DB → Schema → Table picker. Returns (db, schema, table, fqn)."""
    def split_fqn(fqn):
        if not fqn or fqn.count(".") != 2: return None, None, None
        a,b,c = [p.strip('"') for p in fqn.split(".")]
        return a,b,c
    pre_db, pre_sch, pre_tbl = split_fqn(preselect_fqn or "")

    dbs = list_databases(session)
    db_index = next((i for i,v in enumerate(dbs) if pre_db and v.upper()==pre_db.upper()), 0) if dbs else 0
    db_sel = st.selectbox("Database", dbs or ["— none —"], index=db_index if dbs else 0)
    if not dbs or db_sel == "— none —": return None, None, None, ""

    schemas = list_schemas(session, db_sel)
    sch_index = next((i for i,v in enumerate(schemas) if pre_sch and v.upper()==pre_sch.upper()), 0) if schemas else 0
    sch_sel = st.selectbox("Schema", schemas or ["— none —"], index=sch_index if schemas else 0)
    if not schemas or sch_sel == "— none —": return db_sel, None, None, ""

    tables = list_tables(session, db_sel, sch_sel)
    tbl_index = next((i for i,v in enumerate(tables) if pre_tbl and v.upper()==pre_tbl.upper()), 0) if tables else 0
    tbl_sel = st.selectbox("Table", tables or ["— none —"], index=tbl_index if tables else 0)
    if not tables or tbl_sel == "— none —": return db_sel, sch_sel, None, ""

    return db_sel, sch_sel, tbl_sel, fq_table(db_sel, sch_sel, tbl_sel)

def render_home():
    st.title("Zeus Data Quality")
    st.markdown("""
Zeus DQ lets you define, apply, and schedule **data quality checks** directly in Snowflake.

- Column checks: **UNIQUE**, **NULL_COUNT**, **MIN_MAX**, **WHITESPACE**, **FORMAT_DISTRIBUTION**, **VALUE_DISTRIBUTION**
- Table checks (always included): **FRESHNESS**, **ROW_COUNT**

**Attach** creates per-check *views of failing rows*; **Run Now** executes via the Snowflake task. Results are written to DQ_RUN_RESULTS. Open 📊 Monitor to review outcomes. A daily **Task** runs at 08:00 Europe/Berlin.
""")
    st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

    st.subheader("How Zeus DQ helps")
    st.markdown(
        """
✅ **Monitor critical tables** – pick Snowflake objects with the sidebar picker and keep an eye on freshness and row counts.

✅ **Protect key columns** – add column checks that ensure values stay unique, fall within ranges, and follow expected patterns.

✅ **Investigate failures fast** – attach views for failing rows, run checks on demand, and review samples without leaving Snowsight.
"""
    )

    st.subheader("Getting started")
    st.markdown(
        """
1. Use the **Configurations** section to create or edit a data quality config.
2. Select the target table, choose the columns you care about, and enable the checks you need.
3. Save as a draft or **Save & Apply** to create monitoring views and schedule the daily task.
"""
    )

    st.info(
        "Need a refresher? Switch to the Configurations page with the sidebar, or edit an existing setup to reuse its defaults."
    )

def render_config_list():
    st.header("Configurations")
    notices = st.session_state.pop("last_notices", None)
    if notices:
        for note in notices:
            kind = note.get("type", "info")
            message = note.get("message", "")
            if not message:
                continue
            if kind == "success":
                st.success(message)
            elif kind == "warning":
                st.warning(message)
            elif kind == "error":
                st.error(message)
            else:
                st.info(message)
    # Create button on right
    _, monitor_col, create_col = st.columns([6, 2, 2])
    with monitor_col:
        if st.button("📊 Monitor", key="config_list_monitor_link"):
            st.session_state["page"] = "monitor"
            st.rerun()
    with create_col:
        if st.button("➕ Create"):
            st.session_state["cfg_mode"] = "edit"
            st.session_state["selected_config_id"] = None
            st.session_state["editor_target_fqn"] = None
            st.rerun()

    search_query = st.text_input(
        "Search configurations",
        key="config_list_search",
        placeholder="Search by name, table, status, role, or ID",
        label_visibility="collapsed",
    )

    cfgs = list_configs(session)
    if not cfgs:
        st.info("No configurations yet. Click **Create** to add one.")
        return

    if search_query:
        q = search_query.lower()
        cfgs = [
            cfg
            for cfg in cfgs
            if q in (cfg.name or "").lower()
            or q in (cfg.target_table_fqn or "").lower()
            or q in (cfg.status or "").lower()
            or q in (cfg.run_as_role or "").lower()
            or q in (cfg.config_id or "").lower()
        ]

        if not cfgs:
            st.info("No configurations match your search.")
            return

    for i, cfg in enumerate(cfgs):
        active = (cfg.status or "").upper() == "ACTIVE"
        badge = f"<span class='badge {'badge-green' if active else ''}'>{cfg.status or '—'}</span>"
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns([5, 3, 2, 2])
        with c1:
            st.markdown(f"**{cfg.name}** {badge}", unsafe_allow_html=True)
            st.markdown(f"<div class='small'>ID: <span class='kv'>{cfg.config_id}</span></div>", unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='small'>Table:<br><span class='kv'>{cfg.target_table_fqn}</span></div>", unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='small'>RUN_AS_ROLE:<br><span class='kv'>{cfg.run_as_role or '—'}</span></div>", unsafe_allow_html=True)
        with c4:
            a1, a2 = st.columns(2)
            with a1:
                if st.button("✏️ Edit", key=f"edit_{cfg.config_id}"):
                    st.session_state["cfg_mode"] = "edit"
                    st.session_state["selected_config_id"] = cfg.config_id
                    st.session_state["editor_target_fqn"] = cfg.target_table_fqn
                    st.rerun()
            with a2:
                if st.button("🗑️ Delete", key=f"del_{cfg.config_id}"):
                    out = delete_config_full(session, cfg.config_id)
                    msg = f"Deleted `{cfg.name}` — dropped {len(out.get('dmfs_dropped', []))} view(s)."
                    st.success(msg)
                    st.session_state["last_notices"] = [{"type": "success", "message": msg}]
                    st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        if i < len(cfgs)-1:
            st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

def render_config_editor():
    # which config?
    sel_id: Optional[str] = st.session_state.get("selected_config_id")
    cfg = get_config(session, sel_id) if sel_id else None
    existing_checks = get_checks(session, sel_id) if sel_id else []

    # Header
    back, title = st.columns([1, 8])
    with back:
        if st.button("⬅ Back"):
            st.session_state["cfg_mode"] = "list"
            st.rerun()
    with title:
        st.header("Edit Configuration" if cfg else "Create Configuration")

    # Target (picker is stateless, we persist a single FQN)
    st.subheader("Target")
    base_fqn = st.session_state.get("editor_target_fqn") or (cfg.target_table_fqn if cfg else None)
    db_sel, sch_sel, tbl_sel, target_table = stateless_table_picker(base_fqn)
    if target_table:
        st.session_state["editor_target_fqn"] = target_table
    st.caption(f"Target Table: {target_table or '— not selected —'}")

    # Columns (outside form). Sanitize defaults to avoid widget errors.
    available_cols = list_columns(session, db_sel, sch_sel, tbl_sel) if (db_sel and sch_sel and tbl_sel) else []
    st.markdown("### Columns")
    if existing_checks and "dq_cols_ms" not in st.session_state:
        raw_default = sorted({c.column_name for c in existing_checks if c.column_name})
    else:
        raw_default = st.session_state.get("dq_cols_ms", [])
    safe_default = [c for c in (raw_default or []) if isinstance(c, str) and c in (available_cols or [])]
    if not available_cols:
        safe_default = []
    st.multiselect("Columns to check", options=available_cols, default=safe_default, key="dq_cols_ms")
    st.info("Table-level checks **FRESHNESS** and **ROW_COUNT_ANOMALY** are automatically included.")

    # -------- Form --------
    # Pre-populate table-level defaults from existing checks / state
    existing_table_params = {}
    legacy_row_count_params: Dict[str, object] = {}
    for ec in existing_checks:
        if not ec.column_name and ec.params_json:
            key = (ec.check_type or "").upper()
            try:
                parsed_params = json.loads(ec.params_json)
            except Exception:
                parsed_params = {}

            if key == "ROW_COUNT":
                legacy_row_count_params = parsed_params or {}
                continue

            existing_table_params[key] = parsed_params

    freshness_defaults = existing_table_params.get("FRESHNESS", {})
    ts_default = (
        freshness_defaults.get("timestamp_column")
        or legacy_row_count_params.get("timestamp_column")
        or "LOAD_TIMESTAMP"
    )

    if "ROW_COUNT_ANOMALY" not in existing_table_params:
        existing_table_params["ROW_COUNT_ANOMALY"] = {
            "timestamp_column": ts_default,
            "lookback_days": 28,
            "sensitivity": 3.0,
            "min_history_days": 7,
        }

    if target_table:
        last_target = st.session_state.get("_dq_table_ts_target")
        if last_target != target_table:
            st.session_state["_dq_table_ts_col"] = ts_default
            st.session_state["_dq_table_ts_target"] = target_table
    if "_dq_table_ts_col" not in st.session_state:
        st.session_state["_dq_table_ts_col"] = ts_default

    preview_counts = False

    with st.form("cfg_form", clear_on_submit=False):
        st.subheader("Configuration")
        name = st.text_input("Name", value=(cfg.name if cfg else ""))
        desc = st.text_area("Description", value=(cfg.description if cfg else ""))

        check_rows: List[DQCheck] = []
        selected_cols = st.session_state.get("dq_cols_ms", [])

        # Restore per-column settings from existing checks
        existing_by_coltype = {}
        for ec in existing_checks:
            key = (ec.column_name or "", (ec.check_type or "").upper())
            try:
                params = json.loads(ec.params_json) if ec.params_json else {}
            except Exception:
                params = {}
            existing_by_coltype[key] = {"severity": ec.severity, "params": params}

        for col in selected_cols:
            sk = _keyify(col)
            with st.expander(f"Column: {col}", expanded=False):
                sample_n = st.number_input(
                    f"Sample failing rows for {col}",
                    min_value=0, max_value=1000, value=10, key=f"samp_{sk}"
                )

                # UNIQUE
                ex = existing_by_coltype.get((col, "UNIQUE"), {})
                checked = ("UNIQUE" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_unique = st.checkbox("UNIQUE", value=checked, key=f"{sk}_chk_unique")
                if c_unique and target_table:
                    p_ignore_nulls = st.checkbox(
                        "Ignore NULLs",
                        value=ex.get("params", {}).get("ignore_nulls", True),
                        key=f"{sk}_p_un_ignore"
                    )
                    severity_options = ["ERROR", "WARN"]
                    existing_severity = ex.get("severity", "ERROR")
                    severity_index = (
                        severity_options.index(existing_severity)
                        if existing_severity in severity_options
                        else 0
                    )
                    sev = st.selectbox(
                        "Severity (UNIQUE)",
                        severity_options,
                        index=severity_index,
                        key=f"{sk}_sev_unique"
                    )
                    params = {"ignore_nulls": p_ignore_nulls}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "UNIQUE", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_UNIQUE",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="UNIQUE",
                        params_json=json.dumps(params)
                    ))

                # NULL_COUNT
                ex = existing_by_coltype.get((col, "NULL_COUNT"), {})
                checked = ("NULL_COUNT" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_null = st.checkbox("NULL_COUNT", value=checked, key=f"{sk}_chk_nullcount")
                if c_null and target_table:
                    max_nulls = st.number_input(
                        "Max NULL rows",
                        min_value=0,
                        value=int(ex.get("params", {}).get("max_nulls", 0)),
                        key=f"{sk}_p_nc_max",
                    )
                    sev = st.selectbox(
                        "Severity (NULL_COUNT)",
                        ["ERROR", "WARN"],
                        index=(0 if ex.get("severity", "ERROR") == "ERROR" else 1),
                        key=f"{sk}_sev_null",
                    )
                    params = {"max_nulls": int(max_nulls)}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "NULL_COUNT", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_NULL_COUNT",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="NULL_COUNT",
                        params_json=json.dumps(params)
                    ))

                # MIN_MAX
                ex = existing_by_coltype.get((col, "MIN_MAX"), {})
                checked = ("MIN_MAX" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_minmax = st.checkbox("MIN_MAX", value=checked, key=f"{sk}_chk_minmax")
                if c_minmax and target_table:
                    min_v = st.text_input(
                        "Min (inclusive)",
                        value=str(ex.get("params", {}).get("min", "")),
                        key=f"{sk}_p_mm_min",
                    )
                    max_v = st.text_input(
                        "Max (inclusive)",
                        value=str(ex.get("params", {}).get("max", "")),
                        key=f"{sk}_p_mm_max",
                    )
                    sev = st.selectbox(
                        "Severity (MIN_MAX)",
                        ["ERROR", "WARN"],
                        index=(0 if ex.get("severity", "ERROR") == "ERROR" else 1),
                        key=f"{sk}_sev_mm",
                    )
                    params = {"min": min_v, "max": max_v}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "MIN_MAX", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_MIN_MAX",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="MIN_MAX",
                        params_json=json.dumps(params)
                    ))

                # WHITESPACE
                ex = existing_by_coltype.get((col, "WHITESPACE"), {})
                checked = ("WHITESPACE" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_ws = st.checkbox("WHITESPACE", value=checked, key=f"{sk}_chk_ws")
                if c_ws and target_table:
                    options = ["NO_LEADING_TRAILING","NO_INTERNAL_ONLY_WHITESPACE","NON_EMPTY_TRIMMED"]
                    mode = st.selectbox("Mode", options, index=options.index(ex.get("params", {}).get("mode", options[0])), key=f"{sk}_p_ws_mode")
                    sev = st.selectbox("Severity (WHITESPACE)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_ws")
                    params = {"mode": mode}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "WHITESPACE", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_WHITESPACE",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="WHITESPACE",
                        params_json=json.dumps(params)
                    ))

                # FORMAT_DISTRIBUTION
                ex = existing_by_coltype.get((col, "FORMAT_DISTRIBUTION"), {})
                checked = ("FORMAT_DISTRIBUTION" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_fmt = st.checkbox("FORMAT_DISTRIBUTION", value=checked, key=f"{sk}_chk_fmt")
                if c_fmt and target_table:
                    regex = st.text_input("Regex (Snowflake RLIKE)", value=str(ex.get("params", {}).get("regex","")), key=f"{sk}_p_fmt_regex")
                    ratio = st.number_input("Min match ratio (0-1)", min_value=0.0, max_value=1.0, value=float(ex.get("params", {}).get("min_match_ratio",1.0)), step=0.01, key=f"{sk}_p_fmt_ratio")
                    sev = st.selectbox("Severity (FORMAT_DISTRIBUTION)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_fmt")
                    params = {"regex": regex, "min_match_ratio": float(ratio)}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "FORMAT_DISTRIBUTION", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_FORMAT_DIST",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="FORMAT_DISTRIBUTION",
                        params_json=json.dumps(params)
                    ))

                # VALUE_DISTRIBUTION
                ex = existing_by_coltype.get((col, "VALUE_DISTRIBUTION"), {})
                checked = ("VALUE_DISTRIBUTION" in [(ec.check_type or "").upper() for ec in existing_checks if ec.column_name == col])
                c_val = st.checkbox("VALUE_DISTRIBUTION", value=checked, key=f"{sk}_chk_val")
                if c_val and target_table:
                    allowed_csv = st.text_input("Allowed values (CSV)", value=str(ex.get("params", {}).get("allowed_values_csv","")), key=f"{sk}_p_val_csv")
                    ratio = st.number_input("Min in-set ratio (0-1)", min_value=0.0, max_value=1.0, value=float(ex.get("params", {}).get("min_match_ratio",1.0)), step=0.01, key=f"{sk}_p_val_ratio")
                    sev = st.selectbox("Severity (VALUE_DISTRIBUTION)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_val")
                    params = {"allowed_values_csv": allowed_csv, "min_match_ratio": float(ratio)}
                    rule, is_agg = build_rule_for_column_check(target_table, col, "VALUE_DISTRIBUTION", params)
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id=f"{col}_VALUE_DIST",
                        table_fqn=target_table,
                        column_name=col,
                        rule_expr=(f"AGG: {rule}" if is_agg else rule),
                        severity=sev,
                        sample_rows=(0 if is_agg else int(sample_n)),
                        check_type="VALUE_DISTRIBUTION",
                        params_json=json.dumps(params)
                    ))

        # Table-level (always)
        st.markdown("### Table-level checks (always included)")
        ts_col = st.text_input(
            "Timestamp column for table checks",
            key="_dq_table_ts_col",
            value=st.session_state.get("_dq_table_ts_col", ts_default),
        )
        st.caption("Table will FAIL if no data in 30h or if today's volume is a statistical outlier.")

        preview_counts = st.form_submit_button(
            "Preview last 60 days row counts",
            type="secondary",
            help="Preview daily row counts using the selected timestamp column.",
        )

        if target_table:
            fr_params = {"timestamp_column": ts_col, "max_age_minutes": 1800}
            fr_rule = build_rule_for_table_check(target_table, "FRESHNESS", fr_params)
            check_rows.append(DQCheck(
                config_id=(cfg.config_id if cfg else "temp"),
                check_id="TABLE_FRESHNESS",
                table_fqn=target_table, column_name=None,
                rule_expr=f"AGG: {fr_rule}", severity="ERROR",
                sample_rows=0, check_type="FRESHNESS",
                params_json=json.dumps(fr_params)
            ))

            anomaly_params = {
                "timestamp_column": ts_col,
                "lookback_days": 28,
                "sensitivity": 3.0,
                "min_history_days": 7,
            }
            anomaly_rule = build_rule_for_table_check(target_table, "ROW_COUNT_ANOMALY", anomaly_params)
            check_rows.append(DQCheck(
                config_id=(cfg.config_id if cfg else "temp"),
                check_id="TABLE_ROW_COUNT_ANOMALY",
                table_fqn=target_table, column_name=None,
                rule_expr=f"AGG: {anomaly_rule}", severity="ERROR",
                sample_rows=0, check_type="ROW_COUNT_ANOMALY",
                params_json=json.dumps(anomaly_params)
            ))

        st.markdown("### Schedule")
        existing_cron = getattr(cfg, "schedule_cron", None) if cfg else None
        existing_timezone = getattr(cfg, "schedule_timezone", None) if cfg else None
        existing_enabled = getattr(cfg, "schedule_enabled", True) if cfg else True
        default_cron = existing_cron or "0 8 * * *"
        default_timezone = existing_timezone or "Europe/Berlin"
        schedule_enabled = st.checkbox("Enable daily task", value=bool(existing_enabled))
        cron_expr = st.text_input(
            "Cron expression",
            value=default_cron,
            help="Snowflake `USING CRON` expression (e.g. `0 8 * * *`).",
            disabled=not schedule_enabled
        )
        timezone_expr = st.text_input(
            "Timezone",
            value=default_timezone,
            help="IANA timezone name (e.g. `Europe/Berlin`).",
            disabled=not schedule_enabled
        )

        c1, c2, c3, c4 = st.columns(4)
        with c1: apply_now = st.form_submit_button("Save & Apply")
        with c2: save_draft = st.form_submit_button("Save as Draft")
        with c3: run_now_btn = st.form_submit_button("Run Now")
        with c4: delete_btn = st.form_submit_button("Delete", type="secondary")

    safe_ts = None
    if preview_counts:
        if not session:
            st.warning("No active Snowpark session — unable to preview row counts.")
        elif not target_table:
            st.warning("Select a target table to preview row counts.")
        elif not (ts_col and ts_col.strip()):
            st.warning("Enter a timestamp column to preview row counts.")
        else:
            safe_ts = "".join(ch for ch in ts_col.strip().replace('"', '') if ch.isalnum() or ch in ("_", "$"))
            if not safe_ts:
                st.warning("Timestamp column contains unsupported characters — unable to preview row counts.")
    if preview_counts and safe_ts:
        query = f"""
            WITH days AS (
                SELECT DATEADD(day, -seq4(), CURRENT_DATE()) AS day
                FROM TABLE(GENERATOR(ROWCOUNT => 60))
            ),
            counts AS (
                SELECT DATE_TRUNC('day', "{safe_ts}") AS day, COUNT(*) AS cnt
                FROM {target_table}
                WHERE "{safe_ts}" >= DATEADD(day, -59, CURRENT_DATE())
                GROUP BY 1
            )
            SELECT d.day AS "day", COALESCE(c.cnt, 0) AS "cnt"
            FROM days d
            LEFT JOIN counts c ON c.day = d.day
            ORDER BY d.day
        """
        try:
            df = session.sql(query).to_pandas()
        except Exception as exc:
            st.error(f"Failed to preview row counts: {exc}")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True, height=320)

    # After submit
    if apply_now or save_draft or run_now_btn or delete_btn:
        if not session:
            st.error("No active Snowpark session.")
            return
        if delete_btn and cfg:
            out = delete_config_full(session, cfg.config_id)
            msg = f"Deleted config {cfg.config_id}. Dropped: {len(out.get('dmfs_dropped', []))} view(s)."
            st.success(msg)
            st.session_state["last_notices"] = [{"type": "success", "message": msg}]
            st.session_state["cfg_mode"] = "list"; st.rerun(); return

        post_submit_notices: List[Dict[str, str]] = []

        def remember(kind: str, message: str) -> None:
            if message:
                post_submit_notices.append({"type": kind, "message": message})

        new_id = cfg.config_id if cfg else str(uuid4())
        status = 'ACTIVE' if apply_now else 'DRAFT'
        state = get_state()
        dq_cfg = DQConfig(
            config_id=new_id, name=name, description=(desc or None),
            target_table_fqn=target_table, run_as_role=(state.get('run_as_role') or None),
            dmf_role=(state.get('dmf_role') or None), status=status, owner=None,
            schedule_cron=(cron_expr.strip() if cron_expr else "0 8 * * *"),
            schedule_timezone=(timezone_expr.strip() if timezone_expr else "Europe/Berlin"),
            schedule_enabled=bool(schedule_enabled)
        )
        # rebind ids
        checks_rebound: List[DQCheck] = []
        for cr in check_rows:
            cr.config_id = new_id; cr.table_fqn = target_table
            checks_rebound.append(cr)

        out = save_config_and_checks(session, dq_cfg, checks_rebound, apply_now=apply_now)
        base_msg = f"Saved config {new_id} ({status})."
        st.success(base_msg)
        remember("success", base_msg)
        if apply_now:
            dmfs_attached = out.get("dmfs_attached") or []
            if dmfs_attached:
                dmf_msg = "Attached views:\n- " + "\n- ".join(dmfs_attached)
                st.success(dmf_msg)
                remember("success", dmf_msg)
            else:
                info_msg = "No row-level failing-row views were required for this configuration."
                st.info(info_msg)
                remember("info", info_msg)

        if run_now_btn:
            task_fqn = None
            try:
                task_fqn = _task_fqn_for_config(dq_cfg)
                session.sql(f"EXECUTE TASK {task_fqn}").collect()
            except Exception as exc:
                error_msg = f"Failed to execute task {task_fqn or ''}: {exc}"
                st.error(error_msg)
                remember("error", error_msg)
            else:
                success_msg = "Run submitted. Open 📊 Monitor to see results when the task completes."
                st.success(success_msg)
                remember("success", success_msg)

        if apply_now and status == 'ACTIVE':
            sched = schedules.ensure_task_for_config(session, dq_cfg)
            sched_status = sched.get("status")
            if sched_status == "TASK_CREATED":
                cron_disp = dq_cfg.schedule_cron or "0 8 * * *"
                tz_disp = dq_cfg.schedule_timezone or "Europe/Berlin"
                sched_msg = f"Scheduled **{sched['task']}** (`{cron_disp}` {tz_disp})."
                st.success(sched_msg)
                remember("success", sched_msg)
            elif sched_status == "SCHEDULE_DISABLED":
                info_msg = "Schedule disabled — skipped automatic task creation."
                st.info(info_msg)
                remember("info", info_msg)
            elif sched_status == "INVALID_SCHEDULE":
                warn_msg = sched.get("reason") or "Schedule settings were invalid; task not created."
                st.warning(warn_msg)
                remember("warning", warn_msg)
            elif sched_status == "NO_WAREHOUSE":
                warn_msg = (
                    "No active warehouse is set for this session. "
                    "Run `USE WAREHOUSE <name>` in Snowflake or set a default warehouse, then save & apply again."
                )
                st.warning(warn_msg)
                remember("warning", warn_msg)
            else:
                reason = sched.get("reason")
                if reason:
                    warn_msg = (
                        f"Could not create task {sched.get('task') or ''}: {reason}. "
                        "Task intent was stored for manual follow-up."
                    )
                else:
                    warn_msg = "Could not create task automatically; stored fallback intent."
                st.warning(warn_msg)
                remember("warning", warn_msg)

        st.session_state["last_notices"] = post_submit_notices
        st.session_state["cfg_mode"] = "list"; st.rerun()


def render_monitor():
    st.header("Monitor")

    if not session:
        st.info("Monitoring requires an active Snowflake session.")
        return

    config_map = fetch_config_map(session)
    config_ids = sorted(config_map.keys())
    default_configs = config_ids if config_ids else []
    known_check_types = [
        "FRESHNESS",
        "ROW_COUNT",
        "ROW_COUNT_ANOMALY",
        "UNIQUE",
        "NULL_COUNT",
        "MIN_MAX",
        "WHITESPACE",
        "FORMAT_DISTRIBUTION",
        "VALUE_DISTRIBUTION",
    ]

    with st.container():
        c1, c2, c3, c4 = st.columns([1, 1.8, 1.6, 1])
        with c1:
            lookback_days = st.selectbox(
                "Lookback",
                options=[7, 30, 60, 90],
                index=1,
                format_func=lambda d: f"Last {d} days",
            )
        with c2:
            selected_configs = st.multiselect(
                "Config(s)",
                options=config_ids,
                default=default_configs,
                format_func=lambda cid: (config_map.get(cid, {}).get("name") or cid),
            )

        time_from = datetime.utcnow() - timedelta(days=int(lookback_days))
        base_results = fetch_run_results(
            session,
            time_from=time_from,
            config_ids=(selected_configs or None),
            limit=0,
        )

        available_types = sorted({(r.get("check_type") or "").upper() for r in base_results if r.get("check_type")})
        if not available_types:
            available_types = known_check_types

        with c3:
            selected_types = st.multiselect(
                "Check type(s)",
                options=available_types,
                default=available_types,
            )
        with c4:
            status_label = st.selectbox(
                "Status",
                options=["All", "Failed only", "Passed only"],
                index=0,
            )

    st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

    status_filter: Optional[str]
    if status_label == "Failed only":
        status_filter = "fail"
    elif status_label == "Passed only":
        status_filter = "pass"
    else:
        status_filter = None

    filtered_results = base_results
    if selected_types:
        selected_set = {t.upper() for t in selected_types}
        filtered_results = [r for r in filtered_results if (r.get("check_type") or "").upper() in selected_set]
    if status_filter == "fail":
        filtered_results = [r for r in filtered_results if r.get("ok") is not True]
    elif status_filter == "pass":
        filtered_results = [r for r in filtered_results if r.get("ok") is True]

    failed_rows = [r for r in filtered_results if r.get("ok") is not True]
    failed_checks_count = len(failed_rows)
    total_failure_rows = sum(int(r.get("failures") or 0) for r in filtered_results)
    configs_affected = len({r.get("config_id") for r in failed_rows if r.get("config_id")})

    k1, k2, k3 = st.columns(3)
    k1.metric("Failed checks", failed_checks_count)
    k2.metric("Total failures (rows)", total_failure_rows)
    k3.metric("Configs affected", configs_affected)

    ts_data = fetch_timeseries_daily(
        session,
        days=int(lookback_days),
        config_ids=(selected_configs or None),
    )
    ts_df = pd.DataFrame(ts_data)

    if filtered_results:
        tmp_df = pd.DataFrame(filtered_results)
        tmp_df["run_ts"] = pd.to_datetime(tmp_df["run_ts"])
        tmp_df["run_date"] = tmp_df["run_ts"].dt.date
        tmp_df["ok"] = tmp_df["ok"].fillna(False).astype(bool)
        tmp_df["failures"] = tmp_df["failures"].fillna(0).astype(int)
        agg = (
            tmp_df.groupby("run_date")
            .agg(
                fails=("ok", lambda x: int((~x.astype(bool)).sum())),
                failure_rows=("failures", "sum"),
            )
            .reset_index()
        )
        agg["run_date"] = pd.to_datetime(agg["run_date"])
        ts_df = agg
    elif not ts_df.empty:
        ts_df["run_date"] = pd.to_datetime(ts_df["run_date"])

    anomaly_df = pd.DataFrame()
    anomaly_rows = [
        r
        for r in filtered_results
        if (r.get("check_type") or "").upper() == "ROW_COUNT_ANOMALY" and r.get("ok") is False
    ]
    if anomaly_rows:
        anomaly_df = pd.DataFrame(anomaly_rows)
        anomaly_df["run_ts"] = pd.to_datetime(anomaly_df["run_ts"])
        anomaly_df["day"] = anomaly_df["run_ts"].dt.floor("D")
        anomaly_df = (
            anomaly_df.groupby("day")
            .size()
            .reset_index(name="anomaly_events")
        )
        if not ts_df.empty:
            ts_merge = ts_df[["run_date", "fails", "failure_rows"]].copy()
            anomaly_df = anomaly_df.merge(
                ts_merge,
                left_on="day",
                right_on="run_date",
                how="left",
            )
            anomaly_df = anomaly_df.drop(columns=["run_date"])
            anomaly_df["fails"] = anomaly_df["fails"].fillna(0).astype(int)
            anomaly_df["failure_rows"] = anomaly_df["failure_rows"].fillna(0).astype(int)
        else:
            anomaly_df["fails"] = 0
            anomaly_df["failure_rows"] = 0

    st.subheader("Daily trend")
    if ts_df.empty:
        st.info("No results available for the selected filters.")
    else:
        ts_df = ts_df.sort_values("run_date")
        fails_chart = alt.Chart(ts_df).mark_line().encode(
            x=alt.X("run_date:T", title="Run date"),
            y=alt.Y("fails:Q", title="Failed checks"),
        )
        fails_points = (
            alt.Chart(ts_df)
            .transform_filter("datum.fails > 0")
            .mark_point(size=60)
            .encode(x="run_date:T", y="fails:Q")
        )
        fails_layers = [fails_chart, fails_points]
        if not anomaly_df.empty:
            anomaly_layer_fails = (
                alt.Chart(anomaly_df)
                .mark_point(size=80, filled=True, shape="triangle-up", color="#d62728")
                .encode(
                    x=alt.X("day:T", title="Run date"),
                    y=alt.Y("fails:Q", title="Failed checks"),
                    tooltip=[
                        alt.Tooltip("day:T", title="Day"),
                        alt.Tooltip("anomaly_events:Q", title="Anomaly events"),
                    ],
                )
                .transform_filter("datum.anomaly_events > 0")
            )
            fails_layers.append(anomaly_layer_fails)
        st.altair_chart(alt.layer(*fails_layers), use_container_width=True)

        failure_rows_chart = alt.Chart(ts_df).mark_line().encode(
            x=alt.X("run_date:T", title="Run date"),
            y=alt.Y("failure_rows:Q", title="Failure rows"),
        )
        failure_rows_points = (
            alt.Chart(ts_df)
            .transform_filter("datum.failure_rows > 0")
            .mark_point(size=60)
            .encode(x="run_date:T", y="failure_rows:Q")
        )
        failure_layers = [failure_rows_chart, failure_rows_points]
        if not anomaly_df.empty:
            anomaly_layer_failure_rows = (
                alt.Chart(anomaly_df)
                .mark_point(size=80, filled=True, shape="triangle-up", color="#d62728")
                .encode(
                    x=alt.X("day:T", title="Run date"),
                    y=alt.Y("failure_rows:Q", title="Failure rows"),
                    tooltip=[
                        alt.Tooltip("day:T", title="Day"),
                        alt.Tooltip("anomaly_events:Q", title="Anomaly events"),
                    ],
                )
                .transform_filter("datum.anomaly_events > 0")
            )
            failure_layers.append(anomaly_layer_failure_rows)
        st.altair_chart(alt.layer(*failure_layers), use_container_width=True)

        st.caption("_Markers indicate days with ROW_COUNT_ANOMALY failures._")

    st.subheader("Recent results")
    if not filtered_results:
        st.info("No run results match the selected filters.")
        return

    db_schema_cache: Dict[str, Optional[str]] = {}

    def _relation_prefix(config_id: Optional[str]) -> Optional[str]:
        if not config_id:
            return None
        if config_id in db_schema_cache:
            return db_schema_cache[config_id]
        relation = (config_map.get(config_id, {}) or {}).get("table") or ""
        parts = [p.strip('"') for p in relation.split(".") if p]
        prefix = None
        if len(parts) >= 2:
            prefix = f"{parts[0]}.{parts[1]}"
        db_schema_cache[config_id] = prefix
        return prefix

    def _is_row_check(check_type: Optional[str]) -> bool:
        ctype = (check_type or "").upper()
        return ctype not in {"FRESHNESS", "ROW_COUNT", "ROW_COUNT_ANOMALY"}

    table_rows: List[Dict[str, Optional[str]]] = []
    for row in filtered_results[:200]:
        cfg_id = row.get("config_id")
        check_type = (row.get("check_type") or "").upper()
        view_name = None
        if row.get("ok") is False and _is_row_check(check_type):
            prefix = _relation_prefix(cfg_id)
            if prefix:
                view_name = f"{prefix}.DQ_{_sanitize_id(cfg_id)}_{_sanitize_id(row.get('check_id'))}_FAILS"
        ok_value = True if row.get("ok") is True else False
        table_rows.append(
            {
                "timestamp": row.get("run_ts"),
                "config": config_map.get(cfg_id, {}).get("name") or cfg_id,
                "check_id": row.get("check_id"),
                "check_type": check_type,
                "failures": int(row.get("failures") or 0),
                "ok": ok_value,
                "error_msg": row.get("error_msg"),
                "failure_view": view_name,
            }
        )

    results_df = pd.DataFrame(table_rows)
    results_df.sort_values("timestamp", ascending=False, inplace=True)

    st.dataframe(
        results_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "timestamp": st.column_config.DatetimeColumn("Timestamp"),
            "config": st.column_config.TextColumn("Config"),
            "check_id": st.column_config.TextColumn("Check ID"),
            "check_type": st.column_config.TextColumn("Check type"),
            "failures": st.column_config.NumberColumn("Failures"),
            "ok": st.column_config.CheckboxColumn("OK"),
            "error_msg": st.column_config.TextColumn("Error message", width="medium"),
            "failure_view": st.column_config.TextColumn("Failure view", width="large"),
        },
    )
    st.caption("Row checks have views; aggregates do not.")


# ---------- Sidebar + routing ----------
state = get_state()
if "page" not in st.session_state:
    st.session_state["page"] = "home"
if "cfg_mode" not in st.session_state:
    st.session_state["cfg_mode"] = "list"
current_page = st.session_state.get("page", "home")
with st.sidebar:
    st.header("Zeus DQ")
    st.button(
        "🏠 Overview",
        use_container_width=True,
        type="primary" if current_page == "home" else "secondary",
        key="nav_home",
        on_click=navigate_to,
        args=("home",),
    )
    st.button(
        "⚙️ Configurations",
        use_container_width=True,
        type="primary" if current_page == "cfg" else "secondary",
        key="nav_cfg",
        on_click=navigate_to,
        args=("cfg",),
    )
    st.button(
        "📊 Monitor",
        use_container_width=True,
        type="primary" if current_page == "monitor" else "secondary",
        key="nav_monitor",
        on_click=navigate_to,
        args=("monitor",),
    )
    st.divider()
    run_as_role = st.text_input("RUN_AS_ROLE", value=state.get("run_as_role") or "")
    dmf_role = st.text_input("DMF_ROLE", value=state.get("dmf_role") or "")
    set_state(run_as_role or None, dmf_role or None)

if st.session_state.get("page","home") == "cfg":
    if st.session_state.get("cfg_mode","list") == "list":
        render_config_list()
    else:
        render_config_editor()
elif st.session_state.get("page") == "monitor":
    render_monitor()
else:
    render_home()
