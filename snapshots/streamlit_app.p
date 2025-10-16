import json
from typing import Any, Dict, List, Optional
from uuid import uuid4

import streamlit as st

try:
    import altair as alt
except ModuleNotFoundError:
    alt = None  # type: ignore

try:
    import pandas as pd
except ModuleNotFoundError as exc:  # pragma: no cover - env-specific guard
    st.set_page_config(page_title="Zeus Data Quality", layout="wide")
    st.error("Pandas is required to run this app. Please install the `pandas` package and restart.")
    st.exception(exc)
    st.stop()

# --- Snowpark session (works in Snowsight; safe locally) ---
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    session = None

# --- App imports (our modules) ---
from utils.dmfs import (
    run_task_now,
    task_name_for_config,
    ensure_session_context,
    session_snapshot,
    preflight_requirements,
    DEFAULT_WAREHOUSE,
    _q as _q_task,
)
from utils.meta import (
    DQConfig, DQCheck,
    DQ_CONFIG_TBL, _q, _parse_relation_name,
    list_configs, get_config, get_checks,
    list_databases, list_schemas, list_tables, list_columns,
    fq_table,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.state import get_state, set_state
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check
from utils.configs import get_metadata_namespace, get_proc_name

METADATA_DB, METADATA_SCHEMA = get_metadata_namespace()
PROC_NAME = get_proc_name()
RUN_RESULTS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_RUN_RESULTS"

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

def _normalize_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().upper()
    return text in {"TRUE", "T", "YES", "Y", "1"}

def navigate_to(page: str) -> None:
    """Update the current page selection in session state."""
    st.session_state["page"] = page
    if page == "home":
        st.session_state["cfg_mode"] = "list"


def open_config_editor(
    config_id: Optional[str] = None, target_fqn: Optional[str] = None
) -> None:
    """Switch to the configuration editor with the given selection."""
    st.session_state["cfg_mode"] = "edit"
    st.session_state["selected_config_id"] = config_id
    st.session_state["editor_target_fqn"] = target_fqn
    st.rerun()

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

**Attach** creates per-check *views of failing rows*; **Run Now** evaluates checks ad-hoc; a daily **Task** runs at 08:00 Europe/Berlin.
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
            if kind == "success":
                if message:
                    st.success(message)
            elif kind == "warning":
                if message:
                    st.warning(message)
            elif kind == "error":
                if message:
                    st.error(message)
            elif kind == "sql":
                if message:
                    st.caption(message)
                st.code(
                    note.get("code", ""),
                    language=note.get("language", "sql"),
                )
                continue
            else:
                if message:
                    st.info(message)

            code_snippet = note.get("code")
            if code_snippet:
                if message:
                    st.caption(message)
                st.code(code_snippet, language=note.get("language", "text"))
    search_query = st.text_input(
        "Search configurations",
        key="config_list_search",
        placeholder="Search by name, table, status, role, or ID",
        label_visibility="collapsed",
    )

    cfgs = list_configs(session)
    if not cfgs:
        st.info("No configurations yet. Use the sidebar to create one via **Create configuration**.")
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
                    open_config_editor(cfg.config_id, cfg.target_table_fqn)
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

        st.checkbox(
            "Use SYSTEM$TASK_FORCE_RUN fallback",
            value=bool(st.session_state.get("run_task_via_system_proc", False)),
            help=(
                "When enabled, the app will invoke `SYSTEM$TASK_FORCE_RUN` with the generated task name "
                "after scheduling to verify privileges."
            ),
            key="run_task_via_system_proc",
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
            try:
                db, schema, _ = _parse_relation_name(dq_cfg.target_table_fqn or "")
            except Exception as exc:
                err_msg = f"Failed to determine task location: {exc}"
                st.error(err_msg)
                remember("error", err_msg)
            else:
                if not db or not schema:
                    warn_msg = "Run Now requires a fully qualified target table (database and schema)."
                    st.warning(warn_msg)
                    remember("warning", warn_msg)
                else:
                    task_name = task_name_for_config(dq_cfg.config_id)
                    try:
                        result_df = run_task_now(
                            session,
                            METADATA_DB,
                            METADATA_SCHEMA,
                            dq_cfg.config_id,
                        )
                    except Exception as exc:
                        err_msg = f"Failed to trigger task run: {exc}"
                        st.error(err_msg)
                        remember("error", err_msg)
                    else:
                        request_id = None
                        if not result_df.empty and "REQUEST_ID" in result_df.columns:
                            request_id = result_df.iloc[0]["REQUEST_ID"]
                        success_msg = f"Triggered `{task_name}` task run."
                        if request_id:
                            success_msg = (
                                f"Triggered `{task_name}` task run (request `{request_id}`)."
                            )
                        st.success(success_msg)
                        remember("success", success_msg)

        run_task_via_system_proc_flag = bool(st.session_state.get("run_task_via_system_proc", False))

        if apply_now and status == 'ACTIVE':
            st.caption(f"Namespace: {METADATA_DB}.{METADATA_SCHEMA}, Proc: {PROC_NAME}")
            dbg_df = None
            snapshot_error: Optional[Exception] = None
            try:
                dbg_df = session_snapshot(session)
            except Exception as exc:  # pragma: no cover - Snowflake specific
                snapshot_error = exc

            meta_db, meta_schema = METADATA_DB, METADATA_SCHEMA
            metadata_error: Optional[Exception] = None
            task_fqn: Optional[str] = None
            proc_fqn: Optional[str] = None
            if not meta_db or not meta_schema:
                metadata_error = ValueError("Metadata namespace is not configured")
            else:
                task_fqn = _q_task(meta_db, meta_schema, task_name_for_config(dq_cfg.config_id))
                proc_fqn = _q_task(meta_db, meta_schema, PROC_NAME)

            try:
                warehouse_name = session.get_current_warehouse()
            except Exception:  # pragma: no cover - Snowflake specific
                warehouse_name = None
            warehouse_name = (warehouse_name or "").strip()
            run_role_name = (dq_cfg.run_as_role or "").strip()

            task_failure_reported = False
            task_sql_recorded = False
            task_manage_sql: Optional[str] = None

            if meta_db and meta_schema:
                def _quote_ident(value: Optional[str]) -> str:
                    text = "" if value is None else str(value)
                    return '"' + text.replace('"', '""') + '"'

                def _quote_literal(value: Optional[str]) -> str:
                    if value is None:
                        return "NULL"
                    text = str(value)
                    return "'" + text.replace("'", "''") + "'"

                cron_expression = (dq_cfg.schedule_cron or "0 8 * * *").strip() or "0 8 * * *"
                timezone_name = (dq_cfg.schedule_timezone or "Europe/Berlin").strip() or "Europe/Berlin"
                task_manage_sql = (
                    f"CALL {_quote_ident(meta_db)}.{_quote_ident(meta_schema)}.\"SP_DQ_MANAGE_TASK\"("
                    f"{_quote_literal(meta_db)}, {_quote_literal(meta_schema)}, {_quote_literal(DEFAULT_WAREHOUSE)}, "
                    f"{_quote_literal(dq_cfg.config_id)}, {_quote_literal(PROC_NAME)}, "
                    f"{_quote_literal(cron_expression)}, {_quote_literal(timezone_name)}, TRUE)"
                )

            def show_task_failure(message: str) -> None:
                nonlocal task_failure_reported, task_sql_recorded
                task_failure_reported = True
                st.error(message)
                remember("error", message)
                inferred_task_fqn = task_fqn
                inferred_proc_fqn = proc_fqn
                if not inferred_task_fqn:
                    if meta_db and meta_schema:
                        inferred_task_fqn = _q_task(meta_db, meta_schema, task_name_for_config(dq_cfg.config_id))
                    else:
                        inferred_task_fqn = task_name_for_config(dq_cfg.config_id)
                if not inferred_proc_fqn:
                    if meta_db and meta_schema:
                        inferred_proc_fqn = _q_task(meta_db, meta_schema, PROC_NAME)
                    else:
                        inferred_proc_fqn = PROC_NAME
                st.markdown(
                    f"**Task FQN:** `{inferred_task_fqn}`  \\\n+**Procedure FQN:** `{inferred_proc_fqn}`"
                )
                if task_manage_sql:
                    st.caption("Task creation call (for debugging):")
                    st.code(task_manage_sql, language="sql")
                    if not task_sql_recorded:
                        post_submit_notices.append(
                            {
                                "type": "sql",
                                "message": "Task creation call (for debugging):",
                                "code": task_manage_sql,
                                "language": "sql",
                            }
                        )
                        task_sql_recorded = True
                if dbg_df is not None:
                    st.caption("Session snapshot at failure:")
                    st.dataframe(dbg_df, use_container_width=True, hide_index=True)
                elif snapshot_error is not None:
                    st.caption(f"Session snapshot unavailable: {snapshot_error}")

            sched: Dict[str, Any] = {}
            if metadata_error is not None:
                show_task_failure(f"Unable to determine metadata schema: {metadata_error}")
                sched = {
                    "status": "FALLBACK",
                    "reason": str(metadata_error),
                    "task": task_name_for_config(dq_cfg.config_id),
                }
            else:
                preflight_failed = False
                try:
                    ensure_session_context(
                        session,
                        run_role_name,
                        warehouse_name,
                        meta_db or "",
                        meta_schema or "",
                    )
                    if meta_db and meta_schema and warehouse_name:
                        preflight_requirements(
                            session,
                            meta_db,
                            meta_schema,
                            warehouse_name,
                            proc_name=PROC_NAME,
                            arg_sig="(VARCHAR)",
                        )
                        preflight_requirements(
                            session,
                            meta_db,
                            meta_schema,
                            warehouse_name,
                            proc_name="SP_DQ_MANAGE_TASK",
                            arg_sig="(STRING, STRING, STRING, STRING, STRING, STRING, STRING, BOOLEAN)",
                        )
                except Exception as exc:  # pragma: no cover - Snowflake specific
                    show_task_failure(f"Task preflight failed: {exc}")
                    sched = {
                        "status": "FALLBACK",
                        "reason": str(exc),
                        "task": task_fqn or task_name_for_config(dq_cfg.config_id),
                    }
                    preflight_failed = True
                if not preflight_failed:
                    sched = schedules.ensure_task_for_config(session, dq_cfg)
                    if sched.get("status") == "FALLBACK" and sched.get("reason"):
                        show_task_failure(f"Task creation failed: {sched['reason']}")

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
                    "Select a warehouse in Snowflake or configure a default before saving again."
                )
                st.warning(warn_msg)
                remember("warning", warn_msg)
            elif sched_status == "FALLBACK" and task_failure_reported:
                pass
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

            if run_task_via_system_proc_flag:
                if not task_fqn:
                    warn_msg = (
                        "SYSTEM$TASK_FORCE_RUN fallback skipped because the task name could not be determined."
                    )
                    st.warning(warn_msg)
                    remember("warning", warn_msg)
                elif not warehouse_name:
                    warn_msg = (
                        "SYSTEM$TASK_FORCE_RUN fallback skipped because no active warehouse is set."
                    )
                    st.warning(warn_msg)
                    remember("warning", warn_msg)
                elif not (meta_db and meta_schema):
                    warn_msg = (
                        "SYSTEM$TASK_FORCE_RUN fallback skipped due to incomplete database/schema context."
                    )
                    st.warning(warn_msg)
                    remember("warning", warn_msg)
                else:
                    try:
                        ensure_session_context(
                            session,
                            run_role_name,
                            warehouse_name,
                            meta_db,
                            meta_schema,
                        )
                        fallback_df = session.sql(
                            "SELECT SYSTEM$TASK_FORCE_RUN(?) AS REQUEST_ID",
                            params=[task_fqn],
                        ).to_pandas()
                    except Exception as exc:  # pragma: no cover - Snowflake specific
                        warn_msg = f"SYSTEM$TASK_FORCE_RUN fallback failed: {exc}"
                        st.warning(warn_msg)
                        remember("warning", warn_msg)
                    else:
                        request_id = None
                        if not fallback_df.empty and "REQUEST_ID" in fallback_df.columns:
                            request_id = fallback_df.iloc[0]["REQUEST_ID"]
                        info_msg = f"Invoked `SYSTEM$TASK_FORCE_RUN` for `{task_fqn}`."
                        if request_id:
                            info_msg = (
                                f"Invoked `SYSTEM$TASK_FORCE_RUN` for `{task_fqn}` (request `{request_id}`)."
                            )
                        st.info(info_msg)
                        remember("info", info_msg)

        st.session_state["last_notices"] = post_submit_notices
        st.session_state["cfg_mode"] = "list"; st.rerun()

def render_monitor():
    st.header("📊 Monitor")
    if not session:
        st.info("Connect to Snowflake to view recent data quality runs.")
        return

    configs = list_configs(session)
    config_labels: List[str] = []
    config_map: Dict[str, str] = {}
    for cfg in configs:
        label_base = (cfg.name or "").strip()
        if label_base and label_base.upper() != (cfg.config_id or "").upper():
            label = f"{label_base} ({cfg.config_id})"
        else:
            label = cfg.config_id or label_base or "Unnamed"
        config_labels.append(label)
        config_map[label] = cfg.config_id

    if "_mon_config_options" not in st.session_state or st.session_state.get("_mon_config_options") != config_labels:
        st.session_state["mon_configs"] = config_labels.copy()
        st.session_state["_mon_config_options"] = config_labels.copy()

    filters = st.columns([1, 1, 3])
    with filters[0]:
        days = st.selectbox("Days", options=[7, 30, 60, 90], index=1, key="mon_days")
    with filters[1]:
        status_filter = st.selectbox(
            "Status",
            options=["All", "Failed only", "Passed only"],
            index=0,
            key="mon_status",
        )
    with filters[2]:
        selected_labels = st.multiselect(
            "Configurations",
            options=config_labels,
            default=st.session_state.get("mon_configs", config_labels),
            key="mon_configs",
        )

    selected_config_ids = [config_map[label] for label in selected_labels if label in config_map]

    results_table = _q(RUN_RESULTS_TBL)
    config_table = _q(DQ_CONFIG_TBL)
    where_clauses = [f"r.RUN_TS >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())"]
    params: List[object] = []

    if selected_config_ids:
        placeholders = ", ".join(["?"] * len(selected_config_ids))
        where_clauses.append(f"r.CONFIG_ID IN ({placeholders})")
        params.extend(selected_config_ids)

    if status_filter == "Failed only":
        where_clauses.append("COALESCE(r.OK, FALSE) = FALSE")
    elif status_filter == "Passed only":
        where_clauses.append("COALESCE(r.OK, FALSE) = TRUE")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    query = f"""
        SELECT
            r.RUN_TS,
            r.CONFIG_ID,
            c.NAME AS CONFIG_NAME,
            r.CHECK_ID,
            r.CHECK_TYPE,
            r.FAILURES,
            r.OK,
            r.ERROR_MSG
        FROM {results_table} r
        LEFT JOIN {config_table} c ON c.CONFIG_ID = r.CONFIG_ID
        WHERE {where_sql}
        ORDER BY r.RUN_TS DESC
        LIMIT 5000
    """

    try:
        df = session.sql(query, params=params).to_pandas()
    except Exception as exc:
        st.warning(f"Unable to load run results: {exc}")
        return

    df.columns = [str(col).lower() for col in df.columns]
    if "run_ts" not in df.columns:
        st.info("No run results available in the selected window.")
        return

    df["run_ts"] = pd.to_datetime(df["run_ts"], utc=True, errors="coerce")
    df["run_ts"] = df["run_ts"].dt.tz_convert(None)
    df = df.dropna(subset=["run_ts"])

    has_results = not df.empty
    if not has_results:
        st.info("No run results available in the selected window.")

    df["ok_flag"] = df["ok"].apply(_normalize_bool)
    df["failures_num"] = pd.to_numeric(df["failures"], errors="coerce").fillna(0)
    df["run_date"] = df["run_ts"].dt.floor("D")
    df["config_display"] = df["config_name"].fillna("").str.strip()
    missing_config_mask = df["config_display"] == ""
    df.loc[missing_config_mask, "config_display"] = df.loc[missing_config_mask, "config_id"]

    failed_df = df[~df["ok_flag"]]
    failed_checks = int(failed_df.shape[0])
    total_failures = int(failed_df["failures_num"].sum())
    configs_affected = int(failed_df["config_id"].nunique())

    today = pd.Timestamp.utcnow().normalize()
    start_date = today - pd.Timedelta(days=int(days) - 1)
    all_dates = pd.date_range(start=start_date, end=today, freq="D")

    if not all_dates.empty:
        fail_counts = (
            failed_df.groupby("run_date").size().reindex(all_dates, fill_value=0).reset_index()
        )
        fail_counts.columns = ["run_date", "failed_checks"]
        fail_counts["failed_checks"] = fail_counts["failed_checks"].astype(int)

        failure_totals = (
            failed_df.groupby("run_date")["failures_num"].sum().reindex(all_dates, fill_value=0).reset_index()
        )
        failure_totals.columns = ["run_date", "total_failures"]
        failure_totals["total_failures"] = failure_totals["total_failures"].astype(int)
    else:
        fail_counts = pd.DataFrame({"run_date": [], "failed_checks": []})
        failure_totals = pd.DataFrame({"run_date": [], "total_failures": []})

    k1, k2, k3 = st.columns(3)
    k1.metric("Failed checks", f"{failed_checks:,}")
    k2.metric("Total failures (rows)", f"{total_failures:,}")
    k3.metric("Configs affected", f"{configs_affected:,}")

    st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

    if not alt:
        st.warning(
            "Altair is not installed in this environment, so timeline charts are unavailable."
        )
    else:
        st.subheader("Daily failed checks")
        fail_chart = (
            alt.Chart(fail_counts)
            .mark_line()
            .encode(
                x=alt.X("run_date:T", title="Run date"),
                y=alt.Y("failed_checks:Q", title="Failed checks"),
                tooltip=[
                    alt.Tooltip("run_date:T", title="Date"),
                    alt.Tooltip("failed_checks:Q", title="Failed checks"),
                ],
            )
        )
        fail_points = (
            alt.Chart(fail_counts)
            .transform_filter(alt.datum.failed_checks > 0)
            .mark_point(size=70, filled=True)
            .encode(
                x="run_date:T",
                y="failed_checks:Q",
                tooltip=[
                    alt.Tooltip("run_date:T", title="Date"),
                    alt.Tooltip("failed_checks:Q", title="Failed checks"),
                ],
            )
        )
        st.altair_chart((fail_chart + fail_points).properties(height=240), use_container_width=True)

        st.subheader("Daily total failures")
        failure_chart = (
            alt.Chart(failure_totals)
            .mark_line()
            .encode(
                x=alt.X("run_date:T", title="Run date"),
                y=alt.Y("total_failures:Q", title="Total failures"),
                tooltip=[
                    alt.Tooltip("run_date:T", title="Date"),
                    alt.Tooltip("total_failures:Q", title="Total failures"),
                ],
            )
        )
        failure_points = (
            alt.Chart(failure_totals)
            .transform_filter(alt.datum.total_failures > 0)
            .mark_point(size=70, filled=True)
            .encode(
                x="run_date:T",
                y="total_failures:Q",
                tooltip=[
                    alt.Tooltip("run_date:T", title="Date"),
                    alt.Tooltip("total_failures:Q", title="Total failures"),
                ],
            )
        )
        st.altair_chart((failure_chart + failure_points).properties(height=240), use_container_width=True)

    st.subheader("Recent results")
    recent_df = df.sort_values("run_ts", ascending=False).head(200).copy()
    recent_df["time"] = recent_df["run_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    recent_df["failures_display"] = recent_df["failures_num"].round().astype(int)
    recent_df["error_msg"] = recent_df["error_msg"].fillna("")

    table = recent_df[[
        "time",
        "config_display",
        "check_id",
        "check_type",
        "failures_display",
        "ok_flag",
        "error_msg",
    ]].rename(
        columns={
            "time": "Time",
            "config_display": "Config",
            "check_id": "Check ID",
            "check_type": "Check type",
            "failures_display": "Failures",
            "ok_flag": "OK",
            "error_msg": "Error message",
        }
    )

    st.dataframe(table, use_container_width=True, hide_index=True)
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
    if current_page == "cfg" and st.session_state.get("cfg_mode", "list") == "list":
        if st.button(
            "➕ Create configuration",
            use_container_width=True,
            key="sidebar_create_config",
        ):
            open_config_editor()
        st.divider()
    run_as_role = st.text_input("RUN_AS_ROLE", value=state.get("run_as_role") or "")
    dmf_role = st.text_input("DMF_ROLE", value=state.get("dmf_role") or "")
    set_state(run_as_role or None, dmf_role or None)

# Maintain a subtle separation between the sidebar navigation
# and the main content area.
st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

page = st.session_state.get("page", "home")
if page == "cfg":
    if st.session_state.get("cfg_mode","list") == "list":
        render_config_list()
    else:
        render_config_editor()
elif page == "monitor":
    render_monitor()
else:
    render_home()
