===================== FILE: services/configs.py =====================

# services/configs.py
from contextlib import contextmanager
from typing import Dict, Any, List

try:
    from snowflake.snowpark import Session
except Exception:
    Session = Any  # type: ignore

from utils import dmfs, meta

@contextmanager
def transaction(session: Session):
    try:
        session.sql("BEGIN").collect()
        yield
        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

def save_config_and_checks(session: Session, cfg: meta.DQConfig, checks: List[meta.DQCheck], apply_now: bool) -> Dict[str, Any]:
    with transaction(session):
        meta.upsert_config(session, cfg)
        meta.upsert_checks(session, checks)
        result: Dict[str, Any] = {"config_id": cfg.config_id, "status": cfg.status}
        if apply_now:
            created = dmfs.attach_dmfs(session, cfg, checks)
            result["dmfs_attached"] = created
    return result

def delete_config_full(session: Session, config_id: str) -> Dict[str, Any]:
    checks = meta.get_checks(session, config_id)
    dropped = dmfs.detach_dmfs_safe(session, config_id, checks)
    meta.delete_config(session, config_id)
    return {"config_id": config_id, "dmfs_dropped": dropped}

===================== FILE: services/runner.py =====================

# services/runner.py
from typing import Dict, Any, List
from utils.meta import DQCheck, DQConfig

AGG_PREFIX = "AGG:"

def run_now(session, cfg: DQConfig, checks: List[DQCheck]) -> Dict[str, Any]:
    results: Dict[str, Any] = {"config_id": cfg.config_id, "checks": []}
    for chk in checks:
        rule = (chk.rule_expr or '').strip()
        if rule.upper().startswith(AGG_PREFIX):
            sql = rule[len(AGG_PREFIX):].strip()
            df = session.sql(sql)
            r = df.collect()[0]
            ok = bool((r[0] if not hasattr(r, 'asDict') else list(r.asDict().values())[0]))
            failures = 0 if ok else 1
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": True,
                "ok": ok,
                "failures": failures,
                "sample": []
            })
        else:
            df = session.sql(f"SELECT COUNT(*) AS FAILURES FROM {chk.table_fqn} WHERE NOT ({rule})")
            failures = int(df.collect()[0][0])
            sample = []
            if chk.sample_rows and failures:
                s_df = session.sql(
                    f"SELECT * FROM {chk.table_fqn} WHERE NOT ({rule}) LIMIT {int(chk.sample_rows)}"
                )
                sample = [r.asDict() if hasattr(r, 'asDict') else dict(r) for r in s_df.collect()]
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": False,
                "failures": failures,
                "sample": sample,
            })
    return results

===================== FILE: services/state.py =====================

# services/state.py
import streamlit as st
from typing import Any, Dict, Optional

SESSION_KEYS = ["run_as_role", "dmf_role"]

def get_state() -> Dict[str, Any]:
    for k in SESSION_KEYS:
        if k not in st.session_state:
            st.session_state[k] = None
    return {k: st.session_state[k] for k in SESSION_KEYS}

def set_state(run_as_role: Optional[str], dmf_role: Optional[str]) -> None:
    st.session_state["run_as_role"] = run_as_role
    st.session_state["dmf_role"] = dmf_role

===================== FILE: streamlit_app.py =====================

import json
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
    DQ_CONFIG_TBL, _q,
    list_configs, get_config, get_checks,
    list_databases, list_schemas, list_tables, list_columns,
    fq_table,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.state import get_state, set_state
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check

RUN_RESULTS_TBL = "DQ_RUN_RESULTS"

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


def build_task_fqn(target_table_fqn: Optional[str], config_id: Optional[str]) -> Optional[str]:
    """Build the Snowflake task FQN for the given configuration."""
    if not target_table_fqn or not config_id:
        return None
    parts = [part.strip() for part in target_table_fqn.split(".") if part.strip()]
    if len(parts) != 3:
        return None
    db, schema, _ = parts

    def quote_ident(identifier: str) -> str:
        identifier = identifier.strip('"')
        return f'"{identifier}"'

    task_name = f'"DQ_TASK_{config_id.upper()}"'
    return f"{quote_ident(db)}.{quote_ident(schema)}.{task_name}"

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
    _, _, create_col = st.columns([6, 2, 2])
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
            task_fqn = build_task_fqn(target_table, new_id)
            if not task_fqn:
                err_msg = (
                    "Unable to determine the task for this configuration. "
                    "Ensure a target table is selected and try again."
                )
                st.error(err_msg)
                remember("error", err_msg)
            else:
                try:
                    session.sql(f"EXECUTE TASK {task_fqn}").collect()
                except Exception as exc:
                    err_msg = f"Failed to execute task {task_fqn}: {exc}"
                    st.error(err_msg)
                    remember("error", err_msg)
                else:
                    success_msg = (
                        "Run submitted. Open 📊 Monitor to see results when the task completes."
                    )
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
    run_as_role = st.text_input("RUN_AS_ROLE", value=state.get("run_as_role") or "")
    dmf_role = st.text_input("DMF_ROLE", value=state.get("dmf_role") or "")
    set_state(run_as_role or None, dmf_role or None)

nav_cols = st.columns(3)
with nav_cols[0]:
    st.button(
        "🏠 Overview",
        use_container_width=True,
        type="primary" if current_page == "home" else "secondary",
        key="top_nav_home",
        on_click=navigate_to,
        args=("home",),
    )
with nav_cols[1]:
    st.button(
        "⚙️ Configurations",
        use_container_width=True,
        type="primary" if current_page == "cfg" else "secondary",
        key="top_nav_cfg",
        on_click=navigate_to,
        args=("cfg",),
    )
with nav_cols[2]:
    st.button(
        "📊 Monitor",
        use_container_width=True,
        type="primary" if current_page == "monitor" else "secondary",
        key="top_nav_monitor",
        on_click=navigate_to,
        args=("monitor",),
    )

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

===================== FILE: utils/checkdefs.py =====================

SUPPORTED_COLUMN_CHECKS = ["UNIQUE","NULL_COUNT","MIN_MAX","WHITESPACE","FORMAT_DISTRIBUTION","VALUE_DISTRIBUTION"]
SUPPORTED_TABLE_CHECKS  = ["FRESHNESS","ROW_COUNT","ROW_COUNT_ANOMALY"]


def _q(ident: str) -> str:
    parts = [p.strip('"') for p in ident.split('.')]
    return '.'.join([f'"{p}"' for p in parts])


def build_rule_for_column_check(fqn: str, col: str, ctype: str, params: dict):
    colq = f'"{col}"'
    # return row predicate SQL, is_agg=False
    ctype = (ctype or "").upper()
    if ctype == "UNIQUE":
        ignore_nulls = bool(params.get("ignore_nulls", True))
        if ignore_nulls:
            return f"({colq} IS NULL OR {colq} IN (SELECT {colq} FROM {_q(fqn)} GROUP BY {colq} HAVING COUNT(*) = 1))", False
        return f"{colq} IN (SELECT {colq} FROM {_q(fqn)} GROUP BY {colq} HAVING COUNT(*) = 1)", False
    if ctype == "NULL_COUNT":
        # failures are rows where NOT(predicate) -> predicate should be "col IS NOT NULL" if max_nulls=0
        return f"{colq} IS NOT NULL", False
    if ctype == "MIN_MAX":
        min_v = params.get("min"); max_v = params.get("max")
        conds = []
        if min_v not in (None, ""): conds.append(f"{colq} >= {min_v}")
        if max_v not in (None, ""): conds.append(f"{colq} <= {max_v}")
        return "(" + " AND ".join(conds or ["TRUE"]) + ")", False
    if ctype == "WHITESPACE":
        mode = params.get("mode", "NO_LEADING_TRAILING")
        if mode == "NO_LEADING_TRAILING":
            return f"({colq} IS NULL OR {colq} = TRIM({colq}))", False
        if mode == "NO_INTERNAL_ONLY_WHITESPACE":
            return f"({colq} IS NULL OR REGEXP_REPLACE({colq}, '\\s+', ' ') = {colq})", False
        return f"({colq} IS NOT NULL AND LENGTH(TRIM({colq})) > 0)", False
    if ctype == "FORMAT_DISTRIBUTION":
        regex = params.get("regex", ".*")
        return f"({colq} IS NULL OR {colq} RLIKE '{regex}')", False
    if ctype == "VALUE_DISTRIBUTION":
        allowed_csv = params.get("allowed_values_csv", "")
        values = [v.strip() for v in allowed_csv.split(",") if v.strip() != ""]
        if not values: return "(TRUE)", False
        quoted_values = []
        for raw in values:
            sanitized = raw.replace("'", "''")
            quoted_values.append("'" + sanitized + "'")
        quoted = ", ".join(quoted_values)
        return f"({colq} IN ({quoted}))", False
    return "(TRUE)", False


def build_rule_for_table_check(fqn: str, ttype: str, params: dict):
    ttype = (ttype or "").upper()
    if ttype == "FRESHNESS":
        ts_col = params.get("timestamp_column", "LOAD_TIMESTAMP")
        max_age = int(params.get("max_age_minutes", 1920))
        return "\n".join([
            f"SELECT (COUNT(*) > 0 AND COUNT(\"{ts_col}\") > 0 AND",
            f"        TIMESTAMPDIFF('minute', MAX(\"{ts_col}\"), CURRENT_TIMESTAMP()) <= {max_age}) AS OK",
            f"FROM {_q(fqn)}",
        ]), True
    if ttype == "ROW_COUNT":
        min_rows = int(params.get("min_rows", 1))
        return f"SELECT COUNT(*) >= {min_rows} AS OK FROM {_q(fqn)}", True
    if ttype == "ROW_COUNT_ANOMALY":
        ts_col = params.get("timestamp_column", "LOAD_TIMESTAMP")
        lookback_days = int(params.get("lookback_days", 28))
        sensitivity = float(params.get("sensitivity", 3.0))
        min_history_days = int(params.get("min_history_days", 7))
        table_name = _q(fqn)
        return "\n".join([
            "WITH history AS (",
            f"    SELECT DATE_TRUNC('day', \"{ts_col}\") AS day, COUNT(*) AS c",
            f"    FROM {table_name}",
            f"    WHERE \"{ts_col}\" IS NOT NULL",
            f"      AND \"{ts_col}\" >= DATEADD('day', -{lookback_days}, CURRENT_DATE)",
            f"      AND DATE(\"{ts_col}\") < CURRENT_DATE",
            "    GROUP BY 1",
            "), aggregates AS (",
            "    SELECT",
            "        COUNT(*) AS history_days,",
            "        APPROX_PERCENTILE(c, 0.5) AS median_c",
            "    FROM history",
            "), mad_calc AS (",
            "    SELECT",
            "        APPROX_PERCENTILE(ABS(h.c - agg.median_c), 0.5) AS mad",
            "    FROM history h",
            "    CROSS JOIN aggregates agg",
            "), today AS (",
            f"    SELECT COUNT(*) AS c_today",
            f"    FROM {table_name}",
            f"    WHERE \"{ts_col}\" IS NOT NULL",
            f"      AND DATE(\"{ts_col}\") = CURRENT_DATE",
            ")",
            "SELECT (",
            f"    aggregates.history_days >= {min_history_days}",
            f"    AND COALESCE(ABS(today.c_today - aggregates.median_c) / NULLIF(1.4826 * mad_calc.mad, 0) <= {sensitivity}, FALSE)",
            ") AS OK",
            "FROM aggregates",
            "CROSS JOIN mad_calc",
            "CROSS JOIN today",
        ]), True
    return "SELECT TRUE AS OK", True

===================== FILE: utils/dmfs.py =====================

"""Convenience helpers for attaching and detaching Data Monitoring Framework views."""

import re
from typing import List, Set, Tuple
from utils.meta import DQConfig, DQCheck, _q, DQ_CONFIG_TBL, DQ_CHECK_TBL, metadata_db_schema

AGG_PREFIX = "AGG:"

__all__ = ["AGG_PREFIX", "attach_dmfs", "detach_dmfs_safe"]

def _split_fqn(fqn: str) -> Tuple[str,str,str]:
    parts = fqn.split(".")
    if len(parts) != 3: raise ValueError("Need DB.SCHEMA.TABLE")
    return tuple(p.strip('"') for p in parts)  # type: ignore

def _view_name(config_id: str, check_id: str) -> str:
    raw = f"DQ_{config_id}_{check_id}_FAILS".upper()
    return re.sub(r"[^A-Z0-9_]", "_", raw)

def attach_dmfs(session, config: DQConfig, checks: List[DQCheck]) -> List[str]:
    created: List[str] = []
    meta_db, meta_schema = metadata_db_schema(session)
    for chk in checks:
        rule = (chk.rule_expr or "").strip()
        if rule.upper().startswith(AGG_PREFIX):  # skip aggregates
            continue
        db, sch, tbl = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        predicate = rule[:-1] if rule.endswith(";") else rule
        session.sql(f"""
            CREATE OR REPLACE VIEW {_q(meta_db)}.{_q(meta_schema)}.{_q(vname)} AS
            SELECT * FROM {_q(db)}.{_q(sch)}.{_q(tbl)} WHERE NOT ({predicate})
        """).collect()
        created.append(f"{meta_db}.{meta_schema}.{vname}")
    return created

def _active_configs_using_table(session, table_fqn: str) -> Set[str]:
    df = session.sql(f"""
        SELECT DISTINCT k.CONFIG_ID
        FROM {_q(DQ_CONFIG_TBL)} c JOIN {_q(DQ_CHECK_TBL)} k USING(CONFIG_ID)
        WHERE UPPER(c.STATUS)='ACTIVE' AND k.TABLE_FQN = ?
    """, params=[table_fqn])
    out: Set[str] = set()
    for r in df.collect():
        out.add(r[0] if not hasattr(r, "asDict") else r.asDict().get("CONFIG_ID"))
    return out

def detach_dmfs_safe(session, config_id: str, checks: List[DQCheck]) -> List[str]:
    dropped: List[str] = []
    meta_db, meta_schema = metadata_db_schema(session)
    row_checks = [c for c in checks if not (c.rule_expr or "").upper().startswith(AGG_PREFIX)]
    tables = {c.table_fqn for c in row_checks}
    shared = {t for t in tables if len(_active_configs_using_table(session, t) - {config_id}) > 0}
    for chk in row_checks:
        if chk.table_fqn in shared: continue
        vname = _view_name(chk.config_id, chk.check_id)
        fqn = f"{_q(meta_db)}.{_q(meta_schema)}.{_q(vname)}"
        session.sql(f"DROP VIEW IF EXISTS {fqn}").collect()
        dropped.append(fqn)
    return dropped

===================== FILE: utils/meta.py =====================

"""Utility helpers and data models for interacting with DQ metadata tables.

The functions in this module intentionally avoid depending on Snowpark at
import time so they can be reused in environments where the Snowpark Python
client is not installed.  Snowflake objects are loaded lazily via duck typing.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from snowflake.snowpark import Session
except Exception:
    Session = Any  # type: ignore

# Override with fully-qualified names if desired (e.g., "DB.SCHEMA.DQ_CONFIG")
DQ_CONFIG_TBL: str = "DQ_CONFIG"
DQ_CHECK_TBL: str = "DQ_CHECK"

__all__ = [
    "DQ_CONFIG_TBL",
    "DQ_CHECK_TBL",
    "DQConfig",
    "DQCheck",
    "_q",
    "fq_table",
    "metadata_db_schema",
    "ensure_meta_tables",
    "upsert_config",
    "list_configs",
    "get_config",
    "delete_config",
    "upsert_checks",
    "get_checks",
    "list_databases",
    "list_schemas",
    "list_tables",
    "list_columns",
]

# ---------- Models ----------
@dataclass
class DQConfig:
    config_id: str
    name: str
    description: Optional[str]
    target_table_fqn: str
    run_as_role: Optional[str]
    dmf_role: Optional[str]
    status: str
    owner: Optional[str]
    schedule_cron: Optional[str] = None
    schedule_timezone: Optional[str] = None
    schedule_enabled: bool = True

@dataclass
class DQCheck:
    config_id: str
    check_id: str
    table_fqn: str
    column_name: Optional[str]
    rule_expr: str
    severity: str
    sample_rows: int = 0
    check_type: Optional[str] = None
    params_json: Optional[str] = None

# ---------- Helpers ----------
def _q(ident: str) -> str:
    parts = [p.strip('"') for p in ident.split('.')]
    return '.'.join([f'"{p}"' for p in parts])

def fq_table(database: str, schema: str, table: str) -> str:
    return f'{_q(database.upper())}.{_q(schema.upper())}.{_q(table.upper())}'

def _normalize_row(row) -> Dict[str, Any]:
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {str(k).lower(): v for k, v in d.items()}

def _parse_relation_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    parts = [p.strip('"') for p in name.split('.') if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, None, parts[0]
    raise ValueError("Invalid relation name")

def _current_db_schema(session: Session) -> Tuple[Optional[str], Optional[str]]:
    current_db: Optional[str] = None
    current_schema: Optional[str] = None
    if session:
        for attr, holder in (("get_current_database", "db"), ("get_current_schema", "schema")):
            try:
                getter = getattr(session, attr)
                value = getter()
                if holder == "db" and value:
                    current_db = value
                elif holder == "schema" and value:
                    current_schema = value
            except Exception:
                continue
        if not (current_db and current_schema):
            try:
                row = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
                if hasattr(row, "asDict"):
                    d = row.asDict()
                    current_db = current_db or d.get("CURRENT_DATABASE()") or d.get("CURRENT_DATABASE")
                    current_schema = current_schema or d.get("CURRENT_SCHEMA()") or d.get("CURRENT_SCHEMA")
                else:
                    current_db = current_db or row[0]
                    current_schema = current_schema or row[1]
            except Exception:
                pass
    return current_db, current_schema

def metadata_db_schema(session: Session) -> Tuple[str, str]:
    cfg_db, cfg_schema, _ = _parse_relation_name(DQ_CONFIG_TBL)
    chk_db, chk_schema, _ = _parse_relation_name(DQ_CHECK_TBL)
    current_db, current_schema = _current_db_schema(session)

    db = (cfg_db or chk_db or current_db)
    schema = (cfg_schema or chk_schema or current_schema)

    if not db or not schema:
        raise ValueError("Unable to determine metadata schema for DQ views")

    return db, schema

def ensure_meta_tables(session: Session):
    if not session: return
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {_q(DQ_CONFIG_TBL)} (
          CONFIG_ID STRING PRIMARY KEY,
          NAME STRING,
          DESCRIPTION STRING,
          TARGET_TABLE_FQN STRING,
          RUN_AS_ROLE STRING,
          DMF_ROLE STRING,
          STATUS STRING,
          OWNER STRING,
          SCHEDULE_CRON STRING,
          SCHEDULE_TIMEZONE STRING,
          SCHEDULE_ENABLED BOOLEAN,
          CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
          UPDATED_AT TIMESTAMP_LTZ
        )
    """).collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_CRON STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_TIMEZONE STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_ENABLED BOOLEAN").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {_q(DQ_CHECK_TBL)} (
          CONFIG_ID STRING,
          CHECK_ID STRING,
          TABLE_FQN STRING,
          COLUMN_NAME STRING,
          RULE_EXPR STRING,
          SEVERITY STRING,
          SAMPLE_ROWS NUMBER DEFAULT 0,
          CHECK_TYPE STRING,
          PARAMS_JSON STRING,
          PRIMARY KEY (CONFIG_ID, CHECK_ID)
        )
    """).collect()

# ---------- CRUD ----------
def upsert_config(session: Session, cfg: DQConfig):
    ensure_meta_tables(session)
    session.sql(f"""
        MERGE INTO {_q(DQ_CONFIG_TBL)} t
        USING (SELECT ? as CONFIG_ID, ? as NAME, ? as DESCRIPTION, ? as TARGET_TABLE_FQN,
                      ? as RUN_AS_ROLE, ? as DMF_ROLE, ? as STATUS, ? as OWNER,
                      ? as SCHEDULE_CRON, ? as SCHEDULE_TIMEZONE, ? as SCHEDULE_ENABLED,
                      CURRENT_TIMESTAMP() as UPDATED_AT) s
        ON t.CONFIG_ID = s.CONFIG_ID
        WHEN MATCHED THEN UPDATE SET
          NAME = s.NAME, DESCRIPTION = s.DESCRIPTION, TARGET_TABLE_FQN = s.TARGET_TABLE_FQN,
          RUN_AS_ROLE = s.RUN_AS_ROLE, DMF_ROLE = s.DMF_ROLE, STATUS = s.STATUS,
          OWNER = s.OWNER, SCHEDULE_CRON = s.SCHEDULE_CRON,
          SCHEDULE_TIMEZONE = s.SCHEDULE_TIMEZONE,
          SCHEDULE_ENABLED = s.SCHEDULE_ENABLED,
          UPDATED_AT = s.UPDATED_AT
        WHEN NOT MATCHED THEN INSERT (CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
                                      SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED, UPDATED_AT)
        VALUES (s.CONFIG_ID, s.NAME, s.DESCRIPTION, s.TARGET_TABLE_FQN, s.RUN_AS_ROLE, s.DMF_ROLE, s.STATUS, s.OWNER,
                s.SCHEDULE_CRON, s.SCHEDULE_TIMEZONE, s.SCHEDULE_ENABLED, s.UPDATED_AT)
    """, params=[
        cfg.config_id, cfg.name, cfg.description, cfg.target_table_fqn,
        cfg.run_as_role, cfg.dmf_role, cfg.status, cfg.owner,
        cfg.schedule_cron, cfg.schedule_timezone, cfg.schedule_enabled
    ]).collect()

def list_configs(session: Session) -> List[DQConfig]:
    if not session: return []
    ensure_meta_tables(session)
    df = session.sql(
        f"""
        SELECT CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
               SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED
        FROM {_q(DQ_CONFIG_TBL)}
        ORDER BY STATUS DESC, NAME
        """
    )
    out: List[DQConfig] = []
    for r in df.collect():
        d = _normalize_row(r)
        schedule_enabled_raw = d.get("schedule_enabled")
        if schedule_enabled_raw is None:
            schedule_enabled = True
        elif isinstance(schedule_enabled_raw, str):
            schedule_enabled = schedule_enabled_raw.strip().upper() in {"TRUE", "T", "YES", "Y", "1"}
        else:
            schedule_enabled = bool(schedule_enabled_raw)
        out.append(DQConfig(
            config_id=d["config_id"], name=d["name"], description=d.get("description"),
            target_table_fqn=d["target_table_fqn"], run_as_role=d.get("run_as_role"),
            dmf_role=d.get("dmf_role"), status=d.get("status") or "DRAFT", owner=d.get("owner"),
            schedule_cron=d.get("schedule_cron"),
            schedule_timezone=d.get("schedule_timezone"),
            schedule_enabled=schedule_enabled
        ))
    return out

def get_config(session: Session, config_id: str) -> Optional[DQConfig]:
    if not session: return None
    df = session.sql(
        f"""
        SELECT CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
               SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED
        FROM {_q(DQ_CONFIG_TBL)}
        WHERE CONFIG_ID = ?
        """,
        params=[config_id],
    )
    rows = df.collect()
    if not rows: return None
    d = _normalize_row(rows[0])
    schedule_enabled_raw = d.get("schedule_enabled")
    if schedule_enabled_raw is None:
        schedule_enabled = True
    elif isinstance(schedule_enabled_raw, str):
        schedule_enabled = schedule_enabled_raw.strip().upper() in {"TRUE", "T", "YES", "Y", "1"}
    else:
        schedule_enabled = bool(schedule_enabled_raw)
    return DQConfig(
        config_id=d["config_id"], name=d["name"], description=d.get("description"),
        target_table_fqn=d["target_table_fqn"], run_as_role=d.get("run_as_role"),
        dmf_role=d.get("dmf_role"), status=d.get("status") or "DRAFT", owner=d.get("owner"),
        schedule_cron=d.get("schedule_cron"),
        schedule_timezone=d.get("schedule_timezone"),
        schedule_enabled=schedule_enabled
    )

def delete_config(session: Session, config_id: str):
    if not session: return
    session.sql(f"DELETE FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ?", params=[config_id]).collect()
    session.sql(f"DELETE FROM {_q(DQ_CONFIG_TBL)} WHERE CONFIG_ID = ?", params=[config_id]).collect()

def upsert_checks(session: Session, checks: List[DQCheck]):
    if not session or not checks: return
    ensure_meta_tables(session)
    cfg_id = checks[0].config_id
    session.sql(f"DELETE FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ?", params=[cfg_id]).collect()
    for c in checks:
        session.sql(f"""
            INSERT INTO {_q(DQ_CHECK_TBL)} (CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY, SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
        """, params=[c.config_id, c.check_id, c.table_fqn, c.column_name, c.rule_expr, c.severity, int(c.sample_rows), c.check_type, c.params_json]).collect()

def get_checks(session: Session, config_id: str) -> List[DQCheck]:
    if not session: return []
    df = session.sql(f"SELECT CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY, SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ? ORDER BY CHECK_ID", params=[config_id])
    out: List[DQCheck] = []
    for r in df.collect():
        d = _normalize_row(r)
        out.append(DQCheck(
            config_id=d["config_id"], check_id=d["check_id"], table_fqn=d["table_fqn"],
            column_name=d.get("column_name"), rule_expr=d["rule_expr"], severity=d.get("severity") or "ERROR",
            sample_rows=int(d.get("sample_rows") or 0), check_type=d.get("check_type"), params_json=d.get("params_json")
        ))
    return out

# ---------- Discovery (INFO_SCHEMA with safe fallbacks) ----------
def list_databases(session: Session) -> List[str]:
    if not session: return []
    try:
        df = session.sql("SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY 1")
        return [r[0] for r in df.collect()]
    except Exception:
        return []

def list_schemas(session: Session, database: str) -> List[str]:
    if not session or not database: return []
    try:
        df = session.sql(f'SELECT SCHEMA_NAME FROM {_q(database)}.INFORMATION_SCHEMA.SCHEMATA ORDER BY 1')
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f'SHOW SCHEMAS IN DATABASE {_q(database)}')
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []

def list_tables(session: Session, database: str, schema: str) -> List[str]:
    if not session or not (database and schema): return []
    try:
        df = session.sql(f"SELECT TABLE_NAME FROM {_q(database)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE='BASE TABLE' ORDER BY 1", params=[schema.upper()])
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"SHOW TABLES IN SCHEMA {_q(database)}.{_q(schema)}")
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []

def list_columns(session: Session, database: str, schema: str, table: str) -> List[str]:
    if not session or not (database and schema and table): return []
    try:
        df = session.sql(f"SELECT COLUMN_NAME FROM {_q(database)}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", params=[schema.upper(), table.upper()])
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"DESC TABLE {_q(database)}.{_q(schema)}.{_q(table)}")
            return [r[0] for r in df.collect()]  # NAME
        except Exception:
            return []

===================== FILE: utils/schedules.py =====================

from typing import Any, Dict

from utils.meta import _parse_relation_name, _q


def ensure_task_for_config(session, cfg) -> Dict[str, Any]:
    base_task_name = f"DQ_TASK_{cfg.config_id}"

    if not session:
        return {"status": "FALLBACK", "reason": "Missing session", "task": base_task_name}

    target_fqn = getattr(cfg, "target_table_fqn", "")
    try:
        db, schema, _ = _parse_relation_name(target_fqn)
    except Exception as exc:  # pragma: no cover - defensive branch
        return {
            "status": "FALLBACK",
            "reason": f"Invalid target table FQN: {exc}",
            "task": base_task_name,
        }

    if not db or not schema:
        return {
            "status": "FALLBACK",
            "reason": "Target table must include database and schema",
            "task": base_task_name,
        }

    fq_task_identifier = f"{_q(db)}.{_q(schema)}.{_q(base_task_name)}"
    schedule_expression = "USING CRON 0 8 * * * Europe/Berlin"
    call_statement = f"CALL {_q(db)}.{_q(schema)}.SP_RUN_DQ_CONFIG('{cfg.config_id}')"

    try:
        session.sql(
            f"""
            CREATE OR REPLACE TASK {fq_task_identifier}
            WAREHOUSE = IDENTIFIER(CURRENT_WAREHOUSE())
            SCHEDULE = '{schedule_expression}'
            AS {call_statement}
            """
        ).collect()
        session.sql(f"ALTER TASK {fq_task_identifier} RESUME").collect()
        return {
            "status": "TASK_CREATED",
            "task": f"{db}.{schema}.{base_task_name}",
        }
    except Exception as exc:
        return {
            "status": "FALLBACK",
            "reason": str(exc),
            "task": f"{db}.{schema}.{base_task_name}",
        }

===================== FILE: utils/ui.py =====================

# utils/ui.py
import streamlit as st
from typing import List, Optional, Dict, Any

SAFE_EMPTY = st.secrets.get("SAFE_EMPTY_LABEL", "— none —") if hasattr(st, 'secrets') else "— none —"

def safe_select(label: str, options: List[str], index: Optional[int] = None, key: Optional[str] = None) -> Optional[str]:
    opts = [SAFE_EMPTY] + options
    idx = 0 if index is None else index + 1
    choice = st.selectbox(label, opts, index=idx, key=key)
    return None if choice == SAFE_EMPTY else choice

def safe_multiselect(label: str, options: List[str], default: Optional[List[str]] = None, key: Optional[str] = None) -> List[str]:
    default = default or []
    default_display = [o for o in default if o in options]
    vals = st.multiselect(label, options, default=default_display, key=key)
    return list(vals)

def highlight_active(row_status: str) -> Dict[str, Any]:
    if (row_status or '').upper() == 'ACTIVE':
        return {"backgroundColor": "#eaffea"}
    return {}

