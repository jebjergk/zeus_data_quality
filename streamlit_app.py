import json
from typing import Dict, List, Optional
from uuid import uuid4

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
    fq_table,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.runner import run_now
from services.state import get_state, set_state
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

# ---------- Styling (simple Snowflake-ish) ----------
st.markdown("""
<style>
.sf-hr { height:1px; background:#e7ebf3; border:0; margin:.6rem 0 1rem 0; }
.badge { display:inline-block; padding:.15rem .55rem; border-radius:999px; font-size:.75rem; font-weight:600;
 background:#e5f6fd; color:#055e86; border:1px solid #cbeefb; }
.badge-green { background:#eafaf0; border-color:#d4f2df; color:#0a5c2b; }
.card { border:1px solid #e7ebf3; border-radius:12px; padding:.9rem 1rem; background:#fff; box-shadow:0 1px 2px rgba(12,18,28,.04); }
.small { font-size:.85rem; color:#6b7280; }
.kv { color:#111827; font-weight:600; }
</style>
""", unsafe_allow_html=True)

# ---------- Helpers ----------
def _keyify(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s).lower()

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
    st.info("Table-level checks **FRESHNESS** and **ROW_COUNT** are automatically included.")

    # -------- Form --------
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
                    p_ignore_nulls = st.checkbox("Ignore NULLs", value=ex.get("params", {}).get("ignore_nulls", True), key=f"{sk}_p_un_ignore")
                    sev = st.selectbox("Severity (UNIQUE)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_unique")
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
                    max_nulls = st.number_input("Max NULL rows", min_value=0, value=int(ex.get("params", {}).get("max_nulls", 0)), key=f"{sk}_p_nc_max")
                    sev = st.selectbox("Severity (NULL_COUNT)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_null")
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
                    min_v = st.text_input("Min (inclusive)", value=str(ex.get("params", {}).get("min","")), key=f"{sk}_p_mm_min")
                    max_v = st.text_input("Max (inclusive)", value=str(ex.get("params", {}).get("max","")), key=f"{sk}_p_mm_max")
                    sev = st.selectbox("Severity (MIN_MAX)", ["ERROR", "WARN"], index=(0 if ex.get("severity","ERROR")=="ERROR" else 1), key=f"{sk}_sev_mm")
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
        ts_col = st.text_input("Freshness timestamp column", value="LOAD_TIMESTAMP")
        max_age = st.number_input("Freshness max age (minutes)", min_value=1, value=1440)
        min_rows = st.number_input("Minimum row count", min_value=0, value=1)

        if target_table:
            fr_params = {"timestamp_column": ts_col, "max_age_minutes": int(max_age)}
            fr_rule = build_rule_for_table_check(target_table, "FRESHNESS", fr_params)
            check_rows.append(DQCheck(
                config_id=(cfg.config_id if cfg else "temp"),
                check_id="TABLE_FRESHNESS",
                table_fqn=target_table, column_name=None,
                rule_expr=f"AGG: {fr_rule}", severity="ERROR",
                sample_rows=0, check_type="FRESHNESS",
                params_json=json.dumps(fr_params)
            ))
            rc_params = {"min_rows": int(min_rows)}
            rc_rule = build_rule_for_table_check(target_table, "ROW_COUNT", rc_params)
            check_rows.append(DQCheck(
                config_id=(cfg.config_id if cfg else "temp"),
                check_id="TABLE_ROW_COUNT",
                table_fqn=target_table, column_name=None,
                rule_expr=f"AGG: {rc_rule}", severity="ERROR",
                sample_rows=0, check_type="ROW_COUNT",
                params_json=json.dumps(rc_params)
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

        if run_now_btn or apply_now:
            results = run_now(session, dq_cfg, checks_rebound)
            st.info("Run Now results:")
            summary_lines: List[str] = []
            for r in results["checks"]:
                agg = " (aggregate)" if r.get("aggregate") else ""
                st.write(f"**{r['check_id']}** — {r.get('type','')} — failures: {r['failures']}{agg}")
                summary_lines.append(f"{r['check_id']} — {r.get('type','')} — failures: {r['failures']}{agg}")
                if r.get("sample"):
                    st.dataframe(r["sample"])
            if summary_lines:
                remember("info", "Run Now results:\n" + "\n".join(summary_lines))

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

# ---------- Sidebar + routing ----------
state = get_state()
with st.sidebar:
    st.header("Zeus DQ")
    if st.button("🏠 Overview"): st.session_state["page"] = "home"; st.session_state["cfg_mode"] = "list"
    if st.button("⚙️ Configurations"): st.session_state["page"] = "cfg"
    st.divider()
    run_as_role = st.text_input("RUN_AS_ROLE", value=state.get("run_as_role") or "")
    dmf_role = st.text_input("DMF_ROLE", value=state.get("dmf_role") or "")
    set_state(run_as_role or None, dmf_role or None)

if st.session_state.get("page","home") == "cfg":
    if st.session_state.get("cfg_mode","list") == "list":
        render_config_list()
    else:
        render_config_editor()
else:
    render_home()
