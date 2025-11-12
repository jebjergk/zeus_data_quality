"""UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Controls (top to bottom):
1. Header row with two columns sized `[1, 8]`.  Left column must contain the "⬅ Back" button wired to return to the list view.  Right column must render the header "Edit Configuration" when editing or "Create Configuration" when creating.
2. When profile suggestions are applied, show a single success alert summarising rows profiled and sample percentage exactly as implemented; no additional banners precede the Target section.
3. Subheader "Target" with the stateless table picker (database, schema, table) capturing the selected fully qualified name into `editor_target_fqn`.  Immediately below, display the caption `Target Table: <value>` where `<value>` resolves to the selected table or "— not selected —".
4. Heading "### Columns" followed by a multiselect labelled "Columns to check" listing available table columns.  The informational message "Table-level checks **FRESHNESS** and **ROW_COUNT_ANOMALY** are automatically included." must follow directly beneath the multiselect.
5. Configuration form `cfg_form` containing elements in this fixed order:
   a. Subheader "Configuration".
   b. Disabled text input "Name" with automatic derivation help text.  No manual name entry control may be added.
   c. Text area "Description" allowing optional free-form notes.
   d. For each selected column, render an expander titled `Column: <column>` (collapsed by default).  Inside each expander, controls must appear exactly as follows:
      • Number input "Sample failing rows for <column>" (0–1000, default 10).
      • Checkbox "UNIQUE".  When checked, show the `Ignore NULLs` checkbox (defaults True) followed by the selectbox `Severity (UNIQUE)` with options `ERROR`, `WARN` in that order.
      • Checkbox "NULL_COUNT".  When checked, show number input `Max NULL rows` (minimum 0) then selectbox `Severity (NULL_COUNT)` with options `ERROR`, `WARN`.
      • Checkbox "MIN_MAX".  When checked, show text inputs `Min (inclusive)` and `Max (inclusive)` (both default empty strings) followed by selectbox `Severity (MIN_MAX)` with options `ERROR`, `WARN`.
      • Checkbox "WHITESPACE".  When checked, show selectbox `Mode` with the three options `NO_LEADING_TRAILING`, `NO_INTERNAL_ONLY_WHITESPACE`, `NON_EMPTY_TRIMMED` in that exact order, followed by selectbox `Severity (WHITESPACE)` with options `ERROR`, `WARN`.
      • Checkbox "FORMAT_DISTRIBUTION".  When checked, show text input `Regex (Snowflake RLIKE)`, number input `Min match ratio (0-1)` (range 0.0–1.0, step 0.01), then selectbox `Severity (FORMAT_DISTRIBUTION)` with options `ERROR`, `WARN`.
      • Checkbox "VALUE_DISTRIBUTION".  When checked, show text input `Allowed values (CSV)`, number input `Min in-set ratio (0-1)` (range 0.0–1.0, step 0.01), then selectbox `Severity (VALUE_DISTRIBUTION)` with options `ERROR`, `WARN`.
   e. Heading "### Table-level checks (always included)" with the text input `Timestamp column for table checks`, caption explaining failure behaviour, and number input `Freshness max age (minutes)` (range 1–10080, step 30).  The values must synchronise with session state as in code.
   f. `st.form_submit_button` labelled "Preview last 60 days row counts" of type `secondary`.
   g. Heading "### Schedule" followed by checkbox `Enable daily task` (with explanatory help text), text input `Cron expression`, and text input `Timezone`.  The cron and timezone inputs are disabled when scheduling is unchecked.
   h. Final row of four submit buttons laid out in columns `[1,1,1,1]`: `Save & Apply` (primary by default), `Save as Draft`, `Run Now`, and `Delete` (secondary styling).
6. When preview is requested and prerequisites are satisfied, render a dataframe showing the columns `day` and `cnt` in that order with `use_container_width=True`, `hide_index=True`, and `height=320`.  No charts or alternative layouts are permitted.
7. Post-submit handling must reuse the existing success, warning, and info messaging pattern; no extra notifications precede or replace them.

Forbidden patterns:
• Do not reorder sections or controls listed above.
• Do not introduce additional check types, severity dropdowns, or per-column widgets beyond those specified.
• Do not add new scheduling controls, task toggles, or preview visualisations.
• Do not allow manual editing of the configuration name or target caption formatting.
"""

import logging, streamlit as st

st.session_state.setdefault("_rerun_count", 0)
st.session_state["_rerun_count"] += 1
_active_route = st.session_state.get("active_view")
_active_fqn = st.session_state.get("editor_target_fqn")
_RERUN_TELEMETRY_LINE = (
    f"rerun #{st.session_state['_rerun_count']} route={_active_route} fqn={_active_fqn}"
)
logging.info(
    "rerun #%s route=%s fqn=%s",
    st.session_state["_rerun_count"],
    _active_route,
    _active_fqn,
)

import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logging.getLogger("snowflake").setLevel(logging.WARNING)

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

ALLOWED_PAGES = {"home", "cfg", "profile", "monitor", "docs"}

if "active_view" not in st.session_state:
    st.session_state["active_view"] = "home"  # or your desired start view
    logging.info("route:init %s", st.session_state["active_view"])

st.session_state.setdefault("freeze_view", False)
if st.session_state["freeze_view"]:
    st.session_state["active_view"] = "profile"

if "_last_query_page" not in st.session_state:
    st.session_state["_last_query_page"] = st.session_state["active_view"]

if "page" not in st.session_state:
    st.session_state["page"] = st.session_state["active_view"]


def set_view(view: str) -> None:
    """Update the active view explicitly via user navigation."""
    st.session_state["active_view"] = view
    logging.info("route:set %s", view)

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
    _q, _parse_relation_name,
    list_configs, get_config, get_checks,
    list_columns,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.state import get_state, set_state
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check
from utils.configs import get_metadata_namespace, get_proc_name
from utils.flags import DEBUG_PROFILING
from views import profile_view
from views.table_picker import stateless_table_picker, session_cache_token
from views.docs_view import render_docs as render_docs_view
from views.config_editor import render_row_count_preview

METADATA_DB, METADATA_SCHEMA = get_metadata_namespace()
PROC_NAME = get_proc_name()
RUN_RESULTS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_RUN_RESULTS"
# Derive metadata table FQNs locally to avoid NameError
CONFIGS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CONFIG"
CHECKS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CHECK"
# RUN_RESULTS_TBL already defined above

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

try:
    _debug_param = st.query_params.get("debug")  # type: ignore[attr-defined]
except Exception:
    _debug_param = None

if isinstance(_debug_param, list):
    _debug_candidate = next(
        (item for item in _debug_param if isinstance(item, str)), None
    )
elif isinstance(_debug_param, str):
    _debug_candidate = _debug_param
else:
    _debug_candidate = None

_debug_value = (_debug_candidate or "").strip().lower()
_debug_override = _debug_value in {"1", "true", "yes"}
DEBUG_PROFILING_ENABLED = bool(DEBUG_PROFILING or _debug_override)

# Propagate the effective debug flag to the profile view so diagnostics honour
# the query parameter override without requiring signature changes.
profile_view.DEBUG_PROFILING_OVERRIDE = DEBUG_PROFILING_ENABLED  # type: ignore[attr-defined]
profile_view.DEBUG_PROFILING = DEBUG_PROFILING_ENABLED

if DEBUG_PROFILING_ENABLED:
    st.caption(_RERUN_TELEMETRY_LINE)

# ---------- Styling (simple Snowflake-ish) ----------
st.markdown("""
<style>
.sf-hr { height:1px; background:#e7ebf3; border:0; margin=.6rem 0 1rem 0; }
.badge { display:inline-block; padding=.15rem .55rem; border-radius:999px; font-size=.75rem; font-weight:600;
 background:#e5f6fd; color:#055e86; border:1px solid #cbeefb; }
.badge-green { background:#eafaf0; border-color:#d4f2df; color:#0a5c2b; }
.badge-gray { background:#f3f4f6; border-color:#e5e7eb; color:#374151; }
.badge-red { background:#fef2f2; border-color:#fee2e2; color:#7f1d1d; }
.badge-blue { background:#e0f2fe; border-color:#bae6fd; color:#1d4ed8; }
.card { border:1px solid #e7ebf3; border-radius:12px; padding=.9rem 1rem; background:#fff; box-shadow:0 1px 2px rgba(12,18,28,.04); }
.small { font-size:.85rem; color:#6b7280; }
.kv { color:#111827; font-weight:600; }
.metrics-grid { display:flex; flex-wrap:wrap; gap:.6rem 1.2rem; margin-top:.65rem; }
.metric { font-size:.85rem; color:#6b7280; }
.metric-label { font-weight:500; text-transform:uppercase; letter-spacing:.02em; font-size:.7rem; margin-bottom:.15rem; display:block; }
.metric-value { color:#111827; font-weight:600; }
.metric-value .badge { margin-right:.4rem; }
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


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "asDict"):
        return {str(k).lower(): v for k, v in row.asDict().items()}
    try:
        return {str(i): row[i] for i in range(len(row))}  # type: ignore[index]
    except Exception:
        return {}


def _format_timestamp(value: Any) -> str:
    if value is None:
        return "—"
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime()
        except Exception:
            pass
    if isinstance(value, datetime):
        fmt = "%Y-%m-%d %H:%M %Z" if value.tzinfo else "%Y-%m-%d %H:%M"
        formatted = value.strftime(fmt).strip()
        return formatted or value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def _get_page_from_query_params() -> Optional[str]:
    candidate: Optional[str] = None
    try:
        params = dict(st.query_params)  # type: ignore[attr-defined]
    except Exception:
        params = {}
    value = params.get("page") if isinstance(params, dict) else None
    if isinstance(value, list):
        candidate = next((item for item in value if isinstance(item, str)), None)
    elif isinstance(value, str):
        candidate = value
    if candidate:
        candidate_lower = candidate.lower()
        if candidate_lower in ALLOWED_PAGES:
            return candidate_lower
    return None


def navigate_to(page: str) -> None:
    """Update the current page selection in session state."""
    set_view(page)
    st.session_state["page"] = page
    st.session_state["_last_query_page"] = page
    try:
        current = dict(st.query_params)  # type: ignore[attr-defined]
    except Exception:
        current = {}
    current["page"] = page
    try:
        st.query_params = current  # type: ignore[attr-defined]
    except Exception:
        pass
    if page == "home":
        st.session_state["cfg_mode"] = "list"


def open_config_editor(
    config_id: Optional[str] = None, target_fqn: Optional[str] = None
) -> None:
    """Switch to the configuration editor with the given selection."""
    st.session_state["cfg_mode"] = "edit"
    st.session_state["selected_config_id"] = config_id
    if target_fqn is not None:
        st.session_state["editor_target_fqn"] = target_fqn
    st.rerun()


def _list_columns_cached(session_obj, database: str, schema: str, table: str) -> List[str]:
    if not session_obj or not (database and schema and table):
        return []

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_columns(cache_token: Tuple[str, str, str, str]) -> List[str]:
        _, db_name, schema_name, table_name = cache_token
        try:
            return list_columns(session_obj, db_name, schema_name, table_name)
        except Exception as exc:
            st.error(f"Failed to list columns for {db_name}.{schema_name}.{table_name}: {exc}")
            return []

    return _load_columns((session_cache_token(session_obj), database, schema, table))


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
    results_table = _q(RUN_RESULTS_TBL)
    checks_table = _q(CHECKS_TBL)

    last_run_by_config: Dict[str, Dict[str, Any]] = {}
    last_run_available = True
    recent_failures_by_config: Dict[str, int] = {}
    recent_failures_available = True
    column_checks_by_config: Dict[str, int] = {}
    checks_available = True

    if session:
        try:
            df = session.sql(
                f"""
                SELECT CONFIG_ID, RUN_TS, OK
                FROM {results_table}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY CONFIG_ID ORDER BY RUN_TS DESC) = 1
                """
            )
            for row in df.collect():
                data = _row_to_dict(row)
                config_id = data.get("config_id")
                if not config_id:
                    continue
                last_run_by_config[config_id] = {
                    "run_ts": data.get("run_ts"),
                    "ok": data.get("ok"),
                }
        except Exception:
            last_run_available = False

        try:
            df = session.sql(
                f"""
                SELECT CONFIG_ID, COUNT(*) AS FAILURE_COUNT
                FROM {results_table}
                WHERE COALESCE(OK, FALSE) = FALSE
                  AND RUN_TS >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                GROUP BY CONFIG_ID
                """
            )
            for row in df.collect():
                data = _row_to_dict(row)
                config_id = data.get("config_id")
                if not config_id:
                    continue
                count = data.get("failure_count")
                if isinstance(count, (int, float)):
                    recent_failures_by_config[config_id] = int(count)
        except Exception:
            recent_failures_available = False

        try:
            df = session.sql(
                f"""
                SELECT CONFIG_ID, COUNT(*) AS COLUMN_CHECKS
                FROM {checks_table}
                WHERE COLUMN_NAME IS NOT NULL
                GROUP BY CONFIG_ID
                """
            )
            for row in df.collect():
                data = _row_to_dict(row)
                config_id = data.get("config_id")
                if not config_id:
                    continue
                count = data.get("column_checks")
                if isinstance(count, (int, float)):
                    column_checks_by_config[config_id] = int(count)
        except Exception:
            checks_available = False

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
            or q in (("enabled" if _normalize_bool(getattr(cfg, "schedule_enabled", False)) else "disabled"))
        ]

        if not cfgs:
            st.info("No configurations match your search.")
            return

    for i, cfg in enumerate(cfgs):
        status_value = (cfg.status or "").upper() or "—"
        active = status_value == "ACTIVE"
        status_badge_class = "badge-green" if active else "badge-gray"
        status_badge = f"<span class='badge {status_badge_class}'>{status_value}</span>"
        enabled = _normalize_bool(getattr(cfg, "schedule_enabled", False))
        enabled_badge = (
            "<span class='badge badge-blue'>Enabled</span>"
            if enabled
            else "<span class='badge badge-gray'>Disabled</span>"
        )

        last_run_info = last_run_by_config.get(cfg.config_id) if last_run_available else None
        if last_run_info:
            ts_display = _format_timestamp(last_run_info.get("run_ts"))
            ok_value = last_run_info.get("ok")
            if ok_value is True:
                last_run_badge = "<span class='badge badge-green'>✅ OK</span>"
            elif ok_value is False:
                last_run_badge = "<span class='badge badge-red'>❌ Fail</span>"
            else:
                last_run_badge = ""
            if last_run_badge:
                last_run_value = f"{last_run_badge}<span class='kv'>{ts_display}</span>"
            else:
                last_run_value = f"<span class='kv'>{ts_display}</span>"
        else:
            last_run_value = "—"

        if recent_failures_available:
            failure_count = recent_failures_by_config.get(cfg.config_id, 0)
            recent_failures_value = f"<span class='kv'>{failure_count}</span>"
        else:
            recent_failures_value = "—"

        if checks_available:
            column_checks = column_checks_by_config.get(cfg.config_id, 0)
            total_checks = column_checks + 2
            checks_value = f"<span class='kv'>{total_checks}</span>"
        else:
            checks_value = "—"

        cron_text = cfg.schedule_cron or "—"
        tz_text = cfg.schedule_timezone or "—"
        if cron_text == "—" and tz_text == "—":
            schedule_text = "—"
        else:
            schedule_text = f"{cron_text} ({tz_text})"
        if schedule_text != "—":
            schedule_value = f"<span class='kv'>{schedule_text}</span> {enabled_badge}"
        else:
            schedule_value = f"— {enabled_badge}"

        run_as_role_value = cfg.run_as_role or "—"
        run_as_role_display = (
            f"<span class='kv'>{run_as_role_value}</span>"
            if run_as_role_value != "—"
            else "—"
        )

        metrics = [
            ("Last run", last_run_value),
            ("Recent failures (7d)", recent_failures_value),
            ("Checks", checks_value),
            ("Schedule", schedule_value),
            ("Run-as role", run_as_role_display),
        ]

        metrics_html = "<div class='metrics-grid'>" + "".join(
            f"<div class='metric'><span class='metric-label'>{label}</span><div class='metric-value'>{value}</div></div>"
            for label, value in metrics
        ) + "</div>"

        st.markdown("<div class='card'>", unsafe_allow_html=True)
        main_col, action_col = st.columns([7, 2])
        with main_col:
            display_name = cfg.name or cfg.config_id
            st.markdown(
                f"<div class='card-title'><strong>{display_name}</strong> {status_badge}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='small'>ID: <span class='kv'>{cfg.config_id}</span></div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='small' style='margin-top:.4rem;'>Table:<br><span class='kv'>{cfg.target_table_fqn}</span></div>",
                unsafe_allow_html=True,
            )
            st.markdown(metrics_html, unsafe_allow_html=True)
        with action_col:
            edit_col, delete_col = st.columns(2)
            with edit_col:
                if st.button("✏️ Edit", key=f"edit_{cfg.config_id}"):
                    open_config_editor(cfg.config_id, cfg.target_table_fqn)
            with delete_col:
                if st.button("🗑️ Delete", key=f"del_{cfg.config_id}"):
                    out = delete_config_full(session, cfg.config_id)
                    msg = f"Deleted `{cfg.name}` — dropped {len(out.get('dmfs_dropped', []))} view(s)."
                    st.success(msg)
                    st.session_state["last_notices"] = [{"type": "success", "message": msg}]
                    st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        if i < len(cfgs) - 1:
            st.markdown("<div class='sf-hr'></div>", unsafe_allow_html=True)

def render_config_editor():
    # which config?
    sel_id: Optional[str] = st.session_state.get("selected_config_id")
    cfg = get_config(session, sel_id) if sel_id else None
    existing_checks = get_checks(session, sel_id) if sel_id else []

    suggestion_payload = st.session_state.pop("profile_suggestion", None)
    suggestion_summary = suggestion_payload.get("summary") if suggestion_payload else None
    if suggestion_payload:
        target = suggestion_payload.get("target_table")
        if target:
            st.session_state["editor_target_fqn"] = target
        suggested_columns = suggestion_payload.get("columns") or {}
        if suggested_columns:
            st.session_state["dq_cols_ms"] = list(suggested_columns.keys())
        for col_name, col_cfg in suggested_columns.items():
            sk = _keyify(col_name)
            st.session_state[f"samp_{sk}"] = int(col_cfg.get("sample_rows", 25))
            checks_cfg = col_cfg.get("checks") or {}
            for check_name, check_conf in checks_cfg.items():
                check_upper = (check_name or "").upper()
                params = check_conf.get("params", {})
                severity = check_conf.get("severity", "ERROR")
                if check_upper == "UNIQUE":
                    st.session_state[f"{sk}_chk_unique"] = True
                    st.session_state[f"{sk}_p_un_ignore"] = bool(params.get("ignore_nulls", True))
                    st.session_state[f"{sk}_sev_unique"] = severity
                elif check_upper == "NULL_COUNT":
                    st.session_state[f"{sk}_chk_nullcount"] = True
                    st.session_state[f"{sk}_p_nc_max"] = int(params.get("max_nulls", 0))
                    st.session_state[f"{sk}_sev_null"] = severity
                elif check_upper == "MIN_MAX":
                    st.session_state[f"{sk}_chk_minmax"] = True
                    st.session_state[f"{sk}_p_mm_min"] = str(params.get("min", ""))
                    st.session_state[f"{sk}_p_mm_max"] = str(params.get("max", ""))
                    st.session_state[f"{sk}_sev_mm"] = severity
                elif check_upper == "WHITESPACE":
                    st.session_state[f"{sk}_chk_ws"] = True
                    st.session_state[f"{sk}_p_ws_mode"] = params.get("mode", "NO_LEADING_TRAILING")
                    st.session_state[f"{sk}_sev_ws"] = severity
                elif check_upper == "VALUE_DISTRIBUTION":
                    st.session_state[f"{sk}_chk_val"] = True
                    st.session_state[f"{sk}_p_val_csv"] = params.get("allowed_values_csv", "")
                    st.session_state[f"{sk}_p_val_ratio"] = float(params.get("min_match_ratio", 0.8))
                    st.session_state[f"{sk}_sev_val"] = severity
        suggested_table = suggestion_payload.get("table") or {}
        freshness_payload = suggested_table.get("FRESHNESS") or {}
        freshness_params = freshness_payload.get("params") or {}
        ts_candidate = freshness_params.get("timestamp_column")
        if isinstance(ts_candidate, str) and ts_candidate:
            st.session_state["_dq_table_ts_col"] = ts_candidate
        max_age_candidate = freshness_params.get("max_age_minutes")
        if max_age_candidate is not None:
            try:
                st.session_state["_dq_table_max_age"] = int(max_age_candidate)
            except (TypeError, ValueError):
                pass
        rowcount_payload = suggested_table.get("ROW_COUNT_ANOMALY") or {}
        rowcount_params_raw = rowcount_payload.get("params") or {}
        if rowcount_params_raw:
            rc_params: Dict[str, Any] = {}
            for key in ("timestamp_column", "lookback_days", "sensitivity", "min_history_days"):
                if key in rowcount_params_raw and rowcount_params_raw[key] is not None:
                    rc_params[key] = rowcount_params_raw[key]
            if rc_params:
                st.session_state["_dq_rowcount_params"] = rc_params
                ts_from_rc = rc_params.get("timestamp_column")
                if isinstance(ts_from_rc, str) and ts_from_rc and "_dq_table_ts_col" not in st.session_state:
                    st.session_state["_dq_table_ts_col"] = ts_from_rc

    # Header
    back, title = st.columns([1, 8])
    with back:
        if st.button("⬅ Back"):
            st.session_state["cfg_mode"] = "list"
            st.rerun()
    with title:
        st.header("Edit Configuration" if cfg else "Create Configuration")
    if suggestion_payload:
        summary_parts = []
        if suggestion_summary:
            rows = suggestion_summary.get("rows_profiled")
            sample_pct = suggestion_summary.get("sample_pct")
            if rows is not None:
                summary_parts.append(f"rows profiled: {rows:,}")
            if sample_pct is not None:
                summary_parts.append(f"sample: {float(sample_pct):.1f}%")
        details = f" ({', '.join(summary_parts)})" if summary_parts else ""
        st.success(f"Applied profile suggestion{details}. Review the recommended checks below.")

    # Target (picker is stateless, we persist a single FQN)
    st.subheader("Target")
    base_fqn = st.session_state.get("editor_target_fqn") or (cfg.target_table_fqn if cfg else None)
    db_sel, sch_sel, tbl_sel, target_table = stateless_table_picker(session, base_fqn)
    if target_table:
        st.session_state["editor_target_fqn"] = target_table
    st.caption(f"Target Table: {target_table or '— not selected —'}")

    # Columns (outside form). Sanitize defaults to avoid widget errors.
    available_cols = (
        _list_columns_cached(session, db_sel, sch_sel, tbl_sel)
        if (db_sel and sch_sel and tbl_sel)
        else []
    )
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
    existing_table_params: Dict[str, Dict[str, Any]] = {}
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

            existing_table_params[key] = parsed_params or {}

    if "_dq_rowcount_params" in st.session_state:
        stored_params = st.session_state.get("_dq_rowcount_params") or {}
        existing_table_params["ROW_COUNT_ANOMALY"] = {
            **existing_table_params.get("ROW_COUNT_ANOMALY", {}),
            **stored_params,
        }

    session_ts_col = st.session_state.get("_dq_table_ts_col")
    session_max_age = st.session_state.get("_dq_table_max_age")
    if session_ts_col or session_max_age is not None:
        freshness_entry = existing_table_params.setdefault("FRESHNESS", {})
        if session_ts_col and "timestamp_column" not in freshness_entry:
            freshness_entry["timestamp_column"] = session_ts_col
        if session_max_age is not None and "max_age_minutes" not in freshness_entry:
            try:
                freshness_entry["max_age_minutes"] = int(session_max_age)
            except (TypeError, ValueError):
                pass

    freshness_defaults = existing_table_params.get("FRESHNESS", {})
    ts_default = (
        (session_ts_col if isinstance(session_ts_col, str) and session_ts_col else None)
        or freshness_defaults.get("timestamp_column")
        or legacy_row_count_params.get("timestamp_column")
        or "LOAD_TIMESTAMP"
    )
    max_age_source: Any = session_max_age if session_max_age is not None else freshness_defaults.get("max_age_minutes")
    if max_age_source is None:
        max_age_source = 1920
    try:
        max_age_default = int(max_age_source)
    except (TypeError, ValueError):
        max_age_default = 1920

    rowcount_defaults = existing_table_params.get("ROW_COUNT_ANOMALY") or {}
    if not rowcount_defaults:
        rowcount_defaults = {}
    rowcount_defaults.setdefault("timestamp_column", ts_default)
    rowcount_defaults.setdefault("lookback_days", 28)
    rowcount_defaults.setdefault("sensitivity", 3.0)
    rowcount_defaults.setdefault("min_history_days", 7)
    existing_table_params["ROW_COUNT_ANOMALY"] = rowcount_defaults

    current_cfg_id = getattr(cfg, "config_id", None)
    if st.session_state.get("_dq_table_cfg_id") != current_cfg_id:
        st.session_state["_dq_table_max_age"] = max_age_default
        st.session_state["_dq_table_cfg_id"] = current_cfg_id

    if target_table:
        last_target = st.session_state.get("_dq_table_ts_target")
        if last_target != target_table:
            st.session_state["_dq_table_ts_col"] = ts_default
            st.session_state["_dq_table_max_age"] = max_age_default
            st.session_state["_dq_table_ts_target"] = target_table
    if "_dq_table_ts_col" not in st.session_state:
        st.session_state["_dq_table_ts_col"] = ts_default
    if "_dq_table_max_age" not in st.session_state:
        st.session_state["_dq_table_max_age"] = max_age_default

    preview_counts = False
    table_check_error: Optional[str] = None

    derived_name = target_table or ""
    if not derived_name and cfg:
        derived_name = cfg.target_table_fqn or cfg.name or ""

    with st.form("cfg_form", clear_on_submit=False):
        st.subheader("Configuration")
        name = derived_name
        st.text_input("Name", value=name, disabled=True, help="Automatically derived from the selected database, schema, and table.")
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
        st.caption("Table will FAIL if no data arrives within the configured max age or if today's volume is a statistical outlier.")

        fr_max_age = st.number_input(
            "Freshness max age (minutes)",
            min_value=1,
            max_value=10080,
            value=int(st.session_state.get("_dq_table_max_age", max_age_default)),
            step=30,
        )
        st.session_state["_dq_table_max_age"] = int(fr_max_age)

        preview_counts = st.form_submit_button(
            "Preview last 60 days row counts",
            type="secondary",
            help="Preview daily row counts using the selected timestamp column.",
        )

        if target_table:
            fr_params = {"timestamp_column": ts_col, "max_age_minutes": int(fr_max_age)}
            try:
                fr_rule, fr_is_agg = build_rule_for_table_check(target_table, "FRESHNESS", fr_params)
            except ValueError as exc:
                table_check_error = f"Invalid freshness configuration: {exc}"
            else:
                check_rows.append(DQCheck(
                    config_id=(cfg.config_id if cfg else "temp"),
                    check_id="TABLE_FRESHNESS",
                    table_fqn=target_table, column_name=None,
                    rule_expr=(f"AGG: {fr_rule}" if fr_is_agg else fr_rule), severity="ERROR",
                    sample_rows=0, check_type="FRESHNESS",
                    params_json=json.dumps(fr_params)
                ))

                row_defaults = existing_table_params.get("ROW_COUNT_ANOMALY", {}) or {}
                try:
                    lookback_days = int(row_defaults.get("lookback_days", 28))
                except (TypeError, ValueError):
                    lookback_days = 28
                try:
                    sensitivity = float(row_defaults.get("sensitivity", 3.0))
                except (TypeError, ValueError):
                    sensitivity = 3.0
                try:
                    min_history_days = int(row_defaults.get("min_history_days", 7))
                except (TypeError, ValueError):
                    min_history_days = 7
                anomaly_params = {
                    "timestamp_column": ts_col or row_defaults.get("timestamp_column") or ts_default,
                    "lookback_days": lookback_days,
                    "sensitivity": sensitivity,
                    "min_history_days": min_history_days,
                }
                try:
                    anomaly_rule, anomaly_is_agg = build_rule_for_table_check(target_table, "ROW_COUNT_ANOMALY", anomaly_params)
                except ValueError as exc:
                    table_check_error = f"Invalid row count anomaly configuration: {exc}"
                else:
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id="TABLE_ROW_COUNT_ANOMALY",
                        table_fqn=target_table, column_name=None,
                        rule_expr=(f"AGG: {anomaly_rule}" if anomaly_is_agg else anomaly_rule), severity="ERROR",
                        sample_rows=0, check_type="ROW_COUNT_ANOMALY",
                        params_json=json.dumps(anomaly_params)
                    ))

        st.markdown("### Schedule")
        existing_cron = getattr(cfg, "schedule_cron", None) if cfg else None
        existing_timezone = getattr(cfg, "schedule_timezone", None) if cfg else None
        existing_enabled = getattr(cfg, "schedule_enabled", True) if cfg else True
        default_cron = existing_cron or "0 8 * * *"
        default_timezone = existing_timezone or "Europe/Berlin"
        schedule_enabled = st.checkbox(
            "Enable daily task",
            value=bool(existing_enabled),
            help=(
                "When saving as Draft, scheduling is always disabled and any existing task is "
                "suspended. Enable daily task only applies when you Save & Apply."
            ),
        )
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

    if table_check_error:
        st.error(table_check_error)

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
            render_row_count_preview(df)

    # After submit
    if apply_now or save_draft or run_now_btn or delete_btn:
        if (apply_now or save_draft or run_now_btn) and table_check_error:
            st.error(table_check_error)
            return
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
        if apply_now:
            status = 'ACTIVE'
        elif save_draft:
            status = 'DRAFT'
        else:
            status = (cfg.status if cfg and cfg.status else 'DRAFT')
        state = get_state()
        dq_cfg = DQConfig(
            config_id=new_id, name=name or None, description=(desc or None),
            target_table_fqn=target_table, run_as_role=(state.get('run_as_role') or None),
            dmf_role=(state.get('dmf_role') or None), status=status, owner=None,
            schedule_cron=(cron_expr.strip() if cron_expr else "0 8 * * *"),
            schedule_timezone=(timezone_expr.strip() if timezone_expr else "Europe/Berlin"),
            schedule_enabled=(False if save_draft else bool(schedule_enabled))
        )
        if not dq_cfg.name:
            err_msg = "Select a database, schema, and table to generate a configuration name before saving."
            st.error(err_msg)
            remember("error", err_msg)
            return

        normalized_target = (dq_cfg.target_table_fqn or "").strip().lower()
        if normalized_target:
            existing_cfgs = list_configs(session)
            conflict = next(
                (
                    existing
                    for existing in existing_cfgs
                    if (existing.target_table_fqn or "").strip().lower() == normalized_target
                    and existing.config_id != dq_cfg.config_id
                ),
                None,
            )
            if conflict:
                err_msg = (
                    f"A configuration for `{dq_cfg.target_table_fqn}` already exists "
                    f"(ID: {conflict.config_id}). Edit the existing configuration or choose a different table."
                )
                st.error(err_msg)
                remember("error", err_msg)
                return
        # rebind ids
        checks_rebound: List[DQCheck] = []
        for cr in check_rows:
            cr.config_id = new_id; cr.table_fqn = target_table
            checks_rebound.append(cr)

        out = save_config_and_checks(session, dq_cfg, checks_rebound, apply_now=apply_now)
        base_msg = f"Saved config {new_id} ({status})."
        st.success(base_msg)
        remember("success", base_msg)
        if save_draft:
            suspend_result = schedules.suspend_task_for_config(session, dq_cfg.config_id)
            suspend_status = suspend_result.get("status")
            if suspend_status == "FALLBACK":
                warn_msg = (
                    f"Failed to suspend task {suspend_result.get('task') or task_name_for_config(dq_cfg.config_id)}: "
                    f"{suspend_result.get('reason')}"
                )
                st.warning(warn_msg)
                remember("warning", warn_msg)
            else:
                info_msg = (
                    "Saved as Draft. Scheduling is disabled and any existing task has been suspended."
                )
                st.info(info_msg)
                remember("info", info_msg)
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
                    try:
                        result_df = run_task_now(
                            session,
                            METADATA_DB,
                            METADATA_SCHEMA,
                            dq_cfg.config_id,
                            proc_name=PROC_NAME,
                        )
                    except Exception as exc:
                        err_msg = f"Failed to trigger task run: {exc}"
                        st.error(err_msg)
                        remember("error", err_msg)
                    else:
                        result_details = None
                        if result_df is not None and not result_df.empty:
                            first_row = result_df.iloc[0]
                            for value in first_row.tolist():
                                if value:
                                    result_details = str(value)
                                    break
                        success_msg = (
                            f"Ran `{PROC_NAME}` for config `{dq_cfg.config_id}`."
                        )
                        if result_details:
                            success_msg = f"{success_msg} Result: {result_details}"
                        st.success(success_msg)
                        remember("success", success_msg)

        if apply_now and status == 'ACTIVE':
            if not dq_cfg.schedule_enabled:
                suspend_result = schedules.suspend_task_for_config(session, dq_cfg.config_id)
                suspend_status = suspend_result.get("status")
                if suspend_status == "FALLBACK":
                    warn_msg = (
                        f"Failed to suspend task {suspend_result.get('task') or task_name_for_config(dq_cfg.config_id)}: "
                        f"{suspend_result.get('reason')}"
                    )
                    st.warning(warn_msg)
                    remember("warning", warn_msg)
                else:
                    task_label = suspend_result.get("task") or task_name_for_config(dq_cfg.config_id)
                    if suspend_status == "NOT_FOUND":
                        success_msg = (
                            "Task scheduling disabled. No existing task was found, so nothing was suspended."
                        )
                    else:
                        success_msg = f"Task scheduling disabled. Suspended **{task_label}**."
                    st.success(success_msg)
                    remember("success", success_msg)
            else:
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
                        if meta_db and meta_schema:
                            preflight_requirements(
                                session,
                                meta_db,
                                meta_schema,
                                proc_name=PROC_NAME,
                                arg_sig="(VARCHAR)",
                            )
                            preflight_requirements(
                                session,
                                meta_db,
                                meta_schema,
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

    if (
        "_mon_config_options" not in st.session_state
        or st.session_state.get("_mon_config_options") != config_labels
    ):
        previous_selection = st.session_state.get("mon_configs", [])
        updated_selection = [label for label in previous_selection if label in config_labels]
        if not updated_selection:
            updated_selection = config_labels.copy()
        st.session_state["mon_configs"] = updated_selection
        st.session_state["_mon_config_options"] = config_labels.copy()
    elif "mon_configs" not in st.session_state:
        st.session_state["mon_configs"] = config_labels.copy()

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
            key="mon_configs",
        )

    selected_config_ids = [config_map[label] for label in selected_labels if label in config_map]

    results_table = _q(RUN_RESULTS_TBL)
    config_table = _q(CONFIGS_TBL)
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

    today = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)
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


def render_docs() -> None:
    render_docs_view(
        metadata_db=METADATA_DB,
        metadata_schema=METADATA_SCHEMA,
        proc_name=PROC_NAME,
        configs_table=CONFIGS_TBL,
        checks_table=CHECKS_TBL,
        run_results_table=RUN_RESULTS_TBL,
    )

# ---------- Sidebar + routing ----------
state = get_state()
st.session_state.setdefault("editor_target_fqn", None)
query_page = _get_page_from_query_params()
last_query_page = st.session_state.get("_last_query_page")
if query_page and query_page != last_query_page:
    st.session_state["_last_query_page"] = query_page
    if query_page != st.session_state.get("active_view"):
        set_view(query_page)
        st.session_state["page"] = query_page
elif query_page is None:
    if "_last_query_page" not in st.session_state:
        st.session_state["_last_query_page"] = st.session_state.get(
            "active_view", "home"
        )
    elif last_query_page is not None:
        st.session_state["_last_query_page"] = None

page_state_value = st.session_state.get("page")
if (
    page_state_value in ALLOWED_PAGES
    and page_state_value != st.session_state.get("active_view")
):
    navigate_to(page_state_value)
st.session_state.setdefault("cfg_mode", "list")
view = st.session_state.get("active_view", "home")
with st.sidebar:
    st.header("Zeus DQ")
    st.button(
        "🏠 Overview",
        use_container_width=True,
        type="primary" if view == "home" else "secondary",
        key="nav_home",
        on_click=navigate_to,
        args=("home",),
    )
    st.button(
        "⚙️ Configurations",
        use_container_width=True,
        type="primary" if view == "cfg" else "secondary",
        key="nav_cfg",
        on_click=navigate_to,
        args=("cfg",),
    )
    st.button(
        "🧪 Profile Table",
        use_container_width=True,
        type="primary" if view == "profile" else "secondary",
        key="nav_profile",
        on_click=navigate_to,
        args=("profile",),
    )
    st.button(
        "📊 Monitor",
        use_container_width=True,
        type="primary" if view == "monitor" else "secondary",
        key="nav_monitor",
        on_click=navigate_to,
        args=("monitor",),
    )
    st.button(
        "📘 Documentation",
        use_container_width=True,
        type="primary" if view == "docs" else "secondary",
        key="nav_docs",
        on_click=navigate_to,
        args=("docs",),
    )
    st.divider()
    if view == "cfg" and st.session_state.get("cfg_mode", "list") == "list":
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

view = st.session_state.get("active_view", "home")
if DEBUG_PROFILING:
    st.caption(
        "🛠 route="
        f"{view} "
        f"busy_prof={st.session_state.get('busy_profiling')} "
        f"busy_save={st.session_state.get('busy_saving')}"
    )
if view == "cfg":
    if st.session_state.get("cfg_mode","list") == "list":
        render_config_list()
    else:
        render_config_editor()
elif view == "profile":
    if DEBUG_PROFILING_ENABLED:
        st.caption("🛠️ Debug: entering Profile view")
        target_fqn = st.session_state.get("editor_target_fqn") or "—"
        st.caption(f"🧭 Target FQN: {target_fqn}")
    profile_view.render_profile(session, METADATA_DB, METADATA_SCHEMA)
elif view == "monitor":
    render_monitor()
elif view == "docs":
    render_docs()
else:
    render_home()
