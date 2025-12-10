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

import streamlit as st, logging

# Safe inits (no rendering)
st.session_state["_rerun_count"] = st.session_state.get("_rerun_count", 0) + 1
st.session_state.setdefault("active_view", "home")
st.session_state.setdefault("page", st.session_state["active_view"])  # keep if router uses 'page'
st.session_state.setdefault("freeze_view", False)

current_view = st.session_state.get("active_view", "home")

logging.info(
    "rerun #%s route=%s fqn=%s",
    st.session_state["_rerun_count"],
    current_view,
    st.session_state.get("editor_target_fqn"),
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

ALLOWED_PAGES = {"home", "cfg", "profile", "monitor", "docs", "rules"}

if st.session_state["_rerun_count"] == 1:
    logging.info("route:init %s", current_view)

if "_last_query_page" not in st.session_state:
    st.session_state["_last_query_page"] = current_view


def set_view(view: str) -> None:
    """Update the active view explicitly via user navigation."""
    if st.session_state.get("freeze_view"):
        logging.info("route:blocked while frozen (wanted=%s)", view)
        return
    st.session_state["active_view"] = view
    st.session_state["page"] = view
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
    get_library_checks, update_library_check, insert_library_check, delete_check_by_id,
    list_columns, list_tables,
)
from utils import schedules
from services.configs import save_config_and_checks, delete_config_full
from services.state import get_state, set_state
from services import profiling_v2
from services.rule_library import (
    RuleTemplate,
    LEGACY_RULE_KEY_MAP,
    active_rule_map,
    load_active_rules_from_library,
    load_rule_library,
    normalize_rule_key,
)
from utils.checkdefs import build_rule_for_column_check, build_rule_for_table_check
from utils.configs import get_metadata_namespace, get_proc_name
from utils.flags import DEBUG_PROFILING
from utils.version import build_sha, build_time
from views.profile_view import render_profile as render_profiling_view
from views.table_picker import stateless_table_picker, session_cache_token
from views.docs_view import render_docs as render_docs_view
from views.config_editor import render_row_count_preview
from views.rule_admin_view import render_rule_admin

METADATA_DB, METADATA_SCHEMA = get_metadata_namespace()
PROC_NAME = get_proc_name()
RUN_RESULTS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_RUN_RESULTS"
# Derive metadata table FQNs locally to avoid NameError
CONFIGS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CONFIG"
CHECKS_TBL = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CHECK"
# RUN_RESULTS_TBL already defined above

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

if DEBUG_PROFILING:
    st.caption(
        f"rerun #{st.session_state.get('_rerun_count')} "
        f"view={st.session_state.get('active_view')} "
        f"fqn={st.session_state.get('editor_target_fqn')}"
    )

# Hard override: if profiling is running, force Profile render and stop further dispatch
if st.session_state.get("freeze_view"):
    logging.info("dispatch:hard-freeze → profile")
    st.session_state["active_view"] = "profile"
    st.session_state["page"] = "profile"  # keep router key in sync
    from views.profile_view import render_profile

    render_profile()
    st.stop()  # end this rerun so no later code can change view

if DEBUG_PROFILING:
    st.caption(f"🧩 build={build_sha()} time={build_time()}")

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


def _parse_params(raw_value: Any) -> Dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, list):
        return {}
    try:
        return json.loads(raw_value) if raw_value else {}
    except Exception:
        return {}


def _normalize_param_schema(raw_schema: Any) -> List[Dict[str, Any]]:
    if raw_schema is None:
        return []
    if isinstance(raw_schema, str):
        try:
            raw_schema = json.loads(raw_schema)
        except Exception:
            return []

    normalized: List[Dict[str, Any]] = []
    if isinstance(raw_schema, list):
        for item in raw_schema:
            if isinstance(item, str):
                normalized.append({"name": item, "type": "STRING", "required": True})
            elif isinstance(item, dict):
                name = item.get("name")
                if not name:
                    continue
                normalized.append(
                    {
                        "name": name,
                        "type": item.get("type", "STRING"),
                        "required": bool(item.get("required", True)),
                    }
                )
    return normalized


def _render_param_inputs(
    *,
    key_prefix: str,
    param_schema: List[Dict[str, Any]],
    current_values: Dict[str, Any],
    column_options: List[str],
    table_options: Optional[List[str]] = None,
    column_lookup: Optional[Any] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    values: Dict[str, Any] = {}
    error: Optional[str] = None

    for field in param_schema:
        name = field.get("name")
        if not name:
            continue
        param_type = (field.get("type") or "STRING").upper()
        required = bool(field.get("required", True))
        default_value = current_values.get(name)

        input_key = f"{key_prefix}_{_keyify(name)}"
        if param_type == "STRING":
            val = st.text_input(name, value=str(default_value or ""), key=input_key)
            if required and not val.strip():
                error = error or f"Parameter '{name}' is required."
            values[name] = val
        elif param_type == "NUMBER":
            try:
                numeric_default = float(default_value) if default_value is not None else 0.0
            except Exception:
                numeric_default = 0.0
            val = st.number_input(name, value=numeric_default, key=input_key)
            values[name] = val
        elif param_type == "BOOLEAN":
            val = st.checkbox(name, value=bool(default_value), key=input_key)
            values[name] = val
        elif param_type == "FQN_TABLE":
            options = table_options or []
            default_text = str(default_value or "")
            if default_text and default_text not in options:
                options = [default_text] + options
            val = (
                st.selectbox(
                    name,
                    options=options or [default_text],
                    index=(options or [default_text]).index(default_text)
                    if default_text in (options or [default_text])
                    else 0,
                    key=input_key,
                )
                if options
                else st.text_input(name, value=default_text, key=input_key)
            )
            if required and not val.strip():
                error = error or f"Parameter '{name}' is required."
            values[name] = val
        elif param_type == "COLUMN_NAME":
            ref_table = values.get("ref_table") or current_values.get("ref_table")
            options = (
                column_lookup(ref_table)
                if column_lookup and ref_table
                else column_options or [""]
            )
            options = options or [""]
            default_index = options.index(default_value) if default_value in options else 0
            val = st.selectbox(name, options=options, index=default_index, key=input_key)
            values[name] = val
        elif param_type == "STRING_LIST":
            default_list: List[str] = []
            if isinstance(default_value, list):
                default_list = [str(v) for v in default_value]
            elif isinstance(default_value, str):
                default_list = [v.strip() for v in default_value.split(",") if v.strip()]
            joined_default = ", ".join(default_list)
            val = st.text_input(name, value=joined_default, key=input_key)
            values[name] = [v.strip() for v in val.split(",") if v.strip()]
            if required and not values[name]:
                error = error or f"Parameter '{name}' is required."
        else:
            st.write(f"Unsupported parameter type: {param_type}")
    return values, error


def _compile_library_rule(
    session: Any,
    rule_code: str,
    target_table_fqn: str,
    column_name: str,
    params: Dict[str, Any],
) -> Any:
    proc_name = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_COMPILE_RULE_SQL"
    return session.call(proc_name, rule_code, target_table_fqn, [column_name], params)


def _related_table_options(session_obj: Any, target_table_fqn: str) -> List[str]:
    db, schema, _ = _parse_relation_name(target_table_fqn or "")
    if not session_obj or not db or not schema:
        return []
    try:
        return list_tables(
            session_obj,
            database=db,
            schema=schema,
            editor_target_fqn=target_table_fqn,
        )
    except Exception:
        return []


def _columns_for_table(session_obj: Any, table_fqn: str) -> List[str]:
    db, schema, table = _parse_relation_name(table_fqn or "")
    if not session_obj or not db or not schema or not table:
        return []
    try:
        return list_columns(
            session_obj,
            database=db,
            schema=schema,
            table=table,
            editor_target_fqn=table_fqn,
        )
    except Exception:
        return []


def _summarize_rule(rule_code: str, params: Dict[str, Any]) -> str:
    """Build a compact rule summary from parameters."""

    code = (rule_code or "").upper()
    if not params:
        return "—"

    if code == "RANGE_CHECK":
        return f"[{params.get('min_value', '—')}–{params.get('max_value', '—')}]"
    if code == "ALLOWED_VALUES":
        values = params.get("allowed_values")
        if isinstance(values, list):
            preview = ", ".join(map(str, values[:5]))
            if len(values) > 5:
                preview += ", …"
            return f"[{preview}]" if preview else "—"
    if code == "IN_REFERENCE_TABLE":
        ref_table = params.get("ref_table")
        ref_col = params.get("key_column")
        if ref_table and ref_col:
            return f"{ref_table}({ref_col})"
    if "REGEX" in code or "PATTERN" in code:
        pattern = params.get("pattern") or params.get("regex")
        if pattern:
            return str(pattern)

    return ", ".join(f"{k}={v}" for k, v in params.items()) or "—"


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
    library_checks = get_library_checks(session, sel_id) if sel_id else []

    rule_templates = load_rule_library(
        session, METADATA_DB, METADATA_SCHEMA, include_inactive=True
    )
    rule_options = load_active_rules_from_library(session, METADATA_DB, METADATA_SCHEMA)
    active_rules = active_rule_map(rule_templates)
    active_rules_by_code = {t.rule_code.upper(): t for t in rule_templates if t.enabled and t.rule_code}
    all_rules_map: Dict[str, RuleTemplate] = {}
    for t in rule_templates:
        if t.rule_id:
            all_rules_map[t.rule_id.upper()] = t
        if t.rule_code:
            all_rules_map[t.rule_code.upper()] = t
    logging.info("dq_config: loaded %d rule options from DQ_RULE_LIBRARY", len(rule_options))

    if not rule_options:
        st.error(
            "No active rules available from DQ_RULE_LIBRARY. Please verify the DQ library configuration.",
        )

    rule_label_lookup = {
        str(rule.get("rule_key", "")).upper(): (rule.get("label") or "")
        for rule in rule_options
    }

    def _rule_key(raw_key: str) -> str:
        return normalize_rule_key(raw_key, active_rules)

    def _builder_key(rule_id: str, fallback: str) -> str:
        template = active_rules.get(rule_id) or all_rules_map.get(rule_id)
        return (template.check_type or template.rule_id) if template else fallback

    for chk in existing_checks:
        legacy_key = _rule_key(chk.check_type or "")
        if legacy_key and legacy_key not in rule_label_lookup:
            rule_label_lookup[legacy_key] = f"{legacy_key} (legacy)"

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
    table_locked = bool(existing_checks)
    db_sel, sch_sel, tbl_sel, target_table = stateless_table_picker(
        session, base_fqn, disabled=table_locked
    )
    if target_table:
        st.session_state["editor_target_fqn"] = target_table
    if table_locked:
        st.caption(
            "Table is locked because rules exist. Create a new configuration for a different table."
        )
    st.caption(f"Target Table: {target_table or '— not selected —'}")

    # Columns available for rules
    available_cols = (
        _list_columns_cached(session, db_sel, sch_sel, tbl_sel)
        if (db_sel and sch_sel and tbl_sel)
        else []
    )
    column_checks = [c for c in existing_checks if c.column_name]
    table_suggestions = _related_table_options(session, target_table)
    column_lookup = lambda tbl: _columns_for_table(session, tbl)

    st.markdown("### Rules")
    search_col, add_col = st.columns([4, 1])
    rule_search = search_col.text_input(
        "Search rules", key="rule_grid_search", placeholder="Search by column, rule, or code"
    )
    add_clicked = add_col.button(
        "➕ Add rule",
        key="add_rule_global",
        type="secondary",
        disabled=not (cfg and target_table),
        help="Select a target table and save the configuration before adding rules." if not (cfg and target_table) else "",
    )

    filter_col, filter_code, filter_sev = st.columns(3)
    rule_columns = sorted({c.column_name for c in column_checks if c.column_name})
    filter_column = filter_col.selectbox(
        "Filter by column",
        options=["All"] + rule_columns,
        index=0,
        key="rule_filter_column",
    )
    rule_codes = sorted({(chk.get("rule_code") or "").upper() for chk in library_checks if chk.get("rule_code")})
    filter_rule_code = filter_code.selectbox(
        "Filter by rule",
        options=["All"] + rule_codes,
        index=0,
        key="rule_filter_code",
    )
    severities = sorted({(chk.get("severity") or chk.get("rule_severity") or "ERROR") for chk in library_checks})
    filter_severity = filter_sev.selectbox(
        "Filter by severity",
        options=["All"] + severities,
        index=0,
        key="rule_filter_severity",
    )

    st.markdown(
        """
        <style>
        .dq-rule-grid { max-height: 420px; overflow-y: auto; margin-top: .35rem; }
        .dq-rule-row { padding: .4rem 0; border-bottom: 1px solid #e7ebf3; }
        .dq-rule-head { font-weight: 600; font-size: .9rem; color: #4b5563; padding-bottom: .25rem; border-bottom: 1px solid #e7ebf3; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    grid_entries: List[Dict[str, Any]] = []
    for rule in library_checks:
        if not rule.get("column_name"):
            continue
        rule_code_key = (rule.get("rule_code") or "").upper()
        template = (
            active_rules_by_code.get(rule_code_key)
            or all_rules_map.get(rule_code_key)
            or all_rules_map.get((rule.get("rule_id") or "").upper())
        )
        params = _parse_params(rule.get("rule_params") or rule.get("params_json"))
        params_dict = params if isinstance(params, dict) else {}
        grid_entries.append(
            {
                "check_id": rule.get("check_id"),
                "column": rule.get("column_name"),
                "rule_code": rule_code_key,
                "rule_name": (template.rule_id if template else (rule.get("rule_id") or rule_code_key)),
                "severity": rule.get("severity")
                or rule.get("rule_severity")
                or (template.severity if template else None),
                "params": params_dict,
                "param_schema": template.param_schema if template else rule.get("param_schema"),
                "default_params": template.default_params if template else rule.get("default_params"),
                "rule_version": template.version if template else rule.get("version"),
                "template": template,
            }
        )

    def _matches_filters(entry: Dict[str, Any]) -> bool:
        if filter_column != "All" and entry.get("column") != filter_column:
            return False
        if filter_rule_code != "All" and (entry.get("rule_code") or "").upper() != filter_rule_code:
            return False
        if filter_severity != "All" and (entry.get("severity") or "ERROR") != filter_severity:
            return False
        if rule_search:
            query = rule_search.lower()
            return any(
                query in str(entry.get(field, "")).lower()
                for field in ("column", "rule_name", "rule_code")
            )
        return True

    filtered_entries = [e for e in grid_entries if _matches_filters(e)]

    st.caption(f"Showing {len(filtered_entries)} of {len(grid_entries)} rules")
    head_cols = st.columns([2, 3, 3, 1, 1])
    head_cols[0].markdown("<div class='dq-rule-head'>Column</div>", unsafe_allow_html=True)
    head_cols[1].markdown("<div class='dq-rule-head'>Rule</div>", unsafe_allow_html=True)
    head_cols[2].markdown("<div class='dq-rule-head'>Summary</div>", unsafe_allow_html=True)
    head_cols[3].markdown("<div class='dq-rule-head'>Severity</div>", unsafe_allow_html=True)
    head_cols[4].markdown("<div class='dq-rule-head'>Actions</div>", unsafe_allow_html=True)

    with st.container():
        st.markdown("<div class='dq-rule-grid'>", unsafe_allow_html=True)
        for entry in filtered_entries:
            cols = st.columns([2, 3, 3, 1, 1])
            cols[0].markdown(f"**{entry.get('column')}**")
            cols[1].markdown(
                f"{entry.get('rule_name')}\n\n<span style='color:#6b7280;font-size:.85rem;'>{entry.get('rule_code')}</span>",
                unsafe_allow_html=True,
            )
            cols[2].markdown(_summarize_rule(entry.get("rule_code", ""), entry.get("params") or {}))
            cols[3].markdown(entry.get("severity") or "—")
            edit_clicked = cols[4].button("✏️", key=f"edit_rule_{entry.get('check_id')}", help="Edit rule")
            delete_clicked = cols[4].button(
                "🗑️", key=f"delete_rule_{entry.get('check_id')}", help="Delete rule"
            )
            if edit_clicked:
                with st.modal(
                    f"Edit rule: {entry.get('rule_name')} on {entry.get('column')}",
                    key=f"edit_modal_{entry.get('check_id')}",
                ):
                    st.markdown(
                        f"**Config:** {cfg.name if cfg else entry.get('column')}  \n"
                        f"**Table:** `{target_table or cfg.target_table_fqn if cfg else ''}`  \n"
                        f"**Rule code:** `{entry.get('rule_code')}`  \n"
                        f"**Category:** {(entry.get('template').category if entry.get('template') else '') or '—'}  \n"
                        f"**Severity:** {(entry.get('severity') or '—')}  \n"
                        f"**Scope:** {(entry.get('template').scope if entry.get('template') else '') or 'COLUMN'}",
                    )
                    param_schema = _normalize_param_schema(entry.get("param_schema"))
                    defaults_raw = entry.get("default_params")
                    defaults = defaults_raw if isinstance(defaults_raw, dict) else {}
                    start_values = {**defaults, **(entry.get("params") or {})}
                    rendered_params, param_error = _render_param_inputs(
                        key_prefix=f"edit_modal_{entry.get('check_id')}",
                        param_schema=param_schema,
                        current_values=start_values,
                        column_options=available_cols,
                        table_options=table_suggestions,
                        column_lookup=column_lookup,
                    )
                    col_save, col_cancel = st.columns(2)
                    if col_save.button("Save", type="primary", key=f"edit_save_{entry.get('check_id')}"):
                        if param_error:
                            st.error(param_error)
                        elif not session:
                            st.error("No active Snowpark session.")
                        elif not target_table:
                            st.error("Select a target table before editing rules.")
                        else:
                            try:
                                compiled_rule = _compile_library_rule(
                                    session,
                                    entry.get("rule_code", ""),
                                    target_table,
                                    entry.get("column", ""),
                                    rendered_params,
                                )
                            except Exception as exc:
                                st.error(f"Rule compile failed: {exc}")
                            else:
                                update_library_check(
                                    session,
                                    check_id=str(entry.get("check_id")),
                                    rule_params=rendered_params,
                                    rule_version=entry.get("rule_version"),
                                    compiled_rule=compiled_rule,
                                    rule_expr=compiled_rule,
                                    severity=entry.get("severity"),
                                )
                                st.success("Rule updated.")
                                st.rerun()
                    if col_cancel.button("Cancel", key=f"edit_cancel_{entry.get('check_id')}"):
                        pass
            if delete_clicked:
                delete_check_by_id(session, str(entry.get("check_id")))
                st.success("Rule deleted.")
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    if add_clicked:
        with st.modal("Add rule", key="add_rule_modal"):
            selected_column = st.selectbox(
                "Column",
                options=available_cols or ["—"],
                index=0 if available_cols else 0,
                key="add_rule_column",
            )
            available_templates = [
                t for t in rule_templates if t.enabled and (t.scope or "").upper() == "COLUMN"
            ]
            template_labels = {
                t.rule_code: f"{t.rule_id} ({t.rule_code})" if t.rule_code else t.rule_id
                for t in available_templates
            }
            selected_code = st.selectbox(
                "Rule template",
                options=list(template_labels.keys()) or [""],
                format_func=lambda code: template_labels.get(code, code),
                key="add_rule_code",
            )
            selected_template = next((t for t in available_templates if t.rule_code == selected_code), None)
            param_schema = _normalize_param_schema(selected_template.param_schema if selected_template else [])
            defaults_raw = selected_template.default_params if selected_template else {}
            defaults = defaults_raw if isinstance(defaults_raw, dict) else {}
            rendered_params, param_error = _render_param_inputs(
                key_prefix=f"add_modal_{selected_code or 'new'}",
                param_schema=param_schema,
                current_values=defaults,
                column_options=available_cols,
                table_options=table_suggestions,
                column_lookup=column_lookup,
            )
            save_new, cancel_new = st.columns(2)
            if save_new.button("Save", type="primary", key="add_rule_save"):
                if param_error:
                    st.error(param_error)
                elif not selected_template:
                    st.error("Select a rule to add.")
                elif not session:
                    st.error("No active Snowpark session.")
                elif not target_table:
                    st.error("Select a target table before adding rules.")
                elif not selected_column or selected_column == "—":
                    st.error("Select a column for this rule.")
                elif not cfg or not cfg.config_id:
                    st.error("Save the configuration before adding rules.")
                else:
                    duplicate = any(
                        (entry.get("rule_code") == selected_template.rule_code)
                        and (entry.get("column") == selected_column)
                        and (entry.get("params") == rendered_params)
                        for entry in grid_entries
                    )
                    if duplicate:
                        st.warning("This rule already exists for this column.")
                    else:
                        try:
                            compiled_rule = _compile_library_rule(
                                session,
                                selected_template.rule_code,
                                target_table,
                                selected_column,
                                rendered_params,
                            )
                        except Exception as exc:
                            st.error(f"Rule compile failed: {exc}")
                        else:
                            insert_library_check(
                                session,
                                config_id=cfg.config_id,
                                table_fqn=target_table,
                                column_name=selected_column,
                                rule_code=selected_template.rule_code,
                                rule_id=selected_template.rule_id,
                                rule_params=rendered_params,
                                rule_version=selected_template.version,
                                compiled_rule=compiled_rule,
                                severity=selected_template.severity or "ERROR",
                                sample_rows=0,
                            )
                            st.success("Rule added.")
                            st.rerun()
            if cancel_new.button("Cancel", key="add_rule_cancel"):
                pass

    # -------- Form --------
    # Pre-populate table-level defaults from existing checks / state
    existing_table_params: Dict[str, Dict[str, Any]] = {}
    legacy_row_count_params: Dict[str, object] = {}
    for ec in existing_checks:
        if not ec.column_name and ec.params_json:
            key = _rule_key(ec.check_type or "")
            try:
                parsed_params = json.loads(ec.params_json)
            except Exception:
                parsed_params = {}

            if key == "ROW_COUNT":
                legacy_row_count_params = parsed_params or {}
                continue

            existing_table_params[key] = parsed_params or {}

    freshness_key = _rule_key("FRESHNESS")
    rowcount_anomaly_key = _rule_key("ROW_COUNT_ANOMALY")

    if "_dq_rowcount_params" in st.session_state:
        stored_params = st.session_state.get("_dq_rowcount_params") or {}
        existing_table_params[rowcount_anomaly_key] = {
            **existing_table_params.get(rowcount_anomaly_key, {}),
            **stored_params,
        }

    session_ts_col = st.session_state.get("_dq_table_ts_col")
    session_max_age = st.session_state.get("_dq_table_max_age")
    if session_ts_col or session_max_age is not None:
        freshness_entry = existing_table_params.setdefault(freshness_key, {})
        if session_ts_col and "timestamp_column" not in freshness_entry:
            freshness_entry["timestamp_column"] = session_ts_col
        if session_max_age is not None and "max_age_minutes" not in freshness_entry:
            try:
                freshness_entry["max_age_minutes"] = int(session_max_age)
            except (TypeError, ValueError):
                pass

    freshness_defaults = existing_table_params.get(freshness_key, {})
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

    rowcount_defaults = existing_table_params.get(rowcount_anomaly_key) or {}
    if not rowcount_defaults:
        rowcount_defaults = {}
    rowcount_defaults.setdefault("timestamp_column", ts_default)
    rowcount_defaults.setdefault("lookback_days", 28)
    rowcount_defaults.setdefault("sensitivity", 3.0)
    rowcount_defaults.setdefault("min_history_days", 7)
    existing_table_params[rowcount_anomaly_key] = rowcount_defaults

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

        check_rows: List[DQCheck] = list(column_checks)

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
                fr_rule, fr_is_agg = build_rule_for_table_check(
                    target_table, _builder_key(freshness_key, "FRESHNESS"), fr_params
                )
            except ValueError as exc:
                table_check_error = f"Invalid freshness configuration: {exc}"
            else:
                check_rows.append(DQCheck(
                    config_id=(cfg.config_id if cfg else "temp"),
                    check_id="TABLE_FRESHNESS",
                    table_fqn=target_table, column_name=None,
                    rule_expr=(f"AGG: {fr_rule}" if fr_is_agg else fr_rule), severity="ERROR",
                    sample_rows=0, check_type=freshness_key,
                    params_json=json.dumps(fr_params)
                ))

                row_defaults = existing_table_params.get(rowcount_anomaly_key, {}) or {}
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
                    anomaly_rule, anomaly_is_agg = build_rule_for_table_check(
                        target_table, _builder_key(rowcount_anomaly_key, "ROW_COUNT_ANOMALY"), anomaly_params
                    )
                except ValueError as exc:
                    table_check_error = f"Invalid row count anomaly configuration: {exc}"
                else:
                    check_rows.append(DQCheck(
                        config_id=(cfg.config_id if cfg else "temp"),
                        check_id="TABLE_ROW_COUNT_ANOMALY",
                        table_fqn=target_table, column_name=None,
                        rule_expr=(f"AGG: {anomaly_rule}" if anomaly_is_agg else anomaly_rule), severity="ERROR",
                        sample_rows=0, check_type=rowcount_anomaly_key,
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
        "🧪 Profiling",
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
        "DQ Rule Library",
        use_container_width=True,
        type="primary" if view == "rules" else "secondary",
        key="nav_rules",
        on_click=navigate_to,
        args=("rules",),
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
        f"busy_save={st.session_state.get('busy_saving')}"
    )
if view == "cfg":
    if st.session_state.get("cfg_mode","list") == "list":
        render_config_list()
    else:
        render_config_editor()
elif view == "profile":
    render_profiling_view(session, METADATA_DB, METADATA_SCHEMA, profiling_v2)
elif view == "monitor":
    render_monitor()
elif view == "rules":
    render_rule_admin(session, METADATA_DB, METADATA_SCHEMA)
elif view == "docs":
    render_docs()
else:
    render_home()
