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



from typing import Dict, List, Optional

ALLOWED_PAGES = {"home", "cfg", "profile", "monitor", "monitor_v3", "docs", "rules"}


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
from dq_config_view import open_config_editor, render_dq_config_v2
from services import profiling_v2
from services.state import get_state, set_state
from utils.configs import (
    DEFAULT_METADATA_DB,
    DEFAULT_METADATA_SCHEMA,
    get_metadata_namespace,
    get_proc_name,
)
from utils.flags import DEBUG_PROFILING
from utils.meta import _q, list_configs
from utils.version import build_sha, build_time
from views.docs_view import render_docs as render_docs_view
from views.monitor_v3_view import render_monitor_v3
from views.profile_view import render_profile as render_profiling_view
from views.rule_admin_view import render_rule_admin


def _clean_namespace_value(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _resolve_metadata_namespace(metadata_db: Optional[str], metadata_schema: Optional[str]) -> tuple[str, str]:
    cleaned_db = _clean_namespace_value(metadata_db)
    cleaned_schema = _clean_namespace_value(metadata_schema)
    return cleaned_db or DEFAULT_METADATA_DB, cleaned_schema or DEFAULT_METADATA_SCHEMA


_initial_metadata_db, _initial_metadata_schema = get_metadata_namespace()
METADATA_DB, METADATA_SCHEMA = _resolve_metadata_namespace(
    _initial_metadata_db,
    _initial_metadata_schema,
)
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
    render_profiling_view(session, METADATA_DB, METADATA_SCHEMA, profiling_v2)
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


def _normalize_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().upper()
    return text in {"TRUE", "T", "YES", "Y", "1"}


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
    recent_df.loc[recent_df["error_msg"] == "", "error_msg"] = "No error message provided."

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
        "📈 DQ Monitor v3",
        use_container_width=True,
        type="primary" if view == "monitor_v3" else "secondary",
        key="nav_monitor_v3",
        on_click=navigate_to,
        args=("monitor_v3",),
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
    render_dq_config_v2(session, METADATA_DB, METADATA_SCHEMA)
elif view == "profile":
    render_profiling_view(session, METADATA_DB, METADATA_SCHEMA, profiling_v2)
elif view == "monitor":
    render_monitor()
elif view == "monitor_v3":
    render_monitor_v3(session)
elif view == "rules":
    render_rule_admin(session, METADATA_DB, METADATA_SCHEMA)
elif view == "docs":
    render_docs()
else:
    render_home()
