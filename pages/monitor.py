"""Streamlit monitor page for Zeus Data Quality."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import altair as alt
import pandas as pd
import streamlit as st

from .ui_shared import page_header
from utils.checkdefs import SUPPORTED_COLUMN_CHECKS, SUPPORTED_TABLE_CHECKS
from utils.meta import fetch_config_map, fetch_run_results, fetch_timeseries_daily


_DEFAULT_DAYS = 30
_DAY_OPTIONS = [7, 14, 30, 60, 90]
_STATUS_OPTIONS = ["All statuses", "Failed only", "Passed only"]
_CHECK_TYPE_OPTIONS = sorted(
    {
        *(t.upper() for t in SUPPORTED_COLUMN_CHECKS),
        *(t.upper() for t in SUPPORTED_TABLE_CHECKS),
    }
)


def _init_state() -> None:
    st.session_state.setdefault(
        "monitor_filters",
        {
            "days": _DEFAULT_DAYS,
            "status": _STATUS_OPTIONS[0],
            "configs": [],
            "check_types": [],
            "search": "",
        },
    )


def _format_config_option(config_id: str, info: Optional[Dict[str, Optional[str]]]) -> str:
    if not info:
        return config_id
    name = info.get("name") or config_id
    table = info.get("table")
    if table:
        return f"{name} · {table}"
    return name


def _effective_config_filter(
    selected: Iterable[str],
    search_term: str,
    config_map: Dict[str, Dict[str, Optional[str]]],
) -> Optional[List[str]]:
    selected_ids = [cfg for cfg in selected if cfg in config_map]
    if search_term:
        search_term = search_term.lower()
        matching_ids = [
            cfg_id
            for cfg_id, info in config_map.items()
            if search_term in (info.get("name") or "").lower()
            or search_term in (info.get("table") or "").lower()
            or search_term in cfg_id.lower()
        ]
        if selected_ids:
            selected_ids = [cfg for cfg in selected_ids if cfg in matching_ids]
        else:
            selected_ids = matching_ids
    if not selected_ids:
        return None
    return selected_ids


def _build_calendar(days: int) -> pd.DatetimeIndex:
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    return pd.date_range(start=start_date, end=end_date, freq="D")


def _aggregate_results(results_df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    if results_df.empty:
        base = pd.DataFrame(
            {
                "run_date": calendar,
                "runs": [0] * len(calendar),
                "passes": [0] * len(calendar),
                "fails": [0] * len(calendar),
                "failure_rows": [0] * len(calendar),
            }
        )
        return base

    normalized = results_df.copy()
    normalized["run_ts"] = pd.to_datetime(normalized["run_ts"], errors="coerce")
    normalized["run_date"] = normalized["run_ts"].dt.normalize()
    normalized["failures"] = pd.to_numeric(normalized["failures"], errors="coerce").fillna(0)
    grouped = normalized.groupby("run_date").agg(
        runs=("run_id", "count"),
        passes=("ok", lambda s: int(s.fillna(False).astype(bool).sum())),
        failure_rows=("failures", "sum"),
    )
    grouped["fails"] = grouped["runs"] - grouped["passes"]
    grouped = grouped.reindex(calendar, fill_value=0)
    grouped.index.name = "run_date"
    aggregated = grouped.reset_index()
    aggregated["failure_rows"] = aggregated["failure_rows"].astype(int)
    aggregated["runs"] = aggregated["runs"].astype(int)
    aggregated["passes"] = aggregated["passes"].astype(int)
    aggregated["fails"] = aggregated["fails"].astype(int)
    return aggregated


def _apply_timeseries_overlay(
    aggregated: pd.DataFrame,
    timeseries: List[Dict[str, int]],
) -> pd.DataFrame:
    if not timeseries or aggregated.empty:
        return aggregated
    ts_df = pd.DataFrame(timeseries)
    if ts_df.empty:
        return aggregated
    ts_df["run_date"] = pd.to_datetime(ts_df["run_date"], errors="coerce")
    ts_df = ts_df.dropna(subset=["run_date"])
    if ts_df.empty:
        return aggregated
    ts_df = ts_df.set_index("run_date")[["runs", "passes", "fails", "failure_rows"]]
    merged = aggregated.copy()
    merged = merged.set_index(pd.to_datetime(merged["run_date"]))
    merged.update(ts_df)
    merged = merged.reset_index(drop=True)
    return merged


def _build_anomaly_points(
    results_df: pd.DataFrame,
    aggregated: pd.DataFrame,
) -> pd.DataFrame:
    if results_df.empty or aggregated.empty:
        return pd.DataFrame()
    anomaly_df = results_df.copy()
    anomaly_df["check_type_upper"] = anomaly_df["check_type"].astype(str).str.upper()
    anomaly_df = anomaly_df[anomaly_df["check_type_upper"] == "ROW_COUNT_ANOMALY"]
    anomaly_df = anomaly_df[~anomaly_df["ok"].fillna(False)]
    if anomaly_df.empty:
        return pd.DataFrame()
    anomaly_df["run_ts"] = pd.to_datetime(anomaly_df["run_ts"], errors="coerce")
    anomaly_df = anomaly_df.dropna(subset=["run_ts"])
    if anomaly_df.empty:
        return pd.DataFrame()
    anomaly_df["run_date"] = anomaly_df["run_ts"].dt.normalize()
    agg = aggregated.copy()
    agg["run_date"] = pd.to_datetime(agg["run_date"], errors="coerce")
    agg = agg.dropna(subset=["run_date"])
    agg = agg.rename(columns={"fails": "fails_total", "failure_rows": "failure_rows_total"})
    merged = anomaly_df.merge(agg, on="run_date", how="left")
    merged["fails_marker"] = merged["fails_total"].fillna(0)
    merged["failure_rows_marker"] = merged["failure_rows_total"].fillna(0)
    return merged


def _render_kpis(aggregated: pd.DataFrame) -> None:
    total_runs = int(aggregated["runs"].sum()) if not aggregated.empty else 0
    total_passes = int(aggregated["passes"].sum()) if not aggregated.empty else 0
    total_fails = int(aggregated["fails"].sum()) if not aggregated.empty else 0
    failure_rows = int(aggregated["failure_rows"].sum()) if not aggregated.empty else 0
    pass_rate = (total_passes / total_runs * 100) if total_runs else 0.0
    fail_rate = (total_fails / total_runs * 100) if total_runs else 0.0

    col_runs, col_fails, col_rows, col_rate = st.columns(4)
    col_runs.metric("Runs", f"{total_runs:,}")
    col_fails.metric("Failed runs", f"{total_fails:,}", delta=f"{fail_rate:.1f}% fail rate")
    col_rows.metric("Failure rows", f"{failure_rows:,}")
    col_rate.metric("Pass rate", f"{pass_rate:.1f}%")


def _render_charts(aggregated: pd.DataFrame, anomalies: pd.DataFrame) -> None:
    if aggregated.empty:
        st.info("No monitoring activity available for the selected filters.")
        return

    chart_data = aggregated.copy()
    chart_data["run_date"] = pd.to_datetime(chart_data["run_date"], errors="coerce")
    chart_data = chart_data.dropna(subset=["run_date"])

    failed_layer = alt.Chart(chart_data).mark_bar(color="#f39c12").encode(
        x=alt.X("run_date:T", title="Run date"),
        y=alt.Y("fails:Q", title="Failed checks"),
        tooltip=[
            alt.Tooltip("run_date:T", title="Date"),
            alt.Tooltip("fails:Q", title="Failed checks"),
            alt.Tooltip("runs:Q", title="Total runs"),
        ],
    )

    failure_rows_layer = alt.Chart(chart_data).mark_area(color="#5dade2", opacity=0.6).encode(
        x=alt.X("run_date:T", title="Run date"),
        y=alt.Y("failure_rows:Q", title="Failure rows"),
        tooltip=[
            alt.Tooltip("run_date:T", title="Date"),
            alt.Tooltip("failure_rows:Q", title="Failure rows"),
            alt.Tooltip("fails:Q", title="Failed checks"),
        ],
    )

    if not anomalies.empty:
        anomalies_points = anomalies.copy()
        anomalies_points["run_date"] = pd.to_datetime(anomalies_points["run_date"], errors="coerce")
        anomalies_points = anomalies_points.dropna(subset=["run_date"])
        anomaly_tooltip = [
            alt.Tooltip("run_ts:T", title="Run timestamp"),
            alt.Tooltip("config_id:N", title="Config"),
            alt.Tooltip("check_id:N", title="Check"),
            alt.Tooltip("failures:Q", title="Failure rows"),
        ]
        anomaly_marks_fails = alt.Chart(anomalies_points).mark_point(
            shape="triangle-up", color="#d62728", size=90, filled=True
        ).encode(
            x="run_date:T",
            y=alt.Y("fails_marker:Q"),
            tooltip=anomaly_tooltip,
        )
        anomaly_marks_rows = alt.Chart(anomalies_points).mark_point(
            shape="triangle-up", color="#d62728", size=90, filled=True
        ).encode(
            x="run_date:T",
            y=alt.Y("failure_rows_marker:Q"),
            tooltip=anomaly_tooltip,
        )
        fails_chart = alt.layer(failed_layer, anomaly_marks_fails).properties(height=260, title="Daily failed checks")
        failures_chart = alt.layer(failure_rows_layer, anomaly_marks_rows).properties(
            height=260, title="Daily total failures"
        )
    else:
        fails_chart = failed_layer.properties(height=260, title="Daily failed checks")
        failures_chart = failure_rows_layer.properties(height=260, title="Daily total failures")

    st.altair_chart(fails_chart, use_container_width=True)
    st.altair_chart(failures_chart, use_container_width=True)


def _render_results_table(results_df: pd.DataFrame, config_map: Dict[str, Dict[str, Optional[str]]]) -> None:
    if results_df.empty:
        st.info("No recent run results to display.")
        return

    table_df = results_df.copy()
    table_df["run_ts"] = pd.to_datetime(table_df["run_ts"], errors="coerce")
    table_df = table_df.dropna(subset=["run_ts"])
    table_df = table_df.sort_values("run_ts", ascending=False).head(200)
    table_df["run_time"] = table_df["run_ts"].dt.strftime("%Y-%m-%d %H:%M")
    table_df["config"] = table_df["config_id"].apply(lambda cid: _format_config_option(cid, config_map.get(cid)))
    table_df["table"] = table_df["config_id"].apply(lambda cid: config_map.get(cid, {}).get("table"))
    table_df["status"] = table_df["ok"].apply(lambda v: "PASS" if bool(v) else "FAIL")
    table_df["failures"] = pd.to_numeric(table_df["failures"], errors="coerce").fillna(0).astype(int)
    display_cols = [
        "run_time",
        "config",
        "table",
        "check_type",
        "status",
        "failures",
        "error_msg",
    ]
    st.dataframe(
        table_df[display_cols],
        use_container_width=True,
        hide_index=True,
    )


def render_monitor(session, app_version: str | None = None) -> None:
    """Render the monitoring experience with filters, KPIs, charts, and run results."""

    if app_version:
        st.session_state.setdefault("app_version", app_version)
    page_header(
        "Monitor data quality runs",
        "Row checks have views; aggregates don’t.",
        session=session,
        show_version=True,
    )

    _init_state()
    filters = dict(st.session_state.get("monitor_filters", {}))

    config_map = fetch_config_map(session)

    with st.form("monitor_filters"):
        col_days, col_status, col_configs = st.columns([1, 1, 2])
        days_selection = col_days.selectbox(
            "Days",
            options=_DAY_OPTIONS,
            index=_DAY_OPTIONS.index(filters.get("days", _DEFAULT_DAYS)) if filters.get("days", _DEFAULT_DAYS) in _DAY_OPTIONS else 2,
            help="Limit the time horizon for charts and metrics.",
        )
        status_selection = col_status.selectbox(
            "Status",
            options=_STATUS_OPTIONS,
            index=_STATUS_OPTIONS.index(filters.get("status", _STATUS_OPTIONS[0]))
            if filters.get("status") in _STATUS_OPTIONS
            else 0,
            help="Show all runs or focus on passes/failures.",
        )
        config_selection = col_configs.multiselect(
            "Configurations",
            options=list(config_map.keys()),
            default=[cfg for cfg in filters.get("configs", []) if cfg in config_map],
            format_func=lambda cid: _format_config_option(cid, config_map.get(cid)),
            help="Filter results to one or more configurations.",
        )

        with st.expander("Advanced filters"):
            adv_col_types, adv_col_search = st.columns(2)
            check_type_selection = adv_col_types.multiselect(
                "Check types",
                options=_CHECK_TYPE_OPTIONS,
                default=[ct for ct in filters.get("check_types", []) if ct in _CHECK_TYPE_OPTIONS],
                help="Limit to specific rule types, such as ROW_COUNT or FRESHNESS.",
            )
            search_value = adv_col_search.text_input(
                "Search configs",
                value=filters.get("search", ""),
                help="Filter by configuration name, table, or identifier.",
            )

        submitted = st.form_submit_button("Apply")

    if submitted:
        st.session_state["monitor_filters"] = {
            "days": days_selection,
            "status": status_selection,
            "configs": list(config_selection),
            "check_types": [ct.upper() for ct in check_type_selection],
            "search": search_value,
        }
        filters = dict(st.session_state["monitor_filters"])
    else:
        filters = dict(st.session_state.get("monitor_filters", {}))

    if not session:
        st.info("No active Snowflake session detected. Connect to load monitoring data.")
        return

    days = filters.get("days", _DEFAULT_DAYS)
    status = filters.get("status", _STATUS_OPTIONS[0])
    selected_configs = filters.get("configs", [])
    check_types_filter = [ct.upper() for ct in filters.get("check_types", []) if ct]
    search_term = filters.get("search", "").strip()

    effective_config_ids = _effective_config_filter(selected_configs, search_term, config_map)

    calendar = _build_calendar(days)
    time_from = datetime.utcnow() - timedelta(days=days)
    time_to = datetime.utcnow()

    ok_param: Optional[bool] = True if status == "Passed only" else None

    if effective_config_ids is None and selected_configs:
        timeseries_raw: List[Dict[str, int]] = []
        results_raw: List[Dict[str, object]] = []
    else:
        config_ids_param = effective_config_ids or None
        timeseries_raw = fetch_timeseries_daily(
            session,
            days=days,
            config_ids=config_ids_param,
        )
        results_raw = fetch_run_results(
            session,
            time_from=time_from,
            time_to=time_to,
            config_ids=config_ids_param,
            check_types=check_types_filter or None,
            ok=ok_param,
            limit=5000,
        )

    results_df = pd.DataFrame(results_raw)

    if status == "Failed only" and not results_df.empty:
        results_df = results_df[~results_df["ok"].fillna(False)]

    aggregated = _aggregate_results(results_df, calendar)

    if status == "All statuses" and not check_types_filter:
        aggregated = _apply_timeseries_overlay(aggregated, timeseries_raw)

    anomalies_df = _build_anomaly_points(results_df, aggregated)

    _render_kpis(aggregated)
    _render_charts(aggregated, anomalies_df)

    st.subheader("Recent results")
    _render_results_table(results_df, config_map)

