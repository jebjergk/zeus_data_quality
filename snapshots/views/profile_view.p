"""Profiling v2 Streamlit view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import streamlit as st

from services import profiling_v2 as profiling_service
from ui import strings as ui_strings
from utils.flags import DEBUG_PROFILING
from views.table_picker import stateless_table_picker


@dataclass
class _ProfilingData:
    """Container for profiling metadata used by the UI."""

    summary: Dict[str, Any]
    column_features: pd.DataFrame
    column_classification: pd.DataFrame
    suggested_checks: pd.DataFrame
    recent_runs: pd.DataFrame


def _format_timestamp(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _format_ratio(value: Optional[Any]) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    percent = numeric * 100 if 0 <= numeric <= 1 else numeric
    return f"{percent:.1f}%"


def _format_number(value: Optional[Any]) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}"


def _stringify_params(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    try:
        import json

        return json.dumps(value, sort_keys=True)
    except Exception:
        return str(value)


def _lookup_summary(summary: Dict[str, Any], keys: Iterable[str]) -> Optional[Any]:
    for key in keys:
        if key in summary and summary[key] is not None:
            return summary[key]
    return None


def _latest_classifications(class_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if class_df.empty or "COLUMN_NAME" not in class_df.columns:
        return {}
    ordered = class_df.copy()
    sort_cols: List[str] = [col for col in ("SOURCE", "CLASSIFIED_AT") if col in ordered.columns]
    ascending = [False] * len(sort_cols)
    if sort_cols:
        ordered = ordered.sort_values(by=["COLUMN_NAME", *sort_cols], ascending=[True, *ascending])
    deduped = ordered.drop_duplicates(subset=["COLUMN_NAME"], keep="first")
    result: Dict[str, Dict[str, Any]] = {}
    for record in deduped.to_dict("records"):
        column = record.get("COLUMN_NAME")
        if column:
            result[str(column)] = record
    return result


def _merge_column_details(features: pd.DataFrame, classification: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features
    class_lookup = _latest_classifications(classification)
    records: List[Dict[str, Any]] = []
    feature_records = features.to_dict("records")
    for record in feature_records:
        merged = dict(record)
        column_name = str(record.get("COLUMN_NAME", ""))
        class_record = class_lookup.get(column_name)
        if class_record:
            for key in ("CONTENT_TYPE", "SEMANTIC_ROLE", "SOURCE", "CONFIDENCE"):
                if key in class_record:
                    merged[key] = class_record.get(key)
        records.append(merged)
    return pd.DataFrame.from_records(records) if records else pd.DataFrame()


def _prepare_suggested_checks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    if "PARAMS" in working.columns:
        working["PARAMS"] = working["PARAMS"].map(_stringify_params)
    desired_order: List[str] = [
        "COLUMN_NAME",
        "RULE_ID",
        "CHECK_TYPE",
        "SEVERITY",
        "PARAMS",
        "RATIONALE",
        "SUGGESTED_BY",
        "SUGGESTED_AT",
    ]
    existing = [col for col in desired_order if col in working.columns]
    trailing = [col for col in working.columns if col not in existing]
    return working[existing + trailing]


def _prepare_classification(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    desired_order: List[str] = [
        "COLUMN_NAME",
        "CONTENT_TYPE",
        "SEMANTIC_CATEGORY",
        "SEMANTIC_ROLE",
        "SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    ]
    existing = [col for col in desired_order if col in df.columns]
    trailing = [col for col in df.columns if col not in existing]
    return df[existing + trailing]


def _prepare_run_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    desired_order: List[str] = [
        "RUN_ID",
        "TABLE_FQN",
        "PROFILED_AT",
        "ROW_COUNT",
        "SAMPLE_PERCENT",
        "STATUS",
        "DURATION_SECONDS",
    ]
    existing = [col for col in desired_order if col in df.columns]
    trailing = [col for col in df.columns if col not in existing]
    return df[existing + trailing]


def _resolve_helpers(profiling_helpers: Optional[Any]):
    return profiling_helpers or profiling_service


def _load_metadata(
    helpers: Any,
    session: Any,
    table_fqn: str,
) -> _ProfilingData:
    summary_fetch = getattr(helpers, "fetch_table_summary", None)
    summary = summary_fetch(session, table_fqn) if callable(summary_fetch) else {}
    if not summary:
        summary_frame_fn = getattr(helpers, "get_table_profile_summary", None)
        if callable(summary_frame_fn):
            summary_frame = summary_frame_fn(session, table_fqn)
            if isinstance(summary_frame, pd.DataFrame) and not summary_frame.empty:
                order_cols = [
                    col
                    for col in ("PROFILED_AT", "UPDATED_AT", "RUN_TS")
                    if col in summary_frame.columns
                ]
                if order_cols:
                    summary_frame = summary_frame.sort_values(
                        by=order_cols, ascending=False
                    )
                summary = summary_frame.iloc[0].to_dict()
    column_features_fn = getattr(helpers, "get_column_features", None)
    column_features = (
        column_features_fn(session, table_fqn)
        if callable(column_features_fn)
        else pd.DataFrame()
    )
    column_class_fn = getattr(helpers, "get_column_classification", None)
    column_classification = (
        column_class_fn(session, table_fqn)
        if callable(column_class_fn)
        else pd.DataFrame()
    )
    suggested_checks_fn = getattr(helpers, "get_suggested_checks", None)
    suggested_checks = (
        suggested_checks_fn(session, table_fqn)
        if callable(suggested_checks_fn)
        else pd.DataFrame()
    )
    run_history_fn = getattr(helpers, "fetch_recent_runs", None)
    recent_runs = run_history_fn(session, table_fqn) if callable(run_history_fn) else pd.DataFrame()
    return _ProfilingData(
        summary=summary or {},
        column_features=column_features if isinstance(column_features, pd.DataFrame) else pd.DataFrame(),
        column_classification=
        column_classification if isinstance(column_classification, pd.DataFrame) else pd.DataFrame(),
        suggested_checks=suggested_checks if isinstance(suggested_checks, pd.DataFrame) else pd.DataFrame(),
        recent_runs=recent_runs if isinstance(recent_runs, pd.DataFrame) else pd.DataFrame(),
    )


def _render_summary(summary: Dict[str, Any], features: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUMMARY_SUBHEADER)
    if not summary:
        st.info(ui_strings.PROFILE_V2_SUMMARY_EMPTY)
        return
    rows_value = _lookup_summary(summary, ["ROW_COUNT", "ROWS_PROFILED", "PROFILED_ROWS"])
    sample_value = _lookup_summary(summary, ["SAMPLE_PERCENT", "SAMPLE_RATIO"])
    duration_value = _lookup_summary(summary, ["DURATION_SECONDS", "RUNTIME_SECONDS"])
    profiled_ts = _lookup_summary(summary, ["PROFILED_AT", "RUN_TS", "UPDATED_AT"])
    metric_columns = st.columns(3)
    metric_columns[0].metric(ui_strings.PROFILE_V2_SUMMARY_ROWS, _format_number(rows_value))
    metric_columns[1].metric(ui_strings.PROFILE_V2_SUMMARY_SAMPLE, _format_ratio(sample_value))
    metric_columns[2].metric(
        ui_strings.PROFILE_V2_SUMMARY_DURATION,
        _format_number(duration_value),
        help=ui_strings.PROFILE_V2_SUMMARY_TIMESTAMP.format(
            timestamp=_format_timestamp(profiled_ts)
        ),
    )
    column_count = _lookup_summary(summary, ["COLUMN_COUNT", "TOTAL_COLUMNS"])
    avg_null = _lookup_summary(summary, ["AVG_NULL_RATIO", "AVG_NULL_PERCENT"])
    avg_distinct = _lookup_summary(summary, ["AVG_DISTINCT_RATIO", "AVG_DISTINCT_PERCENT"])
    stats_cols = st.columns(4)
    stats_cols[0].metric("Column count", _format_number(column_count or len(features.index)))
    stats_cols[1].metric("Last profiled", _format_timestamp(profiled_ts))
    stats_cols[2].metric("Avg. null ratio", _format_ratio(avg_null))
    stats_cols[3].metric("Avg. distinct ratio", _format_ratio(avg_distinct))


def _render_features_tab(features: pd.DataFrame, classification: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_FEATURES_SUBHEADER)
    if features.empty:
        st.info(ui_strings.PROFILE_V2_FEATURES_EMPTY)
        return
    merged = _merge_column_details(features, classification)
    st.dataframe(
        merged,
        use_container_width=True,
        hide_index=True,
    )


def _render_classification_tab(classification: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_CLASSIFICATION_SUBHEADER)
    prepared = _prepare_classification(classification)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_CLASSIFICATION_EMPTY)
        return
    st.dataframe(prepared, use_container_width=True, hide_index=True)


def _render_suggestions_tab(suggested_checks: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUGGESTIONS_SUBHEADER)
    prepared = _prepare_suggested_checks(suggested_checks)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_SUGGESTIONS_EMPTY)
        return
    st.dataframe(prepared, use_container_width=True, hide_index=True)


def _render_run_history_tab(run_history: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_RUNS_SUBHEADER)
    prepared = _prepare_run_history(run_history)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_RUNS_EMPTY)
        return
    st.dataframe(prepared, use_container_width=True, hide_index=True)


def render_profile(
    session: Any,
    metadata_db: str,
    metadata_schema: str,
    profiling_helpers: Optional[Any] = None,
) -> None:
    """Render the Profiling v2 UI."""

    helpers = _resolve_helpers(profiling_helpers)
    st.header(ui_strings.PROFILE_V2_HEADER_TITLE)
    st.caption(ui_strings.PROFILE_V2_HEADER_CAPTION)
    namespace = getattr(helpers, "DISCOVERY_NAMESPACE", f"{metadata_db}.{metadata_schema}")
    st.caption(ui_strings.PROFILE_V2_METADATA_NOTE.format(namespace=namespace))

    preselect_fqn = st.session_state.get("profile_target_fqn") or st.session_state.get(
        "editor_target_fqn"
    )
    _, _, _, picker_fqn = stateless_table_picker(session, preselect_fqn)
    if "profile_target_fqn" not in st.session_state:
        st.session_state["profile_target_fqn"] = preselect_fqn or ""
    if picker_fqn:
        st.session_state["profile_target_fqn"] = picker_fqn
    target_fqn = st.session_state.get("profile_target_fqn", "") or ""

    st.divider()

    button_cols = st.columns(2)
    run_disabled = not (target_fqn and session)
    refresh_disabled = not (target_fqn and session)
    run_clicked = button_cols[0].button(
        ui_strings.PROFILE_V2_RUN_BUTTON,
        disabled=run_disabled,
        use_container_width=True,
    )
    refresh_clicked = button_cols[1].button(
        ui_strings.PROFILE_V2_REFRESH_BUTTON,
        disabled=refresh_disabled,
        use_container_width=True,
    )
    status_placeholder = st.empty()

    if "profile_data_nonce" not in st.session_state:
        st.session_state["profile_data_nonce"] = 0

    if run_clicked and target_fqn:
        with st.spinner(ui_strings.PROFILE_V2_RUN_SPINNER.format(table=target_fqn)):
            try:
                helpers.run_profiling_v2(session, target_fqn)
            except Exception as exc:
                status_placeholder.error(
                    ui_strings.PROFILE_V2_RUN_ERROR.format(error=str(exc))
                )
            else:
                st.session_state["profile_data_nonce"] += 1
                status_placeholder.success(
                    ui_strings.PROFILE_V2_RUN_SUCCESS.format(table=target_fqn)
                )
    elif refresh_clicked and target_fqn:
        status_placeholder.info(ui_strings.PROFILE_V2_REFRESH_MESSAGE)
        st.session_state["profile_data_nonce"] += 1

    if not target_fqn:
        st.info(ui_strings.PROFILE_V2_NO_TARGET)
        return

    if not session:
        st.warning(ui_strings.PROFILE_V2_SESSION_WARNING)
        return

    st.success(
        ui_strings.PROFILE_V2_TARGET_CAPTION.format(table=target_fqn)
    )

    with st.spinner(ui_strings.PROFILE_V2_LOAD_SPINNER):
        data = _load_metadata(helpers, session, target_fqn)

    _render_summary(data.summary, data.column_features)

    tab_titles = [
        ui_strings.PROFILE_V2_TAB_FEATURES,
        ui_strings.PROFILE_V2_TAB_CLASSIFICATION,
        ui_strings.PROFILE_V2_TAB_SUGGESTIONS,
        ui_strings.PROFILE_V2_TAB_RUNS,
    ]
    tabs = st.tabs(tab_titles)

    with tabs[0]:
        _render_features_tab(data.column_features, data.column_classification)
    with tabs[1]:
        _render_classification_tab(data.column_classification)
    with tabs[2]:
        _render_suggestions_tab(data.suggested_checks)
    with tabs[3]:
        _render_run_history_tab(data.recent_runs)

    if DEBUG_PROFILING:
        debug_payload = {
            "summary": data.summary,
            "column_features": data.column_features.to_dict("records"),
            "column_classification": data.column_classification.to_dict("records"),
            "suggested_checks": data.suggested_checks.to_dict("records"),
            "recent_runs": data.recent_runs.to_dict("records"),
        }
        with st.expander(ui_strings.PROFILE_V2_DEBUG_EXPANDER, expanded=False):
            st.json(debug_payload)
