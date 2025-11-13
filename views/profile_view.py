"""Profiling v2 view backed by ZEUS_ANALYTICS_SIMU.DISCOVERY metadata."""

from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd
import streamlit as st

from services import profiling_v2
from ui import keys as ui_keys
from ui import strings as ui_strings
from utils.flags import DEBUG_PROFILING
from views.table_picker import stateless_table_picker

LOGGER = logging.getLogger(__name__)


def render_profile(session, *_metadata) -> None:
    """Render the Profiling v2 page."""

    st.header(ui_strings.PROFILE_V2_HEADER_TITLE)
    st.caption(ui_strings.PROFILE_V2_HEADER_CAPTION)
    st.caption(
        ui_strings.PROFILE_V2_METADATA_NOTE.format(
            namespace=profiling_v2.DISCOVERY_NAMESPACE
        )
    )
    st.divider()

    if not session:
        st.warning(ui_strings.PROFILE_V2_SESSION_WARNING)
        return

    st.subheader(ui_strings.PROFILE_V2_PICKER_SUBHEADER)
    selected_fqn = st.session_state.get(ui_keys.PROFILE_TARGET_FQN)
    _, _, _, picker_fqn = stateless_table_picker(session, selected_fqn)
    if picker_fqn:
        st.session_state[ui_keys.PROFILE_TARGET_FQN] = picker_fqn

    target_fqn = (
        st.session_state.get(ui_keys.PROFILE_TARGET_FQN)
        or picker_fqn
        or ""
    )

    if target_fqn:
        st.success(ui_strings.PROFILE_V2_TARGET_CAPTION.format(table=target_fqn))
    else:
        st.info(ui_strings.PROFILE_V2_NO_TARGET)

    run_col, refresh_col = st.columns(2)
    run_clicked = run_col.button(
        ui_strings.PROFILE_V2_RUN_BUTTON,
        use_container_width=True,
        disabled=not target_fqn,
    )
    refresh_clicked = refresh_col.button(
        ui_strings.PROFILE_V2_REFRESH_BUTTON,
        use_container_width=True,
        disabled=not target_fqn,
    )

    status_placeholder = st.empty()
    if run_clicked and target_fqn:
        _handle_profile_run(session, target_fqn, status_placeholder)
    elif not run_clicked and refresh_clicked:
        status_placeholder.info(ui_strings.PROFILE_V2_REFRESH_MESSAGE)

    if not target_fqn:
        return

    with st.spinner(ui_strings.PROFILE_V2_LOAD_SPINNER):
        metadata_payload = _load_metadata(session, target_fqn)

    _render_summary(metadata_payload.get("summary") or {})
    _render_results_tabs(metadata_payload)

    if DEBUG_PROFILING:
        with st.expander(ui_strings.PROFILE_V2_DEBUG_EXPANDER, expanded=False):
            st.json(metadata_payload, expanded=False)


def _handle_profile_run(session: Any, table_fqn: str, placeholder) -> None:
    """Run profiling for the given table and surface feedback inline."""

    with st.spinner(ui_strings.PROFILE_V2_RUN_SPINNER.format(table=table_fqn)):
        try:
            profiling_v2.run_full_profile(session, table_fqn)
        except profiling_v2.ProfilingError as exc:
            placeholder.error(
                ui_strings.PROFILE_V2_RUN_ERROR.format(error=str(exc))
            )
            return
        except Exception as exc:  # pragma: no cover - Snowflake specific failure
            LOGGER.exception("profiling_v2:run_failed target=%s", table_fqn)
            placeholder.error(
                ui_strings.PROFILE_V2_RUN_ERROR.format(error=str(exc))
            )
            return

    placeholder.success(
        ui_strings.PROFILE_V2_RUN_SUCCESS.format(table=table_fqn)
    )


def _load_metadata(session: Any, table_fqn: str) -> Dict[str, Any]:
    """Fetch summary, features, classifications, suggestions, and runs."""

    payload = {
        "summary": profiling_v2.fetch_table_summary(session, table_fqn),
        "features": profiling_v2.fetch_column_features(session, table_fqn),
        "classifications": profiling_v2.fetch_column_classifications(
            session, table_fqn
        ),
        "suggestions": profiling_v2.fetch_suggested_checks(session, table_fqn),
        "runs": profiling_v2.fetch_recent_runs(session, table_fqn),
    }
    return payload


def _render_summary(summary: Dict[str, Any]) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUMMARY_SUBHEADER)
    if not summary:
        st.info(ui_strings.PROFILE_V2_SUMMARY_EMPTY)
        return

    rows = _first_value(summary, ("ROW_COUNT", "ROWS_PROFILED", "ROW_CNT"))
    sample_pct = _first_value(
        summary,
        ("SAMPLE_PERCENT", "SAMPLE_PCT", "SAMPLE_RATIO"),
    )
    duration = _first_value(summary, ("DURATION_SECONDS", "DURATION_SEC"))
    profiled_at = _first_value(
        summary,
        ("PROFILED_AT", "UPDATED_AT", "RUN_TS", "LAST_PROFILED_AT"),
    )

    cols = st.columns(3)
    cols[0].metric(
        ui_strings.PROFILE_V2_SUMMARY_ROWS,
        _format_number(rows) if rows is not None else ui_strings.PROFILE_V2_VALUE_UNKNOWN,
    )
    cols[1].metric(
        ui_strings.PROFILE_V2_SUMMARY_SAMPLE,
        _format_percent(sample_pct),
    )
    cols[2].metric(
        ui_strings.PROFILE_V2_SUMMARY_DURATION,
        _format_duration(duration),
        help=ui_strings.PROFILE_V2_SUMMARY_TIMESTAMP.format(
            timestamp=profiled_at or ui_strings.PROFILE_V2_VALUE_UNKNOWN
        ),
    )


def _render_results_tabs(payload: Dict[str, Any]) -> None:
    tabs = st.tabs(
        [
            ui_strings.PROFILE_V2_TAB_FEATURES,
            ui_strings.PROFILE_V2_TAB_CLASSIFICATION,
            ui_strings.PROFILE_V2_TAB_SUGGESTIONS,
            ui_strings.PROFILE_V2_TAB_RUNS,
        ]
    )

    with tabs[0]:
        _render_features(payload.get("features"))
    with tabs[1]:
        _render_classification(payload.get("classifications"))
    with tabs[2]:
        _render_suggestions(payload.get("suggestions"))
    with tabs[3]:
        _render_runs(payload.get("runs"))


def _render_features(df: pd.DataFrame | None) -> None:
    st.subheader(ui_strings.PROFILE_V2_FEATURES_SUBHEADER)
    if df is None or df.empty:
        st.info(ui_strings.PROFILE_V2_FEATURES_EMPTY)
        return

    formatted = df.copy()
    for col in ("NULL_RATIO", "DISTINCT_RATIO", "WHITESPACE_RATIO"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(_format_percent)
    formatted = formatted.rename(
        columns={
            "COLUMN_NAME": "Column",
            "PHYSICAL_TYPE": "Physical Type",
            "NULL_RATIO": "Null %",
            "DISTINCT_RATIO": "Distinct %",
            "WHITESPACE_RATIO": "Whitespace %",
            "AVG_LENGTH": "Avg Length",
            "MIN_VALUE": "Min Value",
            "MAX_VALUE": "Max Value",
            "SAMPLE_PATTERNS": "Sample Patterns",
        }
    )
    st.dataframe(formatted, use_container_width=True, hide_index=True)


def _render_classification(df: pd.DataFrame | None) -> None:
    st.subheader(ui_strings.PROFILE_V2_CLASSIFICATION_SUBHEADER)
    if df is None or df.empty:
        st.info(ui_strings.PROFILE_V2_CLASSIFICATION_EMPTY)
        return

    formatted = df.copy()
    if "CONFIDENCE" in formatted.columns:
        formatted["CONFIDENCE"] = formatted["CONFIDENCE"].apply(_format_percent)
    formatted = formatted.rename(
        columns={
            "COLUMN_NAME": "Column",
            "CONTENT_TYPE": "Content Type",
            "SEMANTIC_ROLE": "Semantic Role",
            "SOURCE": "Source",
            "CONFIDENCE": "Confidence",
        }
    )
    st.dataframe(formatted, use_container_width=True, hide_index=True)


def _render_suggestions(df: pd.DataFrame | None) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUGGESTIONS_SUBHEADER)
    if df is None or df.empty:
        st.info(ui_strings.PROFILE_V2_SUGGESTIONS_EMPTY)
        return

    formatted = df.copy()
    formatted = formatted.rename(
        columns={
            "COLUMN_NAME": "Column",
            "CHECK_TYPE": "Check Type",
            "PARAMETERS": "Parameters",
            "RATIONALE": "Rationale",
            "PRIORITY": "Priority",
        }
    )
    st.dataframe(formatted, use_container_width=True, hide_index=True)


def _render_runs(df: pd.DataFrame | None) -> None:
    st.subheader(ui_strings.PROFILE_V2_RUNS_SUBHEADER)
    if df is None or df.empty:
        st.info(ui_strings.PROFILE_V2_RUNS_EMPTY)
        return

    formatted = df.copy()
    for col in ("SAMPLE_PERCENT",):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(_format_percent)
    formatted = formatted.rename(
        columns={
            "RUN_ID": "Run ID",
            "PROFILED_AT": "Profiled At",
            "ROW_COUNT": "Rows",
            "SAMPLE_PERCENT": "Sample %",
            "STATUS": "Status",
            "DURATION_SECONDS": "Duration (s)",
        }
    )
    st.dataframe(formatted, use_container_width=True, hide_index=True)


def _first_value(summary: Dict[str, Any], keys) -> Any:
    for key in keys:
        if key in summary and summary[key] is not None:
            return summary[key]
    return None


def _format_number(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    return f"{number:,.0f}"


def _format_percent(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if number <= 1:
        number *= 100
    return f"{number:.1f}%"


def _format_duration(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    return f"{number:.2f}s"
