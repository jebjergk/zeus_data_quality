"""Profiling v2 Streamlit view."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Dict, Iterable, List, Optional
import pandas as pd
import streamlit as st, logging

from services import profiling_v2 as profiling_service
from ui import strings as ui_strings
from views.table_picker import stateless_table_picker
from utils.configs import DEFAULT_METADATA_DB, DEFAULT_METADATA_SCHEMA

st.session_state.setdefault("busy_profiling", False)
st.session_state.setdefault("freeze_view", False)
st.session_state.setdefault("last_profile_summary", None)
st.session_state.setdefault("last_profile_rows", [])
st.session_state.setdefault("last_profile_err", None)
st.session_state.setdefault("profile_debug_counts", {})

SUGGESTIONS_TIMEOUT_SECONDS = 60
_OVERVIEW_INTERNAL_COLUMNS = [
    "include_in_dq_config",
    "column_name",
    "data_type",
    "null_info",
    "distinct_info",
    "min_value",
    "max_value",
    "length_info",
    "rule_id",
    "check_type",
    "severity",
    "rationale",
    "confidence",
    "has_suggestion",
    "suggested_rule_count",
    "suggested_rules",
]

LEGACY_PROFILE_STATE_KEYS = (
    "profiling_mode",
    "use_legacy_profiling",
    "profile_view",
    "profiling_view_version",
)


def _clear_legacy_profile_state() -> None:
    for key in LEGACY_PROFILE_STATE_KEYS:
        st.session_state.pop(key, None)


def _call_with_timeout(func, timeout_seconds: float, *args, **kwargs):
    """Execute *func* with a timeout.

    Returns a tuple ``(result, error)`` where *error* is ``None`` when the call
    finished successfully, ``TimeoutError`` when the timeout elapsed, or the
    caught exception instance for other failures.
    """

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds), None
        except TimeoutError as exc:
            future.cancel()
            return None, exc
        except Exception as exc:  # pragma: no cover - passthrough for UI feedback
            future.cancel()
            return None, exc


@dataclass
class _ProfilingData:
    """Container for profiling metadata used by the UI."""

    overview_grid: pd.DataFrame
    suggested_checks: pd.DataFrame
    column_classification: pd.DataFrame
    recent_runs: pd.DataFrame
    run_info: Dict[str, Any]


def _format_timestamp(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


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


def _resolve_metadata_namespace(
    metadata_db: Optional[str], metadata_schema: Optional[str]
) -> tuple[str, str]:
    db = str(metadata_db or "").strip()
    schema = str(metadata_schema or "").strip()
    return db or DEFAULT_METADATA_DB, schema or DEFAULT_METADATA_SCHEMA


def _truncate_details(value: Any, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "\u2026"


def _calculate_duration_seconds(started: Any, finished: Any) -> Optional[float]:
    if started is None or finished is None:
        return None
    try:
        start_ts = pd.to_datetime(started)
        finish_ts = pd.to_datetime(finished)
    except Exception:
        return None
    if pd.isna(start_ts) or pd.isna(finish_ts):
        return None
    duration = finish_ts - start_ts
    if duration.total_seconds() < 0:
        return None
    return float(duration.total_seconds())


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if seconds < 1:
        return f"{seconds:.2f}s"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {int(remainder)}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m"


def _format_count(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if pd.isna(numeric):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.2f}"


def _format_percent(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if pd.isna(numeric):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    return f"{numeric:.2f}%"


def _normalize_checkbox_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"true", "t", "yes", "y", "1"}


def _latest_run_record(run_history: pd.DataFrame) -> Optional[pd.Series]:
    if not isinstance(run_history, pd.DataFrame) or run_history.empty:
        return None
    ordered = run_history
    if "STARTED_AT" in ordered.columns:
        ordered = ordered.sort_values(by="STARTED_AT", ascending=False)
    return ordered.iloc[0]


def _extract_run_info(run_history: pd.DataFrame) -> Dict[str, Any]:
    if not isinstance(run_history, pd.DataFrame) or run_history.empty:
        return {}

    latest = _latest_run_record(run_history)
    if latest is None:
        return {}

    def _normalize_value(key: str):
        value = latest.get(key)
        return None if pd.isna(value) else value

    sample_mode_raw = _normalize_value("SAMPLE_MODE")
    sample_mode = str(sample_mode_raw).upper() if sample_mode_raw is not None else None
    row_count = _normalize_value("ROW_COUNT")
    sample_percent = _normalize_value("SAMPLE_PERCENT")
    sample_est_rows = _normalize_value("SAMPLE_EST_ROWS")

    if sample_mode == "FULL":
        sample_percent = 100 if sample_percent is None else sample_percent
        if sample_est_rows is None and row_count is not None:
            sample_est_rows = row_count

    return {
        "row_count": row_count,
        "sample_mode": sample_mode,
        "sample_percent": sample_percent,
        "sample_est_rows": sample_est_rows,
    }


def _status_banner(status: str):
    normalized = (status or "").upper()
    if normalized in {"SUCCESS", "SUCCEEDED", "DONE"}:
        return st.success
    if normalized in {"RUNNING", "IN_PROGRESS", "STARTED", "QUEUED"}:
        return st.warning
    if normalized in {"FAILED", "ERROR"}:
        return st.error
    return st.info


def _is_failure_status(status: str) -> bool:
    normalized = (status or "").upper()
    return normalized in {"FAILED", "ERROR"}


def _render_last_run_banner(run_history: pd.DataFrame, target_fqn: str) -> None:
    st.subheader(ui_strings.PROFILE_V2_STATUS_SUBHEADER)
    latest = _latest_run_record(run_history)
    if latest is None:
        st.info(ui_strings.PROFILE_V2_STATUS_EMPTY.format(table=target_fqn))
        return
    status_value = latest.get("STATUS") or ui_strings.PROFILE_V2_VALUE_UNKNOWN
    started_at = latest.get("STARTED_AT")
    finished_at = latest.get("FINISHED_AT") or started_at
    run_id = (
        latest.get("RUN_ID")
        or latest.get("PROFILE_RUN_ID")
        or ui_strings.PROFILE_V2_VALUE_UNKNOWN
    )
    duration_seconds = _calculate_duration_seconds(started_at, latest.get("FINISHED_AT"))
    banner = _status_banner(str(status_value))
    banner(
        ui_strings.PROFILE_V2_STATUS_MESSAGE.format(
            status=str(status_value),
            timestamp=_format_timestamp(finished_at),
            duration=_format_duration(duration_seconds),
            run_id=run_id,
        )
    )
    details = latest.get("DETAILS")
    if details and _is_failure_status(str(status_value)):
        st.caption(
            ui_strings.PROFILE_V2_STATUS_DETAILS.format(
                details=_truncate_details(details)
            )
        )


def _render_sampling_summary(run_info: Any) -> None:
    """Render sampling metadata and pie chart for the last profiling run.

    This MUST NOT call st.stop(), even if metadata is missing, so the rest of the
    page still renders.
    """

    if run_info is None:
        st.info("No sampling metadata available for this run.")
        return

    def _get_field(*names: str) -> Any:
        if isinstance(run_info, dict):
            for name in names:
                if name in run_info:
                    return run_info.get(name)
        else:
            for name in names:
                if hasattr(run_info, name):
                    return getattr(run_info, name)
        return None

    row_count = _get_field("ROW_COUNT", "row_count")
    sample_mode = _get_field("SAMPLE_MODE", "sample_mode")
    sample_percent = _get_field("SAMPLE_PERCENT", "sample_percent")
    sample_est_rows = _get_field("SAMPLE_EST_ROWS", "sample_est_rows")

    if (
        row_count is None
        and sample_mode is None
        and sample_percent is None
        and sample_est_rows is None
    ):
        st.info("No sampling metadata available for this run.")
        return

    total_rows = None
    try:
        if row_count is not None:
            total_rows = int(row_count)
    except Exception:
        total_rows = None

    if not total_rows or total_rows <= 0:
        st.info("Sampling metadata is present but total row count is zero or invalid.")
        return

    mode_str = (str(sample_mode) or "").upper() if sample_mode is not None else ""

    if mode_str == "SAMPLE":
        sampled_rows = None
        try:
            if sample_est_rows is not None:
                sampled_rows = int(sample_est_rows)
        except Exception:
            sampled_rows = None

        if sampled_rows is None:
            try:
                if sample_percent is not None:
                    sampled_rows = int(round(total_rows * float(sample_percent) / 100.0))
            except Exception:
                sampled_rows = None

        if sampled_rows is None:
            sampled_rows = total_rows

        display_percent = (
            float(sample_percent)
            if sample_percent is not None
            else (100.0 * sampled_rows / total_rows)
        )
        sampling_label = "SYSTEM sampling"
    else:
        sampled_rows = total_rows
        display_percent = 100.0
        sampling_label = "FULL scan (no sampling)"

    remainder_rows = max(total_rows - sampled_rows, 0)

    st.subheader("Sampling summary")

    summary_col, chart_col = st.columns([2, 1])

    with summary_col:
        st.markdown(f"Table size: **{total_rows:,}** rows")
        st.markdown(
            f"Sample used: approx. **{sampled_rows:,}** rows (~{display_percent:.1f}%)"
        )
        st.markdown(f"Sampling mode: **{sampling_label}**")

    with chart_col:
        if total_rows > 0:
            _render_sampling_pie_chart(sampled_rows, remainder_rows)


def _render_sampling_pie_chart(sample_rows: float, remainder_rows: float) -> None:
    try:
        import altair as alt
    except ModuleNotFoundError:
        st.info("Sampling pie chart is unavailable (charting library not found in this environment).")
        return

    data = pd.DataFrame(
        {
            "Category": ["Sample", "Remainder"],
            "Rows": [sample_rows, remainder_rows],
        }
    )

    chart = (
        alt.Chart(data)
        .mark_arc()
        .encode(theta=alt.Theta(field="Rows", type="quantitative"), color="Category")
        .properties(width=300, height=300)
    )

    st.altair_chart(chart, use_container_width=False)


def _classification_source_badge(source: Any) -> str:
    normalized = str(source or "").strip().upper()
    if normalized == "MANUAL":
        return ui_strings.PROFILE_V2_COLUMNS_SOURCE_MANUAL
    if normalized:
        return ui_strings.PROFILE_V2_COLUMNS_SOURCE_HEURISTIC.format(source=normalized)
    return ui_strings.PROFILE_V2_COLUMNS_SOURCE_UNKNOWN


def _normalize_suggestions_frame(suggestions: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(suggestions, pd.DataFrame):
        return pd.DataFrame()

    working = suggestions.copy()
    working.columns = [str(column).upper() for column in working.columns]
    for column in (
        "COLUMN_NAME",
        "RULE_ID",
        "CHECK_TYPE",
        "SEVERITY",
        "RATIONALE",
        "SUGGESTED_BY",
        "CONFIDENCE",
        "PARAMS",
        "PARAMETERS",
    ):
        if column not in working.columns:
            working[column] = None

    # Prefer PARAMS when both exist
    working["PARAMS"] = working["PARAMS"].combine_first(working["PARAMETERS"])
    working = working.dropna(subset=["COLUMN_NAME"])
    working["COLUMN_NAME"] = working["COLUMN_NAME"].astype(str)

    dedup_subset = [
        "COLUMN_NAME",
        "RULE_ID",
        "CHECK_TYPE",
        "SEVERITY",
        "PARAMS",
        "SUGGESTED_BY",
    ]
    working = working.drop_duplicates(subset=dedup_subset, keep="first")
    return working


def _stringify_params(params: Any) -> str:
    if params is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if isinstance(params, str):
        cleaned = params.strip()
        return cleaned or ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        return json.dumps(params, default=str)
    except Exception:
        return str(params)


def _overview_lookup(overview: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if not isinstance(overview, pd.DataFrame) or overview.empty:
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for record in overview.to_dict("records"):
        column = record.get("column_name") or record.get("COLUMN_NAME")
        if not column:
            continue
        lookup[str(column)] = {
            "data_type": record.get("data_type") or record.get("DATA_TYPE") or "",
            "null_info": record.get("null_info") or record.get("NULL_INFO") or "",
            "distinct_info": record.get("distinct_info")
            or record.get("DISTINCT_INFO")
            or "",
            "min_value": record.get("min_value") or record.get("MIN_VALUE") or "",
            "max_value": record.get("max_value") or record.get("MAX_VALUE") or "",
            "length_info": record.get("length_info")
            or record.get("LENGTH_INFO")
            or "",
        }
    return lookup


def _classification_confidence_lookup(
    classification: pd.DataFrame,
) -> Dict[str, str]:
    latest = _latest_classifications(classification)
    if not latest:
        return {}
    return {
        column: _format_confidence(record.get("CONFIDENCE"))
        for column, record in latest.items()
    }


def _included_overview_columns(overview: pd.DataFrame) -> List[str]:
    if not isinstance(overview, pd.DataFrame) or overview.empty:
        return []

    lower_lookup = {col.lower(): col for col in overview.columns}
    include_column = lower_lookup.get("include_in_dq_config")
    name_column = lower_lookup.get("column_name")
    if not include_column or not name_column:
        return []

    included: List[str] = []
    for _, row in overview.iterrows():
        try:
            include_value = bool(row.get(include_column))
        except Exception:
            include_value = False
        if not include_value:
            continue
        name_value = row.get(name_column)
        if name_value is None:
            continue
        included.append(str(name_value))

    return _normalize_selected_columns(included)


def _selected_suggestion_columns(table_fqn: str) -> List[str]:
    selection = st.session_state.get("dq_config_selection", {}).get(table_fqn, {})
    included: List[str] = []
    for key, value in selection.items():
        if not value:
            continue
        parts = key.split("|")
        if len(parts) < 2:
            continue
        included.append(parts[1])
    seen = set()
    result: List[str] = []
    for column in included:
        folded = column.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        result.append(column)
    return result


def _normalize_selected_columns(columns: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for column in columns:
        name = str(column or "").strip()
        if not name:
            continue
        folded = name.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        result.append(name)
    return result


def _session_selected_columns(table_fqn: str) -> List[str]:
    session_target = st.session_state.get("_profile_selection_target") or st.session_state.get(
        "profile_target_fqn"
    )
    raw_selection = st.session_state.get("profile_selected_columns") or []
    if not raw_selection:
        return []
    if table_fqn and session_target and session_target.casefold() != table_fqn.casefold():
        return []
    return _normalize_selected_columns(raw_selection)


def _persist_selected_columns(table_fqn: str, overview: pd.DataFrame) -> None:
    selected_columns = _included_overview_columns(overview)
    st.session_state["profile_selected_columns"] = selected_columns
    st.session_state["_profile_selection_target"] = table_fqn
    st.session_state["profile_target_fqn"] = table_fqn


def _reset_profile_selection_target(target_fqn: str) -> None:
    current_target = st.session_state.get("_profile_selection_target")
    if target_fqn and current_target and current_target.casefold() == target_fqn.casefold():
        return
    st.session_state["_profile_selection_target"] = target_fqn or ""
    st.session_state["profile_selected_columns"] = []


def _resolve_included_columns(table_fqn: str, overview: pd.DataFrame) -> List[str]:
    session_selected = _session_selected_columns(table_fqn)
    if session_selected:
        return session_selected
    included = _included_overview_columns(overview)
    if included:
        return included
    return _selected_suggestion_columns(table_fqn)


def _prepare_overview_frame(overview: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(overview, pd.DataFrame) or overview.empty:
        return pd.DataFrame(columns=_OVERVIEW_INTERNAL_COLUMNS)

    working = overview.copy()
    working.columns = [str(column).lower() for column in working.columns]
    for column in _OVERVIEW_INTERNAL_COLUMNS:
        if column not in working.columns:
            working[column] = None

    working["column_name"] = working["column_name"].astype(str)
    working["has_suggestion"] = working["has_suggestion"].fillna(False).apply(bool)
    working["suggested_rule_count"] = (
        pd.to_numeric(working["suggested_rule_count"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    working["suggested_rules"] = working["suggested_rules"].apply(lambda value: value or [])
    working["include_in_dq_config"] = working["include_in_dq_config"].apply(
        _normalize_checkbox_value
    )
    working = working.set_index("column_name", drop=False)

    ordered = working[_OVERVIEW_INTERNAL_COLUMNS]
    ordered.attrs = working.attrs
    return ordered


def _overview_grid_widget_key(table_fqn: str, nonce: int = 0) -> str:
    normalized = (table_fqn or "").replace('"', "").replace(".", "_")
    normalized = normalized or "table"
    return f"profile_overview_grid_{normalized}_{nonce}"


def _default_config_name(table_fqn: str) -> str:
    parts = (table_fqn or "").split(".")
    table_name = parts[-1] if parts else "TABLE"
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"PROFILE_{table_name}_{timestamp}"


def _render_suggest_config_summary(
    summary: Dict[str, Any], table_fqn: str, config_name: str
) -> None:
    rules_created = summary.get("rules_created") or []
    rules_skipped = summary.get("rules_skipped") or []
    created_count = len(rules_created)
    skipped_count = len(rules_skipped)

    st.success(
        ui_strings.PROFILE_V2_SUGGEST_CONFIG_SUCCESS.format(
            table=table_fqn,
            config=config_name,
            created=created_count,
            skipped=skipped_count,
        )
    )

    combined_rows: List[Dict[str, Any]] = []
    for entry in rules_created:
        combined_rows.append(
            {
                "Column": entry.get("column"),
                "Rule": entry.get("rule_code"),
                "Status": "Created",
                "Reason": "",
            }
        )
    for entry in rules_skipped:
        combined_rows.append(
            {
                "Column": entry.get("column"),
                "Rule": entry.get("rule_code"),
                "Status": "Skipped",
                "Reason": entry.get("reason", ""),
            }
        )

    if not combined_rows:
        return

    with st.expander(ui_strings.PROFILE_V2_SUGGEST_CONFIG_SUMMARY_TITLE):
        st.dataframe(pd.DataFrame(combined_rows))


def _render_suggest_config_action(
    overview: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
    profile_run_id: Optional[str],
    metadata_db: str,
    metadata_schema: str,
):
    suggest_fn = getattr(helpers, "suggest_config_from_profile", None)
    if not callable(suggest_fn):
        st.info(ui_strings.PROFILE_V2_SUGGEST_CONFIG_UNAVAILABLE)
        return

    suggest_button = st.button(
        ui_strings.PROFILE_V2_SUGGEST_CONFIG_BUTTON,
        use_container_width=False,
    )

    stored_result = st.session_state.get("last_suggest_config_result", {})
    stored_summary: Dict[str, Any] = {}
    stored_config_name: Optional[str] = None
    if isinstance(stored_result, dict) and stored_result.get("table_fqn") == table_fqn:
        summary_candidate = stored_result.get("summary")
        if isinstance(summary_candidate, dict):
            stored_summary = summary_candidate
        stored_config_name = stored_result.get("config_name")

    if not suggest_button:
        if stored_summary:
            _render_suggest_config_summary(
                stored_summary,
                table_fqn,
                stored_config_name or _default_config_name(table_fqn),
            )
        return

    included_columns = _session_selected_columns(table_fqn)
    if not included_columns:
        st.warning("Select at least one column")
        return

    if not profile_run_id:
        st.warning(ui_strings.PROFILE_V2_SUGGEST_CONFIG_NO_RUN_ID)
        return

    config_name = _default_config_name(table_fqn)
    with st.spinner(
        ui_strings.PROFILE_V2_SUGGEST_CONFIG_SPINNER.format(table=table_fqn)
    ):
        try:
            summary = _call_helper_with_metadata(
                suggest_fn,
                session,
                table_fqn,
                profile_run_id,
                included_columns,
                config_name,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception as exc:  # pragma: no cover - UI feedback only
            st.error(
                ui_strings.PROFILE_V2_SUGGEST_CONFIG_ERROR.format(error=str(exc))
            )
            st.caption(ui_strings.PROFILE_V2_SUGGEST_CONFIG_ERROR_HINT)
            return

    if isinstance(summary, dict):
        st.session_state["last_suggest_config_result"] = {
            "table_fqn": table_fqn,
            "config_name": config_name,
            "summary": summary,
        }
    else:
        st.session_state.pop("last_suggest_config_result", None)

    _render_suggest_config_summary(summary or {}, table_fqn, config_name)


def _render_overview_debug(
    overview: pd.DataFrame,
    metadata_db: str,
    metadata_schema: str,
    table_fqn: str,
) -> None:
    debug_counts = {}
    if isinstance(overview, pd.DataFrame):
        debug_counts = overview.attrs.get("dq_debug_counts", {}) or {}
    selected_columns = _session_selected_columns(table_fqn)
    selected_sample = ", ".join(selected_columns[:5]) if selected_columns else "-"

    resolved_db, resolved_schema = _resolve_metadata_namespace(
        debug_counts.get("metadata_db") or metadata_db,
        debug_counts.get("metadata_schema") or metadata_schema,
    )

    feature_count = debug_counts.get("feature_row_count")
    if feature_count is None and isinstance(overview, pd.DataFrame):
        feature_count = len(overview)

    classification_count = debug_counts.get("classification_row_count")
    rendered_count = debug_counts.get("columns_rendered")
    suggestion_count = debug_counts.get("suggestion_row_count")
    features_df_rows = debug_counts.get("features_df_rows", feature_count)
    suggestions_df_rows = debug_counts.get("suggestions_df_rows", suggestion_count)
    grid_df_rows = (
        debug_counts.get("grid_df_rows")
        or debug_counts.get("grid_row_count")
        or rendered_count
    )
    if rendered_count is None and isinstance(overview, pd.DataFrame):
        rendered_count = len(overview)
    if grid_df_rows is None and isinstance(overview, pd.DataFrame):
        grid_df_rows = len(overview)

    features_table_fqn = debug_counts.get("features_table_fqn") or (
        f"{resolved_db}.{resolved_schema}.DQ_COLUMN_FEATURES"
    )
    class_table_fqn = debug_counts.get("class_table_fqn") or (
        f"{resolved_db}.{resolved_schema}.DQ_COLUMN_CLASSIFICATION"
    )
    table_fqn_filter = debug_counts.get("table_fqn_filter") or table_fqn
    feature_sample_columns = debug_counts.get("feature_sample_columns") or []

    if not feature_sample_columns and isinstance(overview, pd.DataFrame):
        if "column_name" in overview.columns:
            feature_sample_columns = (
                overview["column_name"].dropna().astype(str).head(3).tolist()
            )

    with st.expander("Profiling debug", expanded=False):
        st.caption("Renderer")
        st.text("active_renderer: profiling_v2_page_listing")
        st.text(f"selected_columns_count: {len(selected_columns)}")
        st.text(f"selected_columns_sample: {selected_sample}")
        st.caption("Profiling feature source counts")
        st.text(f"feature_row_count: {feature_count if feature_count is not None else 0}")
        st.text(f"features_df_rows: {features_df_rows if features_df_rows is not None else 0}")
        st.text(
            "classification_row_count: "
            f"{classification_count if classification_count is not None else 0}"
        )
        st.text(
            f"suggestion_row_count: {suggestion_count if suggestion_count is not None else 0}"
        )
        st.text(
            f"suggestions_df_rows: {suggestions_df_rows if suggestions_df_rows is not None else 0}"
        )
        st.text(f"columns_rendered: {rendered_count if rendered_count is not None else 0}")
        st.text(f"grid_df_rows: {grid_df_rows if grid_df_rows is not None else 0}")
        st.caption("Resolved metadata sources")
        st.text(f"metadata_db: {resolved_db}")
        st.text(f"metadata_schema: {resolved_schema}")
        st.text(f"features_table_fqn: {features_table_fqn}")
        st.text(f"class_table_fqn: {class_table_fqn}")
        st.text(f"table_fqn_filter: {table_fqn_filter or '-'}")
        sample_text = ", ".join(feature_sample_columns) if feature_sample_columns else "-"
        st.text(f"feature_sample_columns: {sample_text}")


def _normalize_overview_display(overview: pd.DataFrame) -> pd.DataFrame:
    prepared = _prepare_overview_frame(overview)
    if prepared.empty:
        return prepared

    display_columns = [
        "include_in_dq_config",
        "column_name",
        "data_type",
        "null_info",
        "distinct_info",
        "min_value",
        "max_value",
        "length_info",
        "rule_id",
        "check_type",
        "severity",
        "rationale",
        "confidence",
        "suggested_rule_count",
        "suggested_rules",
    ]
    for column in display_columns:
        if column not in prepared.columns:
            prepared[column] = None

    ordered = prepared.reset_index(drop=True)[display_columns]
    ordered.attrs = prepared.attrs
    return ordered


def _render_overview_page(
    overview: pd.DataFrame,
    metadata_db: str,
    metadata_schema: str,
    table_fqn: str,
) -> pd.DataFrame:
    st.subheader("Column statistics")
    normalized = _normalize_overview_display(overview)

    if normalized.empty:
        st.info(ui_strings.PROFILE_V2_NO_FEATURES.format(table=table_fqn))
        _render_overview_debug(normalized, metadata_db, metadata_schema, table_fqn)
        _persist_selected_columns(table_fqn, normalized)
        return normalized

    grid_key = _overview_grid_widget_key(
        table_fqn,
        nonce=st.session_state.get("profile_data_nonce", 0),
    )
    column_config = {
        "include_in_dq_config": st.column_config.CheckboxColumn(
            "Include",
            help="Include this column when suggesting DQ config",
        ),
        "column_name": "Column",
        "data_type": "Type",
        "null_info": "Nulls",
        "distinct_info": "Distinct",
        "min_value": "Min",
        "max_value": "Max",
        "length_info": "Length",
        "rule_id": "Rule ID",
        "check_type": "Check type",
        "severity": "Severity",
        "rationale": "Rationale",
        "confidence": "Confidence",
        "suggested_rule_count": st.column_config.NumberColumn(
            "Suggested rules",
            format="%d",
            help="Number of suggested rules found for this column",
        ),
        "suggested_rules": st.column_config.Column(
            "Suggested rules",
            help="Raw suggested rules for this column",
            width="medium",
        ),
    }
    disabled_columns = [col for col in normalized.columns if col != "include_in_dq_config"]
    edited = st.data_editor(
        normalized,
        column_config=column_config,
        disabled=disabled_columns,
        hide_index=True,
        use_container_width=True,
        key=grid_key,
    )
    edited.attrs["dq_debug_counts"] = normalized.attrs.get("dq_debug_counts", {})
    _persist_selected_columns(table_fqn, edited)
    _render_overview_debug(edited, metadata_db, metadata_schema, table_fqn)
    return edited


def _suggestion_selection_key(
    table_fqn: str, column_name: str, rule_id: Any, check_type: Any
) -> str:
    rule_part = str(rule_id) if rule_id is not None else "rule"
    check_part = str(check_type) if check_type is not None else "check"
    return "|".join([table_fqn or "table", column_name, rule_part, check_part])


def _render_suggestion_sections(
    overview: pd.DataFrame,
    suggestions: pd.DataFrame,
    classification: pd.DataFrame,
    table_fqn: str,
) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUGGESTIONS_SUBHEADER)
    normalized_suggestions = _normalize_suggestions_frame(suggestions)

    if normalized_suggestions.empty:
        st.info(ui_strings.PROFILE_V2_SUGGESTIONS_EMPTY)
        return

    overview_lookup = _overview_lookup(overview)
    confidence_lookup = _classification_confidence_lookup(classification)

    dq_selection = st.session_state.setdefault("dq_config_selection", {})
    table_selection: Dict[str, bool] = dq_selection.setdefault(table_fqn, {})

    for column_name, group in normalized_suggestions.groupby("COLUMN_NAME"):
        column_key = str(column_name)
        metadata = overview_lookup.get(column_key, {})
        confidence_default = confidence_lookup.get(column_key, ui_strings.PROFILE_V2_VALUE_UNKNOWN)

        with st.container(border=True):
            dtype_label = metadata.get("data_type")
            header = f"**{column_key}**"
            if dtype_label:
                header += f"  · `{dtype_label}`"
            st.markdown(header)
            details = []
            for label, value in (
                ("Nulls", metadata.get("null_info")),
                ("Distinct", metadata.get("distinct_info")),
                ("Min", metadata.get("min_value")),
                ("Max", metadata.get("max_value")),
                ("Length", metadata.get("length_info")),
            ):
                if value:
                    details.append(f"**{label}:** {value}")
            if details:
                st.caption(" · ".join(details))

            for _, suggestion in group.iterrows():
                rule_id = suggestion.get("RULE_ID")
                check_type = suggestion.get("CHECK_TYPE") or ui_strings.PROFILE_V2_VALUE_UNKNOWN
                severity = suggestion.get("SEVERITY") or ui_strings.PROFILE_V2_VALUE_UNKNOWN
                rationale = _truncate_details(
                    suggestion.get("RATIONALE"), max_length=500
                ) or ui_strings.PROFILE_V2_VALUE_UNKNOWN
                suggested_by = suggestion.get("SUGGESTED_BY") or ui_strings.PROFILE_V2_VALUE_UNKNOWN
                params = _stringify_params(suggestion.get("PARAMS"))
                suggestion_confidence = suggestion.get("CONFIDENCE")
                confidence_value = (
                    _format_confidence(suggestion_confidence)
                    if suggestion_confidence is not None
                    else confidence_default
                )

                selection_key = _suggestion_selection_key(
                    table_fqn, column_key, rule_id, check_type
                )
                default_selection = table_selection.get(selection_key, True)
                include_value = st.checkbox(
                    "Include in DQ config",
                    key=selection_key,
                    value=default_selection,
                )
                table_selection[selection_key] = include_value

                st.markdown(
                    "  \n".join(
                        [
                            f"**Rule type:** `{check_type}`",
                            f"**Check params:** `{params}`",
                            f"**Severity:** `{severity}`",
                            f"**Rationale:** {rationale}",
                            f"**Suggested by:** `{suggested_by}`",
                            f"**Confidence:** {confidence_value}",
                        ]
                    )
                )

    st.caption(ui_strings.PROFILE_V2_COLUMNS_RULE_METADATA_NOTE)


def _normalize_classification_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_classification_table(classification: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(classification, pd.DataFrame) or classification.empty:
        return pd.DataFrame()

    working = classification.copy()
    working.columns = [str(column).upper() for column in working.columns]
    latest_records = _latest_classifications(working)
    if latest_records:
        working = pd.DataFrame.from_records(list(latest_records.values()))

    required_columns = [
        "COLUMN_NAME",
        "CONTENT_TYPE",
        "SEMANTIC_ROLE",
        "SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    ]
    for column in required_columns:
        if column not in working.columns:
            working[column] = None

    working["COLUMN_NAME"] = working["COLUMN_NAME"].astype(str)
    working = working[required_columns].sort_values(by="COLUMN_NAME")
    display = working.rename(
        columns={
            "COLUMN_NAME": "Column",
            "CONTENT_TYPE": "Content type",
            "SEMANTIC_ROLE": "Semantic role",
            "SOURCE": "Source",
            "CONFIDENCE": "Confidence",
            "CLASSIFIED_AT": "Classified at",
        }
    )
    return display


def _render_semantic_tags_page(
    classification: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
    metadata_db: str,
    metadata_schema: str,
) -> None:
    st.subheader("Semantic tags")
    normalized = _normalize_classification_table(classification)
    if normalized.empty:
        st.info(ui_strings.PROFILE_V2_COLUMNS_EDIT_EMPTY)
    else:
        st.dataframe(normalized, use_container_width=True)

    if isinstance(classification, pd.DataFrame) and not classification.empty:
        _render_column_editors(
            classification, table_fqn, helpers, session, metadata_db, metadata_schema
        )


def _classification_source_detail(source: Any) -> str:
    normalized = str(source or "").strip().upper()
    if normalized == "MANUAL":
        return ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_MANUAL
    if normalized:
        return ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_HEURISTIC.format(
            source=normalized
        )
    return ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_UNKNOWN


def _render_column_editors(
    classification: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
    metadata_db: str,
    metadata_schema: str,
) -> None:
    if not isinstance(classification, pd.DataFrame) or classification.empty:
        st.info(ui_strings.PROFILE_V2_COLUMNS_EDIT_EMPTY)
        return
    latest_records = _latest_classifications(classification)
    if latest_records:
        # dict_values -> list so pandas is happy
        working = pd.DataFrame.from_records(list(latest_records.values()))
    else:
        working = classification.copy()
    save_fn = getattr(helpers, "save_manual_classification", None)
    if not callable(save_fn):
        st.info(ui_strings.PROFILE_V2_COLUMNS_EDIT_UNAVAILABLE)
        return
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMNS_EDIT_HEADER}**")
    st.caption(ui_strings.PROFILE_V2_COLUMNS_EDIT_HELP)
    for record in working.to_dict("records"):
        _render_column_editor_form(
            record, table_fqn, session, save_fn, metadata_db, metadata_schema
        )


def _render_column_editor_form(
    record: Dict[str, Any],
    table_fqn: str,
    session: Any,
    save_fn: Any,
    metadata_db: str,
    metadata_schema: str,
) -> None:
    column_name = str(record.get("COLUMN_NAME") or "").strip()
    if not column_name:
        return
    source_badge = _classification_source_badge(record.get("SOURCE"))
    expander_label = ui_strings.PROFILE_V2_COLUMN_EDIT_EXPANDER.format(
        column=column_name,
        source=source_badge,
    )
    nonce = st.session_state.get("profile_data_nonce", 0)
    form_key = f"profile_class_form_{table_fqn}_{column_name}_{nonce}"
    with st.expander(expander_label, expanded=False):
        st.caption(
            ui_strings.PROFILE_V2_COLUMN_EDIT_STATUS.format(
                confidence=_format_confidence(record.get("CONFIDENCE")),
                classified_at=_format_timestamp(record.get("CLASSIFIED_AT")),
            )
        )
        with st.form(form_key):
            content_default = record.get("CONTENT_TYPE")
            semantic_default = record.get("SEMANTIC_ROLE")
            content_value = st.text_input(
                ui_strings.PROFILE_V2_COLUMN_CONTENT_LABEL,
                value=("" if content_default is None else str(content_default)),
                key=f"{form_key}_content",
            )
            semantic_value = st.text_input(
                ui_strings.PROFILE_V2_COLUMN_SEMANTIC_LABEL,
                value=("" if semantic_default is None else str(semantic_default)),
                key=f"{form_key}_semantic",
            )
            submitted = st.form_submit_button(
                ui_strings.PROFILE_V2_COLUMN_SAVE_BUTTON,
                use_container_width=True,
            )
        if submitted:
            _handle_manual_classification_save(
                save_fn,
                session,
                table_fqn,
                column_name,
                content_value,
                semantic_value,
                metadata_db,
                metadata_schema,
            )


def _format_confidence(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if pd.isna(numeric):
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    return f"{numeric:.2f}"


def _handle_manual_classification_save(
    save_fn: Any,
    session: Any,
    table_fqn: str,
    column_name: str,
    content_value: Optional[str],
    semantic_value: Optional[str],
    metadata_db: str,
    metadata_schema: str,
) -> None:
    content_clean = (content_value or "").strip() or None
    semantic_clean = (semantic_value or "").strip() or None
    spinner = ui_strings.PROFILE_V2_COLUMN_EDIT_SPINNER.format(column=column_name)
    with st.spinner(spinner):
        try:
            _call_helper_with_metadata(
                save_fn,
                session,
                table_fqn,
                column_name,
                content_clean,
                semantic_clean,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception as exc:  # pragma: no cover - UI feedback only
            st.error(
                ui_strings.PROFILE_V2_COLUMN_EDIT_ERROR.format(
                    column=column_name,
                    error=str(exc),
                )
            )
            return
    st.success(
        ui_strings.PROFILE_V2_COLUMN_EDIT_SUCCESS.format(column=column_name)
    )
    st.session_state["profile_data_nonce"] = (
        st.session_state.get("profile_data_nonce", 0) + 1
    )


def _resolve_helpers(profiling_helpers: Optional[Any]):
    return profiling_helpers or profiling_service


def _call_helper_with_metadata(
    fn: Any,
    *args,
    metadata_db: str,
    metadata_schema: str,
    **kwargs,
):
    try:
        return fn(
            *args,
            metadata_db=metadata_db,
            metadata_schema=metadata_schema,
            **kwargs,
        )
    except TypeError as exc:
        message = str(exc)
        if "metadata_db" in message or "metadata_schema" in message:
            return fn(*args, **kwargs)
        raise


def _run_table_profile(
    helpers: Any,
    session: Any,
    table_fqn: str,
    metadata_db: str,
    metadata_schema: str,
) -> Dict[str, Any]:
    run_fn = getattr(helpers, "run_profiling_v2", None)
    summary_fn = getattr(helpers, "fetch_table_summary", None)
    overview_fn = getattr(helpers, "get_overview_grid", None)
    if not callable(run_fn):
        return {
            "ok": False,
            "summary": None,
            "column_rows": [],
            "err": "Profiling engine unavailable",
        }

    with st.spinner(ui_strings.PROFILE_V2_RUN_SPINNER.format(table=table_fqn)):
        try:
            _call_helper_with_metadata(
                run_fn,
                session,
                table_fqn,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception as exc:  # pragma: no cover - UI feedback only
            logging.exception("profiling:run_failed")
            return {
                "ok": False,
                "summary": None,
                "column_rows": [],
                "err": ui_strings.PROFILE_V2_RUN_ERROR.format(error=str(exc)),
            }

    summary = None
    if callable(summary_fn):
        summary = _call_helper_with_metadata(
            summary_fn,
            session,
            table_fqn,
            metadata_db=metadata_db,
            metadata_schema=metadata_schema,
        )
    debug_counts: Dict[str, Any] = {}
    overview = pd.DataFrame()
    if callable(overview_fn):
        overview = _call_helper_with_metadata(
            overview_fn,
            session,
            table_fqn,
            metadata_db=metadata_db,
            metadata_schema=metadata_schema,
        )
    if isinstance(overview, pd.DataFrame):
        debug_counts = overview.attrs.get("dq_debug_counts", {}) or {}
    column_rows = (
        overview.to_dict("records") if isinstance(overview, pd.DataFrame) else []
    )

    if isinstance(overview, pd.DataFrame) and overview.empty:
        warning = ui_strings.PROFILE_V2_NO_FEATURES.format(table=table_fqn)
        logging.warning("profiling:no_features target=%s", table_fqn)
        return {
            "ok": False,
            "summary": summary,
            "column_rows": [],
            "debug_counts": debug_counts,
            "err": warning,
        }
    return {
        "ok": True,
        "summary": summary,
        "column_rows": column_rows,
        "debug_counts": debug_counts,
        "err": None,
    }


def _extract_last_run_id(run_history: pd.DataFrame) -> Optional[str]:
    if not isinstance(run_history, pd.DataFrame) or run_history.empty:
        return None
    if "RUN_ID" not in run_history.columns:
        return None
    ordered = run_history
    for column in ("FINISHED_AT", "STARTED_AT", "PROFILED_AT"):
        if column in ordered.columns:
            ordered = ordered.sort_values(by=column, ascending=False)
            break
    latest = ordered.iloc[0]
    run_id = latest.get("RUN_ID")
    return str(run_id) if run_id is not None else None


def _load_metadata(
    helpers: Any,
    session: Any,
    table_fqn: str,
    metadata_db: str,
    metadata_schema: str,
) -> _ProfilingData:
    overview_grid: pd.DataFrame = pd.DataFrame()
    suggested_checks: pd.DataFrame = pd.DataFrame()
    column_classification: pd.DataFrame = pd.DataFrame()
    recent_runs: pd.DataFrame = pd.DataFrame()

    overview_fn = getattr(helpers, "get_overview_grid", None)
    if callable(overview_fn):
        try:
            overview_grid = _call_helper_with_metadata(
                overview_fn,
                session,
                table_fqn,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception:  # pragma: no cover - Snowflake/IO failures
            logging.exception("profiling:overview_metadata_failed table=%s", table_fqn)
            overview_grid = pd.DataFrame()

    effective_class_fn = getattr(helpers, "get_effective_classification", None)
    column_class_fn = getattr(helpers, "get_column_classification", None)
    classification_fn = effective_class_fn if callable(effective_class_fn) else column_class_fn
    if callable(classification_fn):
        try:
            column_classification = _call_helper_with_metadata(
                classification_fn,
                session,
                table_fqn,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception:  # pragma: no cover - Snowflake/IO failures
            logging.exception(
                "profiling:classification_metadata_failed table=%s", table_fqn
            )
            column_classification = pd.DataFrame()

    suggestions_fn = getattr(helpers, "get_suggested_checks", None)
    if callable(suggestions_fn):
        try:
            suggested_checks = _call_helper_with_metadata(
                suggestions_fn,
                session,
                table_fqn,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception:  # pragma: no cover - Snowflake/IO failures
            logging.exception(
                "profiling:suggestions_metadata_failed table=%s", table_fqn
            )
            suggested_checks = pd.DataFrame()

    run_history_fn = getattr(helpers, "fetch_recent_runs", None)
    if callable(run_history_fn):
        try:
            recent_runs = _call_helper_with_metadata(
                run_history_fn,
                session,
                table_fqn,
                metadata_db=metadata_db,
                metadata_schema=metadata_schema,
            )
        except Exception:  # pragma: no cover - Snowflake/IO failures
            logging.exception("profiling:recent_runs_failed table=%s", table_fqn)
            recent_runs = pd.DataFrame()

    overview_grid = overview_grid if isinstance(overview_grid, pd.DataFrame) else pd.DataFrame()
    column_classification = (
        column_classification
        if isinstance(column_classification, pd.DataFrame)
        else pd.DataFrame()
    )
    suggested_checks = (
        suggested_checks if isinstance(suggested_checks, pd.DataFrame) else pd.DataFrame()
    )
    recent_runs = recent_runs if isinstance(recent_runs, pd.DataFrame) else pd.DataFrame()

    return _ProfilingData(
        overview_grid=overview_grid,
        suggested_checks=suggested_checks,
        column_classification=column_classification,
        recent_runs=recent_runs,
        run_info=_extract_run_info(recent_runs),
    )


def render_profile(
    session: Any,
    metadata_db: str,
    metadata_schema: str,
    profiling_helpers: Optional[Any] = None,
) -> None:
    """Render the Profiling v2 UI."""

    metadata_db, metadata_schema = _resolve_metadata_namespace(
        metadata_db, metadata_schema
    )

    st.session_state.setdefault("busy_profiling", False)
    st.session_state.setdefault("freeze_view", False)
    _clear_legacy_profile_state()

    helpers = _resolve_helpers(profiling_helpers)
    st.header(ui_strings.PROFILE_V2_HEADER_TITLE)
    preselect_fqn = st.session_state.get("profile_target_fqn") or st.session_state.get(
        "editor_target_fqn"
    )
    _, _, _, picker_fqn = stateless_table_picker(session, preselect_fqn)
    if "profile_target_fqn" not in st.session_state:
        st.session_state["profile_target_fqn"] = preselect_fqn or ""
    if picker_fqn:
        st.session_state["profile_target_fqn"] = picker_fqn
    target_fqn = st.session_state.get("profile_target_fqn", "") or ""
    _reset_profile_selection_target(target_fqn)

    st.divider()

    button_cols = st.columns(4)
    run_disabled = not (target_fqn and session)
    refresh_disabled = not (target_fqn and session)
    rerun_disabled = not (target_fqn and session)
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
    classify_clicked = button_cols[2].button(
        ui_strings.PROFILE_V2_CLASSIFY_BUTTON,
        disabled=rerun_disabled,
        use_container_width=True,
    )
    suggestions_clicked = button_cols[3].button(
        ui_strings.PROFILE_V2_SUGGESTIONS_BUTTON,
        disabled=rerun_disabled,
        use_container_width=True,
    )
    status_placeholder = st.empty()

    if "profile_data_nonce" not in st.session_state:
        st.session_state["profile_data_nonce"] = 0

    classify_fn = getattr(helpers, "run_classification_only", None)
    suggestions_fn = getattr(helpers, "run_suggestions_only", None)

    fqn = st.session_state.get("editor_target_fqn") or ""
    if not fqn:
        fqn = st.session_state.get("profile_target_fqn") or ""

    if run_clicked:
        if not fqn:
            st.warning("Select a table first.")
        elif not st.session_state["busy_profiling"]:
            st.session_state["busy_profiling"] = True
            st.session_state["freeze_view"] = True
            try:
                res = _run_table_profile(
                    helpers,
                    session,
                    fqn,
                    metadata_db,
                    metadata_schema,
                )
                if res.get("ok"):
                    st.session_state["last_profile_summary"] = res.get("summary")
                    st.session_state["last_profile_rows"] = (
                        res.get("column_rows") or []
                    )
                    st.session_state["profile_debug_counts"] = (
                        res.get("debug_counts") or {}
                    )
                    st.session_state["last_profile_err"] = None
                    st.session_state["profile_data_nonce"] += 1
                    st.session_state["profile_last_table"] = fqn
                    st.session_state["profile_last_run_id"] = None
                    status_placeholder.success(
                        ui_strings.PROFILE_V2_RUN_SUCCESS.format(table=fqn)
                    )
                else:
                    st.session_state["last_profile_summary"] = None
                    st.session_state["last_profile_rows"] = []
                    st.session_state["last_profile_err"] = res.get("err")
                    st.session_state["profile_debug_counts"] = (
                        res.get("debug_counts") or {}
                    )
            except Exception as e:
                logging.exception("profiling:unhandled")
                st.session_state["last_profile_summary"] = None
                st.session_state["last_profile_rows"] = []
                st.session_state["last_profile_err"] = f"{type(e).__name__}: {e}"
            finally:
                st.session_state["busy_profiling"] = False
                st.session_state["freeze_view"] = False
    elif refresh_clicked and target_fqn:
        status_placeholder.info(ui_strings.PROFILE_V2_REFRESH_MESSAGE)
        st.session_state["profile_data_nonce"] += 1
    elif classify_clicked and target_fqn:
        if not callable(classify_fn):
            status_placeholder.error(ui_strings.PROFILE_V2_CLASSIFY_UNAVAILABLE)
        else:
            with st.spinner(
                ui_strings.PROFILE_V2_CLASSIFY_SPINNER.format(table=target_fqn)
            ):
                try:
                    _call_helper_with_metadata(
                        classify_fn,
                        session,
                        target_fqn,
                        metadata_db=metadata_db,
                        metadata_schema=metadata_schema,
                    )
                except Exception as exc:
                    status_placeholder.error(
                        ui_strings.PROFILE_V2_CLASSIFY_ERROR.format(error=str(exc))
                    )
                else:
                    st.session_state["profile_data_nonce"] += 1
                    status_placeholder.success(
                        ui_strings.PROFILE_V2_CLASSIFY_SUCCESS.format(
                            table=target_fqn
                        )
                    )
    elif suggestions_clicked and target_fqn:
        if not callable(suggestions_fn):
            status_placeholder.error(
                ui_strings.PROFILE_V2_SUGGESTIONS_UNAVAILABLE
            )
        else:
            with st.spinner(
                ui_strings.PROFILE_V2_SUGGESTIONS_SPINNER.format(table=target_fqn)
            ):
                def _suggestions_runner(sess, fqn):
                    return _call_helper_with_metadata(
                        suggestions_fn,
                        sess,
                        fqn,
                        metadata_db=metadata_db,
                        metadata_schema=metadata_schema,
                    )

                _, error = _call_with_timeout(
                    _suggestions_runner,
                    SUGGESTIONS_TIMEOUT_SECONDS,
                    session,
                    target_fqn,
                )

            if isinstance(error, TimeoutError):
                status_placeholder.error(
                    ui_strings.PROFILE_V2_SUGGESTIONS_TIMEOUT.format(
                        table=target_fqn,
                        timeout=SUGGESTIONS_TIMEOUT_SECONDS,
                    )
                )
            elif isinstance(error, Exception):
                status_placeholder.error(
                    ui_strings.PROFILE_V2_SUGGESTIONS_ERROR.format(
                        error=str(error)
                    )
                )
            else:
                st.session_state["profile_data_nonce"] += 1
                status_placeholder.success(
                    ui_strings.PROFILE_V2_SUGGESTIONS_SUCCESS.format(
                        table=target_fqn
                    )
                )

    last_profile_err = st.session_state.get("last_profile_err")
    if last_profile_err:
        status_placeholder.error(last_profile_err)

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
        try:
            data = _load_metadata(
                helpers,
                session,
                target_fqn,
                metadata_db,
                metadata_schema,
            )
        except Exception as exc:
            logging.exception(
                "profiling:metadata_load_failed table=%s", target_fqn
            )
            st.error(
                ui_strings.PROFILE_V2_METADATA_ERROR.format(error=str(exc))
            )
            data = _ProfilingData(
                overview_grid=pd.DataFrame(),
                suggested_checks=pd.DataFrame(),
                column_classification=pd.DataFrame(),
                recent_runs=pd.DataFrame(),
                run_info={},
            )

    st.session_state["profile_last_table"] = target_fqn
    st.session_state["profile_last_run_id"] = _extract_last_run_id(data.recent_runs)
    st.session_state["profile_last_run_info"] = data.run_info

    debug_counts_cache = st.session_state.get("profile_debug_counts") or {}
    if isinstance(data.overview_grid, pd.DataFrame):
        data_debug_counts = data.overview_grid.attrs.get("dq_debug_counts", {}) or {}
        if data_debug_counts:
            debug_counts_cache = data_debug_counts
    st.session_state["profile_debug_counts"] = debug_counts_cache

    cached_rows = st.session_state.get("last_profile_rows") or []
    overview_grid = (
        pd.DataFrame(cached_rows)
        if cached_rows
        else data.overview_grid
    )
    if cached_rows and debug_counts_cache:
        overview_grid.attrs["dq_debug_counts"] = debug_counts_cache
    cached_run_info = st.session_state.get("last_profile_summary")
    run_info = (
        cached_run_info
        if isinstance(cached_run_info, dict) and cached_run_info
        else data.run_info
    )

    _render_last_run_banner(data.recent_runs, target_fqn)
    _render_sampling_summary(run_info)
    st.divider()

    (
        tab_features,
        tab_semantic,
        tab_suggestions,
        tab_history,
    ) = st.tabs(
        [
            "Column features",
            "Semantic tags",
            "Suggested checks",
            "Run history",
        ]
    )

    edited_overview = overview_grid
    with tab_features:
        edited_overview = _render_overview_page(overview_grid, metadata_db, metadata_schema, target_fqn)

    with tab_semantic:
        _render_semantic_tags_page(
            data.column_classification,
            target_fqn,
            helpers,
            session,
            metadata_db,
            metadata_schema,
        )

    with tab_suggestions:
        _render_suggest_config_action(
            edited_overview,
            target_fqn,
            helpers,
            session,
            st.session_state.get("profile_last_run_id"),
            metadata_db,
            metadata_schema,
        )
        _render_suggestion_sections(
            edited_overview,
            data.suggested_checks,
            data.column_classification,
            target_fqn,
        )

    with tab_history:
        st.subheader("Recent profiling runs")
        if isinstance(data.recent_runs, pd.DataFrame) and not data.recent_runs.empty:
            st.dataframe(data.recent_runs, use_container_width=True)
        else:
            st.info("No profiling runs have been logged yet.")
