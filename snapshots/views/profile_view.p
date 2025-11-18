"""Profiling v2 Streamlit view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from services import profiling_v2 as profiling_service
from ui import strings as ui_strings
from views.table_picker import stateless_table_picker


@dataclass
class _ProfilingData:
    """Container for profiling metadata used by the UI."""

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
            for key in (
                "CONTENT_TYPE",
                "SEMANTIC_ROLE",
                "SOURCE",
                "CONFIDENCE",
                "CLASSIFIED_AT",
            ):
                if key in class_record:
                    merged[key] = class_record.get(key)
        records.append(merged)
    return pd.DataFrame.from_records(records) if records else pd.DataFrame()


def _truncate_details(value: Any, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "\u2026"


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


def _prepare_columns_grid(features: pd.DataFrame, classification: pd.DataFrame) -> pd.DataFrame:
    merged = _merge_column_details(features, classification)
    if merged.empty:
        return merged
    desired_order: List[str] = [
        "COLUMN_NAME",
        "DATA_TYPE",
        "NULL_RATIO",
        "DISTINCT_RATIO",
        "MIN_VALUE",
        "MAX_VALUE",
        "CONTENT_TYPE",
        "SEMANTIC_ROLE",
        "SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    ]
    working = merged.copy()
    for column in desired_order:
        if column not in working.columns:
            working[column] = None
    if "COLUMN_NAME" in working.columns:
        working = working.sort_values(by="COLUMN_NAME")
    return working[desired_order]


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


def _latest_run_record(run_history: pd.DataFrame) -> Optional[pd.Series]:
    if not isinstance(run_history, pd.DataFrame) or run_history.empty:
        return None
    ordered = run_history
    if "STARTED_AT" in ordered.columns:
        ordered = ordered.sort_values(by="STARTED_AT", ascending=False)
    return ordered.iloc[0]


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


def _classification_source_badge(source: Any) -> str:
    normalized = str(source or "").strip().upper()
    if normalized == "MANUAL":
        return ui_strings.PROFILE_V2_COLUMNS_SOURCE_MANUAL
    if normalized:
        return ui_strings.PROFILE_V2_COLUMNS_SOURCE_HEURISTIC.format(source=normalized)
    return ui_strings.PROFILE_V2_COLUMNS_SOURCE_UNKNOWN


def _render_columns_grid(
    features: pd.DataFrame,
    classification: pd.DataFrame,
    suggestions: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
) -> None:
    st.subheader(ui_strings.PROFILE_V2_COLUMNS_SUBHEADER)
    prepared = _prepare_columns_grid(features, classification)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_COLUMNS_EMPTY)
        _render_column_detail_panel(
            prepared,
            features,
            classification,
            suggestions,
            table_fqn,
            helpers,
            session,
        )
        return
    working = prepared.copy()
    if "SOURCE" in working.columns:
        working["CLASSIFICATION_SOURCE"] = working["SOURCE"].map(
            _classification_source_badge
        )
    else:
        working["CLASSIFICATION_SOURCE"] = ui_strings.PROFILE_V2_COLUMNS_SOURCE_UNKNOWN
    display_columns: List[str] = [
        "COLUMN_NAME",
        "DATA_TYPE",
        "NULL_RATIO",
        "DISTINCT_RATIO",
        "MIN_VALUE",
        "MAX_VALUE",
        "CONTENT_TYPE",
        "SEMANTIC_ROLE",
        "CLASSIFICATION_SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    ]
    existing = [col for col in display_columns if col in working.columns]
    grid_col, detail_col = st.columns((3, 2))
    with grid_col:
        st.dataframe(working[existing], use_container_width=True, hide_index=True)
    with detail_col:
        _render_column_detail_panel(
            working,
            features,
            classification,
            suggestions,
            table_fqn,
            helpers,
            session,
        )
    _render_column_editors(working, table_fqn, helpers, session)


def _render_column_detail_panel(
    prepared_grid: pd.DataFrame,
    features: pd.DataFrame,
    classification: pd.DataFrame,
    suggestions: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
) -> None:
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMN_DETAIL_HEADER}**")
    column_names: List[str] = []
    if isinstance(prepared_grid, pd.DataFrame) and not prepared_grid.empty:
        if "COLUMN_NAME" in prepared_grid.columns:
            column_names = [
                str(value).strip()
                for value in prepared_grid["COLUMN_NAME"].tolist()
                if str(value or "").strip()
            ]
    placeholder_option = ui_strings.PROFILE_V2_COLUMN_DETAIL_SELECT_PLACEHOLDER
    options: List[str] = [placeholder_option, *column_names]
    stored_selection = st.session_state.get(
        "profile_column_detail_selection", placeholder_option
    )
    if stored_selection not in options:
        stored_selection = placeholder_option
    selection = st.selectbox(
        ui_strings.PROFILE_V2_COLUMN_DETAIL_SELECT_LABEL,
        options,
        index=options.index(stored_selection),
    )
    st.session_state["profile_column_detail_selection"] = selection
    if selection == placeholder_option:
        st.info(ui_strings.PROFILE_V2_COLUMN_DETAIL_PLACEHOLDER)
        return
    detail_data, helper_error = _load_column_detail_payload(
        helpers,
        session,
        table_fqn,
        selection,
        features,
        classification,
        suggestions,
    )
    if helper_error:
        st.warning(ui_strings.PROFILE_V2_COLUMN_DETAIL_ERROR.format(error=helper_error))
    if not detail_data:
        st.info(ui_strings.PROFILE_V2_COLUMN_DETAIL_PLACEHOLDER)
        return
    st.markdown(f"**{table_fqn} – {selection}**")
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMN_DETAIL_FEATURES_HEADER}**")
    _render_detail_key_values(
        detail_data.get("features") or {},
        ui_strings.PROFILE_V2_COLUMN_DETAIL_FEATURES_EMPTY,
    )
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_HEADER}**")
    _render_detail_classification(detail_data.get("classification") or {})
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMN_DETAIL_SUGGESTIONS_HEADER}**")
    _render_detail_suggestions(detail_data.get("suggested_checks") or [])


def _load_column_detail_payload(
    helpers: Any,
    session: Any,
    table_fqn: str,
    column_name: str,
    features: pd.DataFrame,
    classification: pd.DataFrame,
    suggestions: pd.DataFrame,
) -> Tuple[Dict[str, Any], Optional[str]]:
    detail_fn = getattr(helpers, "get_column_detail", None)
    helper_error: Optional[str] = None
    detail_data: Dict[str, Any] = {}
    if callable(detail_fn):
        try:
            detail_data = detail_fn(session, table_fqn, column_name)
        except Exception as exc:  # pragma: no cover - Streamlit runtime feedback only
            helper_error = str(exc)
    if not detail_data:
        detail_data = _build_column_detail_from_frames(
            features,
            classification,
            suggestions,
            table_fqn,
            column_name,
        )
    return detail_data, helper_error


def _build_column_detail_from_frames(
    features: pd.DataFrame,
    classification: pd.DataFrame,
    suggestions: pd.DataFrame,
    table_fqn: str,
    column_name: str,
) -> Dict[str, Any]:
    column = str(column_name or "").strip()
    if not column:
        return {}
    feature_record = _lookup_feature_record(features, column)
    classification_record = _lookup_classification_record(classification, column)
    suggestion_records = _lookup_suggestion_records(suggestions, column)
    feature_fields = (
        "DATA_TYPE",
        "ROW_COUNT",
        "NULL_COUNT",
        "NULL_RATIO",
        "DISTINCT_COUNT",
        "DISTINCT_RATIO",
        "MIN_VALUE",
        "MAX_VALUE",
        "AVG_LENGTH",
        "MAX_LENGTH",
    )
    classification_fields = (
        "CONTENT_TYPE",
        "SEMANTIC_ROLE",
        "SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    )
    return {
        "table_fqn": table_fqn,
        "column_name": column,
        "features": {
            key: feature_record.get(key)
            for key in feature_fields
            if key in feature_record
        },
        "classification": {
            key: classification_record.get(key)
            for key in classification_fields
            if key in classification_record
        },
        "suggested_checks": [
            {
                "RULE_ID": record.get("RULE_ID"),
                "CHECK_TYPE": record.get("CHECK_TYPE"),
                "SEVERITY": record.get("SEVERITY"),
                "PARAMETERS": record.get("PARAMS", record.get("PARAMETERS")),
                "RATIONALE": record.get("RATIONALE"),
            }
            for record in suggestion_records
        ],
    }


def _lookup_feature_record(features: pd.DataFrame, column_name: str) -> Dict[str, Any]:
    if not isinstance(features, pd.DataFrame) or features.empty:
        return {}
    if "COLUMN_NAME" not in features.columns:
        return {}
    folded = features["COLUMN_NAME"].astype(str).str.strip().str.casefold()
    matches = features.loc[folded == column_name.casefold()]
    if matches.empty:
        return {}
    return matches.iloc[0].to_dict()


def _lookup_classification_record(
    classification: pd.DataFrame, column_name: str
) -> Dict[str, Any]:
    latest = _latest_classifications(classification)
    if not latest:
        return {}
    normalized = str(column_name or "").strip()
    record = latest.get(normalized)
    if record:
        return record
    folded = normalized.casefold()
    for key, value in latest.items():
        if str(key or "").strip().casefold() == folded:
            return value
    return {}


def _lookup_suggestion_records(
    suggestions: pd.DataFrame, column_name: str
) -> List[Dict[str, Any]]:
    if not isinstance(suggestions, pd.DataFrame) or suggestions.empty:
        return []
    if "COLUMN_NAME" not in suggestions.columns:
        return []
    folded = suggestions["COLUMN_NAME"].astype(str).str.strip().str.casefold()
    matches = suggestions.loc[folded == column_name.casefold()]
    if matches.empty:
        return []
    return matches.to_dict("records")


def _render_detail_key_values(values: Dict[str, Any], empty_message: str) -> None:
    items = list(values.items())
    if not items:
        st.info(empty_message)
        return
    for start in range(0, len(items), 2):
        cols = st.columns(2)
        for offset, (key, value) in enumerate(items[start : start + 2]):
            cols[offset].markdown(
                f"**{key}**\n\n{_format_detail_value(value)}"
            )


def _render_detail_classification(classification: Dict[str, Any]) -> None:
    if not classification:
        st.info(ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_EMPTY)
        return
    content = classification.get("CONTENT_TYPE")
    semantic = classification.get("SEMANTIC_ROLE")
    st.write(
        f"**{ui_strings.PROFILE_V2_COLUMN_CONTENT_LABEL}:** "
        f"{_format_detail_value(content)}"
    )
    st.write(
        f"**{ui_strings.PROFILE_V2_COLUMN_SEMANTIC_LABEL}:** "
        f"{_format_detail_value(semantic)}"
    )
    confidence = _format_detail_value(classification.get("CONFIDENCE"))
    classified_at = _format_timestamp(classification.get("CLASSIFIED_AT"))
    st.caption(
        ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_STATUS.format(
            confidence=confidence,
            classified_at=classified_at,
        )
    )
    st.caption(
        ui_strings.PROFILE_V2_COLUMN_DETAIL_CLASSIFICATION_SOURCE.format(
            source=_classification_source_detail(classification.get("SOURCE")),
        )
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


def _render_detail_suggestions(records: List[Dict[str, Any]]) -> None:
    if not records:
        st.info(ui_strings.PROFILE_V2_COLUMN_DETAIL_SUGGESTIONS_EMPTY)
        return
    df = pd.DataFrame(records)
    if df.empty:
        st.info(ui_strings.PROFILE_V2_COLUMN_DETAIL_SUGGESTIONS_EMPTY)
        return
    if "PARAMETERS" in df.columns:
        df["PARAMETERS"] = df["PARAMETERS"].map(_stringify_params)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _format_detail_value(value: Any) -> str:
    if value is None:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return ui_strings.PROFILE_V2_VALUE_UNKNOWN
        return str(value)
    text = str(value).strip()
    if not text:
        return ui_strings.PROFILE_V2_VALUE_UNKNOWN
    return text


def _render_column_editors(
    prepared: pd.DataFrame,
    table_fqn: str,
    helpers: Any,
    session: Any,
) -> None:
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_COLUMNS_EDIT_EMPTY)
        return
    save_fn = getattr(helpers, "save_manual_classification", None)
    if not callable(save_fn):
        st.info(ui_strings.PROFILE_V2_COLUMNS_EDIT_UNAVAILABLE)
        return
    st.markdown(f"**{ui_strings.PROFILE_V2_COLUMNS_EDIT_HEADER}**")
    st.caption(ui_strings.PROFILE_V2_COLUMNS_EDIT_HELP)
    for record in prepared.to_dict("records"):
        _render_column_editor_form(record, table_fqn, session, save_fn)


def _render_column_editor_form(
    record: Dict[str, Any],
    table_fqn: str,
    session: Any,
    save_fn: Any,
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
) -> None:
    content_clean = (content_value or "").strip() or None
    semantic_clean = (semantic_value or "").strip() or None
    spinner = ui_strings.PROFILE_V2_COLUMN_EDIT_SPINNER.format(column=column_name)
    with st.spinner(spinner):
        try:
            save_fn(
                session,
                table_fqn,
                column_name,
                content_clean,
                semantic_clean,
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
    st.experimental_rerun()


def _render_suggested_checks_grid(suggested_checks: pd.DataFrame) -> None:
    st.subheader(ui_strings.PROFILE_V2_SUGGESTIONS_SUBHEADER)
    prepared = _prepare_suggested_checks(suggested_checks)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_SUGGESTIONS_EMPTY)
        return
    st.dataframe(prepared, use_container_width=True, hide_index=True)


def _resolve_helpers(profiling_helpers: Optional[Any]):
    return profiling_helpers or profiling_service


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
) -> _ProfilingData:
    column_features_fn = getattr(helpers, "get_column_features", None)
    column_features = (
        column_features_fn(session, table_fqn)
        if callable(column_features_fn)
        else pd.DataFrame()
    )
    effective_class_fn = getattr(helpers, "get_effective_classification", None)
    if callable(effective_class_fn):
        column_classification = effective_class_fn(session, table_fqn)
    else:
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
        column_features=column_features if isinstance(column_features, pd.DataFrame) else pd.DataFrame(),
        column_classification=column_classification if isinstance(column_classification, pd.DataFrame) else pd.DataFrame(),
        suggested_checks=suggested_checks if isinstance(suggested_checks, pd.DataFrame) else pd.DataFrame(),
        recent_runs=recent_runs if isinstance(recent_runs, pd.DataFrame) else pd.DataFrame(),
    )


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
                st.session_state["profile_last_table"] = target_fqn
                st.session_state["profile_last_run_id"] = None
                status_placeholder.success(
                    ui_strings.PROFILE_V2_RUN_SUCCESS.format(table=target_fqn)
                )
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
                    classify_fn(session, target_fqn)
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
                try:
                    suggestions_fn(session, target_fqn)
                except Exception as exc:
                    status_placeholder.error(
                        ui_strings.PROFILE_V2_SUGGESTIONS_ERROR.format(
                            error=str(exc)
                        )
                    )
                else:
                    st.session_state["profile_data_nonce"] += 1
                    status_placeholder.success(
                        ui_strings.PROFILE_V2_SUGGESTIONS_SUCCESS.format(
                            table=target_fqn
                        )
                    )

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
            data = _load_metadata(helpers, session, target_fqn)
        except Exception as exc:
            st.error(
                ui_strings.PROFILE_V2_METADATA_ERROR.format(error=str(exc))
            )
            return

    st.session_state["profile_last_table"] = target_fqn
    st.session_state["profile_last_run_id"] = _extract_last_run_id(data.recent_runs)

    _render_last_run_banner(data.recent_runs, target_fqn)
    st.divider()
    _render_columns_grid(
        data.column_features,
        data.column_classification,
        data.suggested_checks,
        target_fqn,
        helpers,
        session,
    )
    st.divider()
    _render_suggested_checks_grid(data.suggested_checks)
