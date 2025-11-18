"""Profiling v2 Streamlit view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
    table_fqn: str,
    helpers: Any,
    session: Any,
) -> None:
    st.subheader(ui_strings.PROFILE_V2_COLUMNS_SUBHEADER)
    prepared = _prepare_columns_grid(features, classification)
    if prepared.empty:
        st.info(ui_strings.PROFILE_V2_COLUMNS_EMPTY)
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
    st.dataframe(working[existing], use_container_width=True, hide_index=True)
    _render_column_editors(working, table_fqn, helpers, session)


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
        target_fqn,
        helpers,
        session,
    )
    st.divider()
    _render_suggested_checks_grid(data.suggested_checks)
