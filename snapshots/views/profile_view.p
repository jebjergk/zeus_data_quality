from __future__ import annotations
import html
import json
import math
import textwrap
import time
from dataclasses import dataclass, field
from datetime import datetime
from numbers import Integral, Real
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

from services.profile import build_profile_suggestion
from services.profiling import normalize_profile_row, run_table_profile, save_profile_results
from utils.meta import get_table_row_count
from views.table_picker import session_cache_token, stateless_table_picker


FULL_SCAN_WARNING_THRESHOLD = 1_000_000
MAX_TOP_N = 10


def _safe_int(value: Any) -> Optional[int]:
    """Convert a numeric-like value to an int when possible."""

    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Integral):
        return int(value)
    try:
        if isinstance(value, str):
            cleaned = value.replace(",", "").strip()
            if cleaned == "":
                return None
            value = cleaned
        value_float = float(value)
        if math.isnan(value_float):
            return None
        return int(round(value_float))
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    """Convert a numeric-like value to a float when possible."""

    if value is None:
        return None
    if isinstance(value, Real):
        value_float = float(value)
        if math.isnan(value_float):
            return None
        return value_float
    try:
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("%", "").strip()
            if cleaned == "":
                return None
            value = cleaned
        value_float = float(value)
        if math.isnan(value_float):
            return None
        return value_float
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return None
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False
    return None


@dataclass
class ColumnProfile:
    name: str
    data_type: str
    nulls: Optional[int]
    null_pct: Optional[float]
    distincts: Optional[int]
    distinct_pct: Optional[float]
    min_val: Optional[Any]
    max_val: Optional[Any]
    avg_len: Optional[float]
    whitespace_pct: Optional[float]
    whitespace_only_pct: Optional[float] = None
    row_cnt: Optional[int] = None
    distinct_ratio: Optional[float] = None
    top1_ratio: Optional[float] = None
    top3_ratio: Optional[float] = None
    len_min: Optional[float] = None
    len_max: Optional[float] = None
    numeric_like_ratio: Optional[float] = None
    yyyymmdd_ratio: Optional[float] = None
    ddmmyyyy_ratio: Optional[float] = None
    iso_ymd_ratio: Optional[float] = None
    date_parse_ratio_yyyymmdd: Optional[float] = None
    date_parse_ratio_ddmmyyyy: Optional[float] = None
    date_parse_ratio_iso: Optional[float] = None
    date_parse_ratio_best: Optional[float] = None
    date_parse_best_format: Optional[str] = None
    parsed_date_min: Optional[str] = None
    parsed_date_max: Optional[str] = None
    numeric_min: Optional[Any] = None
    numeric_max: Optional[Any] = None
    profile_min: Optional[Any] = None
    profile_max: Optional[Any] = None
    date_parse_success_ratio: Optional[float] = None
    date_sentinel_count: Optional[int] = None
    top_values: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    semantic_type: Optional[str] = None
    confidence: Optional[float] = None
    rationale: Optional[str] = None
    dq_selected: Optional[bool] = None
    dq_reason: Optional[str] = None


def _column_profile_from_payload(column: Dict[str, Any]) -> ColumnProfile:
    normalized = normalize_profile_row(column)
    top_values = normalized.get("top_values") or []
    if isinstance(top_values, list):
        top_values_list = [dict(entry) for entry in top_values if isinstance(entry, dict)]
    else:
        top_values_list = []
    best_format_value = normalized.get("date_parse_best_format")
    best_format_display: Optional[str]
    if best_format_value:
        fmt_key = str(best_format_value).lower()
        format_map = {
            "yyyymmdd": "YYYYMMDD",
            "ddmmyyyy": "DDMMYYYY",
            "iso": "YYYY-MM-DD",
            "iso_slash": "YYYY/MM/DD",
            "iso_dot": "YYYY.MM.DD",
            "dd_mm_yyyy": "DD-MM-YYYY",
            "dd_slash_mm": "DD/MM/YYYY",
            "mm_dd_yyyy": "MM-DD-YYYY",
            "mm_slash_dd": "MM/DD/YYYY",
            "dd_mon_yyyy": "DD-MON-YYYY",
            "mon_dd_yyyy": "MON-DD-YYYY",
        }
        best_format_display = format_map.get(fmt_key, str(best_format_value))
    else:
        best_format_display = None

    numeric_min_raw = normalized.get("numeric_min")
    numeric_max_raw = normalized.get("numeric_max")

    def _normalize_numeric_bound(raw_value: Any) -> Optional[Any]:
        numeric_value = _safe_float(raw_value)
        if numeric_value is not None:
            return numeric_value
        if raw_value is None:
            return None
        text_value = str(raw_value).strip()
        return text_value or None

    numeric_min_value = _normalize_numeric_bound(numeric_min_raw)
    numeric_max_value = _normalize_numeric_bound(numeric_max_raw)
    return ColumnProfile(
        name=str(normalized.get("column_name") or normalized.get("name") or ""),
        data_type=str(normalized.get("data_type") or ""),
        nulls=_safe_int(normalized.get("nulls")),
        null_pct=_safe_float(normalized.get("null_pct")),
        distincts=_safe_int(normalized.get("distincts")),
        distinct_pct=_safe_float(normalized.get("distinct_pct")),
        min_val=normalized.get("min_val"),
        max_val=normalized.get("max_val"),
        avg_len=_safe_float(normalized.get("avg_len")),
        whitespace_pct=_safe_float(normalized.get("whitespace_pct")),
        row_cnt=_safe_int(normalized.get("row_cnt")),
        distinct_ratio=_safe_float(normalized.get("distinct_ratio")),
        top1_ratio=_safe_float(normalized.get("top1_ratio")),
        top3_ratio=_safe_float(normalized.get("top3_ratio")),
        len_min=_safe_float(normalized.get("len_min")),
        len_max=_safe_float(normalized.get("len_max")),
        numeric_like_ratio=_safe_float(normalized.get("numeric_like_ratio")),
        yyyymmdd_ratio=_safe_float(normalized.get("date_pattern_yyyymmdd_ratio")),
        ddmmyyyy_ratio=_safe_float(normalized.get("date_pattern_ddmmyyyy_ratio")),
        iso_ymd_ratio=_safe_float(normalized.get("date_pattern_iso_ymd_ratio")),
        date_parse_ratio_yyyymmdd=_safe_float(normalized.get("date_parse_ratio_yyyymmdd")),
        date_parse_ratio_ddmmyyyy=_safe_float(normalized.get("date_parse_ratio_ddmmyyyy")),
        date_parse_ratio_iso=_safe_float(normalized.get("date_parse_ratio_iso")),
        date_parse_ratio_best=_safe_float(normalized.get("date_parse_ratio_best")),
        date_parse_success_ratio=_safe_float(normalized.get("date_parse_success_ratio")),
        date_parse_best_format=best_format_display,
        parsed_date_min=str(normalized.get("parsed_date_min")) if normalized.get("parsed_date_min") else None,
        parsed_date_max=str(normalized.get("parsed_date_max")) if normalized.get("parsed_date_max") else None,
        numeric_min=numeric_min_value,
        numeric_max=numeric_max_value,
        profile_min=normalized.get("profile_min"),
        profile_max=normalized.get("profile_max"),
        date_sentinel_count=_safe_int(normalized.get("date_sentinel_count")),
        top_values=top_values_list,
        error=normalized.get("error"),
        semantic_type=normalized.get("semantic_type"),
        confidence=_safe_float(normalized.get("confidence")),
        rationale=normalized.get("rationale"),
        whitespace_only_pct=_safe_float(normalized.get("whitespace_only_pct")),
        dq_selected=_safe_bool(normalized.get("dq_selected")),
        dq_reason=_stringify_for_display(normalized.get("dq_reason")),
    )


def _split_fqn(fqn: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not fqn:
        return None, None, None
    parts = [p.strip('"') for p in fqn.split(".") if p]
    if len(parts) != 3:
        return None, None, None
    return parts[0], parts[1], parts[2]


def _recommend_sample_pct(row_count: Optional[int]) -> Tuple[float, str]:
    if row_count is None:
        return 10.0, (
            "Defaulting to a 10% sample because row count metadata was unavailable. "
            "Sampling avoids scanning the full table by default."
        )

    if row_count <= 100_000:
        return 0.0, (
            "Full scan recommended because the table has 100k rows or fewer. "
            "Scanning all rows keeps runtime low while ensuring precise metrics."
        )

    if row_count <= 1_000_000:
        return 10.0, (
            "Sampling 10% keeps the scan under roughly 100k rows while providing representative metrics."
        )

    if row_count <= 10_000_000:
        return 5.0, (
            "Sampling 5% targets at most about 500k rows to balance coverage and cost."
        )

    if row_count <= 100_000_000:
        return 1.0, (
            "Sampling 1% limits the profile to around one million rows on large tables."
        )

    return 0.5, (
        "Sampling 0.5% keeps the profile under roughly 500k rows even on very large tables."
    )


def _load_table_row_count(session_obj, fqn: str) -> Optional[int]:
    if not session_obj or not fqn:
        return None

    @st.cache_data(ttl=600, show_spinner=False)
    def _load_row_count(cache_key: Tuple[str, str]) -> Optional[int]:
        _, fqn_key = cache_key
        db, schema, table = _split_fqn(fqn_key)
        if not (db and schema and table):
            return None
        try:
            return get_table_row_count(session_obj, db, schema, table)
        except Exception:
            return None

    return _load_row_count((session_cache_token(session_obj), fqn))


def _table_picker(session_obj, preselect_fqn: Optional[str]):
    return stateless_table_picker(session_obj, preselect_fqn)


def _stringify_for_display(value: Any) -> Any:
    """Return a compact, human readable representation for complex values."""

    def _stringify_compound(compound: Any) -> str:
        if isinstance(compound, dict):
            if not compound:
                return ""
            if len(compound) == 1:
                (key, single_value), = compound.items()
                if str(key).lower() in {"value", "val", "text"}:
                    return _to_string(single_value)
            parts = []
            for key, sub_value in compound.items():
                parts.append(f"{key}: {_to_string(sub_value)}")
            return ", ".join(parts)
        if isinstance(compound, (list, tuple, set)):
            if isinstance(compound, set):
                try:
                    compound = sorted(compound)
                except Exception:
                    compound = list(compound)
            items = [_to_string(item) for item in compound]
            return ", ".join(item for item in items if item)
        return str(compound)

    def _to_string(obj: Any) -> str:
        if obj is None:
            return ""
        if isinstance(obj, (dict, list, tuple, set)):
            return _stringify_compound(obj)
        if isinstance(obj, str):
            stripped = obj.strip()
            if stripped.startswith("{") or stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except Exception:
                    return obj
                return _stringify_compound(parsed)
            return obj
        return str(obj)

    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        return _stringify_compound(value)
    if isinstance(value, str):
        stripped_value = value.strip()
        if stripped_value.startswith("{") or stripped_value.startswith("["):
            try:
                parsed_value = json.loads(stripped_value)
            except Exception:
                return value
            return _stringify_compound(parsed_value)
    return value


def _format_top_value_cell(value: Any) -> str:
    raw_value = value
    try:
        if pd.isna(raw_value):
            raw_value = None
    except Exception:
        pass

    if raw_value is None:
        return "NULL"
    if isinstance(raw_value, str):
        if raw_value == "":
            return '""'
        ws_prefix = "__WS_LEN__:"
        if raw_value.startswith(ws_prefix):
            length_part = raw_value[len(ws_prefix) :].strip()
            try:
                length_value = int(length_part)
            except Exception:
                length_value = None
            if length_value is not None:
                return f"␣×{length_value}"

    formatted = _stringify_for_display(raw_value)
    if formatted is None:
        return ""
    return str(formatted)


def _profiles_to_frame(profiles: Iterable[ColumnProfile]) -> pd.DataFrame:
    records = []
    for profile in profiles:
        def _pct(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            try:
                return round(float(value) * 100.0, 2)
            except Exception:
                return None

        records.append(
            {
                "column_name": profile.name,
                "data_type": profile.data_type,
                "nulls": profile.nulls,
                "null_pct": round(profile.null_pct, 4) if profile.null_pct is not None else None,
                "distincts": profile.distincts,
                "distinct_pct": round(profile.distinct_pct, 4) if profile.distinct_pct is not None else None,
                "min_val": _stringify_for_display(profile.min_val),
                "max_val": _stringify_for_display(profile.max_val),
                "avg_len": profile.avg_len if profile.avg_len is not None else None,
                "whitespace_pct": round(profile.whitespace_pct, 2) if profile.whitespace_pct is not None else None,
                "whitespace_only_pct": round(profile.whitespace_only_pct, 2)
                if profile.whitespace_only_pct is not None
                else None,
                "row_cnt": profile.row_cnt,
                "len_min": round(profile.len_min, 2) if profile.len_min is not None else None,
                "len_max": round(profile.len_max, 2) if profile.len_max is not None else None,
                "distinct_ratio_pct": _pct(profile.distinct_ratio),
                "top1_ratio_pct": _pct(profile.top1_ratio),
                "top3_ratio_pct": _pct(profile.top3_ratio),
                "numeric_like_pct": _pct(profile.numeric_like_ratio),
                "date_pattern_yyyymmdd_pct": _pct(profile.yyyymmdd_ratio),
                "date_pattern_ddmmyyyy_pct": _pct(profile.ddmmyyyy_ratio),
                "date_pattern_iso_ymd_pct": _pct(profile.iso_ymd_ratio),
                "date_parse_yyyymmdd_pct": _pct(profile.date_parse_ratio_yyyymmdd),
                "date_parse_ddmmyyyy_pct": _pct(profile.date_parse_ratio_ddmmyyyy),
                "date_parse_iso_pct": _pct(profile.date_parse_ratio_iso),
                "date_parse_best_pct": _pct(profile.date_parse_ratio_best),
                "date_parse_success_pct": _pct(profile.date_parse_success_ratio),
                "date_parse_best_format": profile.date_parse_best_format,
                "parsed_date_min": _stringify_for_display(profile.parsed_date_min),
                "parsed_date_max": _stringify_for_display(profile.parsed_date_max),
                "numeric_min": _stringify_for_display(profile.numeric_min),
                "numeric_max": _stringify_for_display(profile.numeric_max),
                "profile_min": _stringify_for_display(profile.profile_min),
                "profile_max": _stringify_for_display(profile.profile_max),
                "numeric_like_ratio": profile.numeric_like_ratio,
                "date_parse_success_ratio": profile.date_parse_success_ratio,
                "date_sentinel_count": profile.date_sentinel_count,
                "top_values": profile.top_values,
                "error": profile.error,
                "semantic_type": profile.semantic_type,
                "confidence": profile.confidence,
                "rationale": profile.rationale,
                "dq_selected": bool(profile.dq_selected)
                if profile.dq_selected is not None
                else False,
                "dq_reason": str(profile.dq_reason)
                if profile.dq_reason is not None
                else "",
            }
        )
    df = pd.DataFrame.from_records(records)
    if not df.empty:
        display_cols = [
            "column_name",
            "data_type",
            "nulls",
            "null_pct",
            "distincts",
            "distinct_pct",
            "min_val",
            "max_val",
            "avg_len",
            "whitespace_pct",
            "error",
            "semantic_type",
            "confidence",
            "rationale",
        ]
        missing = [c for c in display_cols if c not in df.columns]
        df = df.reindex(columns=[c for c in display_cols if c not in missing] + [c for c in df.columns if c not in display_cols])
    return df


def _format_count(value: Any) -> str:
    if value is None:
        return ""
    try:
        value_float = float(value)
        if math.isnan(value_float):
            return ""
        return f"{int(round(value_float)):,}"
    except Exception:
        return ""


def _format_percentage(value: Any) -> str:
    if value is None:
        return ""
    try:
        value_float = float(value)
        if math.isnan(value_float):
            return ""
        return f"{value_float:.2f}"
    except Exception:
        return ""


def _format_card_value(value: Any, *, decimals: int = 2, allow_commas: bool = False) -> str:
    if value is None:
        return "—"
    if isinstance(value, (int, float)):
        try:
            number = float(value)
        except Exception:
            number = math.nan
        if math.isnan(number):
            return "—"
        if math.isclose(number, round(number), abs_tol=1e-9):
            integer_value = int(round(number))
            return f"{integer_value:,}" if allow_commas else f"{integer_value}"
        fmt = f"{{:,.{decimals}f}}" if allow_commas else f"{{:.{decimals}f}}"
        return fmt.format(number)
    text_value = str(value).strip()
    return text_value or "—"


def _render_metric_card(
    title: str,
    metrics: List[Tuple[str, str, Optional[str]]],
    subtitle: Optional[str] = None,
) -> None:
    if not metrics:
        return
    metric_html: List[str] = []
    for label, raw_value, tooltip in metrics:
        label_html = html.escape(label)
        display_value = raw_value if raw_value else "—"
        value_html = html.escape(display_value)
        if tooltip:
            tooltip_html = html.escape(tooltip)
            value_block = f"<div class=\"metric-value\" title=\"{tooltip_html}\">{value_html}</div>"
        else:
            value_block = f"<div class=\"metric-value\">{value_html}</div>"
        metric_html.append(
            textwrap.dedent(
                """
                <div class="metric">
                    <div class="metric-label">{label}</div>
                    {value_block}
                </div>
                """
            ).format(label=label_html, value_block=value_block).strip()
        )

    subtitle_html = (
        f"<div class=\"small\">{html.escape(subtitle)}</div>"
        if subtitle
        else ""
    )
    card_html = textwrap.dedent(
        """
        <div class="card">
            <div class="kv">{title}</div>
            {subtitle}
            <div class="metrics-grid">
                {metrics}
            </div>
        </div>
        """
    ).format(
        title=html.escape(title), subtitle=subtitle_html, metrics="".join(metric_html)
    ).strip()
    st.markdown(card_html, unsafe_allow_html=True)

def render_profile(session, meta_db: str, meta_schema: str) -> None:  # noqa: ARG001 - interface matches requirement
    st.header("🧪 Profile Table")
    st.caption(
        "Profile a table to explore null rates, distinct counts, ranges, and common values before defining data quality checks."
    )

    base_selection = st.session_state.get("profile_target_fqn")
    _db_sel, _sch_sel, _tbl_sel, selected_fqn = _table_picker(session, base_selection)
    if selected_fqn:
        st.session_state["profile_target_fqn"] = selected_fqn

    st.divider()
    controls = st.columns(3)
    suggested_pct = 10.0
    row_count: Optional[int] = None
    selected_reason = (
        "Defaulting to a 10% sample. Enter 0 for a full table scan."
    )
    if selected_fqn:
        row_count = _load_table_row_count(session, selected_fqn)
        suggested_pct, selected_reason = _recommend_sample_pct(row_count)

    sample_pct_state_key = "profile_sample_pct"
    sample_target_key = "profile_sample_target"
    if st.session_state.get(sample_target_key) != selected_fqn:
        st.session_state[sample_target_key] = selected_fqn
        st.session_state[sample_pct_state_key] = float(suggested_pct)
    elif sample_pct_state_key not in st.session_state:
        st.session_state[sample_pct_state_key] = float(suggested_pct)

    approx_rows = None
    if row_count is not None and suggested_pct > 0:
        approx_rows = int(round(row_count * suggested_pct / 100.0))

    reason_parts = [selected_reason]
    if row_count is not None:
        reason_parts.append(f"Table metadata reports approximately {row_count:,} rows.")
    if approx_rows:
        reason_parts.append(f"This sample size profiles about {approx_rows:,} rows.")
    reason_parts.append(
        "The suggested value relies on Snowflake metadata only, so it doesn't trigger an extra table scan."
    )
    reason_parts.append("Enter 0 for a full table scan.")
    sample_help_text = "\n".join(reason_parts)

    with controls[0]:
        sample_pct_input = st.number_input(
            "Sample %",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            help=sample_help_text,
            key=sample_pct_state_key,
        )
        sample_pct = None if math.isclose(sample_pct_input, 0.0, abs_tol=1e-6) else sample_pct_input
    with controls[1]:
        top_n = st.number_input(
            "Top N values",
            min_value=1,
            max_value=MAX_TOP_N,
            value=min(10, MAX_TOP_N),
            step=1,
            help=f"Collect up to {MAX_TOP_N} of the most common values per column.",
        )
    with controls[2]:
        save_profile = st.button("💾 Save Profile", disabled=True, help="Coming soon")
        if save_profile:
            st.info("Saving profiles is not yet supported.")

    stored_profile_result = st.session_state.get("profile_results")
    has_suggested_columns = False
    if stored_profile_result:
        for column_payload in stored_profile_result.get("columns", []):
            if _safe_bool(column_payload.get("dq_selected")):
                has_suggested_columns = True
                break

    selection_counts_state = st.session_state.get("profile_selection_counts")
    if (
        isinstance(selection_counts_state, tuple)
        and len(selection_counts_state) == 2
        and all(isinstance(val, (int, float)) for val in selection_counts_state)
    ):
        selection_counts = (
            int(selection_counts_state[0]),
            int(selection_counts_state[1]),
        )
    elif stored_profile_result:
        selected_count = 0
        total_count = 0
        for column_payload in stored_profile_result.get("columns", []):
            if _safe_bool(column_payload.get("dq_selected")):
                selected_count += 1
            total_count += 1
        selection_counts = (selected_count, total_count)
    else:
        selection_counts = (0, 0)

    st.caption(
        f"Suggested: {int(selection_counts[0])} of {int(selection_counts[1])} columns selected"
    )

    button_cols = st.columns([1, 1, 1, 2])
    with button_cols[0]:
        run_profile = st.button("▶️ Run Profile", type="primary")
    with button_cols[1]:
        suggest_cfg = st.button(
            "✨ Suggest DQ Config",
            type="secondary",
            disabled=not stored_profile_result,
        )
    with button_cols[2]:
        use_suggested_cfg = st.button(
            "Use suggested columns in DQ config",
            type="secondary",
            disabled=not (stored_profile_result and has_suggested_columns),
            help="Load only the columns marked as suggested into the configuration editor.",
        )

    profile_result = stored_profile_result

    if run_profile:
        if not session:
            st.error("No active Snowpark session — unable to profile tables.")
        elif not selected_fqn:
            st.warning("Select a database, schema, and table to profile.")
        else:
            with st.spinner("Profiling table..."):
                start = time.time()
                profile_error: Optional[Exception] = None
                summary_raw: Dict[str, Any] = {}
                column_rows: List[Dict[str, Any]] = []
                try:
                    summary_raw, column_rows = run_table_profile(
                        session=session,
                        fqn=selected_fqn,
                        sample_pct=sample_pct,
                        top_n=int(min(top_n, MAX_TOP_N)),
                    )
                except Exception as exc:  # pragma: no cover - Snowflake specific
                    profile_error = exc
                duration = time.time() - start

            if profile_error is not None:
                st.error(f"Failed to profile table: {profile_error}")
            else:
                rows_profiled = int(summary_raw.get("rows_profiled") or 0)
                profiles: List[ColumnProfile] = []
                columns_payload: List[Dict[str, Any]] = []
                for column in column_rows:
                    normalized_column = normalize_profile_row(column)
                    columns_payload.append(normalized_column)
                    profiles.append(_column_profile_from_payload(normalized_column))
                profile_result = {
                    "target_table": selected_fqn,
                    "summary": {
                        "rows_profiled": rows_profiled,
                        "sample_pct": summary_raw.get("sample_pct"),
                        "duration_sec": duration,
                        "columns": len(profiles),
                    },
                    "columns": columns_payload,
                    "top_n": int(top_n),
                }
                st.session_state["profile_results"] = profile_result
                st.rerun()

    def _load_suggestion(profile_payload: Dict[str, Any], success_message: str) -> None:
        suggestion = build_profile_suggestion(profile_payload)
        if not suggestion:
            st.info("No suggestions available for the current profile.")
            return
        st.session_state["cfg_mode"] = "edit"
        st.session_state["selected_config_id"] = None
        st.session_state["editor_target_fqn"] = profile_payload.get("target_table")
        st.session_state["profile_suggestion"] = suggestion
        try:
            current_params = dict(st.query_params)  # type: ignore[attr-defined]
        except Exception:
            current_params = {}
        current_params["page"] = "cfg"
        try:
            st.query_params = current_params  # type: ignore[attr-defined]
        except Exception:
            pass
        st.session_state["page"] = "cfg"
        st.success(success_message)
        st.rerun()

    def _selected_columns(profile_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        for column_payload in profile_payload.get("columns", []):
            if _safe_bool(column_payload.get("dq_selected")):
                selected.append(column_payload)
        return selected

    if suggest_cfg and profile_result:
        selected_columns = _selected_columns(profile_result)
        if not selected_columns:
            st.info("Select at least one column before generating DQ suggestions.")
        else:
            filtered_profile = dict(profile_result)
            filtered_summary = dict(filtered_profile.get("summary") or {})
            filtered_summary["columns"] = len(selected_columns)
            filtered_profile["summary"] = filtered_summary
            filtered_profile["columns"] = selected_columns
            _load_suggestion(filtered_profile, "Loaded profile suggestion into the configuration editor.")

    if use_suggested_cfg and profile_result:
        selected_columns = _selected_columns(profile_result)
        if not selected_columns:
            st.info("No suggested columns available for the current profile.")
        else:
            filtered_profile = dict(profile_result)
            filtered_summary = dict(filtered_profile.get("summary") or {})
            filtered_profile["columns"] = selected_columns
            filtered_summary["columns"] = len(selected_columns)
            filtered_profile["summary"] = filtered_summary
            _load_suggestion(
                filtered_profile,
                "Loaded suggested columns into the configuration editor.",
            )

    if not profile_result:
        return

    summary = profile_result.get("summary", {})
    metrics_cols = st.columns(3)
    metrics_cols[0].metric("Rows profiled", f"{summary.get('rows_profiled', 0):,}")
    sample_pct_display = summary.get("sample_pct")
    sample_label = "Full scan" if sample_pct_display is None else f"{float(sample_pct_display):.1f}%"
    metrics_cols[1].metric("Sampling", sample_label)
    metrics_cols[2].metric("Duration", f"{summary.get('duration_sec', 0.0):.2f}s")

    if summary.get("sample_pct") is None and summary.get("rows_profiled", 0) > FULL_SCAN_WARNING_THRESHOLD:
        profiled = int(summary.get("rows_profiled", 0))
        st.warning(
            f"Full table scan processed {profiled:,} rows. Consider sampling to improve performance.",
            icon="⚠️",
        )

    profiles_raw = profile_result.get("columns", [])
    profiles = [_column_profile_from_payload(col) for col in profiles_raw]
    df = _profiles_to_frame(profiles)
    required_defaults: Dict[str, Any] = {
        "dq_selected": False,
        "dq_reason": "",
        "whitespace_pct": None,
        "whitespace_only_pct": None,
    }
    for column_name, default_value in required_defaults.items():
        if column_name not in df.columns:
            df[column_name] = default_value
    if "dq_selected" in df.columns:
        df["dq_selected"] = df["dq_selected"].apply(lambda value: (_safe_bool(value) or False))
    if "dq_reason" in df.columns:
        df["dq_reason"] = df["dq_reason"].apply(
            lambda value: _stringify_for_display(value) or ""
        )
    for whitespace_column in ("whitespace_pct", "whitespace_only_pct"):
        if whitespace_column in df.columns:
            df[whitespace_column] = pd.to_numeric(df[whitespace_column], errors="coerce")

    filter_box = st.container()
    with filter_box:
        st.subheader("Filters", anchor=False)
        filter_cols = st.columns(4)
        high_null = filter_cols[0].toggle("High null % (>20%)", value=False)
        unique_candidates = filter_cols[1].toggle("Unique candidates", value=False)
        low_cardinality = filter_cols[2].toggle("Low cardinality", value=False)
        whitespace_risk = filter_cols[3].toggle("Whitespace risk", value=False)
        st.markdown("**Semantic tags**")
        semantic_cols = st.columns(7)
        filter_identifiers = semantic_cols[0].checkbox("Identifiers", value=False)
        filter_financial = semantic_cols[1].checkbox("Financial", value=False)
        filter_instrument = semantic_cols[2].checkbox("Instrument", value=False)
        filter_geo = semantic_cols[3].checkbox("Geo", value=False)
        filter_contact = semantic_cols[4].checkbox("Contact", value=False)
        filter_date_text = semantic_cols[5].checkbox("Date (Text)", value=False)
        filter_ref_codes = semantic_cols[6].checkbox("Reference Codes", value=False)

    save_enabled = bool(session and meta_db and meta_schema)
    if not save_enabled:
        st.session_state.pop("profile_save_toggle", None)
        st.session_state.pop("_profile_save_prev", None)
        st.session_state.pop("profile_saved_run_id", None)

    save_help = (
        "Persist the current profile results to metadata tables."
        if save_enabled
        else "Connect to Snowflake and select metadata targets to enable saving."
    )
    save_toggle = st.toggle(
        "💾 Save Profile",
        key="profile_save_toggle",
        value=False,
        disabled=not save_enabled,
        help=save_help,
    )

    prev_toggle = st.session_state.get("_profile_save_prev", False)
    st.session_state["_profile_save_prev"] = save_toggle

    if save_toggle and save_enabled and not prev_toggle:
        run_info = {**(profile_result.get("summary") or {})}
        run_info.update(
            {
                "target_table": profile_result.get("target_table"),
                "top_n": profile_result.get("top_n"),
                "saved_at": datetime.utcnow().isoformat() + "Z",
            }
        )
        rows_payload: List[Dict[str, Any]] = []
        for column_profile in profile_result.get("columns", []):
            normalized_column = normalize_profile_row(column_profile)
            column_name = normalized_column.get("column_name")
            if not column_name:
                continue
            rows_payload.append(normalized_column)

        try:
            run_id = save_profile_results(
                session=session,
                meta_db=meta_db,
                meta_schema=meta_schema,
                run_info=run_info,
                rows=rows_payload,
            )
        except Exception as exc:  # pragma: no cover - Snowflake specific
            st.error(f"Failed to save profile: {exc}")
        else:
            st.session_state["profile_saved_run_id"] = run_id
            st.success(f"Saved profile run {run_id} to metadata.")

    if not save_toggle:
        st.session_state.pop("profile_saved_run_id", None)

    filtered_df = df.copy()
    if high_null:
        filtered_df = filtered_df[(filtered_df["null_pct"].fillna(0) > 0.20)]
    if unique_candidates and summary.get("rows_profiled"):
        rows = float(summary["rows_profiled"])
        filtered_df = filtered_df[(filtered_df["distincts"].fillna(0) >= rows) & (filtered_df["nulls"].fillna(0) == 0)]
    if low_cardinality and summary.get("rows_profiled"):
        rows = float(summary["rows_profiled"])
        filtered_df = filtered_df[(filtered_df["distincts"].fillna(rows) <= max(20, rows * 0.1))]
    if whitespace_risk:
        filtered_df = filtered_df[(filtered_df["whitespace_pct"].fillna(0) > 5)]

    semantic_filter_map = {
        "Identifiers": {"ACCOUNT_ID", "ORDER_ID", "TRADE_ID", "UUID", "IBAN", "REF_CODE"},
        "Financial": {"PRICE/AMOUNT/QUANTITY", "IBAN", "BIC"},
        "Instrument": {"ISIN", "TICKER/SYMBOL"},
        "Geo": {"COUNTRY_CODE/NAME", "CURRENCY_CODE", "BIC"},
        "Contact": {"EMAIL", "PHONE"},
        "Date (Text)": {"DATE_IN_TEXT"},
        "Reference Codes": {"REF_CODE"},
    }
    active_semantic_filters: List[str] = []
    if filter_identifiers:
        active_semantic_filters.append("Identifiers")
    if filter_financial:
        active_semantic_filters.append("Financial")
    if filter_instrument:
        active_semantic_filters.append("Instrument")
    if filter_geo:
        active_semantic_filters.append("Geo")
    if filter_contact:
        active_semantic_filters.append("Contact")
    if filter_date_text:
        active_semantic_filters.append("Date (Text)")
    if filter_ref_codes:
        active_semantic_filters.append("Reference Codes")

    if active_semantic_filters:
        allowed_types = set()
        for key in active_semantic_filters:
            allowed_types.update(semantic_filter_map.get(key, set()))
        filtered_df = filtered_df[filtered_df["semantic_type"].isin(allowed_types)]
    display_df = filtered_df.copy()
    grid_columns = [
        "Include",
        "Column",
        "Physical Type",
        "Nulls",
        "Distinct",
        "Avg Length",
        "Min Value",
        "Max Value",
        "Whitespace %",
        "Guessed Type",
        "Confidence",
        "Note",
    ]
    if not display_df.empty:
        def _is_string_type_name(type_name: Any) -> bool:
            upper = str(type_name or "").upper()
            return any(token in upper for token in ("CHAR", "STRING", "TEXT", "VARCHAR"))

        def _is_numeric_type_name(type_name: Any) -> bool:
            upper = str(type_name or "").upper()
            return any(
                token in upper
                for token in (
                    "NUMBER",
                    "NUMERIC",
                    "DECIMAL",
                    "INT",
                    "INTEGER",
                    "BIGINT",
                    "SMALLINT",
                    "TINYINT",
                    "BYTEINT",
                    "FLOAT",
                    "DOUBLE",
                    "REAL",
                )
            )

        def _format_count_with_pct(count_value: Any, pct_value: Any) -> str:
            count_int = _safe_int(count_value)
            pct_ratio = _safe_float(pct_value)
            if count_int is None and pct_ratio is None:
                return "—"
            parts: List[str] = []
            if count_int is not None:
                parts.append(f"{count_int:,}")
            if pct_ratio is not None:
                pct_value = max(0.0, pct_ratio * 100.0)
                parts.append(f"({pct_value:.1f}%)")
            return " ".join(parts) if parts else "—"

        def _format_length_stats_cell(row: pd.Series) -> str:
            data_type = row.get("data_type")
            if not (_is_string_type_name(data_type) or _is_numeric_type_name(data_type)):
                return "—"

            len_min_value = _safe_float(row.get("len_min"))
            avg_value = _safe_float(row.get("avg_len"))
            len_max_value = _safe_float(row.get("len_max"))

            def _fmt_bound(value: Optional[float]) -> str:
                if value is None:
                    return "—"
                try:
                    return f"{int(round(value))}"
                except Exception:
                    return "—"

            min_display = _fmt_bound(len_min_value)
            avg_display = f"{avg_value:.1f}" if avg_value is not None else "—"
            max_display = _fmt_bound(len_max_value)

            return "/".join([min_display, avg_display, max_display])

        def _format_value_cell(raw_value: Any) -> str:
            text_value = _stringify_for_display(raw_value)
            if text_value is None:
                return "—"
            text = str(text_value).strip()
            if not text or text.lower() == "nan":
                return "—"
            if len(text) > 50:
                return text[:47] + "..."
            return text

        def _format_semantic_label(value: Any) -> str:
            if value is None:
                return "Unknown"
            text_value = str(value).strip()
            if not text_value:
                return "Unknown"
            normalized = text_value.replace("_", " ")
            return normalized.title()

        def _format_confidence_display(value: Optional[float]) -> str:
            if value is None or math.isnan(value):
                return ""
            if value < 10.0:
                formatted = f"{value:.1f}".rstrip("0").rstrip(".")
            else:
                formatted = f"{value:.0f}"
            return f"{formatted}%"

        def _confidence_style(value: Any) -> Dict[str, str]:
            numeric = _safe_float(value)
            if numeric is None:
                if isinstance(value, str) and value.endswith("%"):
                    numeric = _safe_float(value.replace("%", ""))
            if numeric is None or math.isnan(float(numeric)):
                return {}
            if numeric >= 90.0:
                return {"backgroundColor": "#2e7d32", "color": "#ffffff"}
            if numeric >= 75.0:
                return {"backgroundColor": "#f9a825", "color": "#000000"}
            return {"backgroundColor": "#9e9e9e", "color": "#ffffff"}

        def _format_whitespace_display(value: Any) -> str:
            numeric = _safe_float(value)
            if numeric is None or math.isclose(numeric, 0.0, abs_tol=1e-9):
                return "—"
            return f"{numeric:.1f}%"

        def _whitespace_cell_style(value: Any) -> Dict[str, str]:
            numeric = _safe_float(value)
            if numeric is None:
                if isinstance(value, str) and value.endswith("%"):
                    numeric = _safe_float(value.replace("%", ""))
            if numeric is None or numeric <= 0:
                return {}
            if numeric >= 20.0:
                return {"color": "#c62828", "font-weight": "600"}
            if numeric >= 5.0:
                return {"color": "#f9a825", "font-weight": "600"}
            return {}

        def _column_config_with_optional_style(
            factory: Callable[..., Any], *args: Any, cell_style: Optional[Callable[[Any], Dict[str, str]]] = None, **kwargs: Any
        ) -> Any:
            """Create a column config, gracefully ignoring unsupported cell_style argument."""

            if cell_style is None:
                return factory(*args, **kwargs)
            try:
                return factory(*args, cell_style=cell_style, **kwargs)
            except TypeError:
                return factory(*args, **kwargs)

        def _compose_note(row: pd.Series) -> str:
            text_value = _stringify_for_display(row.get("dq_reason"))
            if not text_value:
                text_value = _stringify_for_display(row.get("rationale"))
            if not text_value:
                text_value = _stringify_for_display(row.get("error"))
            note = str(text_value or "").strip()
            if len(note) > 120:
                return note[:117] + "..."
            return note

        display_df_local = display_df.copy()
        display_df_local["Include"] = (
            display_df_local.get("dq_selected", False).apply(lambda value: _safe_bool(value) or False)
        )
        display_df_local["Include"] = display_df_local["Include"].astype(bool)
        display_df_local["Column"] = display_df_local.get("column_name", "").fillna("").astype(str)
        display_df_local["Physical Type"] = (
            display_df_local.get("data_type", "").fillna("").astype(str)
        )
        display_df_local["Nulls"] = display_df_local.apply(
            lambda row: _format_count_with_pct(row.get("nulls"), row.get("null_pct")), axis=1
        )
        display_df_local["Distinct"] = display_df_local.apply(
            lambda row: _format_count_with_pct(row.get("distincts"), row.get("distinct_pct")), axis=1
        )
        display_df_local["Avg Length"] = display_df_local.apply(
            _format_length_stats_cell, axis=1
        )
        display_df_local["Min Value"] = display_df_local["min_val"].apply(_format_value_cell)
        display_df_local["Max Value"] = display_df_local["max_val"].apply(_format_value_cell)
        display_df_local["Guessed Type"] = display_df_local["semantic_type"].apply(
            _format_semantic_label
        )
        display_df_local["Whitespace %"] = display_df_local["whitespace_pct"].apply(
            _format_whitespace_display
        )

        def _normalize_confidence(raw_value: Any) -> Optional[float]:
            confidence_raw = _safe_float(raw_value)
            if confidence_raw is None:
                return None
            if confidence_raw <= 1.0:
                confidence_raw *= 100.0
            confidence_raw = max(0.0, min(confidence_raw, 100.0))
            return confidence_raw

        display_df_local["_confidence_pct"] = display_df_local["confidence"].apply(
            _normalize_confidence
        )
        display_df_local["Confidence"] = display_df_local["_confidence_pct"].apply(
            lambda value: _format_confidence_display(value) or "—"
        )
        display_df_local["Note"] = display_df_local.apply(_compose_note, axis=1)

        editor_df = display_df_local[grid_columns].copy()
        editor_df["Include"] = editor_df["Include"].astype(bool)
        column_config = {
            "Include": st.column_config.CheckboxColumn(
                "Include",
                help="Toggle to include the column in downstream DQ suggestions.",
            ),
            "Column": st.column_config.Column("Column", disabled=True),
            "Physical Type": st.column_config.Column("Physical Type", disabled=True),
            "Nulls": st.column_config.Column("Nulls", disabled=True),
            "Distinct": st.column_config.Column("Distinct", disabled=True),
            "Avg Length": st.column_config.Column("Avg Length", disabled=True),
            "Min Value": st.column_config.Column("Min Value", disabled=True),
            "Max Value": st.column_config.Column("Max Value", disabled=True),
            "Whitespace %": _column_config_with_optional_style(
                st.column_config.TextColumn,
                "Whitespace %",
                disabled=True,
                cell_style=_whitespace_cell_style,
            ),
            "Guessed Type": st.column_config.Column("Guessed Type", disabled=True),
            "Confidence": _column_config_with_optional_style(
                st.column_config.TextColumn,
                "Confidence",
                disabled=True,
                cell_style=_confidence_style,
            ),
            "Note": st.column_config.Column("Note", disabled=True),
        }

        edited = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            column_config=column_config,
            key="profile_grid_editor",
        )

        selected_total = 0
        include_total = 0
        include_map: Dict[str, bool] = {}
        if isinstance(edited, pd.DataFrame) and not edited.empty:
            include_series = edited.get("Include")
            column_series = edited.get("Column")
            if include_series is not None and column_series is not None:
                include_flags = include_series.fillna(False).astype(bool)
                include_total = int(include_flags.shape[0])
                selected_total = int(include_flags.sum())
                include_map = {
                    str(column_series.iloc[idx]): bool(include_flags.iloc[idx])
                    for idx in range(len(include_flags))
                }
        st.session_state["profile_selection_counts"] = (selected_total, include_total)

        if include_map and profile_result:
            for column_payload in profile_result.get("columns", []):
                column_name = str(column_payload.get("column_name") or "")
                if column_name in include_map:
                    column_payload["dq_selected"] = include_map[column_name]
            st.session_state["profile_results"] = profile_result

    else:
        empty_df = pd.DataFrame(
            {
                column: pd.Series(dtype="bool" if column == "Include" else "object")
                for column in grid_columns
            }
        )
        st.data_editor(
            empty_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Include": st.column_config.CheckboxColumn("Include"),
            },
            key="profile_grid_editor",
        )
        st.session_state["profile_selection_counts"] = (0, 0)

    if filtered_df.empty:
        st.info("No columns matched the selected filters.")

    st.subheader("Top values by column", anchor=False)
    for _, row in filtered_df.iterrows():
        values = row.get("top_values", [])
        if not values:
            continue
        with st.expander(f"{row['column_name']} ({len(values)} values)"):
            tv_df = pd.DataFrame(values)
            if tv_df.empty:
                st.table(tv_df)
                continue

            # Normalize common column names when present; otherwise fall back gracefully
            rename_map = {}
            value_column_name: Optional[str] = None
            for candidate in tv_df.columns:
                if str(candidate).lower() in {"value", "val", "values"}:
                    value_column_name = candidate
                    break
            if "value" in tv_df.columns:
                rename_map["value"] = "Value"
            if "count" in tv_df.columns:
                rename_map["count"] = "Count"
            if rename_map:
                tv_df = tv_df.rename(columns=rename_map)
                if value_column_name in rename_map:
                    value_column_name = rename_map[value_column_name]
            elif tv_df.shape[1] == 2:
                tv_df.columns = ["Value", "Count"]
                value_column_name = "Value"

            if value_column_name is None and len(tv_df.columns) > 0:
                value_column_name = tv_df.columns[0]

            nulls = _safe_int(row.get("nulls"))
            row_cnt = _safe_int(row.get("row_cnt"))
            null_pct = _safe_float(row.get("null_pct"))
            is_all_null = False
            if nulls is not None and row_cnt is not None and row_cnt > 0:
                is_all_null = nulls >= row_cnt
            if not is_all_null and null_pct is not None:
                if math.isclose(null_pct, 1.0, rel_tol=1e-9) or math.isclose(
                    null_pct, 100.0, rel_tol=1e-9
                ):
                    is_all_null = True

            if is_all_null and value_column_name in tv_df.columns:
                value_series = tv_df[value_column_name]
                null_mask = value_series.isna()
                if null_mask.any():
                    tv_df = tv_df[null_mask].head(1).copy()
                else:
                    tv_df = tv_df.head(1).copy()

            if "Count" in tv_df.columns:
                tv_df["Count"] = tv_df["Count"].apply(_format_count)
            for col in tv_df.columns:
                if col == "Count":
                    continue
                if value_column_name and col == value_column_name:
                    tv_df[col] = tv_df[col].apply(_format_top_value_cell)
                else:
                    tv_df[col] = tv_df[col].apply(_stringify_for_display)
                if "pct" in col.lower():
                    tv_df[col] = tv_df[col].apply(_format_percentage)

            st.table(tv_df)
