"""UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Controls (top to bottom):
1. Page header labelled "🧪 Profile Table" immediately followed by the caption "Profile a table to explore null rates, distinct counts, ranges, and common values before defining data quality checks."  The header and caption must remain paired with no intervening widgets.
2. Stateless table picker with three selectors (database, schema, table) sourced from `stateless_table_picker`.  It must appear directly under the caption and persist the selected fully qualified name in `st.session_state["profile_target_fqn"]`.
3. A horizontal divider separating the picker from the run controls.
4. Control row rendered as three equally spaced columns:
   • Column 1 contains the number input labelled "Sample %" (range 0–100, step 1).  Help text must include the metadata rationale and the instruction "Enter 0 for a full table scan."  The widget key stays `profile_sample_pct`.
   • Column 2 contains the number input labelled "Top N values" (range 1–10, default 10, step 1) with helper text "Collect up to 10 of the most common values per column."  No additional controls share this column.
   • Column 3 contains a selectbox labelled "Load saved profile" followed by a "Load" button.  The selectbox must always render, using "— Select a saved run —" when runs exist or the disabled option "— No saved profiles —" otherwise.  The Load button lives directly under the selectbox and is disabled unless a run is chosen and metadata targets are configured.  Caption messaging under the button must cover the Snowflake connection requirement or the "No saved profiles" notice as in code.
5. Caption `Suggested: X of Y columns selected` sourced from `selection_counts` showing immediately below the control row.
6. Action row with three columns sized `[1, 1, 2]`:
   • Column 1 hosts the primary button "▶️ Run Profile" (disabled when a saved run is loaded).
   • Column 2 hosts the secondary button "✨ Suggest DQ Config" (disabled until profile results exist).
   • Column 3 shows an info box `Viewing a saved profile.` plus a "Clear loaded profile" button only when `profile_loaded_run_id` is set; otherwise it must remain empty.
7. Metrics row of three `st.metric` widgets labelled "Rows profiled", "Sampling", and "Duration".  These appear once results exist and must remain in this order.
8. Optional warning banner firing for full scans over the threshold with the message "Full table scan processed {rows} rows. Consider sampling to improve performance."  The icon stays `⚠️`.
9. Filters container that starts with subheader "Filters" then four toggles: "High null % (>20%)", "Unique candidates", "Low cardinality", "Whitespace risk".  Immediately afterwards render the bold label "Semantic tags" and a single row of checkboxes: "Identifiers", "Financial", "Instrument", "Geo", "Contact", "Date (Text)", "Reference Codes"—all defaulting to `False`.
10. Toggle "💾 Save Profile" (key `profile_save_toggle`) with dynamic help text explaining persistence.  This control is always rendered; it is disabled rather than hidden when metadata prerequisites fail.
11. Selection editor grid created with `st.data_editor` that exposes only two columns: `Select` (checkbox) and `Column` (read-only).  The editor must precede the main dataframe and use key `profile_results_selection`.
12. Main profile dataframe displayed with `st.dataframe` using the styled `grid_df`.  Required columns in order: `Select`, `Column`, `Physical Type`, `Nulls`, `Distinct`, `Avg Length`, `Min Value`, `Max Value`, `Whitespace %`, `Guessed Type`, `Confidence`, `Note`.  Confidence styling and legend text (green ≥90%, amber 75–89, grey otherwise) must remain untouched.
13. Subheader "Top values by column" followed by one expander per column in the filtered results.  Each expander title follows the pattern `<column> (<N values>)`.  Inside, render exactly one `st.table` using the processed `tv_df` (columns "Value", "Count", and any metadata-supplied percentage columns).  When the table is empty, show the existing informational messages instead of alternative layouts.

Forbidden patterns:
• Do not add, remove, or reorder the control groups above.
• Do not introduce extra tabs, accordions, or secondary grids beyond the selection editor, main dataframe, and per-column tables described.
• Do not alter widget labels, keys, or default states except through existing logic.
• Do not surface additional persistence toggles or sampling controls; `💾 Save Profile` and `Sample %` are the sole persistence and sampling mechanisms.
"""

from __future__ import annotations

import html
import json
import math
import textwrap
import time
from dataclasses import dataclass, field
from datetime import datetime
from numbers import Integral, Real
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from services.profile import build_profile_suggestion
from services.profiling import (
    list_saved_profiles,
    load_profile_run,
    normalize_profile_row,
    run_table_profile,
    save_profile_results,
)
from ui.keys import (
    PROFILE_CLEAR_LOADED,
    PROFILE_LOAD_BUTTON,
    PROFILE_LOAD_SELECT,
    PROFILE_RESULTS_SELECTION,
    PROFILE_SAMPLE_PCT,
    PROFILE_SAMPLE_TARGET,
    PROFILE_SAVE_TOGGLE,
)
from ui.strings import ProfileStrings as PS
from utils.flags import (
    DEBUG_PROFILING,
    DEMO_LOCK,
    PROFILE_INLINE_SELECT,
    PROFILE_TOP_VALUES_NULLS,
    UI_CONTRACT_STRICT,
)
from utils.meta import get_table_row_count
from utils.state import bulk_set_includes, get_include_map, set_include
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


def _selection_key(table_fqn: str, column_name: str) -> str:
    table_str = str(table_fqn or "")
    column_str = str(column_name or "")
    if table_str:
        return f"{table_str}::{column_str}"
    return column_str


def _contract_message(message: str) -> None:
    """Emit a contract warning or error depending on env configuration."""

    strict = UI_CONTRACT_STRICT or DEMO_LOCK
    if strict:
        st.error(message)
    else:
        st.warning(message)


def _validate_grid_columns(
    actual: Sequence[str], expected: Sequence[str], grid_name: str
) -> bool:
    """Ensure a rendered grid exposes the expected columns and ordering."""

    actual_list = list(actual)
    expected_list = list(expected)
    if actual_list != expected_list:
        _contract_message(
            "UI contract violation in "
            f"{grid_name}: expected columns {expected_list} but found {actual_list}."
        )
        return False
    return True


def _detect_secondary_selector_keys(base_key: str) -> List[str]:
    """Return suspicious session-state keys that hint at duplicate selectors."""

    suspicious: List[str] = []
    for key in list(st.session_state.keys()):
        if not isinstance(key, str) or key == base_key:
            continue
        if not key.startswith(base_key):
            continue
        remainder = key[len(base_key) :]
        if remainder and remainder[0] in {"_", "-", ":", "."}:
            suspicious.append(key)
    return suspicious


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
    st.header(PS.HEADER)
    st.caption(PS.CAPTION)

    base_selection = st.session_state.get("profile_target_fqn")
    _db_sel, _sch_sel, _tbl_sel, selected_fqn = _table_picker(session, base_selection)
    if selected_fqn:
        st.session_state["profile_target_fqn"] = selected_fqn

    st.divider()

    def _format_saved_run_label(run: Dict[str, Any]) -> str:
        run_at = run.get("run_at")
        if isinstance(run_at, datetime):
            run_at_display = run_at.strftime("%Y-%m-%d %H:%M")
        else:
            run_at_display = str(run_at) if run_at else "Unknown time"
        summary_payload = run.get("summary") or {}
        if isinstance(summary_payload, dict):
            target_table = summary_payload.get("target_table") or summary_payload.get("table")
            top_n_raw = summary_payload.get("top_n")
            try:
                top_n = int(float(top_n_raw)) if top_n_raw is not None else None
            except Exception:
                top_n = None
        else:
            target_table = None
            top_n = None
        table_display = str(target_table or "Unknown table")
        run_id_value = str(run.get("run_id") or "")
        short_id = run_id_value[:8] if run_id_value else "—"
        top_n_suffix = f" (Top {top_n})" if isinstance(top_n, (int, float)) else ""
        return f"{run_at_display} — {table_display}{top_n_suffix} — {short_id}"

    saved_profiles_enabled = bool(session and meta_db and meta_schema)
    saved_profile_runs: List[Dict[str, Any]] = []
    if saved_profiles_enabled:
        saved_profile_runs = list_saved_profiles(session, meta_db, meta_schema)

    saved_run_lookup: Dict[str, str] = {}
    saved_run_labels: List[str] = []
    for run in saved_profile_runs:
        run_id_value = run.get("run_id")
        if not run_id_value:
            continue
        run_id_str = str(run_id_value)
        label = _format_saved_run_label(run)
        saved_run_lookup[label] = run_id_str
        saved_run_labels.append(label)

    controls = st.columns(3)
    suggested_pct = 10.0
    row_count: Optional[int] = None
    selected_reason = PS.SAMPLE_DEFAULT_REASON
    if selected_fqn:
        row_count = _load_table_row_count(session, selected_fqn)
        suggested_pct, selected_reason = _recommend_sample_pct(row_count)

    sample_pct_state_key = PROFILE_SAMPLE_PCT
    sample_target_key = PROFILE_SAMPLE_TARGET
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
        reason_parts.append(
            PS.SAMPLE_REASON_TABLE_ROWS.format(rows=row_count)
        )
    if approx_rows:
        reason_parts.append(
            PS.SAMPLE_REASON_APPROX_ROWS.format(rows=approx_rows)
        )
    reason_parts.append(PS.SAMPLE_REASON_METADATA)
    reason_parts.append(PS.SAMPLE_REASON_FULL_SCAN)
    sample_help_text = "\n".join(reason_parts)

    with controls[0]:
        sample_pct_input = st.number_input(
            PS.SAMPLE_LABEL,
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            help=sample_help_text,
            key=PROFILE_SAMPLE_PCT,
        )
        sample_pct = None if math.isclose(sample_pct_input, 0.0, abs_tol=1e-6) else sample_pct_input
    with controls[1]:
        top_n = st.number_input(
            PS.TOP_N_LABEL,
            min_value=1,
            max_value=MAX_TOP_N,
            value=min(10, MAX_TOP_N),
            step=1,
            help=PS.TOP_N_HELP_TEMPLATE.format(max_top=MAX_TOP_N),
        )
    load_selected_run_id: Optional[str] = None
    load_button_clicked = False
    with controls[2]:
        if saved_run_labels:
            load_options = [PS.LOAD_SAVED_PLACEHOLDER] + saved_run_labels
            selected_option = st.selectbox(
                PS.LOAD_SAVED_LABEL,
                options=load_options,
                key=PROFILE_LOAD_SELECT,
                disabled=not saved_profiles_enabled,
            )
            if selected_option in saved_run_lookup:
                load_selected_run_id = saved_run_lookup[selected_option]
        else:
            st.selectbox(
                PS.LOAD_SAVED_LABEL,
                options=[PS.LOAD_SAVED_EMPTY],
                key=PROFILE_LOAD_SELECT,
                disabled=True,
            )
        load_button_clicked = st.button(
            PS.LOAD_BUTTON,
            key=PROFILE_LOAD_BUTTON,
            disabled=not (saved_profiles_enabled and load_selected_run_id),
        )
        if not saved_profiles_enabled:
            st.caption(PS.LOAD_CAPTION_NEEDS_CONNECTION)
        elif not saved_run_labels:
            st.caption(PS.LOAD_CAPTION_NO_SAVED)

    if load_button_clicked:
        if not saved_profiles_enabled:
            st.warning(PS.WARNING_NO_CONNECTION)
        elif not load_selected_run_id:
            st.warning(PS.WARNING_SELECT_PROFILE)
        else:
            try:
                loaded_profile = load_profile_run(
                    session=session,
                    meta_db=meta_db,
                    meta_schema=meta_schema,
                    run_id=load_selected_run_id,
                )
            except Exception as exc:  # pragma: no cover - Snowflake specific
                st.error(PS.LOAD_ERROR.format(error=exc))
            else:
                if not loaded_profile:
                    st.warning(PS.INFO_NO_SAVED_PROFILE)
                else:
                    st.session_state["profile_results"] = loaded_profile
                    st.session_state["profile_loaded_run_id"] = load_selected_run_id
                    target_table = loaded_profile.get("target_table")
                    if target_table:
                        st.session_state["profile_target_fqn"] = target_table
                    st.success(PS.SUCCESS_LOADED_PROFILE.format(run_id=load_selected_run_id))
                    st.rerun()

    stored_profile_result = st.session_state.get("profile_results")
    loaded_run_id = st.session_state.get("profile_loaded_run_id")
    if loaded_run_id and not stored_profile_result:
        st.session_state.pop("profile_loaded_run_id", None)
        loaded_run_id = None
    selection_state_raw = st.session_state.get("profile_select")
    if isinstance(selection_state_raw, dict):
        selection_state: Dict[str, bool] = dict(selection_state_raw)
    else:
        selection_state = {}
    st.session_state["profile_select"] = selection_state
    current_target_fqn = ""
    if stored_profile_result:
        current_target_fqn = str(
            stored_profile_result.get("target_table") or selected_fqn or ""
        )
    else:
        current_target_fqn = str(selected_fqn or "")

    def _apply_selection_state(
        profile_payload: Optional[Dict[str, Any]],
        table_fqn: str,
        initialize_missing: bool,
    ) -> Tuple[int, int]:
        if not profile_payload:
            return (0, 0)
        columns_payload = profile_payload.get("columns", [])
        selected = 0
        total = 0
        active_keys: set[str] = set()
        for column_payload in columns_payload:
            column_name = str(column_payload.get("column_name") or "")
            if not column_name:
                continue
            key = _selection_key(table_fqn, column_name)
            active_keys.add(key)
            if initialize_missing and key not in selection_state:
                selection_state[key] = _safe_bool(column_payload.get("dq_selected")) or False
            column_selected = bool(selection_state.get(key, False))
            column_payload["dq_selected"] = column_selected
            if column_selected:
                selected += 1
            total += 1
        prefix = f"{table_fqn}::" if table_fqn else ""
        if prefix:
            for key in list(selection_state.keys()):
                if key.startswith(prefix) and key not in active_keys:
                    selection_state.pop(key, None)
        return (selected, total)

    selection_counts = _apply_selection_state(
        stored_profile_result,
        current_target_fqn,
        initialize_missing=True,
    )
    st.session_state["profile_selection_counts"] = selection_counts

    st.caption(
        PS.SAMPLE_CAPTION.format(
            selected=int(selection_counts[0]),
            total=int(selection_counts[1]),
        )
    )

    button_cols = st.columns([1, 1, 2])
    run_disabled = bool(loaded_run_id)
    with button_cols[0]:
        run_profile = st.button(PS.RUN_BUTTON, type="primary", disabled=run_disabled)
    with button_cols[1]:
        suggest_cfg = st.button(
            PS.SUGGEST_BUTTON,
            type="secondary",
            disabled=not stored_profile_result,
        )

    clear_loaded = False
    if loaded_run_id:
        with button_cols[2]:
            st.info(PS.SAVED_PROFILE_INFO)
            clear_loaded = st.button(PS.CLEAR_LOADED_BUTTON, key=PROFILE_CLEAR_LOADED)
    else:
        button_cols[2].empty()

    if clear_loaded:
        st.session_state.pop("profile_loaded_run_id", None)
        loaded_run_id = None
        st.rerun()

    profile_result = stored_profile_result

    if run_profile:
        st.session_state.pop("profile_loaded_run_id", None)
        loaded_run_id = None
        if not session:
            st.error(PS.PROFILE_WARNING_NO_SESSION)
        elif not selected_fqn:
            st.warning(PS.PROFILE_WARNING_SELECT_TABLE)
        else:
            with st.spinner(PS.SAMPLE_SPINNER):
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
                st.error(PS.PROFILE_ERROR.format(error=profile_error))
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
            st.info(PS.NO_SUGGESTIONS)
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
            st.warning(PS.PROFILE_WARNING_SELECT_COLUMNS)
        else:
            filtered_profile = dict(profile_result)
            filtered_summary = dict(filtered_profile.get("summary") or {})
            filtered_summary["columns"] = len(selected_columns)
            filtered_profile["summary"] = filtered_summary
            filtered_profile["columns"] = selected_columns
            _load_suggestion(
                filtered_profile,
                PS.PROFILE_SUCCESS_SUGGESTION,
            )

    if not profile_result:
        return

    summary = profile_result.get("summary", {})
    metrics_cols = st.columns(3)
    metrics_cols[0].metric(PS.METRIC_ROWS_PROFILED, f"{summary.get('rows_profiled', 0):,}")
    sample_pct_display = summary.get("sample_pct")
    sample_label = (
        PS.SAMPLE_LABEL_FULL_SCAN
        if sample_pct_display is None
        else f"{float(sample_pct_display):.1f}%"
    )
    metrics_cols[1].metric(PS.METRIC_SAMPLING, sample_label)
    metrics_cols[2].metric(PS.METRIC_DURATION, f"{summary.get('duration_sec', 0.0):.2f}s")

    if summary.get("sample_pct") is None and summary.get("rows_profiled", 0) > FULL_SCAN_WARNING_THRESHOLD:
        profiled = int(summary.get("rows_profiled", 0))
        st.warning(
            PS.SAMPLE_WARNING_FULL_SCAN.format(rows=profiled),
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
        st.subheader(PS.FILTERS_SUBHEADER, anchor=False)
        filter_cols = st.columns(4)
        high_null = filter_cols[0].toggle(PS.FILTER_HIGH_NULL, value=False)
        unique_candidates = filter_cols[1].toggle(PS.FILTER_UNIQUE, value=False)
        low_cardinality = filter_cols[2].toggle(PS.FILTER_LOW_CARDINALITY, value=False)
        whitespace_risk = filter_cols[3].toggle(PS.FILTER_WHITESPACE, value=False)
        st.markdown(f"**{PS.FILTER_SEMANTIC_LABEL}**")
        semantic_cols = st.columns(len(PS.FILTER_SEMANTIC_OPTIONS))
        filter_identifiers = semantic_cols[0].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[0], value=False
        )
        filter_financial = semantic_cols[1].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[1], value=False
        )
        filter_instrument = semantic_cols[2].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[2], value=False
        )
        filter_geo = semantic_cols[3].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[3], value=False
        )
        filter_contact = semantic_cols[4].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[4], value=False
        )
        filter_date_text = semantic_cols[5].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[5], value=False
        )
        filter_ref_codes = semantic_cols[6].checkbox(
            PS.FILTER_SEMANTIC_OPTIONS[6], value=False
        )

    save_enabled = bool(session and meta_db and meta_schema)
    if not save_enabled:
        st.session_state.pop(PROFILE_SAVE_TOGGLE, None)
        st.session_state.pop("_profile_save_prev", None)
        st.session_state.pop("profile_saved_run_id", None)

    save_help = (
        PS.SAVE_ENABLED_MESSAGE if save_enabled else PS.SAVE_DISABLED_MESSAGE
    )
    save_toggle = st.toggle(
        PS.SAVE_TOGGLE,
        key=PROFILE_SAVE_TOGGLE,
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
            st.error(PS.CLEAR_PROFILE_ERROR.format(error=exc))
        else:
            st.session_state["profile_saved_run_id"] = run_id
            st.success(PS.PROFILE_SUCCESS_SAVE.format(run_id=run_id))

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
        PS.FILTER_SEMANTIC_OPTIONS[0]: {"ACCOUNT_ID", "ORDER_ID", "TRADE_ID", "UUID", "IBAN", "REF_CODE"},
        PS.FILTER_SEMANTIC_OPTIONS[1]: {"PRICE/AMOUNT/QUANTITY", "IBAN", "BIC"},
        PS.FILTER_SEMANTIC_OPTIONS[2]: {"ISIN", "TICKER/SYMBOL"},
        PS.FILTER_SEMANTIC_OPTIONS[3]: {"COUNTRY_CODE/NAME", "CURRENCY_CODE", "BIC"},
        PS.FILTER_SEMANTIC_OPTIONS[4]: {"EMAIL", "PHONE"},
        PS.FILTER_SEMANTIC_OPTIONS[5]: {"DATE_IN_TEXT"},
        PS.FILTER_SEMANTIC_OPTIONS[6]: {"REF_CODE"},
    }
    active_semantic_filters: List[str] = []
    if filter_identifiers:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[0])
    if filter_financial:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[1])
    if filter_instrument:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[2])
    if filter_geo:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[3])
    if filter_contact:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[4])
    if filter_date_text:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[5])
    if filter_ref_codes:
        active_semantic_filters.append(PS.FILTER_SEMANTIC_OPTIONS[6])

    if active_semantic_filters:
        allowed_types = set()
        for key in active_semantic_filters:
            allowed_types.update(semantic_filter_map.get(key, set()))
        filtered_df = filtered_df[filtered_df["semantic_type"].isin(allowed_types)]
    display_df = filtered_df.copy()
    select_label = PS.GRID_COLUMN_SELECT
    column_label = PS.GRID_COLUMN_COLUMN
    physical_type_label = PS.GRID_COLUMN_PHYSICAL_TYPE
    nulls_label = PS.GRID_COLUMN_NULLS
    distinct_label = PS.GRID_COLUMN_DISTINCT
    avg_length_label = PS.GRID_COLUMN_AVG_LENGTH
    min_value_label = PS.GRID_COLUMN_MIN_VALUE
    max_value_label = PS.GRID_COLUMN_MAX_VALUE
    whitespace_label = PS.GRID_COLUMN_WHITESPACE
    guessed_type_label = PS.GRID_COLUMN_GUESSED_TYPE
    confidence_label = PS.GRID_COLUMN_CONFIDENCE
    note_label = PS.GRID_COLUMN_NOTE

    grid_columns = [
        select_label,
        column_label,
        physical_type_label,
        nulls_label,
        distinct_label,
        avg_length_label,
        min_value_label,
        max_value_label,
        whitespace_label,
        guessed_type_label,
        confidence_label,
        note_label,
    ]

    if not PROFILE_INLINE_SELECT:
        _contract_message(PS.INLINE_SELECT_DISABLED)
    confidence_legend_html = """
    <div style="display:flex; gap:12px; align-items:center; font-size:0.85rem; margin:0.5rem 0;">
        <span style="display:flex; align-items:center; gap:4px;">
            <span style="width:12px; height:12px; border-radius:2px; background-color:#2e7d32; display:inline-block;"></span>
            <span>Green ≥90% (High)</span>
        </span>
        <span style="display:flex; align-items:center; gap:4px;">
            <span style="width:12px; height:12px; border-radius:2px; background-color:#f9a825; display:inline-block;"></span>
            <span>Amber 75–89% (Medium)</span>
        </span>
        <span style="display:flex; align-items:center; gap:4px;">
            <span style="width:12px; height:12px; border-radius:2px; background-color:#9e9e9e; display:inline-block;"></span>
            <span>Grey &lt;75% (Low/Unknown)</span>
        </span>
    </div>
    """

    def _format_confidence_display(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return ""
        v = float(value)
        if v < 10.0:
            s = f"{v:.1f}".rstrip("0").rstrip(".")
        else:
            s = f"{v:.0f}"
        return f"{s}%"

    def _confidence_style(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return ""
        v = float(value)
        if v >= 90.0:
            return "background-color: #2e7d32; color: #ffffff;"
        if v >= 75.0:
            return "background-color: #f9a825; color: #000000;"
        return "background-color: #9e9e9e; color: #ffffff;"

    target_table = str(profile_result.get("target_table") or current_target_fqn)
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

        def _format_whitespace_display(value: Any) -> str:
            numeric = _safe_float(value)
            if numeric is None or math.isclose(numeric, 0.0, abs_tol=1e-9):
                return "—"
            return f"{numeric:.1f}%"

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
        display_df_local[column_label] = (
            display_df_local.get("column_name", "").fillna("").astype(str)
        )
        display_df_local[select_label] = display_df_local[column_label].apply(
            lambda name: bool(selection_state.get(_selection_key(target_table, name), False))
        )
        display_df_local[select_label] = display_df_local[select_label].astype(bool)
        display_df_local[physical_type_label] = (
            display_df_local.get("data_type", "").fillna("").astype(str)
        )
        display_df_local[nulls_label] = display_df_local.apply(
            lambda row: _format_count_with_pct(row.get("nulls"), row.get("null_pct")), axis=1
        )
        display_df_local[distinct_label] = display_df_local.apply(
            lambda row: _format_count_with_pct(row.get("distincts"), row.get("distinct_pct")), axis=1
        )
        display_df_local[avg_length_label] = display_df_local.apply(
            _format_length_stats_cell, axis=1
        )
        display_df_local[min_value_label] = display_df_local["min_val"].apply(
            _format_value_cell
        )
        display_df_local[max_value_label] = display_df_local["max_val"].apply(
            _format_value_cell
        )
        display_df_local[guessed_type_label] = display_df_local["semantic_type"].apply(
            _format_semantic_label
        )
        display_df_local[whitespace_label] = display_df_local["whitespace_pct"].apply(
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
        display_df_local[note_label] = display_df_local.apply(_compose_note, axis=1)
        grid_container = st.container()

        confidence_numeric = pd.to_numeric(
            display_df_local["_confidence_pct"], errors="coerce"
        ).clip(lower=0.0, upper=100.0)
        display_df_local[confidence_label] = confidence_numeric

        formatted_rows: List[List[Any]] = []
        for _, row in display_df_local.iterrows():
            formatted_rows.append(
                [
                    bool(row.get(select_label, False)),
                    row.get(column_label),
                    row.get(physical_type_label),
                    row.get(nulls_label),
                    row.get(distinct_label),
                    row.get(avg_length_label),
                    row.get(min_value_label),
                    row.get(max_value_label),
                    row.get(whitespace_label),
                    row.get(guessed_type_label),
                    row.get(confidence_label),
                    row.get(note_label),
                ]
            )

        grid_df = pd.DataFrame(formatted_rows, columns=grid_columns)
        grid_render_allowed = _validate_grid_columns(
            grid_df.columns, grid_columns, "profile results grid"
        )
        if grid_render_allowed:
            grid_df[select_label] = grid_df[select_label].fillna(False).astype(bool)
            grid_df[confidence_label] = grid_df[confidence_label].apply(_safe_float)

        with grid_container:
            st.markdown(confidence_legend_html, unsafe_allow_html=True)
            include_map = get_include_map()
            selection_editor: Optional[pd.DataFrame] = None
            if grid_render_allowed and not grid_df.empty:
                selection_editor_df = grid_df[[select_label, column_label]].copy()
                selection_render_allowed = _validate_grid_columns(
                    selection_editor_df.columns,
                    [select_label, column_label],
                    "profile selection editor",
                )
                if selection_render_allowed:
                    selection_editor_df[select_label] = (
                        selection_editor_df[select_label].fillna(False).astype(bool)
                    )
                    suspicious_keys = _detect_secondary_selector_keys(
                        PROFILE_RESULTS_SELECTION
                    )
                    if suspicious_keys:
                        _contract_message(
                            "UI contract violation: secondary selector state detected "
                            f"({', '.join(sorted(suspicious_keys))}). Skipping selector."
                        )
                    else:
                        selection_editor = st.data_editor(
                            selection_editor_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                select_label: st.column_config.CheckboxColumn(
                                    select_label,
                                    help=PS.GRID_CHECKBOX_HELP,
                                ),
                                column_label: st.column_config.Column(
                                    column_label, disabled=True
                                ),
                            },
                            disabled=[column_label],
                            key=PROFILE_RESULTS_SELECTION,
                        )

            if (
                grid_render_allowed
                and isinstance(selection_editor, pd.DataFrame)
                and not selection_editor.empty
            ):
                select_series = selection_editor.get(select_label)
                column_series = selection_editor.get(column_label)
                if select_series is not None and column_series is not None:
                    select_flags = select_series.fillna(False).astype(bool)
                    include_map_local = {
                        str(column_series.iloc[idx]): bool(select_flags.iloc[idx])
                        for idx in range(len(select_flags))
                    }
                    column_names = list(include_map_local.keys())
                    if column_names:
                        bulk_set_includes(column_names, False)
                        for column_name, selected_flag in include_map_local.items():
                            set_include(column_name, selected_flag)
                        include_map = get_include_map()

            if grid_render_allowed and include_map:
                for column_name, selected_flag in include_map.items():
                    key = _selection_key(target_table, column_name)
                    selection_state[key] = selected_flag
                    mask = grid_df[column_label] == column_name
                    if mask.any():
                        grid_df.loc[mask, select_label] = selected_flag

            if grid_render_allowed:
                styler = grid_df.style.format(
                    {confidence_label: _format_confidence_display}
                )
                styler = styler.applymap(
                    _confidence_style, subset=[confidence_label]
                )
                st.dataframe(styler, hide_index=True, use_container_width=True)

        selection_counts = _apply_selection_state(
            profile_result,
            target_table,
            initialize_missing=False,
        )
        st.session_state["profile_results"] = profile_result
        st.session_state["profile_selection_counts"] = selection_counts

    else:
        empty_df = pd.DataFrame(
            {
                column: pd.Series(
                    dtype="bool" if column == select_label else "object"
                )
                for column in grid_columns
            }
        )
        st.markdown(confidence_legend_html, unsafe_allow_html=True)
        empty_styler = empty_df.style.format(
            {confidence_label: _format_confidence_display}
        )
        empty_styler = empty_styler.applymap(
            _confidence_style, subset=[confidence_label]
        )
        st.dataframe(empty_styler, hide_index=True, use_container_width=True)

    if filtered_df.empty:
        st.info(PS.NO_COLUMNS_MATCHED)

    st.subheader(PS.TOP_VALUES_SUBHEADER, anchor=False)
    for _, row in filtered_df.iterrows():
        values = row.get("top_values", [])
        non_nulls_value = _safe_int(row.get("non_nulls"))
        label_suffix = f"{len(values)} values"
        with st.expander(f"{row['column_name']} ({label_suffix})"):
            if not values:
                if non_nulls_value is not None and non_nulls_value <= 0:
                    st.info(PS.INFO_NO_NON_NULL)
                else:
                    st.info(PS.INFO_NO_VALUES)
                continue

            tv_df = pd.DataFrame(values)
            if tv_df.empty:
                if non_nulls_value is not None and non_nulls_value <= 0:
                    st.info(PS.INFO_NO_NON_NULL)
                else:
                    st.info(PS.INFO_NO_VALUES)
                continue

            # Normalize common column names when present; otherwise fall back gracefully
            rename_map: Dict[Any, str] = {}
            value_column_name: Optional[str] = None
            count_column_name: Optional[str] = None
            for candidate in tv_df.columns:
                lowered = str(candidate).lower()
                if lowered in {"value", "val", "values"} and value_column_name is None:
                    value_column_name = candidate
                if lowered in {"count", "cnt"} and count_column_name is None:
                    count_column_name = candidate
            if value_column_name is not None:
                rename_map[value_column_name] = PS.TOP_VALUES_VALUE_HEADER
            if count_column_name is not None:
                rename_map[count_column_name] = PS.TOP_VALUES_COUNT_HEADER
            if rename_map:
                tv_df = tv_df.rename(columns=rename_map)
                if value_column_name is not None:
                    value_column_name = PS.TOP_VALUES_VALUE_HEADER
                if count_column_name is not None:
                    count_column_name = PS.TOP_VALUES_COUNT_HEADER
            elif tv_df.shape[1] == 2:
                tv_df.columns = [
                    PS.TOP_VALUES_VALUE_HEADER,
                    PS.TOP_VALUES_COUNT_HEADER,
                ]
                value_column_name = PS.TOP_VALUES_VALUE_HEADER
                count_column_name = PS.TOP_VALUES_COUNT_HEADER

            if value_column_name is None and len(tv_df.columns) > 0:
                value_column_name = tv_df.columns[0]
            if (
                count_column_name is None
                and PS.TOP_VALUES_COUNT_HEADER in tv_df.columns
            ):
                count_column_name = PS.TOP_VALUES_COUNT_HEADER

            pct_columns = [col for col in tv_df.columns if "pct" in str(col).lower()]

            if value_column_name and count_column_name:
                empty_mask = tv_df[value_column_name] == "__EMPTY__"
                if empty_mask.any():
                    total_empty = (
                        tv_df.loc[empty_mask, count_column_name]
                        .apply(lambda x: _safe_int(x) or 0)
                        .sum()
                    )
                    tv_df = tv_df.loc[~empty_mask].copy()
                    new_row = {col: None for col in tv_df.columns}
                    new_row[value_column_name] = "__EMPTY__"
                    new_row[count_column_name] = total_empty
                    if pct_columns:
                        denom_raw = non_nulls_value
                        if denom_raw is None or denom_raw <= 0:
                            pct_value = 0.0
                        else:
                            pct_value = (float(total_empty) / float(denom_raw)) * 100.0
                        for pct_col in pct_columns:
                            new_row[pct_col] = pct_value
                    tv_df = pd.concat([tv_df, pd.DataFrame([new_row])], ignore_index=True)

            if value_column_name and value_column_name in tv_df.columns:
                null_bucket_mask = tv_df[value_column_name] == "__NULL__"
                if null_bucket_mask.any() and not PROFILE_TOP_VALUES_NULLS:
                    tv_df = tv_df.loc[~null_bucket_mask].copy()
                tv_df = tv_df.loc[~tv_df[value_column_name].isna()].copy()
                tv_df[value_column_name] = tv_df[value_column_name].replace(
                    {"__EMPTY__": '"" (empty/whitespace)'}
                )

            if tv_df.empty:
                if non_nulls_value is not None and non_nulls_value <= 0:
                    st.info("No non-null values to display.")
                else:
                    st.info("No top values available.")
                continue

            for pct_col in pct_columns:
                tv_df[pct_col] = tv_df[pct_col].apply(_safe_float)
                tv_df[pct_col] = tv_df[pct_col].apply(
                    lambda x: None if x is None else min(100.0, max(0.0, x))
                )

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

            if count_column_name and count_column_name in tv_df.columns:
                tv_df[count_column_name] = tv_df[count_column_name].apply(_format_count)
            for col in tv_df.columns:
                if count_column_name and col == count_column_name:
                    continue
                if value_column_name and col == value_column_name:
                    tv_df[col] = tv_df[col].apply(_format_top_value_cell)
                else:
                    tv_df[col] = tv_df[col].apply(_stringify_for_display)
                if "pct" in col.lower():
                    tv_df[col] = tv_df[col].apply(_format_percentage)

            st.table(tv_df)

    if DEBUG_PROFILING and profile_result:
        with st.expander(PS.DEBUG_PROFILE_PAYLOAD):
            st.json(profile_result)
        include_state = get_include_map()
        if include_state:
            with st.expander(PS.DEBUG_SELECTION_STATE):
                st.write(include_state)
