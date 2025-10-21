from __future__ import annotations
import html
import math
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
    top_values: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    semantic_type: Optional[str] = None
    confidence: Optional[float] = None
    rationale: Optional[str] = None


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
        date_parse_best_format=best_format_display,
        parsed_date_min=str(normalized.get("parsed_date_min")) if normalized.get("parsed_date_min") else None,
        parsed_date_max=str(normalized.get("parsed_date_max")) if normalized.get("parsed_date_max") else None,
        numeric_min=numeric_min_value,
        numeric_max=numeric_max_value,
        top_values=top_values_list,
        error=normalized.get("error"),
        semantic_type=normalized.get("semantic_type"),
        confidence=_safe_float(normalized.get("confidence")),
        rationale=normalized.get("rationale"),
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
                "null_pct": round(profile.null_pct, 2) if profile.null_pct is not None else None,
                "distincts": profile.distincts,
                "distinct_pct": round(profile.distinct_pct, 2) if profile.distinct_pct is not None else None,
                "min_val": profile.min_val,
                "max_val": profile.max_val,
                "avg_len": round(profile.avg_len, 2) if profile.avg_len is not None else None,
                "whitespace_pct": round(profile.whitespace_pct, 2) if profile.whitespace_pct is not None else None,
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
                "date_parse_best_format": profile.date_parse_best_format,
                "parsed_date_min": profile.parsed_date_min,
                "parsed_date_max": profile.parsed_date_max,
                "numeric_min": profile.numeric_min,
                "numeric_max": profile.numeric_max,
                "top_values": profile.top_values,
                "error": profile.error,
                "semantic_type": profile.semantic_type,
                "confidence": profile.confidence,
                "rationale": profile.rationale,
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


def _confidence_badge_label(confidence: Optional[float]) -> str:
    if confidence is None or (isinstance(confidence, float) and math.isnan(confidence)):
        return "Unknown"
    if confidence >= 0.9:
        return "High"
    if confidence >= 0.75:
        return "Medium"
    return "Low"


def _format_confidence(value: Any) -> str:
    try:
        if value is None:
            return ""
        value_float = float(value)
        if math.isnan(value_float):
            return ""
        return f"{value_float:.0%}"
    except Exception:
        return ""


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


def _badge_css(value: Any) -> str:
    label = str(value or "Unknown")
    styles = {
        "High": "background-color: #0f9960; color: #ffffff;",
        "Medium": "background-color: #f7b731; color: #2b2b2b;",
        "Low": "background-color: #9aa0a6; color: #1f1f1f;",
        "Unknown": "background-color: #dfe1e5; color: #1f1f1f;",
    }
    base = styles.get(label, styles["Unknown"])
    return "; ".join(
        [
            base.rstrip(";"),
            "border-radius: 12px",
            "font-weight: 600",
            "text-align: center",
            "padding: 0.15rem 0.4rem",
            "display: inline-block",
            "min-width: 4rem",
        ]
    )


def _format_ratio_pct(value: Optional[float], decimals: int = 0) -> str:
    if value is None:
        return "—"
    try:
        ratio = float(value)
        if math.isnan(ratio):
            return "—"
        return f"{ratio * 100:.{decimals}f}%"
    except Exception:
        return "—"


def _ratio_badge_status(ratio: Optional[float]) -> str:
    if ratio is None:
        return "muted"
    try:
        value = float(ratio)
    except Exception:
        return "muted"
    if math.isnan(value):
        return "muted"
    if value >= 0.9:
        return "green"
    if value >= 0.75:
        return "amber"
    return "red"


def _badge_display_text(status: str, label: str) -> str:
    if status == "muted":
        return ""
    return label


def _status_to_badge_css(status: str) -> str:
    palette = {
        "green": "background-color: #0f9960; color: #ffffff;",
        "amber": "background-color: #f7b731; color: #2b2b2b;",
        "red": "background-color: #d64541; color: #ffffff;",
        "muted": "background-color: transparent; color: inherit; border: 1px solid rgba(0, 0, 0, 0.08);",
    }
    base = palette.get(status, palette["muted"]).rstrip(";")
    return "; ".join(
        [
            base,
            "border-radius: 12px",
            "font-weight: 600",
            "text-align: center",
            "padding: 0.15rem 0.4rem",
            "display: inline-block",
            "min-width: 4rem",
        ]
    )


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
            """
            <div class="metric">
                <div class="metric-label">{label}</div>
                {value_block}
            </div>
            """.format(label=label_html, value_block=value_block)
        )

    subtitle_html = (
        f"<div class=\"small\">{html.escape(subtitle)}</div>"
        if subtitle
        else ""
    )
    card_html = """
    <div class="card">
        <div class="kv">{title}</div>
        {subtitle}
        <div class="metrics-grid">
            {metrics}
        </div>
    </div>
    """.format(
        title=html.escape(title), subtitle=subtitle_html, metrics="".join(metric_html)
    )
    st.markdown(card_html, unsafe_allow_html=True)


def _render_min_max_assessments(profiles: Iterable[ColumnProfile]) -> None:
    cards: List[Tuple[str, Optional[str], List[Tuple[str, str, Optional[str]]]]] = []

    for profile in profiles:
        semantic_type = (profile.semantic_type or "").upper()
        if semantic_type == "DATE_IN_TEXT":
            min_date = profile.parsed_date_min
            max_date = profile.parsed_date_max
            if not (min_date or max_date):
                continue
            success_ratio = profile.date_parse_ratio_best
            tooltip = "parsed from text"
            if success_ratio is not None:
                try:
                    ratio_float = float(success_ratio)
                except Exception:
                    ratio_float = math.nan
                if not math.isnan(ratio_float):
                    tooltip = (
                        f"parsed from text; success ratio = {_format_ratio_pct(ratio_float, 0)}"
                    )
            metrics = [
                ("Min date", _format_card_value(min_date), tooltip),
                ("Max date", _format_card_value(max_date), tooltip),
            ]
            cards.append((profile.name, "DATE_IN_TEXT", metrics))
            continue

        if semantic_type == "REF_CODE":
            distinct_display = _format_card_value(
                profile.distincts, decimals=0, allow_commas=True
            )
            top3_display = _format_ratio_pct(profile.top3_ratio, 1)
            length_min = _format_card_value(profile.len_min, decimals=1)
            length_max = _format_card_value(profile.len_max, decimals=1)
            metrics: List[Tuple[str, str, Optional[str]]] = [
                ("Distinct count", distinct_display, None),
                ("Top 3 coverage", top3_display, None),
                ("Length min/max", f"{length_min} / {length_max}", None),
            ]
            numeric_ratio = profile.numeric_like_ratio or 0.0
            if numeric_ratio >= 0.9 and (profile.numeric_min is not None or profile.numeric_max is not None):
                metrics.append(
                    (
                        "Numeric min",
                        _format_card_value(profile.numeric_min, allow_commas=True),
                        None,
                    )
                )
                metrics.append(
                    (
                        "Numeric max",
                        _format_card_value(profile.numeric_max, allow_commas=True),
                        None,
                    )
                )
            cards.append((profile.name, "REF_CODE", metrics))

    if not cards:
        return

    st.subheader("Min/Max assessments", anchor=False)
    st.caption("Profiler-derived bounds tailored to semantic type hints.")

    chunk = 2
    for idx in range(0, len(cards), chunk):
        row_cards = cards[idx : idx + chunk]
        cols = st.columns(len(row_cards))
        for col, card in zip(cols, row_cards):
            with col:
                title, subtitle, metrics = card
                _render_metric_card(title, metrics, subtitle)


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

    button_cols = st.columns([1, 1, 2])
    with button_cols[0]:
        run_profile = st.button("▶️ Run Profile", type="primary")
    with button_cols[1]:
        suggest_cfg = st.button(
            "✨ Suggest DQ Config",
            type="secondary",
            disabled=not st.session_state.get("profile_results"),
        )

    profile_result = st.session_state.get("profile_results")

    if run_profile:
        if not session:
            st.error("No active Snowpark session — unable to profile tables.")
        elif not selected_fqn:
            st.warning("Select a database, schema, and table to profile.")
        else:
            with st.spinner("Profiling table..."):
                start = time.time()
                try:
                    summary_raw, column_rows = run_table_profile(
                        session=session,
                        fqn=selected_fqn,
                        sample_pct=sample_pct,
                        top_n=int(min(top_n, MAX_TOP_N)),
                    )
                except Exception as exc:  # pragma: no cover - Snowflake specific
                    st.error(f"Failed to profile table: {exc}")
                    summary_raw, column_rows = {}, []
                duration = time.time() - start
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

    if suggest_cfg and profile_result:
        suggestion = build_profile_suggestion(profile_result)
        if not suggestion:
            st.info("No suggestions available for the current profile.")
        else:
            st.session_state["cfg_mode"] = "edit"
            st.session_state["selected_config_id"] = None
            st.session_state["editor_target_fqn"] = profile_result.get("target_table")
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
            st.success("Loaded profile suggestion into the configuration editor.")
            st.rerun()

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
    _render_min_max_assessments(profiles)
    df = _profiles_to_frame(profiles)

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
        filtered_df = filtered_df[(filtered_df["null_pct"].fillna(0) > 20)]
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
    display_df = filtered_df.drop(columns=["top_values"], errors="ignore").copy()
    if not display_df.empty:
        semantic_series = (
            display_df.get("semantic_type", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.upper()
        )
        has_date_text = semantic_series.eq("DATE_IN_TEXT").any()
        has_ref_code = semantic_series.eq("REF_CODE").any()
        has_length_bounds = {"len_min", "len_max"}.issubset(display_df.columns) and (
            display_df[["len_min", "len_max"]].notna().any().any()
        )
        has_numeric_bounds = {"numeric_min", "numeric_max"}.issubset(display_df.columns) and (
            display_df[["numeric_min", "numeric_max"]].notna().any().any()
        )
        badge_styles: Dict[str, List[str]] = {}
        tooltip_columns: Dict[str, List[str]] = {}
        if "confidence" in display_df.columns:
            display_df["Confidence"] = display_df["confidence"].apply(
                lambda val: float(val) if val is not None else None
            )
            display_df["Confidence Badge"] = display_df["confidence"].apply(_confidence_badge_label)
            display_df = display_df.drop(columns=["confidence"], errors="ignore")
        else:
            display_df["Confidence"] = None
            display_df["Confidence Badge"] = "Unknown"
        if has_length_bounds:
            display_df["Len Min/Max"] = display_df.apply(
                lambda row: (
                    ""
                    if pd.isna(row.get("len_min")) and pd.isna(row.get("len_max"))
                    else f"{_format_card_value(row.get('len_min'), decimals=0)} / {_format_card_value(row.get('len_max'), decimals=0)}"
                ),
                axis=1,
            )
        display_df = display_df.drop(columns=["len_min", "len_max"], errors="ignore")

        if has_date_text:
            date_labels: List[str] = []
            date_tooltips: List[str] = []
            date_styles: List[str] = []
            for _, row in display_df.iterrows():
                is_date = str(row.get("semantic_type") or "").upper() == "DATE_IN_TEXT"
                if not is_date:
                    date_labels.append("")
                    date_tooltips.append("")
                    date_styles.append(_status_to_badge_css("muted"))
                    continue
                ratio_pct = row.get("date_parse_best_pct")
                ratio_norm: Optional[float] = None
                if ratio_pct is not None:
                    try:
                        ratio_norm = float(ratio_pct) / 100.0
                        if math.isnan(ratio_norm):
                            ratio_norm = None
                    except Exception:
                        ratio_norm = None
                status = _ratio_badge_status(ratio_norm)
                label = _badge_display_text(status, "Date (Text)")
                fmt = row.get("date_parse_best_format") or "—"
                ratio_display = _format_ratio_pct(ratio_norm, 0)
                tooltip = f"Format: {fmt}; Success ratio: {ratio_display}"
                date_labels.append(label)
                date_tooltips.append(tooltip)
                date_styles.append(_status_to_badge_css(status))
            display_df["Date (Text)"] = date_labels
            tooltip_columns["Date (Text)"] = date_tooltips
            badge_styles["Date (Text)"] = date_styles
        else:
            display_df = display_df.drop(
                columns=[
                    "date_parse_best_pct",
                    "date_parse_best_format",
                    "parsed_date_min",
                    "parsed_date_max",
                ],
                errors="ignore",
            )

        if has_ref_code:
            ref_labels: List[str] = []
            ref_tooltips: List[str] = []
            ref_styles: List[str] = []
            for _, row in display_df.iterrows():
                is_ref = str(row.get("semantic_type") or "").upper() == "REF_CODE"
                if not is_ref:
                    ref_labels.append("")
                    ref_tooltips.append("")
                    ref_styles.append(_status_to_badge_css("muted"))
                    continue
                distinct_ratio_pct = row.get("distinct_ratio_pct")
                distinct_ratio: Optional[float] = None
                if distinct_ratio_pct is not None:
                    try:
                        distinct_ratio = float(distinct_ratio_pct) / 100.0
                        if math.isnan(distinct_ratio):
                            distinct_ratio = None
                    except Exception:
                        distinct_ratio = None
                status = _ratio_badge_status(distinct_ratio)
                label = _badge_display_text(status, "Reference Codes")
                top3_ratio_pct = row.get("top3_ratio_pct")
                top3_ratio: Optional[float] = None
                if top3_ratio_pct is not None:
                    try:
                        top3_ratio = float(top3_ratio_pct) / 100.0
                        if math.isnan(top3_ratio):
                            top3_ratio = None
                    except Exception:
                        top3_ratio = None
                distinct_display = _format_ratio_pct(distinct_ratio, 0)
                top3_display = _format_ratio_pct(top3_ratio, 0)
                tooltip = (
                    f"Distinct ratio: {distinct_display}; Top-3 coverage: {top3_display}"
                )
                ref_labels.append(label)
                ref_tooltips.append(tooltip)
                ref_styles.append(_status_to_badge_css(status))
            display_df["Reference Codes"] = ref_labels
            tooltip_columns["Reference Codes"] = ref_tooltips
            badge_styles["Reference Codes"] = ref_styles
        else:
            display_df = display_df.drop(columns=["Reference Codes"], errors="ignore")

        if not has_length_bounds:
            display_df = display_df.drop(columns=["Len Min/Max"], errors="ignore")

        if not has_numeric_bounds:
            display_df = display_df.drop(columns=["numeric_min", "numeric_max"], errors="ignore")

        rename_map = {
            "semantic_type": "Guessed Type",
            "rationale": "Confidence Rationale",
            "row_cnt": "Non-null Rows",
            "distincts": "Distinct Count",
            "distinct_ratio_pct": "Distinct Ratio %",
            "top1_ratio_pct": "Top 1 Ratio %",
            "top3_ratio_pct": "Top-3 Coverage %",
            "numeric_like_pct": "Numeric-like %",
            "numeric_min": "Numeric Min",
            "numeric_max": "Numeric Max",
            "date_pattern_yyyymmdd_pct": "YYYYMMDD Pattern %",
            "date_pattern_ddmmyyyy_pct": "DDMMYYYY Pattern %",
            "date_pattern_iso_ymd_pct": "ISO Pattern %",
            "date_parse_yyyymmdd_pct": "YYYYMMDD Parse %",
            "date_parse_ddmmyyyy_pct": "DDMMYYYY Parse %",
            "date_parse_iso_pct": "ISO Parse %",
            "date_parse_best_pct": "Best Parse %",
            "date_parse_best_format": "Best Parse Format",
            "parsed_date_min": "Parsed Min Date",
            "parsed_date_max": "Parsed Max Date",
        }
        display_df = display_df.rename(columns={k: v for k, v in rename_map.items() if k in display_df.columns})
        if "Guessed Type" in display_df.columns:
            display_df["Guessed Type"] = display_df["Guessed Type"].fillna("Unknown")
        ordered_columns = [
            "column_name",
            "data_type",
            "Non-null Rows",
            "nulls",
            "null_pct",
            "Distinct Count",
            "distinct_pct",
            "Distinct Ratio %",
            "Top 1 Ratio %",
            "Top-3 Coverage %",
            "Numeric-like %",
            "min_val",
            "max_val",
            "Numeric Min",
            "Numeric Max",
            "avg_len",
            "Len Min/Max",
            "whitespace_pct",
            "YYYYMMDD Pattern %",
            "DDMMYYYY Pattern %",
            "ISO Pattern %",
            "YYYYMMDD Parse %",
            "DDMMYYYY Parse %",
            "ISO Parse %",
            "Best Parse %",
            "Best Parse Format",
            "Parsed Min Date",
            "Parsed Max Date",
            "Date (Text)",
            "Reference Codes",
            "error",
            "Guessed Type",
            "Confidence Badge",
            "Confidence",
            "Confidence Rationale",
        ]
        display_df = display_df[[col for col in ordered_columns if col in display_df.columns] + [
            col for col in display_df.columns if col not in ordered_columns
        ]]
        formatters: Dict[str, Any] = {"Confidence": _format_confidence}
        for count_col in ("nulls", "Distinct Count", "Non-null Rows"):
            if count_col in display_df.columns:
                formatters[count_col] = _format_count
        percentage_columns = (
            "null_pct",
            "distinct_pct",
            "whitespace_pct",
            "Distinct Ratio %",
            "Top 1 Ratio %",
            "Top-3 Coverage %",
            "Numeric-like %",
            "YYYYMMDD Pattern %",
            "DDMMYYYY Pattern %",
            "ISO Pattern %",
            "YYYYMMDD Parse %",
            "DDMMYYYY Parse %",
            "ISO Parse %",
            "Best Parse %",
        )
        for pct_col in percentage_columns:
            if pct_col in display_df.columns:
                formatters[pct_col] = _format_percentage
        styler = display_df.style.format(formatters)
        if "Confidence Badge" in display_df.columns:
            styler = styler.applymap(_badge_css, subset=["Confidence Badge"])
        if badge_styles:
            def _style_from_list(styles: List[str]) -> Callable[[pd.Series], List[str]]:
                def _apply(_: pd.Series) -> List[str]:
                    return styles

                return _apply

            for column, styles in badge_styles.items():
                if column in display_df.columns:
                    styler = styler.apply(_style_from_list(styles), subset=[column])
        if tooltip_columns:
            tooltip_df = pd.DataFrame(tooltip_columns, index=display_df.index)
            existing_cols = [col for col in tooltip_df.columns if col in display_df.columns]
            if existing_cols:
                styler = styler.set_tooltips(tooltip_df[existing_cols])
        column_config: Dict[str, st.column_config.BaseColumn] = {}
        if "Confidence Rationale" in display_df.columns:
            column_config["Confidence Rationale"] = st.column_config.TextColumn(
                "Confidence Rationale",
                help="Explanation for how the guessed type confidence was determined.",
                width="medium",
            )
        st.dataframe(
            styler,
            hide_index=True,
            use_container_width=True,
            column_config=column_config or None,
        )
    else:
        st.dataframe(display_df, hide_index=True, use_container_width=True)

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
            if "value" in tv_df.columns:
                rename_map["value"] = "Value"
            if "count" in tv_df.columns:
                rename_map["count"] = "Count"
            if rename_map:
                tv_df = tv_df.rename(columns=rename_map)
            elif tv_df.shape[1] == 2:
                tv_df.columns = ["Value", "Count"]

            if "Count" in tv_df.columns:
                tv_df["Count"] = tv_df["Count"].apply(_format_count)
            for col in tv_df.columns:
                if col == "Count":
                    continue
                if "pct" in col.lower():
                    tv_df[col] = tv_df[col].apply(_format_percentage)

            st.table(tv_df)
