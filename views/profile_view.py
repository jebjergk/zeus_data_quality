from __future__ import annotations
import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st

from services.profile import build_profile_suggestion
from services.profiling import run_table_profile, save_profile_results
from utils.meta import get_table_row_count
from views.table_picker import session_cache_token, stateless_table_picker


FULL_SCAN_WARNING_THRESHOLD = 1_000_000
MAX_TOP_N = 10


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
    top_values: List[Dict[str, Any]]
    error: Optional[str] = None
    semantic_type: Optional[str] = None
    confidence: Optional[float] = None
    rationale: Optional[str] = None


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
            "semantic_type",
            "confidence",
            "rationale",
            "error",
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


def _tooltip_styles() -> List[Dict[str, Any]]:
    base_class = "confidence-tooltip"
    text_class = f"{base_class}-text"
    return [
        {
            "selector": f".{base_class}",
            "props": [
                ("position", "relative"),
                ("display", "inline-block"),
            ],
        },
        {
            "selector": f".{base_class} .{text_class}",
            "props": [
                ("visibility", "hidden"),
                ("width", "240px"),
                ("background-color", "#31333F"),
                ("color", "#ffffff"),
                ("text-align", "left"),
                ("border-radius", "4px"),
                ("padding", "0.4rem"),
                ("position", "absolute"),
                ("z-index", "1"),
                ("bottom", "125%"),
                ("left", "50%"),
                ("margin-left", "-120px"),
                ("box-shadow", "0 2px 6px rgba(0, 0, 0, 0.2)"),
                ("font-size", "0.75rem"),
            ],
        },
        {
            "selector": f".{base_class}:hover .{text_class}",
            "props": [("visibility", "visible")],
        },
    ]


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
                for column in column_rows:
                    profiles.append(
                        ColumnProfile(
                            name=str(column.get("column_name") or column.get("name") or ""),
                            data_type=str(column.get("data_type") or ""),
                            nulls=column.get("nulls"),
                            null_pct=column.get("null_pct"),
                            distincts=column.get("distincts"),
                            distinct_pct=column.get("distinct_pct"),
                            min_val=column.get("min_val"),
                            max_val=column.get("max_val"),
                            avg_len=column.get("avg_len"),
                            whitespace_pct=column.get("whitespace_pct"),
                            top_values=column.get("top_values") or [],
                            error=column.get("error"),
                            semantic_type=column.get("semantic_type"),
                            confidence=column.get("confidence"),
                            rationale=column.get("rationale"),
                        )
                    )
                profile_result = {
                    "target_table": selected_fqn,
                    "summary": {
                        "rows_profiled": rows_profiled,
                        "sample_pct": summary_raw.get("sample_pct"),
                        "duration_sec": duration,
                        "columns": len(profiles),
                    },
                    "columns": [profile.__dict__ for profile in profiles],
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
    profiles = [ColumnProfile(**col) for col in profiles_raw]
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
        semantic_cols = st.columns(5)
        filter_identifiers = semantic_cols[0].checkbox("Identifiers", value=False)
        filter_financial = semantic_cols[1].checkbox("Financial", value=False)
        filter_instrument = semantic_cols[2].checkbox("Instrument", value=False)
        filter_geo = semantic_cols[3].checkbox("Geo", value=False)
        filter_contact = semantic_cols[4].checkbox("Contact", value=False)

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
            column_name = column_profile.get("column_name") or column_profile.get("name")
            if not column_name:
                continue
            rows_payload.append({**column_profile, "column_name": column_name})

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
        "Identifiers": {"ACCOUNT_ID", "ORDER_ID", "TRADE_ID", "UUID", "IBAN"},
        "Financial": {"PRICE/AMOUNT/QUANTITY", "IBAN", "BIC"},
        "Instrument": {"ISIN", "TICKER/SYMBOL"},
        "Geo": {"COUNTRY_CODE/NAME", "CURRENCY_CODE", "BIC"},
        "Contact": {"EMAIL", "PHONE"},
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

    if active_semantic_filters:
        allowed_types = set()
        for key in active_semantic_filters:
            allowed_types.update(semantic_filter_map.get(key, set()))
        filtered_df = filtered_df[filtered_df["semantic_type"].isin(allowed_types)]
    display_df = filtered_df.drop(columns=["top_values"], errors="ignore").copy()
    if not display_df.empty:
        if "confidence" in display_df.columns:
            display_df["Confidence"] = display_df["confidence"].apply(
                lambda val: float(val) if val is not None else None
            )
            display_df["Confidence Badge"] = display_df["confidence"].apply(_confidence_badge_label)
            display_df = display_df.drop(columns=["confidence"], errors="ignore")
        else:
            display_df["Confidence"] = None
            display_df["Confidence Badge"] = "Unknown"
        rationale_series = filtered_df.get("rationale", pd.Series(dtype="object"))
        display_df = display_df.rename(columns={"semantic_type": "Guessed Type"})
        if "Guessed Type" in display_df.columns:
            display_df["Guessed Type"] = display_df["Guessed Type"].fillna("Unknown")
        ordered_columns = [
            "column_name",
            "data_type",
            "Guessed Type",
            "Confidence Badge",
            "Confidence",
            "nulls",
            "null_pct",
            "distincts",
            "distinct_pct",
            "min_val",
            "max_val",
            "avg_len",
            "whitespace_pct",
            "error",
            "rationale",
        ]
        display_df = display_df[[col for col in ordered_columns if col in display_df.columns] + [
            col for col in display_df.columns if col not in ordered_columns
        ]]
        tooltip_df = pd.DataFrame("", index=display_df.index, columns=display_df.columns)
        if "Confidence Badge" in tooltip_df.columns:
            tooltip_df["Confidence Badge"] = rationale_series.reindex(display_df.index).fillna("")
        styler = display_df.style.format({"Confidence": _format_confidence})
        if "Confidence Badge" in display_df.columns:
            styler = styler.applymap(_badge_css, subset=["Confidence Badge"])
        styler = styler.set_tooltips(tooltip_df, css_class="confidence-tooltip")
        hide_columns: List[str] = []
        if "rationale" in display_df.columns:
            hide_columns.append("rationale")
        if hide_columns:
            styler = styler.hide(axis="columns", subset=hide_columns)
        styler = styler.set_table_styles(_tooltip_styles(), overwrite=False)
        st.dataframe(styler, hide_index=True, use_container_width=True)
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

            st.table(tv_df)
