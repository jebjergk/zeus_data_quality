from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
import streamlit as st

from services.profile import build_profile_suggestion
from services.profiling import save_profile_results
from utils.meta import _q
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


def _table_picker(session_obj, preselect_fqn: Optional[str]):
    return stateless_table_picker(session_obj, preselect_fqn)


def _fetch_columns(session, fqn: str) -> List[Tuple[str, str]]:
    """Return column metadata as (name, data_type)."""

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_columns(_session_token: str, table_fqn: str) -> List[Tuple[str, str]]:
        try:
            rows = session.sql(f"DESC TABLE {table_fqn}").collect()
        except Exception as exc:  # pragma: no cover - Snowflake specific
            st.error(f"Failed to describe table {table_fqn}: {exc}")
            return []
        columns: List[Tuple[str, str]] = []
        for row in rows:
            try:
                name = row[0]
                dtype = row[1]
            except Exception:
                data = getattr(row, "asDict", lambda: {})()
                name = data.get("name")
                dtype = data.get("type")
            if not name:
                continue
            columns.append((str(name), str(dtype or "")))
        return columns

    session_token = session_cache_token(session) if session else ""  # pragma: no cover - defensive
    return _load_columns(session_token, fqn)


def _create_sample_table(session, source_fqn: str, sample_pct: Optional[float]) -> Tuple[str, int]:
    pct: Optional[float]
    if sample_pct is None:
        pct = None
    else:
        pct = max(0.0, min(float(sample_pct), 100.0))
        if pct <= 0 or math.isclose(pct, 100.0, abs_tol=1e-6):
            pct = None

    temp_name = f"PROFILE_SAMPLE_{uuid4().hex.upper()}"
    sample_clause = "" if pct is None else f" TABLESAMPLE BERNOULLI ({pct})"
    create_sql = f"CREATE OR REPLACE TEMP TABLE {_q(temp_name)} AS SELECT * FROM {source_fqn}{sample_clause}"
    session.sql(create_sql).collect()
    count_row = session.sql(f"SELECT COUNT(*) AS CNT FROM {_q(temp_name)}").collect()[0]
    total = count_row[0] if isinstance(count_row, tuple) else count_row["CNT"]  # type: ignore[index]
    try:
        total_int = int(total)
    except Exception:
        total_int = 0
    return temp_name, total_int


def _profile_column(
    session,
    temp_table: str,
    column_name: str,
    data_type: str,
    total_rows: int,
    top_n: int,
) -> ColumnProfile:
    quoted_col = f'"{column_name}"'
    top_n = max(0, min(int(top_n), MAX_TOP_N))
    supports_text_metrics = any(key in data_type.upper() for key in ("CHAR", "TEXT", "STRING", "VARCHAR"))
    whitespace_pct: Optional[float] = None
    avg_len: Optional[float] = None
    nulls = distincts = None
    min_val = max_val = None
    error: Optional[str] = None

    metrics_sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT_IF({quoted_col} IS NULL) AS nulls,
            COUNT(DISTINCT {quoted_col}) AS distincts,
            MIN({quoted_col}) AS min_val,
            MAX({quoted_col}) AS max_val,
            AVG(LENGTH({quoted_col}::STRING)) AS avg_len,
            AVG(CASE WHEN REGEXP_LIKE({quoted_col}::STRING, '^\\s*$') THEN 1 ELSE 0 END) AS whitespace_ratio
        FROM {_q(temp_table)}
    """
    try:
        row = session.sql(metrics_sql).collect()[0]
        if hasattr(row, "asDict"):
            data = row.asDict()
            nulls = data.get("NULLS") or data.get("nulls")
            distincts = data.get("DISTINCTS") or data.get("distincts")
            min_val = data.get("MIN_VAL") or data.get("min_val")
            max_val = data.get("MAX_VAL") or data.get("max_val")
            avg_len_raw = data.get("AVG_LEN") or data.get("avg_len")
            whitespace_raw = data.get("WHITESPACE_RATIO") or data.get("whitespace_ratio")
        else:
            nulls = row[1]
            distincts = row[2]
            min_val = row[3]
            max_val = row[4]
            avg_len_raw = row[5]
            whitespace_raw = row[6]
    except Exception as exc:  # pragma: no cover - Snowflake specific
        error = str(exc)
        avg_len_raw = None
        whitespace_raw = None

    if supports_text_metrics:
        avg_len = float(avg_len_raw) if avg_len_raw not in (None, "") else None
        whitespace_pct = (
            float(whitespace_raw) * 100.0
            if whitespace_raw not in (None, "")
            else None
        )
    else:
        avg_len = None
        whitespace_pct = None

    try:
        null_count = int(nulls) if nulls is not None else None
    except Exception:
        null_count = None
    try:
        distinct_count = int(distincts) if distincts is not None else None
    except Exception:
        distinct_count = None

    null_pct = None
    if total_rows:
        null_pct = (float(null_count) / float(total_rows) * 100.0) if null_count is not None else None
        distinct_pct = (
            (float(distinct_count) / float(total_rows) * 100.0)
            if distinct_count is not None
            else None
        )
    else:
        distinct_pct = None

    top_rows: List[Dict[str, Any]] = []
    if top_n > 0 and total_rows:
        top_sql = f"""
            SELECT {quoted_col} AS value, COUNT(*) AS cnt
            FROM {_q(temp_table)}
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT {top_n}
        """
        try:
            top_data = session.sql(top_sql).collect()
        except Exception:  # pragma: no cover - Snowflake specific
            top_data = []
        for item in top_data:
            if hasattr(item, "asDict"):
                data = item.asDict()
                value = data.get("VALUE") if "VALUE" in data else data.get("value")
                count = data.get("CNT") if "CNT" in data else data.get("cnt")
            else:
                value, count = item[0], item[1]
            top_rows.append({"value": value, "count": count})

    return ColumnProfile(
        name=column_name,
        data_type=data_type,
        nulls=null_count,
        null_pct=null_pct,
        distincts=distinct_count,
        distinct_pct=distinct_pct,
        min_val=min_val,
        max_val=max_val,
        avg_len=avg_len,
        whitespace_pct=whitespace_pct,
        top_values=top_rows,
        error=error,
    )


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
        ]
        missing = [c for c in display_cols if c not in df.columns]
        df = df.reindex(columns=[c for c in display_cols if c not in missing] + [c for c in df.columns if c not in display_cols])
    return df


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
    with controls[0]:
        sample_pct_input = st.number_input(
            "Sample %",
            min_value=0.0,
            max_value=100.0,
            value=10.0,
            step=1.0,
            help="Enter 0 for a full table scan.",
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
                    temp_table, row_count = _create_sample_table(session, selected_fqn, sample_pct)
                except Exception as exc:  # pragma: no cover - Snowflake specific
                    st.error(f"Failed to sample table: {exc}")
                    temp_table = ""
                    row_count = 0
                columns = _fetch_columns(session, selected_fqn) if temp_table else []
                profiles: List[ColumnProfile] = []
                if temp_table and columns:
                    for name, dtype in columns:
                        profiles.append(
                            _profile_column(
                                session=session,
                                temp_table=temp_table,
                                column_name=name,
                                data_type=dtype,
                                total_rows=row_count,
                                top_n=int(min(top_n, MAX_TOP_N)),
                            )
                        )
                if temp_table:
                    try:
                        session.sql(f"DROP TABLE IF EXISTS {_q(temp_table)}").collect()
                    except Exception:
                        pass
                duration = time.time() - start
                profile_result = {
                    "target_table": selected_fqn,
                    "summary": {
                        "rows_profiled": row_count,
                        "sample_pct": float(sample_pct) if sample_pct is not None else None,
                        "duration_sec": duration,
                        "columns": len(profiles),
                    },
                    "columns": [profile.__dict__ for profile in profiles],
                    "top_n": int(top_n),
                }
                st.session_state["profile_results"] = profile_result

    if suggest_cfg and profile_result:
        suggestion = build_profile_suggestion(profile_result)
        if not suggestion:
            st.info("No suggestions available for the current profile.")
        else:
            st.session_state["cfg_mode"] = "edit"
            st.session_state["selected_config_id"] = None
            st.session_state["editor_target_fqn"] = profile_result.get("target_table")
            st.session_state["profile_suggestion"] = suggestion
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

    st.dataframe(
        filtered_df.drop(columns=["top_values"], errors="ignore"),
        hide_index=True,
        use_container_width=True,
    )

    if filtered_df.empty:
        st.info("No columns matched the selected filters.")

    st.subheader("Top values by column", anchor=False)
    for _, row in filtered_df.iterrows():
        values = row.get("top_values", [])
        if not values:
            continue
        with st.expander(f"{row['column_name']} ({len(values)} values)"):
            tv_df = pd.DataFrame(values)
            tv_df.columns = ["Value", "Count"]
            st.table(tv_df)
