from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from utils.configs import get_metadata_namespace


def _safe_to_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def _schema_from_fqn(table_fqn: str) -> str:
    parts = str(table_fqn).split(".")
    if len(parts) <= 1:
        return ""
    if len(parts) == 2:
        return parts[0]
    return ".".join(parts[:-1])


def _distinct_options(series: Iterable[str]) -> list[str]:
    cleaned = set()
    for item in series:
        if item is None:
            continue
        if isinstance(item, float) and pd.isna(item):
            continue
        text = str(item).strip()
        if text:
            cleaned.add(text)
    return sorted(cleaned)


def _extract_error(detail: object) -> str:
    if isinstance(detail, dict):
        for key in ("error", "message", "error_message", "reason"):
            if key in detail and detail[key]:
                return str(detail[key])
    if isinstance(detail, str):
        return detail
    return ""


def _format_detail(detail: object) -> str:
    if detail is None:
        return ""
    if isinstance(detail, str):
        return detail
    try:
        return json.dumps(detail, indent=2, default=str)
    except Exception:
        return str(detail)


def _status_style(row: pd.Series) -> list[str]:
    status = str(row.get("Status", "")).upper()
    failures = row.get("Failures", 0) or 0
    bg = "#e5e7eb"
    fg = "#111827"
    if status == "PASS":
        bg, fg = "#eafaf0", "#065f46"
    elif status == "FAIL":
        if isinstance(failures, (int, float)) and failures and failures < 5:
            bg, fg = "#fef3c7", "#92400e"
        else:
            bg, fg = "#fee2e2", "#991b1b"
    return [f"background-color:{bg}; color:{fg}; font-weight:600"]


def render_monitor_v3(session):
    """Render Monitor v3 connected to DQ_RUN_RESULT_DETAIL."""

    st.header("📈 DQ Monitor v3")
    st.caption("Interactive view of recent DQ run diagnostics.")

    if session is None:
        st.warning("Connect to Snowflake to explore Monitor v3.")
        return

    metadata_db, metadata_schema = get_metadata_namespace()
    detail_table = f"{metadata_db}.{metadata_schema}.DQ_RUN_RESULT_DETAIL"

    today = date.today()
    default_start = today - timedelta(days=7)

    try:
        schema_df = session.sql(
            f"SELECT DISTINCT TABLE_FQN FROM {detail_table} WHERE TABLE_FQN IS NOT NULL"
        ).to_pandas()
    except Exception:
        schema_df = pd.DataFrame()

    table_fqns: Iterable[str] = schema_df["TABLE_FQN"].tolist() if "TABLE_FQN" in schema_df else []
    available_schemas = _distinct_options(_schema_from_fqn(value) for value in table_fqns)
    available_tables = _distinct_options(table_fqns)

    try:
        categories_df = session.sql(
            f"SELECT DISTINCT RULE_CATEGORY FROM {detail_table} WHERE RULE_CATEGORY IS NOT NULL"
        ).to_pandas()
    except Exception:
        categories_df = pd.DataFrame()
    available_categories = _distinct_options(categories_df.get("RULE_CATEGORY", []))

    filters = st.columns([2, 1.2, 1.2, 1])
    with filters[0]:
        date_range = st.date_input("Date range", value=(default_start, today))
    with filters[1]:
        schema_selection = st.selectbox(
            "Schema",
            ["All schemas", *available_schemas] if available_schemas else ["All schemas"],
            key="monitor_v3_schema_filter",
        )
    with filters[2]:
        table_selection = st.selectbox(
            "Table",
            ["All tables", *available_tables] if available_tables else ["All tables"],
            key="monitor_v3_table_filter",
        )
    with filters[3]:
        category_selection = st.selectbox(
            "Rule category",
            ["All categories", *available_categories]
            if available_categories
            else ["All categories"],
            key="monitor_v3_rule_category",
        )

    search_term = st.text_input(
        "Search", placeholder="Search tables, columns, or rules", key="monitor_v3_search"
    ).strip()

    if not isinstance(date_range, (list, tuple)) or len(date_range) != 2:
        st.error("Please select a start and end date for filtering.")
        return

    start_date = _safe_to_datetime(date_range[0])
    end_date = _safe_to_datetime(date_range[1]) + timedelta(days=1) - timedelta(seconds=1)

    from snowflake.snowpark.functions import col

    df = session.table(detail_table)
    df = df.filter((col("EXECUTED_AT") >= start_date) & (col("EXECUTED_AT") <= end_date))

    if schema_selection != "All schemas" and schema_selection:
        df = df.filter(col("TABLE_FQN").like(f"{schema_selection}.%"))
    if table_selection != "All tables" and table_selection:
        df = df.filter(col("TABLE_FQN") == table_selection)
    if category_selection != "All categories" and category_selection:
        df = df.filter(col("RULE_CATEGORY") == category_selection)

    result_df = df.to_pandas()
    result_df.columns = [col.upper() for col in result_df.columns]

    if result_df.empty:
        st.info("No results match the selected filters yet.")
        return

    result_df["EXECUTED_AT"] = pd.to_datetime(result_df["EXECUTED_AT"], errors="coerce")

    total = len(result_df)
    pass_count = (result_df["STATUS"].str.upper() == "PASS").sum()
    fail_count = (result_df["STATUS"].str.upper() == "FAIL").sum()
    pass_rate = (pass_count / total) * 100 if total else 0.0
    dq_score = max(0.0, min(100.0, ((pass_count - fail_count) / total) * 100)) if total else 0.0
    critical_issues = result_df[
        (result_df.get("RULE_CATEGORY", "").str.upper() == "CRITICAL")
        & (result_df["STATUS"].str.upper() == "FAIL")
    ].shape[0]

    st.divider()
    metrics = st.columns(4)
    metrics[0].metric("Pass Rate", f"{pass_rate:0.1f}%", delta=None)
    metrics[1].metric("Failed Checks", f"{fail_count}")
    metrics[2].metric("Critical Issues", f"{critical_issues}")
    metrics[3].metric("Average DQ Score", f"{dq_score:0.1f}")

    st.divider()
    st.subheader("Heatmap")

    heatmap_df = (
        result_df.groupby(["TABLE_FQN", "RULE_CATEGORY"])["STATUS"]
        .apply(lambda s: (s.str.upper() == "PASS").mean())
        .reset_index(name="PASS_RATE")
    )

    if not heatmap_df.empty:
        pivot_df = heatmap_df.pivot(
            index="TABLE_FQN", columns="RULE_CATEGORY", values="PASS_RATE"
        ).fillna(0)
        fig, ax = plt.subplots(figsize=(8, max(3, len(pivot_df) * 0.4)))
        cax = ax.imshow(pivot_df, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1)
        ax.set_xticks(range(len(pivot_df.columns)), pivot_df.columns, rotation=45, ha="right")
        ax.set_yticks(range(len(pivot_df.index)), pivot_df.index)
        ax.set_xlabel("Rule category")
        ax.set_ylabel("Table")
        ax.set_title("Pass rate by table and category")
        fig.colorbar(cax, ax=ax, fraction=0.046, pad=0.04, label="Pass rate")
        st.pyplot(fig, use_container_width=True)
    else:
        st.caption("No data available for the selected filters.")

    st.subheader("Trend")
    trend_df = (
        result_df.assign(DAY=result_df["EXECUTED_AT"].dt.floor("D"))
        .groupby("DAY")["STATUS"]
        .apply(lambda s: (s.str.upper() == "PASS").mean() * 100)
        .reset_index(name="PASS_RATE")
        .sort_values("DAY")
    )
    if not trend_df.empty:
        trend_df = trend_df.set_index("DAY")
        trend_df.columns = ["Pass rate"]
        st.line_chart(trend_df)
    else:
        st.caption("No daily trend available for the selected range.")

    st.subheader("Top failing checks")
    failing_df = result_df[result_df["STATUS"].str.upper() == "FAIL"]
    top_failures = (
        failing_df.groupby(["RULE_CODE", "TABLE_FQN"])["FAILURE_COUNT"]
        .sum()
        .reset_index()
        .sort_values("FAILURE_COUNT", ascending=False)
        .head(10)
    )
    if not top_failures.empty:
        top_failures["Rule"] = (
            top_failures["RULE_CODE"] + " — " + top_failures["TABLE_FQN"]
        )
        st.bar_chart(
            top_failures.set_index("Rule")["FAILURE_COUNT"],
            use_container_width=True,
        )
    else:
        st.caption("No failing checks in the selected window.")

    st.subheader("Drill-down")

    drilldown_df = result_df[
        [
            "TABLE_FQN",
            "COLUMN_NAME",
            "RULE_CODE",
            "RULE_CATEGORY",
            "STATUS",
            "FAILURE_COUNT",
            "EXECUTED_AT",
            "DETAIL",
        ]
    ].rename(
        columns={
            "TABLE_FQN": "Table",
            "COLUMN_NAME": "Column",
            "RULE_CODE": "Rule",
            "RULE_CATEGORY": "Category",
            "STATUS": "Status",
            "FAILURE_COUNT": "Failures",
            "EXECUTED_AT": "Executed at",
            "DETAIL": "Diagnostics",
        }
    )

    drilldown_df["Error message"] = drilldown_df["Diagnostics"].apply(_extract_error)
    drilldown_df["Diagnostics"] = drilldown_df["Diagnostics"].apply(_format_detail)
    if search_term:
        lowered = search_term.lower()
        drilldown_df = drilldown_df[
            drilldown_df.apply(
                lambda row: any(
                    str(row.get(col, "")).lower().find(lowered) >= 0
                    for col in ("Table", "Column", "Rule", "Error message")
                ),
                axis=1,
            )
        ]

    drilldown_df.sort_values("Executed at", ascending=False, inplace=True)
    styled = drilldown_df.style.apply(_status_style, subset=["Status"], axis=1)
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
    )
