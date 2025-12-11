from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from views.table_picker import stateless_table_picker


def render_monitor_v3(session):
    """Render the scaffolding for the Monitor v3 dashboard."""

    st.header("📈 DQ Monitor v3")
    st.caption("Early preview of the upcoming monitoring dashboard.")

    today = date.today()
    default_start = today - timedelta(days=7)

    filters = st.columns([2, 1.2, 1.2, 1])
    with filters[0]:
        st.date_input("Date range", value=(default_start, today))
    with filters[1]:
        st.selectbox(
            "Schema",
            ["All schemas", "PUBLIC", "ANALYTICS", "RAW"],
            key="monitor_v3_schema_filter",
        )
    with filters[2]:
        st.selectbox(
            "Table",
            ["All tables", "ORDERS", "CUSTOMERS", "INVOICES"],
            key="monitor_v3_table_filter",
        )
    with filters[3]:
        st.selectbox(
            "Rule category",
            [
                "All categories",
                "Freshness",
                "Completeness",
                "Validity",
                "Anomaly detection",
            ],
            key="monitor_v3_rule_category",
        )

    st.text_input("Search", placeholder="Search tables, columns, or rules", key="monitor_v3_search")

    st.divider()

    metrics = st.columns(4)
    metrics[0].metric("Pass Rate", "—")
    metrics[1].metric("Failed Checks", "—")
    metrics[2].metric("Critical Issues", "—")
    metrics[3].metric("Average DQ Score", "—")

    st.divider()

    st.subheader("Heatmap")
    st.empty().write("Heatmap placeholder")

    st.subheader("Trend")
    trend_data = pd.DataFrame(
        {
            "Date": pd.date_range(end=today, periods=7),
            "Failures": [5, 3, 6, 2, 4, 1, 3],
        }
    )
    st.line_chart(trend_data.set_index("Date"))

    st.subheader("Top failing checks")
    top_failing = pd.DataFrame(
        {
            "Rule": ["Null count", "Freshness", "Row count", "Format distribution"],
            "Failures": [23, 17, 12, 9],
        }
    )
    st.bar_chart(top_failing.set_index("Rule"))

    st.subheader("Rule outcomes")
    outcome_placeholder = st.empty()
    outcome_placeholder.write("Pie chart placeholder")

    st.divider()
    st.subheader("Drill-down")

    st.caption("Table and column filters use the stateless picker pattern from Profiling v2.")
    stateless_table_picker(session, None, disabled=not session)

    drilldown_df = pd.DataFrame(
        {
            "Table": ["ORDERS", "CUSTOMERS", "PAYMENTS"],
            "Column": ["ORDER_ID", "EMAIL", "AMOUNT"],
            "Rule": ["Uniqueness", "Format", "Min/Max"],
            "Status": ["Failed", "Failed", "Passed"],
            "Failures": [5, 12, 0],
            "Timestamp": pd.date_range(end=today, periods=3, freq="12H"),
        }
    )
    st.dataframe(
        drilldown_df,
        use_container_width=True,
        hide_index=True,
    )
