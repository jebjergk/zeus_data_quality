"""Streamlit view for administering the DQ rule library."""

from __future__ import annotations

import json
from typing import Any, List, Optional

import pandas as pd
import streamlit as st

try:  # pragma: no cover - Streamlit runtime handles Snowpark availability
    from snowflake.snowpark import Session  # type: ignore
except Exception:  # pragma: no cover
    Session = Any  # type: ignore

from utils.meta import _q

DISPLAY_COLUMNS: List[str] = [
    "RULE_ID",
    "CHECK_TYPE",
    "DEFAULT_SEVERITY",
    "ACTIVE",
    "UPDATED_AT",
    "DESCRIPTION",
]


def _validate_expression_template(session: Session, expression_template: str) -> str:
    template = (expression_template or "").strip()
    if not template:
        return "ERROR: Expression template is empty."
    try:
        df = session.sql(
            "CALL ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_VALIDATE_RULE_TEMPLATE(:1)",
            params=[template],
        ).to_pandas()
    except Exception as exc:  # pragma: no cover - surfacing Snowflake errors to UI
        return f"ERROR: Validation failed: {exc}"
    if df.empty or df.shape[1] == 0:
        return "ERROR: Validation returned no result."
    value = df.iloc[0, 0]
    return str(value or "")


def _display_validation_feedback(status: str) -> None:
    if status == "OK":
        st.success("Expression is valid.")
    elif status.upper().startswith("ERROR"):
        st.error(status)
    else:
        st.warning(status)


def _fq_rule_table(database: str, schema: str) -> str:
    return f"{_q(database)}.{_q(schema)}.{_q('DQ_RULE_LIBRARY')}"


def _normalize_param_schema(value: Any) -> str:
    if value is None or value == "":
        return "[]"
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return "[]"
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
    else:
        parsed = value
    try:
        return json.dumps(parsed, indent=2, default=str)
    except TypeError:
        return str(parsed)


def _load_rules(session: Session, table: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            RULE_UID,
            RULE_ID,
            CHECK_TYPE,
            DEFAULT_SEVERITY,
            DESCRIPTION,
            ACTIVE,
            UPDATED_AT
        FROM {table}
    """
    df = session.sql(sql).to_pandas()
    df.columns = [col.upper() for col in df.columns]
    if "UPDATED_AT" in df.columns:
        df["UPDATED_AT"] = pd.to_datetime(
            df["UPDATED_AT"], errors="coerce", utc=True
        ).dt.tz_convert(None)
    return df


def _severity_options(df: pd.DataFrame) -> List[str]:
    values = sorted(
        {str(val).strip() for val in df.get("DEFAULT_SEVERITY", []) if str(val).strip()}
    )
    if not values:
        values = ["LOW", "MEDIUM", "HIGH"]
    return values


def _render_rule_edit_page(session: Session, metadata_db: str, metadata_schema: str) -> None:
    """Placeholder for the dedicated rule edit page (RL3A)."""

    st.info("Rule edit page will be implemented in RL3A.")


def _apply_filters(
    df: pd.DataFrame, *, search_text: str = "", check_type: Optional[str] = None
) -> pd.DataFrame:
    filtered = df.copy()
    if search_text:
        needle = search_text.lower()
        mask = (
            filtered["RULE_ID"].astype(str).str.lower().str.contains(needle)
            | filtered["CHECK_TYPE"].astype(str).str.lower().str.contains(needle)
            | filtered["DESCRIPTION"].fillna("").astype(str).str.lower().str.contains(needle)
        )
        filtered = filtered[mask]
    if check_type and check_type != "All":
        filtered = filtered[filtered["CHECK_TYPE"].astype(str) == check_type]
    return filtered


def _render_rule_list(
    *, session: Session, table_name: str, rules_df: pd.DataFrame
) -> None:
    st.subheader("Rule list")

    st.session_state.setdefault("dq_rules_search", "")
    st.session_state.setdefault("dq_rules_check_type_filter", "All")

    search = st.text_input("Search rules", value=st.session_state["dq_rules_search"], key="dq_rules_search")

    distinct_types = sorted({str(val) for val in rules_df.get("CHECK_TYPE", []) if pd.notna(val)})
    check_type_options = ["All"] + distinct_types
    check_type_filter = st.selectbox(
        "Filter by check type",
        options=check_type_options,
        index=check_type_options.index(st.session_state.get("dq_rules_check_type_filter", "All"))
        if st.session_state.get("dq_rules_check_type_filter", "All") in check_type_options
        else 0,
        key="dq_rules_check_type_filter",
    )

    filtered_df = _apply_filters(rules_df, search_text=search, check_type=check_type_filter)

    display_df = filtered_df.copy()
    display_df["DESCRIPTION"] = display_df["DESCRIPTION"].fillna("").astype(str).str.slice(stop=120)
    display_df = display_df[DISPLAY_COLUMNS]
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()
    st.caption("Actions")
    for rule in filtered_df.to_dict("records"):
        rule_uid = rule.get("RULE_UID")
        rule_id = rule.get("RULE_ID")
        check_type = rule.get("CHECK_TYPE")
        col_label, col_edit, col_delete = st.columns([3, 1, 1])
        with col_label:
            st.markdown(f"**{rule_id}** — {check_type}")
        with col_edit:
            if st.button("Edit", key=f"edit_rule_{rule_uid}"):
                st.session_state["dq_rules_mode"] = "edit_existing"
                st.session_state["dq_rules_selected_uid"] = rule_uid
                st.experimental_rerun()
        with col_delete:
            if st.button("Delete", key=f"delete_rule_{rule_uid}"):
                _delete_rule(session, table_name, rule_uid)
                st.experimental_rerun()


def _delete_rule(session: Session, table_name: str, rule_uid: Any) -> None:
    try:
        session.sql(
            f"DELETE FROM {table_name} WHERE RULE_UID = :1",
            params=[rule_uid],
        ).collect()
        st.success("Rule deleted.")
    except Exception as exc:  # pragma: no cover - surface Snowflake errors
        st.error(f"Unable to delete rule: {exc}")


def render_rule_admin(session: Optional[Session], metadata_db: str, metadata_schema: str) -> None:
    st.title("DQ Rule Library")

    if session is None:
        st.info("A Snowflake session is required to manage the rule library.")
        return

    table_name = _fq_rule_table(metadata_db, metadata_schema)
    st.caption(f"Source table: {table_name}")

    mode = st.session_state.get("dq_rules_mode", "list")

    if mode != "list":
        _render_rule_edit_page(session, metadata_db, metadata_schema)
        return

    create_col, _ = st.columns([1, 3])
    with create_col:
        if st.button("Create new rule", key="dq_rules_create_btn"):
            st.session_state["dq_rules_mode"] = "create_new"
            st.session_state["dq_rules_selected_uid"] = None
            st.experimental_rerun()

    try:
        rules_df = _load_rules(session, table_name)
    except Exception as exc:
        st.error(f"Unable to load rule library: {exc}")
        return

    if rules_df.empty:
        st.info("No rules available. Use 'Create new rule' to add the first rule.")
        return

    _render_rule_list(session=session, table_name=table_name, rules_df=rules_df)
