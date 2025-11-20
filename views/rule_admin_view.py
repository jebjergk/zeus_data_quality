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
    mode = st.session_state.get("dq_rules_mode")
    selected_uid = st.session_state.get("dq_rules_selected_uid")

    if mode not in {"edit_existing", "create_new"}:
        st.info("Rule editor is available only in edit or create mode.")
        return

    table_name = _fq_rule_table(metadata_db, metadata_schema)

    back_col, _ = st.columns([1, 3])
    with back_col:
        if st.button("Back to rule list", key="dq_rules_back_to_list"):
            st.session_state["dq_rules_mode"] = "list"
            st.session_state["dq_rules_selected_uid"] = None
            st.experimental_rerun()

    rule_defaults: dict[str, Any] = {
        "RULE_ID": "",
        "CHECK_TYPE": "",
        "DEFAULT_SEVERITY": "",
        "DESCRIPTION": "",
        "EXPRESSION_TEMPLATE": "",
        "PARAM_SCHEMA": "[]",
        "ACTIVE": True,
    }

    if mode == "edit_existing":
        if selected_uid is None:
            st.info("No rule selected for editing.")
            return
        try:
            rule_df = session.sql(
                f"""
                SELECT
                    RULE_UID,
                    RULE_ID,
                    CHECK_TYPE,
                    DEFAULT_SEVERITY,
                    DESCRIPTION,
                    EXPRESSION_TEMPLATE,
                    PARAM_SCHEMA,
                    ACTIVE
                FROM {table_name}
                WHERE RULE_UID = :1
                """,
                params=[selected_uid],
            ).to_pandas()
        except Exception as exc:  # pragma: no cover - surface Snowflake errors to UI
            st.error(f"Unable to load rule: {exc}")
            return

        if rule_df.empty:
            st.error("Rule not found.")
            return

        record = rule_df.iloc[0].to_dict()
        rule_defaults.update(
            {
                "RULE_ID": record.get("RULE_ID", ""),
                "CHECK_TYPE": record.get("CHECK_TYPE", ""),
                "DEFAULT_SEVERITY": record.get("DEFAULT_SEVERITY", ""),
                "DESCRIPTION": record.get("DESCRIPTION", ""),
                "EXPRESSION_TEMPLATE": record.get("EXPRESSION_TEMPLATE", ""),
                "PARAM_SCHEMA": _normalize_param_schema(record.get("PARAM_SCHEMA")),
                "ACTIVE": bool(record.get("ACTIVE", True)),
            }
        )
        st.header(f"Editing rule: {rule_defaults['RULE_ID']}")
    else:
        st.header("Create new rule")

    with st.form(key="dq_rule_editor_form"):
        rule_id = st.text_input("Rule ID", value=rule_defaults["RULE_ID"])
        check_type = st.text_input("Check type", value=rule_defaults["CHECK_TYPE"])
        default_severity = st.text_input(
            "Default severity", value=rule_defaults["DEFAULT_SEVERITY"]
        )
        active = st.checkbox("Active", value=rule_defaults["ACTIVE"])
        description = st.text_area("Description", value=rule_defaults["DESCRIPTION"])
        expression_template = st.text_area(
            "Expression template", value=rule_defaults["EXPRESSION_TEMPLATE"], height=160
        )
        param_schema_text = st.text_area(
            "Parameter schema (JSON)", value=rule_defaults["PARAM_SCHEMA"], height=140
        )

        validate_clicked = st.form_submit_button(
            "Validate rule", type="secondary", use_container_width=False
        )
        save_clicked = st.form_submit_button("Save", type="primary")
        cancel_clicked = st.form_submit_button("Cancel", type="secondary")

    if cancel_clicked:
        st.session_state["dq_rules_mode"] = "list"
        st.session_state["dq_rules_selected_uid"] = None
        st.experimental_rerun()
        return

    def _run_validation() -> str:
        try:
            return _validate_expression_template(session, expression_template)
        except NameError:  # pragma: no cover - validator not available
            st.info("Validation is not configured.")
            return "VALIDATION_NOT_CONFIGURED"

    validation_status: Optional[str] = None
    if validate_clicked:
        validation_status = _run_validation()
        if validation_status != "VALIDATION_NOT_CONFIGURED":
            _display_validation_feedback(validation_status)

    if not save_clicked:
        return

    validation_status = validation_status or _run_validation()
    if validation_status != "VALIDATION_NOT_CONFIGURED" and validation_status != "OK":
        if active:
            _display_validation_feedback(validation_status)
            return
        st.warning("Rule saved as inactive due to validation issues.")

    normalized_param_schema = param_schema_text.strip() or "[]"

    try:
        if mode == "edit_existing":
            session.sql(
                f"""
                UPDATE {table_name}
                SET
                    RULE_ID = :1,
                    CHECK_TYPE = :2,
                    DEFAULT_SEVERITY = :3,
                    ACTIVE = :4,
                    DESCRIPTION = :5,
                    EXPRESSION_TEMPLATE = :6,
                    PARAM_SCHEMA = PARSE_JSON(:7),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RULE_UID = :8
                """,
                params=
                [
                    rule_id,
                    check_type,
                    default_severity,
                    active,
                    description,
                    expression_template,
                    normalized_param_schema,
                    selected_uid,
                ],
            ).collect()
        else:
            session.sql(
                f"""
                INSERT INTO {table_name} (
                    RULE_ID,
                    CHECK_TYPE,
                    DEFAULT_SEVERITY,
                    ACTIVE,
                    DESCRIPTION,
                    EXPRESSION_TEMPLATE,
                    PARAM_SCHEMA,
                    CREATED_AT,
                    UPDATED_AT
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, PARSE_JSON(:7), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """,
                params=
                [
                    rule_id,
                    check_type,
                    default_severity,
                    active,
                    description,
                    expression_template,
                    normalized_param_schema,
                ],
            ).collect()
    except Exception as exc:  # pragma: no cover - surfacing Snowflake errors to UI
        st.error(f"Unable to save rule: {exc}")
        return

    st.success("Rule saved.")
    st.session_state["dq_rules_mode"] = "list"
    st.session_state["dq_rules_selected_uid"] = None
    st.experimental_rerun()


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
