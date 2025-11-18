"""Streamlit view for administering the DQ rule library."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

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
    "DESCRIPTION",
    "ACTIVE",
    "UPDATED_AT",
]


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
    df = session.table(table).to_pandas()
    df.columns = [col.upper() for col in df.columns]
    if "UPDATED_AT" in df.columns:
        df["UPDATED_AT"] = pd.to_datetime(
            df["UPDATED_AT"], errors="coerce", utc=True
        ).dt.tz_convert(None)
    return df


def _run_insert(
    session: Session,
    table: str,
    *,
    rule_id: str,
    check_type: str,
    expression_template: str,
    param_schema_json: str,
    default_severity: Optional[str],
    description: Optional[str],
) -> None:
    sql = f"""
        INSERT INTO {table} (
            RULE_ID,
            CHECK_TYPE,
            EXPRESSION_TEMPLATE,
            PARAM_SCHEMA,
            DEFAULT_SEVERITY,
            DESCRIPTION,
            ACTIVE,
            CREATED_AT,
            UPDATED_AT
        )
        SELECT ?, ?, ?, PARSE_JSON(?), ?, ?, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    """
    params: List[Any] = [
        rule_id,
        check_type,
        expression_template,
        param_schema_json,
        default_severity,
        description,
    ]
    session.sql(sql, params=params).collect()


def _run_update(
    session: Session,
    table: str,
    *,
    rule_id: str,
    description: Optional[str],
    default_severity: Optional[str],
    active: bool,
) -> None:
    sql = f"""
        UPDATE {table}
        SET
            DESCRIPTION = ?,
            DEFAULT_SEVERITY = ?,
            ACTIVE = ?,
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE RULE_ID = ?
    """
    params: List[Any] = [description, default_severity, active, rule_id]
    session.sql(sql, params=params).collect()


def _severity_options(df: pd.DataFrame) -> List[str]:
    values = sorted(
        {str(val).strip() for val in df.get("DEFAULT_SEVERITY", []) if str(val).strip()}
    )
    if not values:
        values = ["LOW", "MEDIUM", "HIGH"]
    return values


def render_rule_admin(session: Optional[Session], metadata_db: str, metadata_schema: str) -> None:
    st.title("DQ Rule Library")

    if session is None:
        st.info("A Snowflake session is required to manage the rule library.")
        return

    table_name = _fq_rule_table(metadata_db, metadata_schema)
    st.caption(f"Source table: {table_name}")

    st.session_state.setdefault("rule_admin_create_mode", False)

    if st.button("Create rule", use_container_width=False, key="rule_admin_create_btn"):
        st.session_state["rule_admin_create_mode"] = True

    if st.session_state.get("rule_admin_create_mode"):
        _render_create_form(session, table_name)

    try:
        rules_df = _load_rules(session, table_name)
    except Exception as exc:
        st.error(f"Unable to load rule library: {exc}")
        return

    if rules_df.empty:
        st.info("No rules available. Use the create form above to add the first rule.")
        return

    display_df = rules_df.copy()
    missing_cols = [col for col in DISPLAY_COLUMNS if col not in display_df.columns]
    for col in missing_cols:
        display_df[col] = ""
    display_df = display_df[DISPLAY_COLUMNS]
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    severity_choices = _severity_options(rules_df)

    for rule in rules_df.to_dict("records"):
        rule_id = str(rule.get("RULE_ID"))
        header = f"{rule_id} – {rule.get('CHECK_TYPE') or ''}"
        with st.expander(header):
            st.caption("Expression template")
            st.code(rule.get("EXPRESSION_TEMPLATE") or "", language="sql")
            st.caption("Parameter schema")
            st.code(_normalize_param_schema(rule.get("PARAM_SCHEMA")), language="json")

            with st.form(f"edit_rule_{rule_id}"):
                description_value = st.text_area(
                    "Description",
                    value=rule.get("DESCRIPTION") or "",
                    help="Optional free-form notes about this rule.",
                )
                severity_value = st.selectbox(
                    "Default severity",
                    options=severity_choices,
                    index=_resolve_default_index(severity_choices, rule.get("DEFAULT_SEVERITY")),
                )
                active_value = st.checkbox(
                    "Active",
                    value=bool(rule.get("ACTIVE")),
                    help="Inactive rules will not appear in configuration suggestions.",
                )
                submit = st.form_submit_button("Save changes", use_container_width=False)
                if submit:
                    try:
                        _run_update(
                            session,
                            table_name,
                            rule_id=rule_id,
                            description=description_value.strip() or None,
                            default_severity=severity_value.strip() or None,
                            active=bool(active_value),
                        )
                        st.success(f"Rule {rule_id} updated.")
                        st.experimental_rerun()
                    except Exception as exc:
                        st.error(f"Unable to update rule {rule_id}: {exc}")


def _resolve_default_index(options: List[str], current_value: Optional[str]) -> int:
    if not current_value:
        return 0
    normalized = str(current_value).strip()
    try:
        return options.index(normalized)
    except ValueError:
        options.insert(0, normalized)
        return 0


def _render_create_form(session: Session, table_name: str) -> None:
    st.subheader("Create new rule")
    with st.form("rule_admin_create_form", clear_on_submit=False):
        rule_id = st.text_input("Rule ID", help="Unique identifier for the rule.")
        check_type = st.text_input("Check type", help="Logical grouping for the rule.")
        expression_template = st.text_area(
            "Expression template",
            help="SQL expression using placeholders such as {column_expr}.",
        )
        param_schema = st.text_area(
            "Parameter schema (JSON)",
            help="Provide a JSON array describing template parameters.",
        )
        default_severity = st.text_input("Default severity", value="MEDIUM")
        description = st.text_area("Description", help="Optional rationale.")
        col1, col2 = st.columns(2)
        save = col1.form_submit_button("Save rule")
        cancel = col2.form_submit_button("Cancel")
        if cancel:
            st.session_state["rule_admin_create_mode"] = False
        if save:
            errors = _validate_create_inputs(
                rule_id=rule_id,
                check_type=check_type,
                expression_template=expression_template,
                param_schema=param_schema,
            )
            if errors:
                for err in errors:
                    st.error(err)
            else:
                param_schema_json = _canonicalize_schema(param_schema)
                try:
                    _run_insert(
                        session,
                        table_name,
                        rule_id=rule_id.strip(),
                        check_type=check_type.strip(),
                        expression_template=expression_template.strip(),
                        param_schema_json=param_schema_json,
                        default_severity=default_severity.strip() or None,
                        description=description.strip() or None,
                    )
                    st.session_state["rule_admin_create_mode"] = False
                    st.success(f"Rule {rule_id} created.")
                    st.experimental_rerun()
                except Exception as exc:
                    st.error(f"Unable to create rule: {exc}")


def _canonicalize_schema(input_text: str) -> str:
    text = input_text.strip() if input_text else "[]"
    if not text:
        text = "[]"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError("Parameter schema must be valid JSON.")
    return json.dumps(parsed)


def _validate_create_inputs(
    *,
    rule_id: str,
    check_type: str,
    expression_template: str,
    param_schema: str,
) -> List[str]:
    errors: List[str] = []
    if not rule_id.strip():
        errors.append("Rule ID is required.")
    if not check_type.strip():
        errors.append("Check type is required.")
    if not expression_template.strip():
        errors.append("Expression template is required.")
    try:
        _canonicalize_schema(param_schema)
    except ValueError as exc:
        errors.append(str(exc))
    return errors
