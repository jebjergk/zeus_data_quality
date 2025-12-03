"""Streamlit view for administering the DQ rule library."""

from __future__ import annotations

import json
from typing import Any, List, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from snowflake.snowpark import Session  # type: ignore
else:  # pragma: no cover - runtime fallback to avoid hard dependency
    Session = Any  # type: ignore

from utils.meta import _q


def _fq_rule_table(database: str, schema: str) -> str:
    return f"{_q(database)}.{_q(schema)}.{_q('DQ_RULE_LIBRARY')}"


def _normalize_json_field(value: Any, *, empty_default: str) -> str:
    if value is None or value == "":
        return empty_default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return empty_default
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


def _parse_json_text(raw_text: str, *, expected_type: str) -> Any:
    text = (raw_text or "").strip()
    if not text:
        return [] if expected_type == "array" else {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    if expected_type == "array" and not isinstance(parsed, list):
        raise ValueError("Value must be a JSON array (e.g. [] or [...]).")
    if expected_type == "object" and not isinstance(parsed, dict):
        raise ValueError("Value must be a JSON object (e.g. {} or {\"key\": ...}).")
    return parsed


def _load_rules(session: Session, table: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            RULE_UID,
            RULE_CODE,
            NAME,
            SCOPE,
            CATEGORY,
            SEVERITY,
            ENGINE_TYPE,
            EXPRESSION,
            PARAM_SCHEMA,
            DEFAULT_PARAMS,
            ENABLED,
            VERSION,
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


def _load_scope_options(session: Session, table_name: str) -> List[str]:
    try:
        scope_df = session.sql(
            f"SELECT DISTINCT SCOPE FROM {table_name} WHERE SCOPE IS NOT NULL"
        ).to_pandas()
    except Exception:  # pragma: no cover - surfaced elsewhere
        return []
    return sorted({str(val).strip() for val in scope_df.get("SCOPE", []) if str(val).strip()})


def _load_severity_options(session: Session, table_name: str) -> List[str]:
    try:
        severity_df = session.sql(
            f"SELECT DISTINCT SEVERITY FROM {table_name} WHERE SEVERITY IS NOT NULL"
        ).to_pandas()
    except Exception:  # pragma: no cover - surfaced elsewhere
        severity_df = pd.DataFrame()
    return _severity_options(severity_df)


def _severity_options(df: pd.DataFrame) -> List[str]:
    values = sorted(
        {str(val).strip() for val in df.get("SEVERITY", []) if str(val).strip()}
    )
    default_values = ["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"]
    return values or default_values


def _rule_defaults() -> dict[str, Any]:
    return {
        "RULE_CODE": "",
        "NAME": "",
        "CATEGORY": "",
        "SEVERITY": "",
        "SCOPE": "",
        "ENGINE_TYPE": "DSL",
        "EXPRESSION": "",
        "PARAM_SCHEMA": "[]",
        "DEFAULT_PARAMS": "{}",
        "ENABLED": True,
        "VERSION": "",
    }


def _reset_rule_state() -> None:
    st.session_state["dq_rules_mode"] = "list"
    st.session_state["dq_rules_selected_uid"] = None


def _load_single_rule(session: Session, table_name: str, rule_uid: Any) -> Optional[dict]:
    try:
        rule_df = session.sql(
            f"""
            SELECT
                RULE_UID,
                RULE_CODE,
                NAME,
                SCOPE,
                CATEGORY,
                SEVERITY,
                ENGINE_TYPE,
                EXPRESSION,
                PARAM_SCHEMA,
                DEFAULT_PARAMS,
                ENABLED,
                VERSION
            FROM {table_name}
            WHERE RULE_UID = :1
            """,
            params=[rule_uid],
        ).to_pandas()
    except Exception as exc:  # pragma: no cover - surface Snowflake errors to UI
        st.error(f"Unable to load rule: {exc}")
        return None

    if rule_df.empty:
        st.error("Rule not found.")
        return None

    return rule_df.iloc[0].to_dict()


def _toggle_enabled(session: Session, table_name: str, rule_uid: Any, state_key: str) -> None:
    new_value = bool(st.session_state.get(state_key, False))
    try:
        session.sql(
            f"""
            UPDATE {table_name}
            SET ENABLED = :1, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RULE_UID = :2
            """,
            params=[new_value, rule_uid],
        ).collect()
        st.success("Rule updated.")
    except Exception as exc:  # pragma: no cover - surface Snowflake errors
        st.error(f"Unable to update rule: {exc}")


def _apply_filters(
    df: pd.DataFrame,
    *,
    search_text: str = "",
    scope_filter: str = "All",
    category_filter: str = "All",
) -> pd.DataFrame:
    filtered = df.copy()
    if search_text:
        needle = search_text.lower()
        mask = (
            filtered["NAME"].astype(str).str.lower().str.contains(needle)
            | filtered["RULE_CODE"].astype(str).str.lower().str.contains(needle)
            | filtered["SCOPE"].fillna("").astype(str).str.lower().str.contains(needle)
            | filtered["CATEGORY"].fillna("").astype(str).str.lower().str.contains(needle)
        )
        filtered = filtered[mask]
    if scope_filter and scope_filter != "All":
        filtered = filtered[filtered["SCOPE"].astype(str) == scope_filter]
    if category_filter and category_filter != "All":
        filtered = filtered[filtered["CATEGORY"].astype(str) == category_filter]
    return filtered


def _render_rule_edit_page(session: Session, metadata_db: str, metadata_schema: str) -> None:
    mode = st.session_state.get("dq_rules_mode", "list")
    selected_uid = st.session_state.get("dq_rules_selected_uid")

    if mode not in {"edit_existing", "create_new"}:
        st.info("Rule editor is available only in edit or create mode.")
        return

    table_name = _fq_rule_table(metadata_db, metadata_schema)

    back_col, _ = st.columns([1, 3])
    with back_col:
        if st.button("Back to rule list", key="rule_edit_back_to_list"):
            _reset_rule_state()
            st.rerun()

    rule_defaults = _rule_defaults()

    if mode == "edit_existing":
        if selected_uid is None:
            st.info("No rule selected for editing.")
            return
        record = _load_single_rule(session, table_name, selected_uid)
        if not record:
            return
        rule_defaults.update(
            {
                "RULE_CODE": record.get("RULE_CODE", ""),
                "NAME": record.get("NAME", ""),
                "CATEGORY": record.get("CATEGORY", ""),
                "SEVERITY": record.get("SEVERITY", ""),
                "SCOPE": record.get("SCOPE", ""),
                "ENGINE_TYPE": record.get("ENGINE_TYPE", "DSL"),
                "EXPRESSION": record.get("EXPRESSION", ""),
                "PARAM_SCHEMA": _normalize_json_field(
                    record.get("PARAM_SCHEMA"), empty_default="[]"
                ),
                "DEFAULT_PARAMS": _normalize_json_field(
                    record.get("DEFAULT_PARAMS"), empty_default="{}"
                ),
                "ENABLED": bool(record.get("ENABLED", True)),
                "VERSION": record.get("VERSION", ""),
            }
        )
        header_rule_code = rule_defaults.get("RULE_CODE", "").strip()
        st.header(f"Editing rule: {header_rule_code}")
    else:
        st.header("Create new data quality rule")

    scope_options = _load_scope_options(session, table_name)
    severity_options = _load_severity_options(session, table_name)

    scope_choices = scope_options.copy()
    if rule_defaults.get("SCOPE") and rule_defaults["SCOPE"] not in scope_choices:
        scope_choices.append(rule_defaults["SCOPE"])
    if not scope_choices:
        scope_choices = ["COLUMN", "TABLE", "DATASET"]

    severity_default = rule_defaults.get("SEVERITY") or severity_options[0]
    scope_default_index = (
        scope_choices.index(rule_defaults.get("SCOPE"))
        if rule_defaults.get("SCOPE") in scope_choices
        else 0
    )

    with st.form("dq_rule_form"):
        rule_code = st.text_input(
            "Rule code",
            value=rule_defaults["RULE_CODE"],
            help="Unique identifier for the rule template.",
            disabled=mode == "edit_existing",
        )
        name = st.text_input(
            "Display name",
            value=rule_defaults["NAME"],
            help="Human-friendly rule name shown in listings.",
        )

        col_category, col_severity = st.columns(2)
        with col_category:
            category = st.text_input("Category", value=rule_defaults["CATEGORY"])
        with col_severity:
            severity = st.selectbox(
                "Severity",
                options=severity_options,
                index=severity_options.index(severity_default)
                if severity_default in severity_options
                else 0,
            )

        col_scope, col_engine = st.columns(2)
        with col_scope:
            scope_value = st.selectbox(
                "Scope",
                options=scope_choices,
                index=scope_default_index,
                disabled=mode == "edit_existing",  # Avoid changing scope for existing rules.
            )
        with col_engine:
            engine_type = st.text_input(
                "Engine type",
                value=rule_defaults["ENGINE_TYPE"],
                disabled=True,
            )

        expression = st.text_area(
            "Expression (DSL)",
            value=rule_defaults["EXPRESSION"],
            height=200,
            help="Provide the DSL expression for this rule (e.g. ASSERT ...).",
        )
        param_schema_text = st.text_area(
            "Parameter schema (JSON array)",
            value=rule_defaults["PARAM_SCHEMA"],
            height=140,
        )
        default_params_text = st.text_area(
            "Default parameters (JSON object)",
            value=rule_defaults["DEFAULT_PARAMS"],
            height=140,
        )
        enabled = st.checkbox("Enabled", value=rule_defaults["ENABLED"])
        version_text = st.text_input(
            "Version",
            value=str(rule_defaults.get("VERSION") or ""),
            help="Optional version number for the rule template.",
        )

        action_col1, action_col2 = st.columns(2)
        with action_col1:
            save_clicked = st.form_submit_button("Save", type="primary")
        with action_col2:
            cancel_clicked = st.form_submit_button("Cancel", type="secondary")

    if cancel_clicked:
        _reset_rule_state()
        st.info("Edit cancelled")
        st.rerun()

    if not save_clicked:
        return

    errors: List[str] = []
    rule_code_val = (rule_code or "").strip()
    name_val = (name or "").strip()
    category_val = (category or "").strip()
    severity_val = (severity or "").strip()
    scope_val = (scope_value or "").strip()
    engine_val = (engine_type or "").strip()
    expression_val = (expression or "").strip()

    if not rule_code_val:
        errors.append("Rule code is required.")
    if not name_val:
        errors.append("Display name is required.")
    if engine_val.upper() == "DSL" and not expression_val:
        errors.append("Expression is required for DSL rules.")
    if mode == "create_new" and not scope_val:
        errors.append("Scope is required for new rules.")

    try:
        parsed_param_schema = _parse_json_text(param_schema_text, expected_type="array")
    except ValueError as exc:
        errors.append(f"Parameter schema: {exc}")
        parsed_param_schema = []
    try:
        parsed_default_params = _parse_json_text(default_params_text, expected_type="object")
    except ValueError as exc:
        errors.append(f"Default parameters: {exc}")
        parsed_default_params = {}

    version_val: Optional[int]
    version_text_clean = (version_text or "").strip()
    if version_text_clean:
        try:
            version_val = int(version_text_clean)
        except ValueError:
            errors.append("Version must be a whole number if provided.")
            version_val = None
    else:
        version_val = None

    if errors:
        st.error("\n".join(errors))
        return

    param_schema_json = json.dumps(parsed_param_schema, default=str)
    default_params_json = json.dumps(parsed_default_params, default=str)

    if mode == "create_new":
        try:
            dup_check = session.sql(
                f"""
                SELECT 1 FROM {table_name}
                WHERE UPPER(RULE_CODE) = UPPER(:1)
                FETCH FIRST 1 ROW ONLY
                """,
                params=[rule_code_val],
            ).to_pandas()
        except Exception as exc:  # pragma: no cover - surface Snowflake errors to UI
            st.error(f"Unable to validate rule code: {exc}")
            return
        if not dup_check.empty:
            st.error("Rule code must be unique.")
            return

    try:
        if mode == "edit_existing":
            session.sql(
                f"""
                UPDATE {table_name}
                SET
                    NAME = :1,
                    CATEGORY = :2,
                    SEVERITY = :3,
                    SCOPE = :4,
                    ENGINE_TYPE = :5,
                    EXPRESSION = :6,
                    PARAM_SCHEMA = PARSE_JSON(:7),
                    DEFAULT_PARAMS = PARSE_JSON(:8),
                    ENABLED = :9,
                    VERSION = :10,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RULE_UID = :11
                """,
                params=[
                    name_val,
                    category_val or None,
                    severity_val or None,
                    scope_val,
                    engine_val,
                    expression_val,
                    param_schema_json,
                    default_params_json,
                    enabled,
                    version_val,
                    selected_uid,
                ],
            ).collect()
        else:
            session.sql(
                f"""
                INSERT INTO {table_name} (
                    RULE_CODE,
                    NAME,
                    CATEGORY,
                    SEVERITY,
                    SCOPE,
                    ENGINE_TYPE,
                    EXPRESSION,
                    PARAM_SCHEMA,
                    DEFAULT_PARAMS,
                    ENABLED,
                    VERSION,
                    CREATED_AT,
                    UPDATED_AT
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, PARSE_JSON(:8), PARSE_JSON(:9), :10, :11, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
                """,
                params=[
                    rule_code_val,
                    name_val,
                    category_val or None,
                    severity_val or None,
                    scope_val,
                    engine_val,
                    expression_val,
                    param_schema_json,
                    default_params_json,
                    enabled,
                    version_val,
                ],
            ).collect()
    except Exception as exc:  # pragma: no cover - surfacing Snowflake errors to UI
        st.error(f"Unable to save rule: {exc}")
        return

    _reset_rule_state()
    st.success("Rule saved")
    st.rerun()


def _render_rule_list(
    *, session: Session, table_name: str, rules_df: pd.DataFrame
) -> None:
    st.subheader("Rule list")

    st.session_state.setdefault("dq_rules_search", "")
    st.session_state.setdefault("dq_rules_scope_filter", "All")
    st.session_state.setdefault("dq_rules_category_filter", "All")

    if st.button("Create new rule", key="create_new_rule"):
        st.session_state["dq_rules_mode"] = "create_new"
        st.session_state["dq_rules_selected_uid"] = None
        st.rerun()

    search = st.text_input(
        "Search rules",
        value=st.session_state["dq_rules_search"],
        key="dq_rules_search",
        help="Search by name, rule code, scope, or category.",
    )

    distinct_scopes = sorted(
        {str(val) for val in rules_df.get("SCOPE", []) if pd.notna(val) and str(val).strip()}
    )
    scope_options = ["All"] + distinct_scopes
    scope_filter = st.selectbox(
        "Filter by scope",
        options=scope_options,
        index=scope_options.index(st.session_state.get("dq_rules_scope_filter", "All"))
        if st.session_state.get("dq_rules_scope_filter", "All") in scope_options
        else 0,
        key="dq_rules_scope_filter",
    )

    distinct_categories = sorted(
        {str(val) for val in rules_df.get("CATEGORY", []) if pd.notna(val) and str(val).strip()}
    )
    category_options = ["All"] + distinct_categories
    category_filter = st.selectbox(
        "Filter by category",
        options=category_options,
        index=category_options.index(
            st.session_state.get("dq_rules_category_filter", "All")
        )
        if st.session_state.get("dq_rules_category_filter", "All") in category_options
        else 0,
        key="dq_rules_category_filter",
    )

    filtered_df = _apply_filters(
        rules_df,
        search_text=search,
        scope_filter=scope_filter,
        category_filter=category_filter,
    )

    if filtered_df.empty:
        st.info("No rules match the current search or filter. Adjust filters or create a new rule.")
        return

    display_df = filtered_df[
        [
            "NAME",
            "RULE_CODE",
            "SCOPE",
            "CATEGORY",
            "SEVERITY",
            "ENGINE_TYPE",
            "ENABLED",
            "VERSION",
        ]
    ].rename(
        columns={
            "NAME": "Display name",
            "RULE_CODE": "Rule code",
            "SCOPE": "Scope",
            "CATEGORY": "Category",
            "SEVERITY": "Severity",
            "ENGINE_TYPE": "Engine",
            "ENABLED": "Enabled",
            "VERSION": "Version",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.caption("Use the toggles below to enable/disable a rule or open it for editing.")

    for rule in filtered_df.to_dict("records"):
        rule_uid = rule.get("RULE_UID")
        toggle_key = f"rule_enabled_{rule_uid}"
        rule_name = rule.get("NAME", "")
        rule_code = rule.get("RULE_CODE", "")
        rule_scope = rule.get("SCOPE", "")
        rule_category = rule.get("CATEGORY", "")
        rule_severity = rule.get("SEVERITY", "")
        rule_engine = rule.get("ENGINE_TYPE", "")
        is_enabled = bool(rule.get("ENABLED"))
        rule_version = rule.get("VERSION", "")

        with st.container():
            (
                col_name,
                col_code,
                col_scope,
                col_category,
                col_severity,
                col_engine,
                col_version,
                col_enabled,
                col_edit,
            ) = st.columns([3, 2, 2, 2, 2, 2, 1, 1, 1])

            with col_name:
                st.markdown(f"**{rule_name or '—'}**")
            with col_code:
                st.markdown(f"`{rule_code}`")
            with col_scope:
                st.markdown(f"Scope: {rule_scope or '—'}")
            with col_category:
                st.markdown(f"Category: {rule_category or '—'}")
            with col_severity:
                st.markdown(f"Severity: {rule_severity or '—'}")
            with col_engine:
                st.markdown(f"Engine: {rule_engine or '—'}")
            with col_version:
                st.markdown(f"Version: {rule_version or '—'}")
            with col_enabled:
                st.checkbox(
                    "Enabled",
                    value=is_enabled,
                    key=toggle_key,
                    on_change=_toggle_enabled,
                    kwargs={
                        "session": session,
                        "table_name": table_name,
                        "rule_uid": rule_uid,
                        "state_key": toggle_key,
                    },
                )
            with col_edit:
                if st.button("Edit", key=f"edit_rule_{rule_uid}"):
                    st.session_state["dq_rules_mode"] = "edit_existing"
                    st.session_state["dq_rules_selected_uid"] = rule_uid
                    st.rerun()

            st.markdown("---")


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

    if "dq_rules_mode" not in st.session_state:
        st.session_state["dq_rules_mode"] = "list"

    mode = st.session_state.get("dq_rules_mode", "list")

    if mode in {"edit_existing", "create_new"}:
        _render_rule_edit_page(session, metadata_db, metadata_schema)
        return

    if mode != "list":
        st.session_state["dq_rules_mode"] = "list"
        st.session_state["dq_rules_selected_uid"] = None
        mode = "list"

    try:
        rules_df = _load_rules(session, table_name)
    except Exception as exc:
        st.error(f"Unable to load rule library: {exc}")
        return

    if rules_df.empty:
        st.info("No rules found in the library. Create the first rule to get started.")
        if st.button("Create first rule", key="create_first_rule"):
            st.session_state["dq_rules_mode"] = "create_new"
            st.session_state["dq_rules_selected_uid"] = None
            st.stop()
        return

    _render_rule_list(session=session, table_name=table_name, rules_df=rules_df)
