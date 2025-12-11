"""Streamlit view for administering the DQ rule library."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

if TYPE_CHECKING:  # pragma: no cover - import for type checking only
    from snowflake.snowpark import Session  # type: ignore
else:  # pragma: no cover - runtime fallback to avoid hard dependency
    Session = Any  # type: ignore

from utils.meta import _q
from services.dq_dsl_compiler import compile_expression


def _fq_rule_table(database: str, schema: str) -> str:
    return f"{_q(database)}.{_q(schema)}.{_q('DQ_RULE_LIBRARY')}"


def _fq_test_table(database: str, schema: str) -> str:
    return f"{_q(database)}.{_q(schema)}.{_q('DQ_RULE_TEST_TARGET')}"


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


def _parse_applicability_tags(raw_text: str) -> List[str]:
    text = (raw_text or "").strip()
    if not text:
        return []
    if text.lstrip().startswith("["):
        parsed = _parse_json_text(text, expected_type="array")
        if not all(isinstance(item, (str, int, float)) for item in parsed):
            raise ValueError("Applicability tags must be an array of strings.")
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [tag.strip() for tag in text.split(",") if tag.strip()]


def _normalize_param_schema(raw_schema: Any) -> List[Dict[str, Any]]:
    if raw_schema is None:
        return []
    normalized: List[Dict[str, Any]] = []
    if isinstance(raw_schema, list):
        for item in raw_schema:
            if isinstance(item, str):
                normalized.append({"name": item, "type": "STRING", "required": True})
            elif isinstance(item, dict):
                name = item.get("name")
                if not name:
                    raise ValueError("PARAM_SCHEMA entries must include a name")
                normalized.append(
                    {
                        "name": name,
                        "type": item.get("type", "STRING"),
                        "required": bool(item.get("required", True)),
                    }
                )
            else:
                raise ValueError("Unsupported PARAM_SCHEMA entry")
    return normalized


def _validate_param_value(value: Any, type_name: str, name: str) -> None:
    type_upper = (type_name or "STRING").upper()
    if type_upper == "STRING":
        if not isinstance(value, str):
            raise ValueError(f"Parameter '{name}' must be STRING")
    elif type_upper == "NUMBER":
        if not isinstance(value, (int, float)):
            raise ValueError(f"Parameter '{name}' must be NUMBER")
    elif type_upper == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError(f"Parameter '{name}' must be BOOLEAN")
    elif type_upper in {"FQN_TABLE", "COLUMN_NAME"}:
        if not isinstance(value, str):
            raise ValueError(f"Parameter '{name}' must be {type_upper}")
    elif type_upper == "STRING_LIST":
        if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
            raise ValueError(f"Parameter '{name}' must be STRING_LIST")
    else:
        raise ValueError(f"Unsupported parameter type {type_name} for '{name}'")


def _load_rules(session: Session, table: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            RULE_UID,
            RULE_CODE,
            RULE_ID,
            SCOPE,
            CATEGORY,
            SEVERITY,
            ENGINE_TYPE,
            EXPRESSION,
            PARAM_SCHEMA,
            DEFAULT_PARAMS,
            DATA_TYPE_FAMILY,
            APPLICABILITY_TAGS,
            DEFAULT_SUGGEST,
            SUGGESTION_PRIORITY,
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
        "RULE_ID": "",
        "CATEGORY": "",
        "SEVERITY": "",
        "SCOPE": "",
        "ENGINE_TYPE": "DSL",
        "EXPRESSION": "",
        "PARAM_SCHEMA": "[]",
        "DEFAULT_PARAMS": "{}",
        "DATA_TYPE_FAMILY": "ANY",
        "APPLICABILITY_TAGS": "[]",
        "DEFAULT_SUGGEST": True,
        "SUGGESTION_PRIORITY": 50,
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
                RULE_ID,
                SCOPE,
                CATEGORY,
                SEVERITY,
                ENGINE_TYPE,
                EXPRESSION,
                PARAM_SCHEMA,
                DEFAULT_PARAMS,
                DATA_TYPE_FAMILY,
                APPLICABILITY_TAGS,
                DEFAULT_SUGGEST,
                SUGGESTION_PRIORITY,
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
            filtered["RULE_ID"].astype(str).str.lower().str.contains(needle)
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
                "RULE_ID": record.get("RULE_ID", ""),
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
                "DATA_TYPE_FAMILY": record.get("DATA_TYPE_FAMILY", "ANY") or "ANY",
                "APPLICABILITY_TAGS": _normalize_json_field(
                    record.get("APPLICABILITY_TAGS"), empty_default="[]"
                ),
                "DEFAULT_SUGGEST": bool(record.get("DEFAULT_SUGGEST", True)),
                "SUGGESTION_PRIORITY": record.get("SUGGESTION_PRIORITY", 50) or 50,
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
        rule_id = st.text_input(
            "Rule ID",
            value=rule_defaults["RULE_ID"],
            help="Human-friendly rule identifier shown in listings.",
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

        col_data_type, col_default_suggest = st.columns(2)
        with col_data_type:
            data_type_family = st.selectbox(
                "Data type family",
                options=["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"],
                index=max(
                    0,
                    ["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"].index(
                        (rule_defaults.get("DATA_TYPE_FAMILY") or "ANY").upper()
                    )
                    if (rule_defaults.get("DATA_TYPE_FAMILY") or "ANY").upper()
                    in ["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"]
                    else 0,
                ),
                help="Column type family this rule targets for suggestions.",
            )
        with col_default_suggest:
            default_suggest = st.checkbox(
                "Use this rule in automatic suggestions",
                value=bool(rule_defaults.get("DEFAULT_SUGGEST", True)),
                help="Disable to exclude this rule from default suggestion generation.",
            )

        applicability_tags_text = st.text_input(
            "Applicability tags (JSON array or comma-separated)",
            value=rule_defaults["APPLICABILITY_TAGS"],
            help="Tags like ID, COUNTRY_CODE, CURRENCY_CODE. Stored as JSON array.",
        )

        suggestion_priority_val = st.number_input(
            "Suggestion priority (1-100)",
            min_value=1,
            max_value=100,
            value=int(rule_defaults.get("SUGGESTION_PRIORITY", 50) or 50),
            help="Higher values are suggested first.",
        )

        expression = st.text_area(
            "Expression (DSL)",
            value=rule_defaults["EXPRESSION"],
            height=200,
            help="Provide the DSL expression for this rule (e.g. ASSERT ...).",
        )
        with st.expander("DSL reference (quick guide)"):
            st.markdown(
                """
                **DQ DSL v1 – Cheat Sheet**

                * Basic rule shape: `ASSERT <predicate>`
                * Operators: `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, comparison operators (`=`, `!=`, `<`, `<=`, `>`, `>=`)
                * Null checks: `IS NULL`, `IS NOT NULL`
                * String helpers: `LEN(x)`, `LOWER(x)`, `UPPER(x)`, `CONTAINS(x, substring)`, `LIKE(pattern)`
                * Number helpers: `ABS(x)`, `ROUND(x, decimals)`, `BETWEEN low AND high`
                * Column/param references: use column names directly (quoted if needed) and parameters as `${param_name}`
                * Implication: `A -> B` expands to `NOT (A) OR (B)`
                * Lists: `IN (${list_param})` where `list_param` is `STRING_LIST`
                * Table/column params: `FQN_TABLE` for fully-qualified tables, `COLUMN_NAME` for single column names
                * Examples:
                    * `ASSERT NOT (price IS NULL) AND price > 0`
                    * `ASSERT amount BETWEEN ${min_amt} AND ${max_amt}`
                    * `ASSERT LOWER(email) LIKE '%@example.com'`
                """
            )

        param_schema_text = st.text_area(
            "Parameter schema (JSON array)",
            value=rule_defaults["PARAM_SCHEMA"],
            height=140,
        )
        st.caption(
            """
            Define parameters as a JSON array of objects. Each definition supports
            `name`, `type` (STRING, NUMBER, BOOLEAN, FQN_TABLE, COLUMN_NAME, STRING_LIST),
            `required` (defaults to true), and `default` (used when not provided).
            Example: `[{"name": "min_amt", "type": "NUMBER", "required": true, "default": 0}]`.
            """
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

        action_col1, action_col2, action_col3 = st.columns(3)
        with action_col1:
            save_clicked = st.form_submit_button("Save", type="primary")
        with action_col2:
            cancel_clicked = st.form_submit_button("Cancel", type="secondary")
        with action_col3:
            test_compile_clicked = st.form_submit_button("Test Compile")

    if cancel_clicked:
        _reset_rule_state()
        st.info("Edit cancelled")
        st.rerun()

    action: Optional[str]
    if save_clicked:
        action = "save"
    elif test_compile_clicked:
        action = "test"
    else:
        action = None

    if action is None:
        return

    errors: List[str] = []
    rule_code_val = (rule_code or "").strip()
    rule_id_val = (rule_id or "").strip()
    category_val = (category or "").strip()
    severity_val = (severity or "").strip()
    scope_val = (scope_value or "").strip()
    engine_val = (engine_type or "").strip()
    expression_val = (expression or "").strip()

    if not rule_code_val:
        errors.append("Rule code is required.")
    if not rule_id_val:
        errors.append("Rule ID is required.")
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

    try:
        parsed_applicability_tags = _parse_applicability_tags(applicability_tags_text)
    except ValueError as exc:
        errors.append(f"Applicability tags: {exc}")
        parsed_applicability_tags = []

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

    param_schema_value = json.dumps(parsed_param_schema)
    default_params_value = json.dumps(parsed_default_params)
    applicability_tags_value = json.dumps(parsed_applicability_tags)
    suggestion_priority_value = int(suggestion_priority_val)

    if action == "test":
        _run_test_compile(
            metadata_db=metadata_db,
            metadata_schema=metadata_schema,
            expression=expression_val,
            scope=scope_val,
            param_schema=parsed_param_schema,
            rule_code=rule_code_val,
            default_params=parsed_default_params,
        )
        return

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
                    RULE_ID = :1,
                    CATEGORY = :2,
                    SEVERITY = :3,
                    SCOPE = :4,
                    ENGINE_TYPE = :5,
                    EXPRESSION = :6,
                    PARAM_SCHEMA = PARSE_JSON(:7),
                    DEFAULT_PARAMS = PARSE_JSON(:8),
                    DATA_TYPE_FAMILY = :9,
                    APPLICABILITY_TAGS = PARSE_JSON(:10),
                    DEFAULT_SUGGEST = :11,
                    SUGGESTION_PRIORITY = :12,
                    ENABLED = :13,
                    VERSION = :14,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RULE_UID = :15
                """,
                params=[
                    rule_id_val,
                    category_val or None,
                    severity_val or None,
                    scope_val,
                    engine_val,
                    expression_val,
                    param_schema_value,
                    default_params_value,
                    data_type_family,
                    applicability_tags_value,
                    default_suggest,
                    suggestion_priority_value,
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
                    RULE_ID,
                    CATEGORY,
                    SEVERITY,
                    SCOPE,
                    ENGINE_TYPE,
                    EXPRESSION,
                    PARAM_SCHEMA,
                    DEFAULT_PARAMS,
                    DATA_TYPE_FAMILY,
                    APPLICABILITY_TAGS,
                    DEFAULT_SUGGEST,
                    SUGGESTION_PRIORITY,
                    ENABLED,
                    VERSION,
                    CREATED_AT,
                    UPDATED_AT
                )
                SELECT
                    :1,
                    :2,
                    :3,
                    :4,
                    :5,
                    :6,
                    :7,
                    PARSE_JSON(:8),
                    PARSE_JSON(:9),
                    :10,
                    PARSE_JSON(:11),
                    :12,
                    :13,
                    :14,
                    :15,
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP()
                """,
                params=[
                    rule_code_val,
                    rule_id_val,
                    category_val or None,
                    severity_val or None,
                    scope_val,
                    engine_val,
                    expression_val,
                    param_schema_value,
                    default_params_value,
                    data_type_family,
                    applicability_tags_value,
                    default_suggest,
                    suggestion_priority_value,
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


def _run_test_compile(
    *,
    expression: str,
    metadata_db: str,
    metadata_schema: str,
    scope: str,
    param_schema: List[Dict[str, Any]],
    rule_code: str,
    default_params: Dict[str, Any],
) -> None:
    target_table = _fq_test_table(metadata_db, metadata_schema)
    target_columns = ["DUMMY_COL"] if (scope or "").upper() == "COLUMN" else []
    params = default_params or {}

    try:
        normalized_schema = _normalize_param_schema(param_schema)
        param_types: Dict[str, str] = {}
        for param_def in normalized_schema:
            name = param_def.get("name")
            type_name = param_def.get("type", "STRING")
            param_types[name] = type_name
            if param_def.get("required", True) and name not in params:
                raise ValueError(f"Missing required parameter '{name}'")
            if name in params:
                _validate_param_value(params[name], type_name, name)

        target_column = target_columns[0] if target_columns else "DUMMY_COL"
        compiled_predicate = compile_expression(
            expression, target_column, params, param_types
        )
    except Exception as exc:  # pragma: no cover - surface errors to UI
        st.error(f"Test compile failed: {exc}")
        return

    violation_query = (
        f"SELECT * FROM {target_table} AS T WHERE NOT ({compiled_predicate})"
    )

    st.success("✅ Rule compiled successfully.")
    st.json(
        {
            "rule_code": rule_code,
            "compiled_predicate": compiled_predicate,
            "violation_query": violation_query,
            "params": params,
            "scope": scope,
        }
    )


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

    st.caption("Use the toggles below to enable/disable a rule or open it for editing.")

    for rule in filtered_df.to_dict("records"):
        rule_uid = rule.get("RULE_UID")
        toggle_key = f"rule_enabled_{rule_uid}"
        rule_name = rule.get("RULE_ID", "")
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
