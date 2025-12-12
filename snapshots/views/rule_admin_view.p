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


def _fq_tag_table(database: str, schema: str) -> str:
    return f"{_q(database)}.{_q(schema)}.{_q('DQ_TAG_DIM')}"


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


def _coerce_tag_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            return _coerce_tag_list(parsed)
        except json.JSONDecodeError:
            return [tag.strip() for tag in text.split(",") if tag.strip()]
    return []


CATEGORY_CHOICES: List[Dict[str, str]] = [
    {"code": "COMPLETENESS", "label": "Completeness"},
    {"code": "VALIDITY", "label": "Validity"},
    {"code": "ACCURACY", "label": "Accuracy"},
    {"code": "CONSISTENCY", "label": "Consistency"},
    {"code": "UNIQUENESS", "label": "Uniqueness"},
    {"code": "TIMELINESS", "label": "Timeliness"},
    {"code": "REFERENTIAL_INTEGRITY", "label": "Referential Integrity"},
    {"code": "ANOMALY", "label": "Anomaly"},
]


RULE_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "NOT_NULL": {
        "name": "Not Null",
        "defaults": {
            "CATEGORY_CODE": "COMPLETENESS",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 100,
            "APPLICABILITY_TAGS": [],
        },
        "param_schema": [],
        "dsl_expression": ":col IS NOT NULL",
    },
    "RANGE_BETWEEN": {
        "name": "Range Between",
        "defaults": {
            "CATEGORY_CODE": "VALIDITY",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 80,
            "APPLICABILITY_TAGS": ["AMOUNT"],
        },
        "param_schema": [
            {"name": "min_value", "type": "NUMBER", "required": True},
            {"name": "max_value", "type": "NUMBER", "required": True},
        ],
        "dsl_expression": ":col BETWEEN :min_value AND :max_value",
    },
    "REGEX_PATTERN": {
        "name": "Regex Pattern",
        "defaults": {
            "CATEGORY_CODE": "VALIDITY",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 85,
            "APPLICABILITY_TAGS": ["COUNTRY_CODE"],
        },
        "param_schema": [
            {"name": "pattern", "type": "STRING", "required": True},
        ],
        "dsl_expression": "REGEXP_LIKE(:col, :pattern)",
    },
    "IN_LIST": {
        "name": "In List",
        "defaults": {
            "CATEGORY_CODE": "VALIDITY",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 75,
            "APPLICABILITY_TAGS": [],
        },
        "param_schema": [
            {"name": "allowed_values", "type": "STRING_LIST", "required": True},
        ],
        "dsl_expression": ":col IN (:allowed_values)",
    },
    "LOOKUP_EXISTS_SINGLE_KEY": {
        "name": "Lookup Exists (single key)",
        "defaults": {
            "CATEGORY_CODE": "REFERENTIAL_INTEGRITY",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 90,
            "APPLICABILITY_TAGS": [],
        },
        "param_schema": [
            {"name": "ref_table", "type": "FQN_TABLE", "required": True},
            {"name": "ref_value_col", "type": "COLUMN_NAME", "required": True},
        ],
        "dsl_expression": "EXISTS (SELECT 1 FROM :ref_table R WHERE R.:ref_value_col = :col)",
    },
    "LOOKUP_EXISTS_TWO_KEYS": {
        "name": "Lookup Exists (two keys)",
        "defaults": {
            "CATEGORY_CODE": "REFERENTIAL_INTEGRITY",
            "DEFAULT_SUGGEST": True,
            "SUGGESTION_PRIORITY": 92,
            "APPLICABILITY_TAGS": [],
        },
        "param_schema": [
            {"name": "ref_table", "type": "FQN_TABLE", "required": True},
            {"name": "ref_name_col", "type": "COLUMN_NAME", "required": True},
            {"name": "ref_value_col", "type": "COLUMN_NAME", "required": True},
        ],
        "dsl_expression": (
            "EXISTS (SELECT 1 FROM :ref_table R WHERE R.:ref_name_col = :col_name "
            "AND R.:ref_value_col = :col)"
        ),
    },
}


def _load_active_tags(session: Session, database: str, schema: str) -> List[Dict[str, str]]:
    tag_table = _fq_tag_table(database, schema)
    try:
        tag_df = session.sql(
            f"""
            SELECT TAG_CODE, COALESCE(TAG_LABEL, TAG_CODE) AS TAG_LABEL
            FROM {tag_table}
            WHERE IS_ACTIVE IS NULL OR IS_ACTIVE = TRUE
            """
        ).to_pandas()
    except Exception:  # pragma: no cover - errors surfaced elsewhere
        return []

    return [
        {"code": str(row.TAG_CODE), "label": str(row.TAG_LABEL)}
        for row in tag_df.itertuples()
        if str(row.TAG_CODE).strip()
    ]


def _apply_template_to_form_state(template_key: str, *, form_state_key: str) -> None:
    template = RULE_TEMPLATES.get(template_key)
    if not template:
        return

    state = st.session_state.get(form_state_key, {})
    new_state = dict(state)
    defaults = template.get("defaults", {})

    for key, value in defaults.items():
        new_state[key] = value
        if key == "CATEGORY_CODE":
            new_state["CATEGORY"] = value

    new_state["EXPRESSION"] = template.get(
        "dsl_expression", state.get("EXPRESSION", "")
    )
    new_state["PARAM_SCHEMA"] = json.dumps(
        template.get("param_schema", []), indent=2
    )
    new_state.setdefault("DEFAULT_PARAMS", "{}")

    st.session_state[form_state_key] = new_state


def _load_rules(session: Session, table: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            RULE_UID,
            RULE_CODE,
            RULE_ID,
            SCOPE,
            CATEGORY,
            CATEGORY_CODE,
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
    if "CATEGORY_CODE" in df.columns:
        df["CATEGORY"] = df.get("CATEGORY").where(
            pd.notna(df.get("CATEGORY")), df.get("CATEGORY_CODE")
        )
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
        "CATEGORY_CODE": "VALIDITY",
        "CATEGORY": "",
        "SEVERITY": "",
        "SCOPE": "",
        "ENGINE_TYPE": "DSL",
        "EXPRESSION": "",
        "PARAM_SCHEMA": "[]",
        "DEFAULT_PARAMS": "{}",
        "DATA_TYPE_FAMILY": "ANY",
        "APPLICABILITY_TAGS": [],
        "DEFAULT_SUGGEST": True,
        "SUGGESTION_PRIORITY": 50,
        "ENABLED": True,
        "VERSION": 1,
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
                CATEGORY_CODE,
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
                "CATEGORY_CODE": record.get("CATEGORY_CODE")
                or record.get("CATEGORY")
                or "VALIDITY",
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
                "APPLICABILITY_TAGS": _coerce_tag_list(
                    record.get("APPLICABILITY_TAGS")
                ),
                "DEFAULT_SUGGEST": bool(record.get("DEFAULT_SUGGEST", True)),
                "SUGGESTION_PRIORITY": record.get("SUGGESTION_PRIORITY", 50) or 50,
                "ENABLED": bool(record.get("ENABLED", True)),
                "VERSION": record.get("VERSION", 1) or 1,
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

    form_state_key = "dq_rule_form_state"
    current_form_uid = selected_uid or "create_new"
    if (
        form_state_key not in st.session_state
        or st.session_state.get("dq_rule_form_state_uid") != current_form_uid
    ):
        st.session_state[form_state_key] = dict(rule_defaults)
        st.session_state["dq_rule_form_state_uid"] = current_form_uid

    tag_options = _load_active_tags(session, metadata_db, metadata_schema)
    tag_labels = {tag["code"]: tag.get("label", tag["code"]) for tag in tag_options}
    tag_codes = [tag["code"] for tag in tag_options]
    tag_codes = sorted(
        {*(tag_codes), *(_coerce_tag_list(form_state.get("APPLICABILITY_TAGS", [])))}
    )

    form_state = st.session_state[form_state_key]
    severity_default = form_state.get("SEVERITY") or severity_default
    if form_state.get("SCOPE") in scope_choices:
        scope_default_index = scope_choices.index(form_state.get("SCOPE"))

    with st.form("dq_rule_form"):
        rule_code = st.text_input(
            "Rule code",
            value=form_state.get("RULE_CODE", rule_defaults["RULE_CODE"]),
            help="Unique identifier for the rule template.",
            disabled=mode == "edit_existing",
        )
        rule_id = st.text_input(
            "Rule ID",
            value=form_state.get("RULE_ID", rule_defaults["RULE_ID"]),
            help="Human-friendly rule identifier shown in listings.",
        )

        col_category, col_severity = st.columns(2)
        with col_category:
            category_codes = [choice["code"] for choice in CATEGORY_CHOICES]
            category_labels = {
                choice["code"]: choice.get("label", choice["code"])
                for choice in CATEGORY_CHOICES
            }
            category_code = st.selectbox(
                "Category",
                options=category_codes,
                index=category_codes.index(
                    form_state.get("CATEGORY_CODE", "VALIDITY")
                )
                if form_state.get("CATEGORY_CODE", "VALIDITY") in category_codes
                else 1,
                format_func=lambda code: category_labels.get(code, code),
            )
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
                value=form_state.get("ENGINE_TYPE", "DSL"),
                disabled=True,
                help="Only DSL engine is supported in v1.",
            )

        col_data_type, col_default_suggest = st.columns(2)
        with col_data_type:
            data_type_family = st.selectbox(
                "Data type family",
                options=["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"],
                index=max(
                    0,
                    ["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"].index(
                        (form_state.get("DATA_TYPE_FAMILY") or "ANY").upper()
                    )
                    if (form_state.get("DATA_TYPE_FAMILY") or "ANY").upper()
                    in ["ANY", "NUMERIC", "STRING", "DATE", "BOOLEAN"]
                    else 0,
                ),
                help="Column type family this rule targets for suggestions.",
            )
        with col_default_suggest:
            default_suggest = st.checkbox(
                "Use this rule in automatic suggestions",
                value=bool(form_state.get("DEFAULT_SUGGEST", True)),
                help="Disable to exclude this rule from default suggestion generation.",
            )

        selected_tags = st.multiselect(
            "Applicability tags",
            options=tag_codes,
            default=form_state.get("APPLICABILITY_TAGS", []),
            format_func=lambda code: tag_labels.get(code, code),
            help="Choose which controlled tags this rule applies to.",
        )

        suggestion_priority_val = st.number_input(
            "Suggestion priority (1-100)",
            min_value=1,
            max_value=100,
            value=int(form_state.get("SUGGESTION_PRIORITY", 50) or 50),
            help="Higher values are suggested first.",
        )

        template_choice = st.selectbox(
            "Template",
            options=["(None)"] + list(RULE_TEMPLATES.keys()),
            format_func=lambda key: RULE_TEMPLATES.get(key, {}).get("name", key)
            if key != "(None)"
            else "— Select a template —",
            key="dq_rule_template_choice",
            help="Use a template to prefill DSL, parameters, and metadata.",
        )
        apply_template_clicked = st.form_submit_button(
            "Apply template",
            type="secondary",
            use_container_width=False,
        )

        expression = st.text_area(
            "Expression (DSL)",
            value=form_state.get("EXPRESSION", ""),
            height=200,
            help="Provide the DSL expression for this rule (e.g. ASSERT ...).",
        )
        with st.expander("DSL Reference"):
            st.markdown(
                """
                **Macros**

                * `:col` → column value expression (uses alias `T`).
                * `:col_name` → string literal of the column name (for lookup patterns).
                * `:param_name` → parameters defined in the schema (for example `:min_value`).
                * `:table` → table macro when available in your DSL context.

                **Supported constructs (examples)**

                * Null checks: `:col IS NULL`, `:col IS NOT NULL`
                * Regex: `REGEXP_LIKE(:col, :pattern)`
                * Range: `:col BETWEEN :min_value AND :max_value`
                * IN list: `:col IN (:allowed_values)`
                * Exists lookup: `EXISTS (SELECT 1 FROM :ref_table R WHERE R.:ref_value_col = :col)`
                * String helpers: `TRIM(:col)`, `UPPER(:col)`, `LOWER(:col)`, `LENGTH(:col)`

                **Common mistakes**

                * Snowflake uses `IS NULL` / `IS NOT NULL` (not `ISNULL()`).
                * Quote literals, not macros. Macros like `:col` and `:param_name` should stay unquoted.
                * Table aliases: expressions expect column references to use alias `T` if needed.
                """
            )

        param_schema_text = st.text_area(
            "Parameter schema (JSON array)",
            value=form_state.get("PARAM_SCHEMA", "[]"),
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
            value=form_state.get("DEFAULT_PARAMS", "{}"),
            height=140,
        )
        enabled = st.checkbox(
            "Enabled", value=bool(form_state.get("ENABLED", True))
        )
        version_value = st.number_input(
            "Version",
            min_value=1,
            value=int(form_state.get("VERSION", 1) or 1),
            help="Optional version number for the rule template.",
            disabled=True,
        )

        action_col1, action_col2, action_col3 = st.columns(3)
        with action_col1:
            save_clicked = st.form_submit_button("Save", type="primary")
        with action_col2:
            cancel_clicked = st.form_submit_button("Cancel", type="secondary")
        with action_col3:
            test_compile_clicked = st.form_submit_button("Test Compile")

    st.session_state[form_state_key] = {
        "RULE_CODE": rule_code,
        "RULE_ID": rule_id,
        "CATEGORY_CODE": category_code,
        "CATEGORY": category_code,
        "SEVERITY": severity,
        "SCOPE": scope_value,
        "ENGINE_TYPE": engine_type,
        "EXPRESSION": expression,
        "PARAM_SCHEMA": param_schema_text,
        "DEFAULT_PARAMS": default_params_text,
        "DATA_TYPE_FAMILY": data_type_family,
        "APPLICABILITY_TAGS": selected_tags,
        "DEFAULT_SUGGEST": default_suggest,
        "SUGGESTION_PRIORITY": suggestion_priority_val,
        "ENABLED": enabled,
        "VERSION": int(version_value),
    }

    if apply_template_clicked and template_choice != "(None)":
        _apply_template_to_form_state(
            template_choice,
            form_state_key=form_state_key,
        )
        st.info("Template applied. Review the prefilled values before saving.")
        st.rerun()

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
    category_val = (category_code or "").strip()
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
    parsed_applicability_tags = [tag for tag in selected_tags if tag]
    version_val: Optional[int] = int(version_value)

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
                    CATEGORY_CODE = :3,
                    SEVERITY = :4,
                    SCOPE = :5,
                    ENGINE_TYPE = :6,
                    EXPRESSION = :7,
                    PARAM_SCHEMA = PARSE_JSON(:8),
                    DEFAULT_PARAMS = PARSE_JSON(:9),
                    DATA_TYPE_FAMILY = :10,
                    APPLICABILITY_TAGS = PARSE_JSON(:11),
                    DEFAULT_SUGGEST = :12,
                    SUGGESTION_PRIORITY = :13,
                    ENABLED = :14,
                    VERSION = :15,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RULE_UID = :16
                """,
                params=[
                    rule_id_val,
                    category_val or None,
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
                    CATEGORY_CODE,
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
                    :16,
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP()
                """,
                params=[
                    rule_code_val,
                    rule_id_val,
                    category_val or None,
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
