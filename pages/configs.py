from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from pages.ui_shared import danger_note, divider, page_header, pill
from services.configs import delete_config_full, save_config_and_checks
from utils import dmfs
from utils.checkdefs import (
    SUPPORTED_COLUMN_CHECKS,
    SUPPORTED_TABLE_CHECKS,
    build_rule_for_column_check,
    build_rule_for_table_check,
)
from utils.meta import (
    DQCheck,
    DQConfig,
    fq_table,
    get_checks,
    get_config,
    list_columns,
    list_configs,
    list_databases,
    list_schemas,
    list_tables,
)
from utils.schedules import ensure_task_for_config

NEW_CONFIG_ID = "__new__"
COLUMN_CHECK_TYPES = [c.upper() for c in SUPPORTED_COLUMN_CHECKS]
TABLE_CHECK_TYPES = [c.upper() for c in SUPPORTED_TABLE_CHECKS]
SEVERITY_OPTIONS = ["ERROR", "WARN"]


def _keyify(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value)


def _parse_target_parts(fqn: str) -> Tuple[str, str, str]:
    parts = [p.strip().strip('"') for p in (fqn or "").split(".") if p.strip()]
    if len(parts) >= 3:
        return parts[-3], parts[-2], parts[-1]
    return "", "", ""


def _reset_dynamic_keys() -> None:
    keys: List[str] = st.session_state.get("editor_dynamic_keys", [])
    for key in keys:
        st.session_state.pop(key, None)
    st.session_state["editor_dynamic_keys"] = []


def _register_dynamic_key(key: str, default: Any) -> None:
    keys: List[str] = st.session_state.setdefault("editor_dynamic_keys", [])
    if key not in keys:
        keys.append(key)
    if key not in st.session_state:
        st.session_state[key] = default


def _ensure_base_state() -> None:
    st.session_state.setdefault("selected_config_id", None)
    st.session_state.setdefault("editor_is_new", False)
    st.session_state.setdefault("editor_needs_load", False)
    st.session_state.setdefault("editor_active_id", None)
    st.session_state.setdefault("duplicate_source_id", None)
    st.session_state.setdefault("config_search", "")
    st.session_state.setdefault("config_sort", "Status & name")
    st.session_state.setdefault("dq_cols_ms", [])
    st.session_state.setdefault("col_checks_state", {})
    st.session_state.setdefault("table_checks_state", {})
    st.session_state.setdefault("config_feedback", [])


def _clear_editor_selection() -> None:
    """Reset editor-related state so the configuration list can be shown."""

    _reset_dynamic_keys()
    st.session_state["selected_config_id"] = None
    st.session_state["editor_active_id"] = None
    st.session_state["editor_is_new"] = False
    st.session_state["editor_needs_load"] = False
    st.session_state["duplicate_source_id"] = None


def _column_param_specs(ctype: str) -> List[Dict[str, Any]]:
    ctype = (ctype or "").upper()
    if ctype == "UNIQUE":
        return [
            {"name": "ignore_nulls", "type": "checkbox", "label": "Ignore NULL values", "default": True},
        ]
    if ctype == "MIN_MAX":
        return [
            {"name": "min", "type": "text", "label": "Minimum value", "default": ""},
            {"name": "max", "type": "text", "label": "Maximum value", "default": ""},
        ]
    if ctype == "WHITESPACE":
        return [
            {
                "name": "mode",
                "type": "select",
                "label": "Mode",
                "options": [
                    ("NO_LEADING_TRAILING", "No leading or trailing whitespace"),
                    ("NO_INTERNAL_ONLY_WHITESPACE", "Collapse internal whitespace"),
                    ("NO_BLANK", "Disallow blank-only values"),
                ],
                "default": "NO_LEADING_TRAILING",
            }
        ]
    if ctype == "FORMAT_DISTRIBUTION":
        return [
            {"name": "regex", "type": "text", "label": "Allowed pattern (regex)", "default": ".*"},
        ]
    if ctype == "VALUE_DISTRIBUTION":
        return [
            {
                "name": "allowed_values_csv",
                "type": "textarea",
                "label": "Allowed values (comma separated)",
                "default": "",
                "height": 90,
            },
        ]
    return []


def _table_param_specs(ctype: str) -> List[Dict[str, Any]]:
    ctype = (ctype or "").upper()
    if ctype == "FRESHNESS":
        return [
            {"name": "timestamp_column", "type": "text", "label": "Timestamp column", "default": "LOAD_TIMESTAMP"},
            {
                "name": "max_age_minutes",
                "type": "number",
                "label": "Max age in minutes",
                "default": 1920,
                "min_value": 1,
                "step": 30,
            },
        ]
    if ctype == "ROW_COUNT":
        return [
            {
                "name": "min_rows",
                "type": "number",
                "label": "Minimum rows",
                "default": 1,
                "min_value": 0,
                "step": 100,
            },
        ]
    if ctype == "ROW_COUNT_ANOMALY":
        return [
            {
                "name": "timestamp_column",
                "type": "text",
                "label": "Timestamp column",
                "default": "LOAD_TIMESTAMP",
            },
            {
                "name": "lookback_days",
                "type": "number",
                "label": "Lookback days",
                "default": 28,
                "min_value": 1,
                "step": 1,
            },
            {
                "name": "sensitivity",
                "type": "number",
                "label": "MAD sensitivity",
                "default": 3.0,
                "min_value": 0.1,
                "step": 0.1,
                "format": "%.1f",
            },
            {
                "name": "min_history_days",
                "type": "number",
                "label": "Minimum history days",
                "default": 7,
                "min_value": 1,
                "step": 1,
            },
        ]
    return []


def _trigger_editor_load(config_id: Optional[str], is_new: bool) -> None:
    st.session_state["selected_config_id"] = config_id
    st.session_state["editor_is_new"] = is_new
    st.session_state["editor_needs_load"] = True


def _filter_sort_configs(configs: List[DQConfig]) -> List[DQConfig]:
    search = (st.session_state.get("config_search") or "").strip().lower()
    sort_option = st.session_state.get("config_sort") or "Status & name"

    if search:
        configs = [
            cfg for cfg in configs if search in (cfg.name or "").lower()
            or search in (cfg.target_table_fqn or "").lower()
            or search in (cfg.description or "").lower()
            or search in (cfg.config_id or "").lower()
        ]

    sort_option = sort_option.strip()
    if sort_option == "Name A→Z":
        configs = sorted(configs, key=lambda c: (c.name or "").lower())
    elif sort_option == "Name Z→A":
        configs = sorted(configs, key=lambda c: (c.name or "").lower(), reverse=True)
    elif sort_option == "Owner":
        configs = sorted(configs, key=lambda c: (c.owner or "").lower())
    elif sort_option == "Target table":
        configs = sorted(configs, key=lambda c: (c.target_table_fqn or "").lower())
    else:  # Status & name
        configs = sorted(
            configs,
            key=lambda c: (
                0 if (c.status or "").upper() == "ACTIVE" else 1,
                (c.name or "").lower(),
            ),
        )
    return configs


def _render_config_card(session, cfg: DQConfig) -> None:
    with st.container():
        info_col, edit_col, delete_col = st.columns([1.0, 0.12, 0.12])
        with info_col:
            st.markdown(
                f"**{cfg.name}** \n"
                f"<span class='small'>Target: {cfg.target_table_fqn or '—'}</span>",
                unsafe_allow_html=True,
            )
            meta_row = []
            if cfg.status:
                tone = "success" if (cfg.status or "").upper() == "ACTIVE" else "info"
                meta_row.append(pill(cfg.status, tone=tone))
            if cfg.owner:
                meta_row.append(f"<span class='small'>Owner: {cfg.owner}</span>")
            if cfg.run_as_role:
                meta_row.append(f"<span class='small'>Role: {cfg.run_as_role}</span>")
            if meta_row:
                st.markdown(" ".join(meta_row), unsafe_allow_html=True)
        with edit_col:
            if st.button(
                "✏️",
                key=f"edit_{cfg.config_id}",
                help="Edit configuration",
                use_container_width=True,
            ):
                _trigger_editor_load(cfg.config_id, False)
        with delete_col:
            if st.button(
                "🗑️",
                key=f"delete_{cfg.config_id}",
                help="Delete configuration",
                use_container_width=True,
                type="secondary",
            ):
                if session:
                    try:
                        result = delete_config_full(session, cfg.config_id)
                        st.session_state["config_feedback"].append(
                            (
                                "success",
                                f"Deleted configuration {cfg.config_id} (dropped {len(result.get('dmfs_dropped', []))} views)",
                            )
                        )
                        if st.session_state.get("selected_config_id") == cfg.config_id:
                            st.session_state["selected_config_id"] = None
                            st.session_state["editor_active_id"] = None
                    except Exception as exc:
                        st.session_state["config_feedback"].append(("error", f"Delete failed: {exc}"))
                else:
                    st.session_state["config_feedback"].append(("warning", "No active session; cannot delete."))


def _render_list_panel(session) -> None:
    st.subheader("Configurations")
    if not session:
        st.info("Connect to Snowflake to load and manage configurations.")
    configs = list_configs(session) if session else []

    st.text_input("Search", key="config_search", placeholder="Name, table, owner…")
    st.selectbox(
        "Sort by",
        ["Status & name", "Name A→Z", "Name Z→A", "Owner", "Target table"],
        key="config_sort",
    )
    if st.button("➕ Create configuration", use_container_width=True):
        _trigger_editor_load(NEW_CONFIG_ID, True)

    filtered = _filter_sort_configs(configs)
    if not filtered:
        st.caption("No configurations found with the current filters.")
    for cfg in filtered:
        _render_config_card(session, cfg)
        divider()


def _ensure_table_picker_defaults() -> None:
    st.session_state.setdefault("editor_db", "")
    st.session_state.setdefault("editor_schema", "")
    st.session_state.setdefault("editor_table", "")
    st.session_state.setdefault("editor_target_fqn", "")


def _load_editor_state(session, config_id: Optional[str], is_new: bool) -> None:
    _reset_dynamic_keys()
    st.session_state["col_checks_state"] = {}
    st.session_state["table_checks_state"] = {}

    cfg_obj: Optional[DQConfig] = None
    checks: List[DQCheck] = []

    duplicate_source = st.session_state.pop("duplicate_source_id", None)

    source_id: Optional[str] = None
    if duplicate_source and session:
        source_id = duplicate_source
        is_new = True
    elif not is_new and config_id and config_id != NEW_CONFIG_ID and session:
        source_id = config_id

    if source_id and session:
        cfg_obj = get_config(session, source_id)
        if cfg_obj:
            checks = get_checks(session, source_id)
    if cfg_obj is None:
        cfg_obj = DQConfig(
            config_id="" if is_new else (config_id or ""),
            name="",
            description=None,
            target_table_fqn="",
            run_as_role=None,
            dmf_role=None,
            status="DRAFT",
            owner=None,
            schedule_cron=None,
            schedule_timezone=None,
            schedule_enabled=True,
        )
        checks = []
        is_new = True

    duplicating = bool(duplicate_source)

    if is_new:
        st.session_state["editor_active_id"] = NEW_CONFIG_ID
        st.session_state["selected_config_id"] = NEW_CONFIG_ID
    else:
        st.session_state["editor_active_id"] = cfg_obj.config_id
        st.session_state["selected_config_id"] = cfg_obj.config_id
    st.session_state["editor_is_new"] = is_new

    st.session_state["cfg_config_id"] = "" if duplicating else cfg_obj.config_id
    st.session_state["cfg_name"] = f"{cfg_obj.name} (copy)" if duplicating and cfg_obj.name else cfg_obj.name
    st.session_state["cfg_description"] = cfg_obj.description or ""
    st.session_state["cfg_status"] = cfg_obj.status or "DRAFT"
    st.session_state["cfg_owner"] = cfg_obj.owner or ""
    st.session_state["cfg_run_as_role"] = cfg_obj.run_as_role or ""
    st.session_state["cfg_dmf_role"] = cfg_obj.dmf_role or ""
    st.session_state["cfg_schedule_enabled"] = bool(cfg_obj.schedule_enabled)
    st.session_state["cfg_schedule_cron"] = cfg_obj.schedule_cron or "0 8 * * *"
    st.session_state["cfg_schedule_timezone"] = cfg_obj.schedule_timezone or "Europe/Berlin"
    st.session_state["cfg_apply_dmfs"] = True

    db, schema, table = _parse_target_parts(cfg_obj.target_table_fqn or "")
    _ensure_table_picker_defaults()
    st.session_state["editor_db"] = db
    st.session_state["editor_schema"] = schema
    st.session_state["editor_table"] = table
    st.session_state["editor_target_fqn"] = cfg_obj.target_table_fqn or ""
    st.session_state["editor_target_fqn_display"] = st.session_state["editor_target_fqn"]

    col_state: Dict[str, Dict[str, Dict[str, Any]]] = {}
    table_state: Dict[str, Dict[str, Any]] = {}
    selected_columns: List[str] = []

    for chk in checks:
        params = {}
        if chk.params_json:
            try:
                params = json.loads(chk.params_json)
            except Exception:
                params = {}
        state_entry = {
            "check_id": None if duplicating else chk.check_id,
            "severity": (chk.severity or "ERROR").upper(),
            "sample_rows": int(chk.sample_rows or 0),
            "params": params,
            "enabled": True,
        }
        ctype = (chk.check_type or "").upper()
        if chk.column_name:
            column = chk.column_name
            selected_columns.append(column)
            col_checks = col_state.setdefault(column, {})
            col_checks[ctype] = state_entry
        else:
            table_state[ctype] = state_entry

    st.session_state["dq_cols_ms"] = sorted(set(selected_columns))
    st.session_state["col_checks_state"] = col_state
    st.session_state["table_checks_state"] = table_state


def _ensure_editor_loaded(session) -> None:
    selected = st.session_state.get("selected_config_id")
    is_new = st.session_state.get("editor_is_new", False)
    active = st.session_state.get("editor_active_id")
    needs = st.session_state.get("editor_needs_load", False)
    if selected is None:
        return
    if needs or active is None or (not is_new and active != selected and selected != NEW_CONFIG_ID):
        _load_editor_state(session, selected, is_new)
        st.session_state["editor_needs_load"] = False
    elif needs or (is_new and active != NEW_CONFIG_ID):
        _load_editor_state(session, selected, True)
        st.session_state["editor_needs_load"] = False


def _clear_column_dynamic_keys(column: str) -> None:
    col_key = _keyify(column.lower())
    prefix = f"col_{col_key}_"
    keys = st.session_state.get("editor_dynamic_keys", [])
    remove = [k for k in keys if k.startswith(prefix)]
    for key in remove:
        st.session_state.pop(key, None)
    st.session_state["editor_dynamic_keys"] = [k for k in keys if k not in remove]


def _clear_table_dynamic_keys(ctype: str) -> None:
    prefix = f"tbl_{ctype.upper()}_"
    keys = st.session_state.get("editor_dynamic_keys", [])
    remove = [k for k in keys if k.startswith(prefix)]
    for key in remove:
        st.session_state.pop(key, None)
    st.session_state["editor_dynamic_keys"] = [k for k in keys if k not in remove]


def _render_column_checks_section() -> None:
    st.markdown("### Column checks")
    columns = st.session_state.get("dq_cols_ms", [])
    if not columns:
        st.info("Select columns above to configure checks.")
        return

    col_states: Dict[str, Dict[str, Dict[str, Any]]] = st.session_state.setdefault("col_checks_state", {})
    for column in columns:
        col_state = col_states.setdefault(column, {})
        with st.expander(f"Column: {column}", expanded=False):
            col_key = _keyify(column.lower())
            for ctype in COLUMN_CHECK_TYPES:
                state = col_state.setdefault(ctype, {
                    "enabled": False,
                    "severity": "ERROR",
                    "sample_rows": 0,
                    "params": {},
                    "check_id": None,
                })
                enabled_key = f"col_{col_key}_{ctype}_enabled"
                _register_dynamic_key(enabled_key, state.get("enabled", False))
                enabled = st.checkbox(f"{ctype.title().replace('_', ' ')}", key=enabled_key)
                state["enabled"] = bool(enabled)

                severity_key = f"col_{col_key}_{ctype}_severity"
                _register_dynamic_key(severity_key, state.get("severity", "ERROR"))
                severity = st.selectbox(
                    "Severity",
                    SEVERITY_OPTIONS,
                    key=severity_key,
                    disabled=not enabled,
                )
                if severity is None:
                    severity = "ERROR"
                state["severity"] = severity

                sample_key = f"col_{col_key}_{ctype}_sample"
                _register_dynamic_key(sample_key, int(state.get("sample_rows", 0)))
                sample_rows = st.number_input(
                    "Sample failed rows",
                    min_value=0,
                    max_value=10000,
                    value=int(st.session_state.get(sample_key, 0)),
                    step=10,
                    key=sample_key,
                    disabled=not enabled,
                )
                state["sample_rows"] = int(sample_rows)

                params: Dict[str, Any] = {}
                for spec in _column_param_specs(ctype):
                    param_key = f"col_{col_key}_{ctype}_param_{spec['name']}"
                    _register_dynamic_key(param_key, state.get("params", {}).get(spec["name"], spec.get("default")))
                    if spec["type"] == "checkbox":
                        params[spec["name"]] = bool(
                            st.checkbox(spec["label"], key=param_key, disabled=not enabled)
                        )
                    elif spec["type"] == "textarea":
                        params[spec["name"]] = st.text_area(
                            spec["label"],
                            key=param_key,
                            height=spec.get("height", 120),
                            disabled=not enabled,
                        )
                    elif spec["type"] == "select":
                        options = spec.get("options", [])
                        values = [val for val, _ in options]
                        labels_map = {val: label for val, label in options}
                        params[spec["name"]] = st.selectbox(
                            spec["label"],
                            options=values,
                            key=param_key,
                            format_func=lambda val, mapping=labels_map: mapping.get(val, str(val)),
                            disabled=not enabled,
                        )
                    else:
                        params[spec["name"]] = st.text_input(
                            spec["label"],
                            key=param_key,
                            disabled=not enabled,
                        )
                state["params"] = params
                st.markdown("<hr />", unsafe_allow_html=True)
    # cleanup removed columns
    for column in list(col_states.keys()):
        if column not in columns:
            _clear_column_dynamic_keys(column)
            del col_states[column]


def _render_table_checks_section() -> None:
    st.markdown("### Table-level checks")
    table_states: Dict[str, Dict[str, Any]] = st.session_state.setdefault("table_checks_state", {})
    for ctype in TABLE_CHECK_TYPES:
        state = table_states.setdefault(ctype, {
            "enabled": False,
            "severity": "ERROR",
            "params": {},
            "check_id": None,
        })
        with st.expander(ctype.title().replace('_', ' '), expanded=False):
            enabled_key = f"tbl_{ctype}_enabled"
            _register_dynamic_key(enabled_key, state.get("enabled", False))
            enabled = st.checkbox("Enable", key=enabled_key)
            state["enabled"] = bool(enabled)

            severity_key = f"tbl_{ctype}_severity"
            _register_dynamic_key(severity_key, state.get("severity", "ERROR"))
            severity = st.selectbox(
                "Severity",
                SEVERITY_OPTIONS,
                key=severity_key,
                disabled=not enabled,
            )
            if severity is None:
                severity = "ERROR"
            state["severity"] = severity

            params: Dict[str, Any] = {}
            for spec in _table_param_specs(ctype):
                param_key = f"tbl_{ctype}_param_{spec['name']}"
                _register_dynamic_key(param_key, state.get("params", {}).get(spec["name"], spec.get("default")))
                widget_disabled = not enabled
                if spec["type"] == "number":
                    number_kwargs: Dict[str, Any] = {
                        "label": spec["label"],
                        "key": param_key,
                        "value": st.session_state.get(param_key, spec.get("default", 0)),
                        "step": spec.get("step", 1),
                        "disabled": widget_disabled,
                    }
                    if spec.get("min_value") is not None:
                        number_kwargs["min_value"] = spec.get("min_value")
                    if spec.get("max_value") is not None:
                        number_kwargs["max_value"] = spec.get("max_value")
                    if spec.get("format") is not None:
                        number_kwargs["format"] = spec.get("format")
                    params[spec["name"]] = st.number_input(**number_kwargs)
                else:
                    params[spec["name"]] = st.text_input(
                        spec["label"],
                        key=param_key,
                        disabled=widget_disabled,
                    )
            state["params"] = params
    for ctype in list(table_states.keys()):
        if ctype not in TABLE_CHECK_TYPES:
            _clear_table_dynamic_keys(ctype)
            del table_states[ctype]


def _render_target_picker(session) -> None:
    st.markdown("### Target table")
    _ensure_table_picker_defaults()
    if session:
        current_db = st.session_state.get("editor_db") or ""
        db_options = list_databases(session)
        if current_db and current_db not in db_options:
            db_options.append(current_db)
        db_choices = [""] + sorted({db for db in db_options if db})
        st.selectbox(
            "Database",
            options=db_choices,
            key="editor_db",
            format_func=lambda v: "— Select —" if v == "" else v,
        )

        db_choice = st.session_state.get("editor_db") or ""
        schema_options = list_schemas(session, db_choice) if db_choice else []
        current_schema = st.session_state.get("editor_schema") or ""
        if current_schema and current_schema not in schema_options:
            schema_options.append(current_schema)
        schema_choices = [""] + sorted({sch for sch in schema_options if sch})
        st.selectbox(
            "Schema",
            options=schema_choices,
            key="editor_schema",
            format_func=lambda v: "— Select —" if v == "" else v,
        )

        db_choice = st.session_state.get("editor_db") or ""
        schema_choice = st.session_state.get("editor_schema") or ""
        table_options = list_tables(session, db_choice, schema_choice) if db_choice and schema_choice else []
        current_table = st.session_state.get("editor_table") or ""
        if current_table and current_table not in table_options:
            table_options.append(current_table)
        table_choices = [""] + sorted({tbl for tbl in table_options if tbl})
        st.selectbox(
            "Table",
            options=table_choices,
            key="editor_table",
            format_func=lambda v: "— Select —" if v == "" else v,
        )

        db_choice = st.session_state.get("editor_db")
        schema_choice = st.session_state.get("editor_schema")
        table_choice = st.session_state.get("editor_table")
        if db_choice and schema_choice and table_choice:
            fqn = fq_table(db_choice, schema_choice, table_choice)
            st.session_state["editor_target_fqn"] = fqn
            st.session_state["editor_target_fqn_display"] = fqn
        else:
            st.session_state["editor_target_fqn"] = ""
            st.session_state["editor_target_fqn_display"] = ""
        st.text_input(
            "Target table (FQN)",
            value=st.session_state.get("editor_target_fqn", ""),
            key="editor_target_fqn_display",
            disabled=True,
        )
    else:
        st.text_input("Target table (FQN)", key="editor_target_fqn")
        st.session_state["editor_target_fqn_display"] = st.session_state.get("editor_target_fqn", "")


def _render_column_selector(session) -> None:
    st.markdown("### Columns to monitor")
    options: List[str] = []
    db = st.session_state.get("editor_db")
    schema = st.session_state.get("editor_schema")
    table = st.session_state.get("editor_table")
    if session and db and schema and table:
        options = list_columns(session, db, schema, table)
    if st.session_state.get("dq_cols_ms"):
        for col in st.session_state["dq_cols_ms"]:
            if col not in options:
                options.append(col)
    options = sorted(set(options))
    st.multiselect(
        "Columns",
        options=options,
        key="dq_cols_ms",
        help="Select columns to configure column-level checks.",
    )


def _coerce_param_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def _generate_check_id(prefix: str, column: str, ctype: str) -> str:
    base = f"{prefix}_{column}_{ctype}" if column else f"{prefix}_{ctype}"
    sanitized = _keyify(base.upper())
    return sanitized


def _gather_checks(config_id: str, target_fqn: str) -> List[DQCheck]:
    checks: List[DQCheck] = []
    col_states: Dict[str, Dict[str, Dict[str, Any]]] = st.session_state.get("col_checks_state", {})
    for column, check_map in col_states.items():
        for ctype, state in check_map.items():
            if not state.get("enabled"):
                continue
            params = {k: _coerce_param_value(v) for k, v in state.get("params", {}).items() if v not in (None, "")}
            rule, is_agg = build_rule_for_column_check(target_fqn, column, ctype, params)
            rule_expr = rule.strip()
            if is_agg:
                rule_expr = f"{dmfs.AGG_PREFIX}{rule_expr}"
            check_id = state.get("check_id") or _generate_check_id("COL", column, ctype)
            state["check_id"] = check_id
            checks.append(
                DQCheck(
                    config_id=config_id,
                    check_id=check_id,
                    table_fqn=target_fqn,
                    column_name=column,
                    rule_expr=rule_expr,
                    severity=(state.get("severity") or "ERROR").upper(),
                    sample_rows=int(state.get("sample_rows") or 0),
                    check_type=ctype,
                    params_json=json.dumps(params) if params else None,
                )
            )

    table_states: Dict[str, Dict[str, Any]] = st.session_state.get("table_checks_state", {})
    for ctype, state in table_states.items():
        if not state.get("enabled"):
            continue
        params = {k: _coerce_param_value(v) for k, v in state.get("params", {}).items() if v not in (None, "")}
        rule, is_agg = build_rule_for_table_check(target_fqn, ctype, params)
        rule_expr = rule.strip()
        if is_agg:
            rule_expr = f"{dmfs.AGG_PREFIX}{rule_expr}"
        check_id = state.get("check_id") or _generate_check_id("TBL", "", ctype)
        state["check_id"] = check_id
        checks.append(
            DQCheck(
                config_id=config_id,
                check_id=check_id,
                table_fqn=target_fqn,
                column_name=None,
                rule_expr=rule_expr,
                severity=(state.get("severity") or "ERROR").upper(),
                sample_rows=0,
                check_type=ctype,
                params_json=json.dumps(params) if params else None,
            )
        )
    return checks


def _render_schedule_section() -> None:
    st.markdown("### Scheduling")
    st.checkbox("Enable schedule", key="cfg_schedule_enabled")
    cols = st.columns(2)
    cols[0].text_input("Cron expression", key="cfg_schedule_cron")
    cols[1].text_input("Timezone", key="cfg_schedule_timezone")
    st.text_input("Run as role", key="cfg_run_as_role")
    st.text_input("DMF role", key="cfg_dmf_role")


def _render_general_details(is_new: bool) -> None:
    st.markdown("### Configuration details")
    st.text_input("Configuration ID", key="cfg_config_id", disabled=not is_new)
    st.text_input("Display name", key="cfg_name")
    st.text_area("Description", key="cfg_description", height=120)
    status_options = ["DRAFT", "ACTIVE", "PAUSED", "INACTIVE"]
    if st.session_state["cfg_status"] not in status_options:
        status_options.append(st.session_state["cfg_status"])
    st.selectbox("Status", status_options, key="cfg_status")
    st.text_input("Owner", key="cfg_owner")


def _save_configuration(session) -> None:
    config_id = (st.session_state.get("cfg_config_id") or "").strip()
    if not config_id:
        st.session_state["config_feedback"].append(("error", "Configuration ID is required."))
        return
    target_fqn = st.session_state.get("editor_target_fqn") or st.session_state.get("editor_target_fqn_display")
    if not target_fqn:
        st.session_state["config_feedback"].append(("error", "Select a target table before saving."))
        return
    name = (st.session_state.get("cfg_name") or "").strip()
    if not name:
        st.session_state["config_feedback"].append(("error", "Name is required."))
        return
    cfg_obj = DQConfig(
        config_id=config_id,
        name=name,
        description=(st.session_state.get("cfg_description") or "").strip() or None,
        target_table_fqn=target_fqn,
        run_as_role=(st.session_state.get("cfg_run_as_role") or "").strip() or None,
        dmf_role=(st.session_state.get("cfg_dmf_role") or "").strip() or None,
        status=(st.session_state.get("cfg_status") or "DRAFT").strip() or "DRAFT",
        owner=(st.session_state.get("cfg_owner") or "").strip() or None,
        schedule_cron=(st.session_state.get("cfg_schedule_cron") or "").strip() or None,
        schedule_timezone=(st.session_state.get("cfg_schedule_timezone") or "").strip() or None,
        schedule_enabled=bool(st.session_state.get("cfg_schedule_enabled")),
    )
    checks = _gather_checks(cfg_obj.config_id, cfg_obj.target_table_fqn)
    apply_now = bool(st.session_state.get("cfg_apply_dmfs"))
    if not session:
        st.session_state["config_feedback"].append(("warning", "No active session; cannot save configuration."))
        return
    try:
        save_config_and_checks(session, cfg_obj, checks, apply_now)
        st.session_state["config_feedback"].append(("success", f"Configuration {cfg_obj.config_id} saved. Open 📊 Monitor to review results."))
        st.session_state["selected_config_id"] = cfg_obj.config_id
        st.session_state["editor_is_new"] = False
        st.session_state["editor_active_id"] = cfg_obj.config_id
    except Exception as exc:
        st.session_state["config_feedback"].append(("error", f"Save failed: {exc}"))


def _render_run_now(session) -> None:
    if st.button("Run Now (EXECUTE TASK)", key="cfg_run_now", type="secondary"):
        config_id = st.session_state.get("cfg_config_id")
        if not session:
            st.session_state["config_feedback"].append(("warning", "No active session; cannot execute task."))
            return
        if not config_id:
            st.session_state["config_feedback"].append(("error", "Save the configuration before running."))
            return
        cfg_obj = get_config(session, config_id)
        if not cfg_obj:
            st.session_state["config_feedback"].append(("error", "Configuration not found in metadata. Save first."))
            return
        task_info = ensure_task_for_config(session, cfg_obj)
        task_name = task_info.get("task")
        status = task_info.get("status")
        if status in {"INVALID_TARGET", "INVALID_SCHEDULE", "NO_WAREHOUSE"}:
            reason = task_info.get("reason") or ""
            st.session_state["config_feedback"].append(("error", f"Task setup failed: {status} {reason}"))
            return
        if not task_name:
            st.session_state["config_feedback"].append(("error", "Unable to determine task name for execution."))
            return
        try:
            session.sql(f"EXECUTE TASK {task_name}").collect()
            st.session_state["config_feedback"].append(("success", f"Triggered task {task_name} for immediate execution."))
        except Exception as exc:
            st.session_state["config_feedback"].append(("error", f"Task execution failed: {exc}"))


def _render_feedback() -> None:
    messages = st.session_state.get("config_feedback", [])
    if not messages:
        return
    remaining: List[Tuple[str, str]] = []
    for level, message in messages:
        if level == "success":
            st.success(message)
        elif level == "warning":
            st.warning(message)
        elif level == "error":
            st.error(message)
        else:
            st.info(message)
    # clear after showing
    st.session_state["config_feedback"] = remaining


def render_configs(session, app_version: str | None = None) -> None:
    if app_version:
        st.session_state.setdefault("app_version", app_version)
    _ensure_base_state()
    page_header(
        "Data Quality Configurations",
        "Manage data quality rules, schedules, and monitoring targets.",
        session=session,
        show_version=True,
    )

    _ensure_editor_loaded(session)
    selected = st.session_state.get("selected_config_id")

    if not selected:
        st.info("Select a configuration or create a new one to begin editing.")
        _render_list_panel(session)
        if st.session_state.get("selected_config_id"):
            st.experimental_rerun()
    else:
        if st.button(
            "← Back to configuration list",
            key="cfg_back_to_list",
            type="secondary",
        ):
            _clear_editor_selection()
            st.experimental_rerun()

        with st.expander("Browse other configurations", expanded=False):
            _render_list_panel(session)
        is_new = st.session_state.get("editor_is_new", False)
        _render_feedback()
        _render_general_details(is_new)
        divider()
        _render_target_picker(session)
        _render_column_selector(session)
        target_fqn = st.session_state.get("editor_target_fqn") or st.session_state.get("editor_target_fqn_display")
        if target_fqn:
            _render_column_checks_section()
            _render_table_checks_section()
        else:
            danger_note("Select a target table to configure checks.")
        divider()
        _render_schedule_section()
        st.checkbox(
            "Attach DMF views after saving",
            key="cfg_apply_dmfs",
            help="Applies check views immediately when saving.",
        )
        _render_run_now(session)
        with st.form("config_save_form"):
            submitted = st.form_submit_button("Save & Apply", use_container_width=True)
        if submitted:
            _save_configuration(session)
    _render_feedback()
