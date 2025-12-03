-- Python-based compiler for DSL DQ rules. Profiling and DQ Config will call this API later.
CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COMPILE_RULE_SQL(
    RULE_CODE STRING,
    TARGET_TABLE_FQN STRING,
    TARGET_COLUMNS ARRAY,
    PARAM_VALUES VARIANT
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'compile_rule'
EXECUTE AS CALLER
AS
$$
from typing import Any, Dict, Iterable, List, Optional
import re

SUPPORTED_ENGINE = {"DSL"}


def quote_identifier(name: str) -> str:
    if not isinstance(name, str):
        raise ValueError("Identifier must be a string")
    segments = [seg.strip() for seg in name.split(".") if seg.strip()]
    if not segments:
        raise ValueError("Identifier cannot be empty")
    return ".".join('"' + seg.replace('"', '""') + '"' for seg in segments)


def quote_fqn(fqn: str) -> str:
    return quote_identifier(fqn)


def to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _split_args(arg_str: str) -> List[str]:
    args: List[str] = []
    depth = 0
    current = []
    for ch in arg_str:
        if ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        current.append(ch)
    if current:
        args.append("".join(current).strip())
    return args


def _replace_function_calls(expr: str, name: str, handler) -> str:
    pattern = re.compile(fr"{name}\s*\(")
    idx = 0
    while True:
        match = pattern.search(expr, idx)
        if not match:
            break
        start = match.end()
        depth = 1
        end = start
        while end < len(expr) and depth > 0:
            ch = expr[end]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            end += 1
        args_str = expr[start : end - 1]
        replacement = handler(_split_args(args_str))
        expr = expr[: match.start()] + replacement + expr[end:]
        idx = match.start() + len(replacement)
    return expr


def _expand_implications(expr: str) -> str:
    idx = expr.rfind("->")
    if idx == -1:
        return expr
    left = expr[:idx].strip()
    right = expr[idx + 2 :].strip()
    expanded_left = _expand_implications(left)
    expanded_right = _expand_implications(right)
    return f"((NOT ({expanded_left})) OR ({expanded_right}))"


def format_param_value(value: Any, param_type: Optional[str]) -> str:
    type_name = (param_type or "STRING").upper()
    if type_name == "STRING":
        if not isinstance(value, str):
            raise ValueError("Expected STRING parameter")
        return to_sql_literal(value)
    if type_name == "NUMBER":
        if not isinstance(value, (int, float)):
            raise ValueError("Expected NUMBER parameter")
        return str(value)
    if type_name == "BOOLEAN":
        if not isinstance(value, bool):
            raise ValueError("Expected BOOLEAN parameter")
        return "TRUE" if value else "FALSE"
    if type_name == "FQN_TABLE":
        if not isinstance(value, str):
            raise ValueError("Expected FQN_TABLE parameter")
        return quote_fqn(value)
    if type_name == "COLUMN_NAME":
        if not isinstance(value, str):
            raise ValueError("Expected COLUMN_NAME parameter")
        return quote_identifier(value)
    if type_name == "STRING_LIST":
        if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
            raise ValueError("Expected STRING_LIST parameter")
        parts = [to_sql_literal(item) for item in value]
        return "(" + ", ".join(parts) + ")"
    raise ValueError(f"Unsupported parameter type: {param_type}")


def compile_expression(
    expression: str,
    target_column: str,
    resolved_params: Dict[str, Any],
    param_types: Optional[Dict[str, str]] = None,
    table_alias: str = "T",
) -> str:
    param_types = param_types or {}
    expr = (expression or "").strip()
    if not expr:
        raise ValueError("Expression is required for compilation")

    if expr.upper().startswith("ASSERT "):
        expr = expr[6:].strip()
    expr = _expand_implications(expr)

    column_sql = f"{table_alias}.{quote_identifier(target_column)}"
    expr = re.sub(r"\bvalue\b", column_sql, expr)

    def _param_replacer(match: re.Match) -> str:
        name = match.group(1)
        if name not in resolved_params:
            raise ValueError(f"Missing parameter value for '{name}'")
        return format_param_value(resolved_params[name], param_types.get(name))

    expr = re.sub(r"param\(\s*\"([^\"]+)\"\s*\)", _param_replacer, expr)

    expr = _replace_function_calls(expr, "is_null", lambda args: f"({args[0]} IS NULL)")
    expr = _replace_function_calls(expr, "matches", lambda args: f"REGEXP_LIKE({args[0]}, {args[1]})")

    def _lookup_handler(args: List[str]) -> str:
        if len(args) != 3:
            raise ValueError("lookup_exists requires table, column, value")
        table_arg, column_arg, value_arg = args
        column_expr = (
            column_arg
            if column_arg.strip().startswith('"') and column_arg.strip().endswith('"')
            else quote_identifier(column_arg)
        )
        return (
            "EXISTS (SELECT 1 FROM "
            + table_arg
            + f" WHERE {column_expr} = {value_arg})"
        )

    expr = _replace_function_calls(expr, "lookup_exists", _lookup_handler)

    return expr


def normalize_param_schema(raw_schema: Any) -> List[Dict[str, Any]]:
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


def merge_params(defaults: Optional[Dict[str, Any]], overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(defaults, dict):
        merged.update(defaults)
    if isinstance(overrides, dict):
        merged.update(overrides)
    return merged


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


def compile_rule(session, RULE_CODE: str, TARGET_TABLE_FQN: str, TARGET_COLUMNS: List[str], PARAM_VALUES: Dict[str, Any]):
    rule_rows = session.sql(
        """
        SELECT RULE_CODE, ENGINE_TYPE, SCOPE, EXPRESSION, PARAM_SCHEMA, DEFAULT_PARAMS, ENABLED, VERSION
        FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY
        WHERE RULE_CODE = ? AND ENABLED = TRUE
        """,
        params=[RULE_CODE],
    ).collect()

    if not rule_rows:
        raise ValueError(f"Rule not found or disabled for RULE_CODE={RULE_CODE}")

    rule = rule_rows[0].as_dict()
    engine_type = (rule.get("ENGINE_TYPE") or "").upper()
    if engine_type not in SUPPORTED_ENGINE:
        raise ValueError(f"Rule {RULE_CODE} is not supported by DSL compiler")

    scope = (rule.get("SCOPE") or "COLUMN").upper()
    if TARGET_COLUMNS is None:
        target_columns = []
    elif isinstance(TARGET_COLUMNS, list):
        target_columns = TARGET_COLUMNS
    else:
        raise ValueError("TARGET_COLUMNS must be an array")
    if scope == "COLUMN" and len(target_columns) != 1:
        raise ValueError("SCOPE=COLUMN requires exactly one target column in v1")

    if PARAM_VALUES is not None and not isinstance(PARAM_VALUES, dict):
        raise ValueError("PARAM_VALUES must be an object")

    param_schema = normalize_param_schema(rule.get("PARAM_SCHEMA"))
    resolved_params = merge_params(rule.get("DEFAULT_PARAMS"), PARAM_VALUES)

    param_types: Dict[str, str] = {}
    for param_def in param_schema:
        name = param_def.get("name")
        type_name = param_def.get("type", "STRING")
        param_types[name] = type_name
        if param_def.get("required", True):
            if name not in resolved_params:
                raise ValueError(f"Missing required parameter '{name}'")
        if name in resolved_params:
            _validate_param_value(resolved_params[name], type_name, name)

    target_column = target_columns[0] if target_columns else None
    compiled_predicate = compile_expression(
        rule.get("EXPRESSION"), target_column, resolved_params, param_types
    )

    violation_query = f"SELECT * FROM {TARGET_TABLE_FQN} AS T WHERE NOT ({compiled_predicate})"

    return {
        "rule_code": rule.get("RULE_CODE"),
        "scope": rule.get("SCOPE"),
        "target_table": TARGET_TABLE_FQN,
        "target_columns": target_columns,
        "params": resolved_params,
        "compiled_predicate": compiled_predicate,
        "violation_query": violation_query,
        "version": rule.get("VERSION"),
    }
$$;
