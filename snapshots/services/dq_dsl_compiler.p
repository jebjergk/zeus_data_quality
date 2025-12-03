"""Minimal DSL compiler for DQ rules.

This module converts simple assertion-style DSL expressions into SQL predicates
that can be embedded in ``WHERE NOT (<predicate>)`` clauses. It intentionally
supports only the subset of features needed by the core DSL rules.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import re


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier or dotted path using double quotes.

    Each segment between dots is quoted independently to reduce the risk of SQL
    injection via identifiers.
    """

    if not isinstance(name, str):
        raise ValueError("Identifier must be a string")
    segments = [seg.strip() for seg in name.split(".") if seg.strip()]
    if not segments:
        raise ValueError("Identifier cannot be empty")
    return ".".join(f'"{seg.replace("\"", "\"\"")}"' for seg in segments)


def quote_fqn(fqn: str) -> str:
    """Quote a fully qualified name (db.schema.table)."""

    return quote_identifier(fqn)


def to_sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal."""

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
    """Recursively expand ``A -> B`` into ``((NOT (A)) OR (B))``."""

    idx = expr.rfind("->")
    if idx == -1:
        return expr
    left = expr[:idx].strip()
    right = expr[idx + 2 :].strip()
    expanded_left = _expand_implications(left)
    expanded_right = _expand_implications(right)
    return f"((NOT ({expanded_left})) OR ({expanded_right}))"


def format_param_value(value: Any, param_type: Optional[str]) -> str:
    """Convert a parameter value into SQL-ready text based on its type."""

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
    """Compile a DSL expression into a SQL predicate string."""

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
