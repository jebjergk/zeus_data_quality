# services/runner.py
import json
import re
from typing import Any, Dict, List, Sequence
from utils.checkdefs import _RULE_PARAMS_KEY
from utils.meta import DQCheck, DQConfig

AGG_PREFIX = "AGG:"


def _extract_rule_params(check: DQCheck) -> Sequence[Any]:
    if not check.params_json:
        return ()
    try:
        parsed = json.loads(check.params_json)
    except Exception:
        return ()
    if not isinstance(parsed, dict):
        return ()
    params = parsed.get(_RULE_PARAMS_KEY, ())
    if params is None:
        return ()
    if isinstance(params, (list, tuple)):
        return tuple(params)
    return (params,)


def _normalize_rule_expression(check: DQCheck) -> str:
    """Return a SQL predicate for a rule, handling JSON-encoded payloads."""

    raw_expr = (check.compiled_rule or check.rule_expr or "").strip()
    rule_expr = raw_expr
    try:
        parsed = json.loads(raw_expr)
        parsed_compiled = None
        if isinstance(parsed, dict):
            parsed_compiled = (parsed.get("compiled_predicate") or "").strip()
        elif isinstance(parsed, str):
            # Handle double-encoded JSON payloads or quoted predicates.
            parsed_compiled = parsed.strip()
            try:
                nested = json.loads(parsed)
                if isinstance(nested, dict):
                    nested_compiled = (nested.get("compiled_predicate") or "").strip()
                    if nested_compiled:
                        parsed_compiled = nested_compiled
            except Exception:
                pass
        if parsed_compiled:
            rule_expr = parsed_compiled
    except Exception:
        if raw_expr.startswith("{"):
            # Attempt a defensive extraction when the payload is not valid JSON.
            patterns = [
                r'"compiled_predicate"\s*:\s*"(?P<predicate>.*?)"\s*,\s*"',
                r'"compiled_predicate"\s*:\s*"(?P<predicate>(?:[^"\\]|\\.)*)"',
            ]
            for pattern in patterns:
                match = re.search(pattern, raw_expr, flags=re.DOTALL)
                if match:
                    compiled = match.group("predicate")
                    compiled = (
                        compiled.replace("\\\"", '"')
                        .replace("\\n", "\n")
                        .strip()
                    )
                    if compiled:
                        rule_expr = compiled
                        break

            if rule_expr == raw_expr:
                start = raw_expr.find('"compiled_predicate"')
                if start != -1:
                    remainder = raw_expr[start:]
                    colon_idx = remainder.find(":")
                    if colon_idx != -1:
                        value_part = remainder[colon_idx + 1 :].lstrip()
                        if value_part.startswith('"'):
                            value_part = value_part[1:]
                            end_idx = value_part.find('",')
                            if end_idx == -1:
                                end_idx = value_part.find('"\n')
                            if end_idx != -1:
                                compiled = (
                                    value_part[:end_idx]
                                    .replace("\\\"", '"')
                                    .replace("\\n", "\n")
                                    .strip()
                                )
                                if compiled:
                                    rule_expr = compiled
    return rule_expr


def _sql_with_params(session, sql: str, params: Sequence[Any]):
    if params:
        return session.sql(sql, params=tuple(params))
    return session.sql(sql)


def run_now(session, cfg: DQConfig, checks: List[DQCheck]) -> Dict[str, Any]:
    results: Dict[str, Any] = {"config_id": cfg.config_id, "checks": []}
    for chk in checks:
        rule = _normalize_rule_expression(chk)
        rule_params = _extract_rule_params(chk)
        if rule.upper().startswith(AGG_PREFIX):
            sql = rule[len(AGG_PREFIX):].strip()
            if sql:
                # Remove any wrapping quotes that may surround the SQL text.
                while len(sql) >= 2 and sql[0] == sql[-1] and sql[0] in {'"', "'"}:
                    sql = sql[1:-1].strip()
                # Snowflake can surface statements such as `'SELECT ...''` when
                # values were stored with escaped quotes. Strip any leading or
                # trailing quote characters that remain so we execute the raw
                # SQL statement.
                sql = sql.lstrip()
                while sql and sql[0] in {'"', "'"}:
                    sql = sql[1:].lstrip()
                sql = sql.rstrip()
                while sql and sql[-1] in {'"', "'"}:
                    sql = sql[:-1].rstrip()
            try:
                df = _sql_with_params(session, sql, rule_params)
            except Exception as exc:
                raise RuntimeError(f"Failed to execute aggregate check SQL: {exc}\nSQL:\n{sql}") from exc
            r = df.collect()[0]
            ok = bool((r[0] if not hasattr(r, 'asDict') else list(r.asDict().values())[0]))
            failures = 0 if ok else 1
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": True,
                "ok": ok,
                "failures": failures,
                "sample": []
            })
        else:
            failure_sql = (
                f"SELECT COUNT(*) AS FAILURES FROM {chk.table_fqn} AS T WHERE NOT ({rule})"
            )
            try:
                df = _sql_with_params(session, failure_sql, rule_params)
            except Exception as exc:
                raise RuntimeError(f"Failed to execute row check SQL: {exc}\nSQL:\n{failure_sql}") from exc
            failures = int(df.collect()[0][0])
            sample = []
            if chk.sample_rows and failures:
                sample_sql = (
                    f"SELECT * FROM {chk.table_fqn} AS T WHERE NOT ({rule}) LIMIT {int(chk.sample_rows)}"
                )
                try:
                    s_df = _sql_with_params(session, sample_sql, rule_params)
                except Exception as exc:
                    raise RuntimeError(f"Failed to fetch sample rows using SQL: {exc}\nSQL:\n{sample_sql}") from exc
                sample = [r.asDict() if hasattr(r, 'asDict') else dict(r) for r in s_df.collect()]
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": False,
                "failures": failures,
                "sample": sample,
            })
    return results
