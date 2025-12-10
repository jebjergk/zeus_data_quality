import json

from services.runner import _normalize_rule_expression
from utils.meta import DQCheck


def _make_check(**overrides):
    defaults = dict(
        config_id="cfg",
        check_id="chk",
        table_fqn="DB.SCHEMA.TABLE",
        column_name=None,
        rule_expr="",
        severity="ERROR",
    )
    defaults.update(overrides)
    return DQCheck(**defaults)


def test_normalize_rule_expression_from_json_payload():
    compiled = 'NOT (T."COL" IS NULL)'
    payload = json.dumps({
        "compiled_predicate": compiled,
        "rule_code": "NOT_NULL",
    })
    check = _make_check(rule_expr=payload)

    assert _normalize_rule_expression(check) == compiled


def test_normalize_rule_expression_prefers_compiled_rule():
    compiled_rule = "T.COL > 0"
    payload = json.dumps({"compiled_predicate": "SHOULD_NOT_USE"})
    check = _make_check(rule_expr=payload, compiled_rule=compiled_rule)

    assert _normalize_rule_expression(check) == compiled_rule
