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


def test_normalize_rule_expression_handles_invalid_json_payload():
    compiled = 'T."COL" > 0'
    payload = '{\n  "compiled_predicate": "' + compiled + '",\n  "rule_code": "GT_ZERO"\n'
    check = _make_check(rule_expr=payload)

    assert _normalize_rule_expression(check) == compiled


def test_normalize_rule_expression_handles_quoted_json_string():
    compiled = 'T."COL" > 10'
    payload = json.dumps({"compiled_predicate": compiled})
    quoted = json.dumps(payload)  # Stored with extra quotes
    check = _make_check(rule_expr=quoted)

    assert _normalize_rule_expression(check) == compiled


def test_normalize_rule_expression_handles_wrapped_predicate_string():
    compiled = "T.COL < 5"
    wrapped = json.dumps(compiled)
    check = _make_check(rule_expr=wrapped)

    assert _normalize_rule_expression(check) == compiled
