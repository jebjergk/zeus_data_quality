from services.profiling import (
    _avg_length_metric,
    _column_length_expression,
    _string_column_sql_fragments,
)
from utils.checkdefs import build_rule_for_column_check


def test_string_whitespace_fragments_are_stable():
    qcol = '"CUSTOMER_NAME"'
    fragments = _string_column_sql_fragments(qcol)

    assert (
        fragments.empty_expr
        == "SUM(CASE WHEN \"CUSTOMER_NAME\"::STRING = '' THEN 1 ELSE 0 END) AS EMPTY_STR_ROWS"
    )
    assert (
        fragments.whitespace_only_expr
        == "SUM(CASE WHEN \"CUSTOMER_NAME\" IS NOT NULL AND REGEXP_LIKE(\"CUSTOMER_NAME\"::STRING, '^\\\\s+$') THEN 1 ELSE 0 END) AS WS_ONLY_ROWS"
    )
    assert (
        fragments.whitespace_expr
        == "SUM(CASE WHEN \"CUSTOMER_NAME\" IS NOT NULL AND REGEXP_LIKE(\"CUSTOMER_NAME\"::STRING, '^\\\\s|\\\\s$|\\\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
    )
    assert (
        fragments.lead_trail_expr
        == "SUM(CASE WHEN \"CUSTOMER_NAME\" IS NOT NULL AND \"CUSTOMER_NAME\"::STRING != TRIM(\"CUSTOMER_NAME\"::STRING) THEN 1 ELSE 0 END) AS LEAD_TRAIL_WS_ROWS"
    )


def test_string_numeric_date_fragment_is_stable():
    qcol = '"CUSTOMER_NAME"'
    fragments = _string_column_sql_fragments(qcol)
    expected = (
        "CASE WHEN LENGTH(REGEXP_REPLACE(\"CUSTOMER_NAME\"::STRING, '[^0-9]', '')) = 8 "
        "AND REGEXP_REPLACE(\"CUSTOMER_NAME\"::STRING, '[^0-9]', '') NOT IN ('00000000') "
        "THEN TRY_TO_DATE(REGEXP_REPLACE(\"CUSTOMER_NAME\"::STRING, '[^0-9]', ''), 'YYYYMMDD') ELSE NULL END"
    )
    assert fragments.numeric_date_expr == expected


def test_avg_length_metric_uses_case_guard():
    qcol = '"CUSTOMER_NAME"'
    length_expr = _column_length_expression(qcol, is_string=True, is_numeric=False)
    assert length_expr == "LENGTH(\"CUSTOMER_NAME\"::STRING)"
    metric = _avg_length_metric(qcol, length_expr)
    assert (
        metric
        == "AVG(CASE WHEN \"CUSTOMER_NAME\" IS NOT NULL THEN LENGTH(\"CUSTOMER_NAME\"::STRING) END) AS AVG_LEN"
    )


def test_checkdef_whitespace_internal_only_pattern():
    sql, is_agg = build_rule_for_column_check(
        "DB.SCHEMA.TABLE",
        "customer_name",
        "WHITESPACE",
        {"mode": "NO_INTERNAL_ONLY_WHITESPACE"},
    )
    assert not is_agg
    assert (
        sql
        == "(\"customer_name\" IS NULL OR REGEXP_REPLACE(\"customer_name\", '\\s+', ' ') = \"customer_name\")"
    )
