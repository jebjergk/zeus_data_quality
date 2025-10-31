import copy

import pytest

from services import profiling
from utils import checkdefs


@pytest.fixture
def string_profile_sql(monkeypatch):
    captured_sql = []
    metrics_row = _build_metrics_row()

    def fake_collect(_session, sql, params=None):
        captured_sql.append(sql)
        if "COUNT(*) AS CNT" in sql:
            return {"CNT": 10}
        return copy.deepcopy(metrics_row)

    monkeypatch.setattr(profiling, "_collect_single_row", fake_collect)
    monkeypatch.setattr(
        profiling,
        "list_columns",
        lambda *_args, **_kwargs: [
            {"column_name": "STR_COL", "data_type": "VARCHAR"}
        ],
    )
    monkeypatch.setattr(
        profiling,
        "_load_reference_sets",
        lambda *_args, **_kwargs: {
            "country_codes": {"US"},
            "country_names": {"UNITED STATES"},
            "currency_codes": {"USD"},
            "exchange_codes": {"NYSE"},
        },
    )

    profiling.run_table_profile(object(), "DB.SCHEMA.TABLE", sample_pct=None, top_n=0)

    return next(sql for sql in captured_sql if "AVG_LEN" in sql)


def _build_metrics_row():
    row = {
        "ROW_CNT": 10,
        "NULLS": 0,
        "DISTINCTS": 5,
        "NON_NULLS_COUNT": 10,
        "MIN_VAL": "a",
        "MAX_VAL": "z",
        "EMPTY_STR_ROWS": 0,
        "WS_ONLY_ROWS": 0,
        "WHITESPACE_ROWS": 0,
        "LEAD_TRAIL_WS_ROWS": 0,
        "NUMDATE_PARSE_COUNT": 0,
        "NUMDATE_MIN": None,
        "NUMDATE_MAX": None,
        "AVG_LEN": 5.0,
        "LEN_MIN": 1.0,
        "LEN_MAX": 9.0,
        "LEN_STDDEV": 2.0,
        "STRING_DATE_SENTINELS": 0,
        "STRING_NUMERIC_LIKE_ROWS": 0,
        "STRING_DATE_PARSE_ANY": 0,
        "STRING_DATE_PARSE_ANY_MIN": None,
        "STRING_DATE_PARSE_ANY_MAX": None,
    }

    for regex_key in profiling.SEMANTIC_REGEX_PATTERNS:
        row[f"REGEX_{regex_key.upper()}_MATCHES"] = 0
    for char_key in profiling.CHAR_CLASS_PATTERNS:
        row[f"CHAR_{char_key.upper()}_MATCHES"] = 0
    for ref_key in ("country_codes", "country_names", "currency_codes", "exchange_codes"):
        row[f"REF_{ref_key.upper()}_MATCHES"] = 0
    for config in profiling.DATE_PARSE_CONFIGS:
        alias_key = (
            config["key"].upper().replace("-", "_").replace("/", "_").replace(".", "_")
        )
        row[f"STRING_DATE_PARSE_{alias_key}"] = 0
        row[f"STRING_DATE_MIN_{alias_key}"] = None
        row[f"STRING_DATE_MAX_{alias_key}"] = None
        if config.get("regex"):
            row[f"STRING_DATE_PATTERN_{alias_key}"] = 0
    return row


def test_profile_sql_contains_whitespace_detectors(string_profile_sql):
    assert "SUM(CASE WHEN \"STR_COL\"::STRING = '' THEN 1 ELSE 0 END) AS EMPTY_STR_ROWS" in string_profile_sql
    assert 'REGEXP_LIKE("STR_COL"::STRING, \'^\\s+$\')' in string_profile_sql
    assert 'REGEXP_LIKE("STR_COL"::STRING, \'^\\s|\\s$|\\s{2,}\')' in string_profile_sql
    assert '"STR_COL"::STRING != TRIM("STR_COL"::STRING)' in string_profile_sql


def test_profile_sql_contains_date_guard_case(string_profile_sql):
    expected_case = (
        "CASE WHEN TRIM(\"STR_COL\"::STRING) IN ('0', '00000000', '0000-00-00', '0000/00/00') "
        "THEN NULL ELSE \"STR_COL\"::STRING END"
    )
    assert expected_case in string_profile_sql


def test_profile_sql_contains_avg_length_case(string_profile_sql):
    assert (
        "AVG(CASE WHEN \"STR_COL\" IS NOT NULL THEN LENGTH(\"STR_COL\"::STRING) END) AS AVG_LEN"
        in string_profile_sql
    )


def test_column_check_whitespace_modes():
    expr, is_agg = checkdefs.build_rule_for_column_check(
        "DB.SCHEMA.TBL", "my_col", "WHITESPACE", {"mode": "NO_LEADING_TRAILING"}
    )
    assert not is_agg
    assert expr == '("my_col" IS NULL OR "my_col" = TRIM("my_col"))'

    expr, _ = checkdefs.build_rule_for_column_check(
        "DB.SCHEMA.TBL", "my_col", "WHITESPACE", {"mode": "NO_INTERNAL_ONLY_WHITESPACE"}
    )
    assert expr == '("my_col" IS NULL OR REGEXP_REPLACE("my_col", \'\\s+\', \' \') = "my_col")'

    expr, _ = checkdefs.build_rule_for_column_check(
        "DB.SCHEMA.TBL", "my_col", "WHITESPACE", {"mode": "NON_EMPTY"}
    )
    assert expr == '("my_col" IS NOT NULL AND LENGTH(TRIM("my_col")) > 0)'
