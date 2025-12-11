CREATE TABLE IF NOT EXISTS DQ_RUN_RESULTS (
    RUN_ID STRING,
    CONFIG_ID STRING,
    CHECK_ID STRING,
    CHECK_TYPE STRING,
    RUN_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FAILURES NUMBER,
    OK BOOLEAN,
    ERROR_MSG STRING
);

CREATE TABLE IF NOT EXISTS ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RUN_RESULT_DETAIL (
    RUN_ID STRING,
    CONFIG_ID STRING,
    TABLE_FQN STRING,
    COLUMN_NAME STRING,
    RULE_CODE STRING,
    RULE_CATEGORY STRING,
    STATUS STRING,
    FAILURE_COUNT NUMBER,
    DETAIL VARIANT,
    EXECUTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    SQL_SNIPPET STRING
);

CREATE OR REPLACE PROCEDURE DQ_RUN_CONFIG(CONFIG_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_dq_config'
EXECUTE AS CALLER
AS
$$
import json
import uuid
from typing import Any, Dict
from snowflake.snowpark import Session

AGG_PREFIX = "AGG:"


def _safe_json_loads(text):
    if not text:
        return None
    if not isinstance(text, str):
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _compiled_from_json(text):
    """Best-effort extraction of ``compiled_predicate`` from a JSON payload."""

    if not isinstance(text, str):
        return None

    trimmed = text.strip()
    if not trimmed or "compiled_predicate" not in trimmed:
        return None

    try:
        parsed = json.loads(trimmed)
    except Exception:
        return None

    if isinstance(parsed, dict):
        return parsed.get("compiled_predicate")

    if isinstance(parsed, str):
        try:
            nested = json.loads(parsed)
        except Exception:
            return None
        if isinstance(nested, dict):
            return nested.get("compiled_predicate")

    return None


def _extract_predicate(compiled_rule, rule_expr_raw):
    """
    Derive the predicate string from compiled_rule (preferred) or rule_expr_raw (fallback).
    Supports VARIANT JSON objects produced by the compiler as well as legacy string rules.
    """

    predicate = None

    if compiled_rule is not None:
        if isinstance(compiled_rule, str):
            predicate = _compiled_from_json(compiled_rule) or compiled_rule
        elif isinstance(compiled_rule, dict):
            predicate = compiled_rule.get("compiled_predicate")
        elif hasattr(compiled_rule, "get"):
            # Defensive fallback for objects that expose mapping-style access
            predicate = compiled_rule.get("compiled_predicate")

    if not predicate:
        predicate = _compiled_from_json(rule_expr_raw) or rule_expr_raw

    return (predicate or "").strip()


def _normalize_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().upper()
    if text in {"TRUE", "T", "YES", "Y", "1"}:
        return True
    return False


def _rowcount_anomaly_stats(session: Session, table_fqn: str, params: Dict[str, Any]):
    if not session or not table_fqn or not params:
        return None

    ts_col = (params.get("timestamp_column") or "").strip()
    if not ts_col:
        return None

    lookback_days = params.get("lookback_days") or 0
    sensitivity = params.get("sensitivity") or 0
    min_history_days = params.get("min_history_days") or 0

    sql = f"""
        WITH DAY_COUNTS AS (
            SELECT DATE_TRUNC('DAY', {ts_col}) AS DAY_KEY, COUNT(*) AS ROW_COUNT
            FROM {table_fqn}
            WHERE {ts_col} IS NOT NULL
            GROUP BY 1
        ),
        LATEST_DAY AS (
            SELECT DAY_KEY, ROW_COUNT
            FROM DAY_COUNTS
            QUALIFY ROW_NUMBER() OVER (ORDER BY DAY_KEY DESC) = 1
        ),
        WINDOWED AS (
            SELECT d.ROW_COUNT
            FROM DAY_COUNTS d
            CROSS JOIN (SELECT COALESCE(MAX(DAY_KEY), CURRENT_DATE()) AS MAX_DAY FROM DAY_COUNTS) m
            WHERE d.DAY_KEY >= DATEADD('DAY', -?, m.MAX_DAY)
        ),
        METRICS AS (
            SELECT
                (SELECT ROW_COUNT FROM LATEST_DAY) AS OBSERVED,
                AVG(ROW_COUNT) AS MEAN_VAL,
                STDDEV_SAMP(ROW_COUNT) AS STDDEV_VAL,
                COUNT(*) AS HISTORY_POINTS
            FROM WINDOWED
        )
        SELECT
            OBSERVED,
            MEAN_VAL,
            STDDEV_VAL,
            HISTORY_POINTS,
            (MEAN_VAL - (? * COALESCE(STDDEV_VAL, 0))) AS EXPECTED_MIN,
            (MEAN_VAL + (? * COALESCE(STDDEV_VAL, 0))) AS EXPECTED_MAX
        FROM METRICS
    """

    try:
        stats_row = session.sql(sql, params=[lookback_days, sensitivity, sensitivity]).collect()
    except Exception:
        return None

    if not stats_row:
        return None

    data = stats_row[0].as_dict()
    observed = data.get("OBSERVED")
    mean_val = data.get("MEAN_VAL")
    stddev_val = data.get("STDDEV_VAL")
    history_points = data.get("HISTORY_POINTS")
    expected_min = data.get("EXPECTED_MIN")
    expected_max = data.get("EXPECTED_MAX")

    if history_points is None or history_points < max(1, int(min_history_days or 0)):
        return {
            "observed": observed,
            "mean": mean_val,
            "stddev": stddev_val,
            "expected_min": expected_min,
            "expected_max": expected_max,
            "history_points": history_points,
            "ok": None,
        }

    ok = None
    if observed is not None and expected_min is not None and expected_max is not None:
        ok = expected_min <= observed <= expected_max

    return {
        "observed": observed,
        "mean": mean_val,
        "stddev": stddev_val,
        "expected_min": expected_min,
        "expected_max": expected_max,
        "history_points": history_points,
        "ok": ok,
    }


def run_dq_config(session: Session, CONFIG_ID: str) -> str:
    run_id = str(uuid.uuid4())
    rule_categories = {}
    try:
        for row in session.sql(
            """
            SELECT RULE_CODE, CATEGORY
            FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY
            WHERE RULE_CODE IS NOT NULL
            """
        ).collect():
            data = row.as_dict()
            rule_categories[data.get("RULE_CODE")] = data.get("CATEGORY")
    except Exception:
        rule_categories = {}
    checks = session.sql(
        """
        SELECT CONFIG_ID, CHECK_ID, CHECK_TYPE, TABLE_FQN, COLUMN_NAME, RULE_EXPR, RULE_CODE, RULE_PARAMS, PARAMS_JSON, COMPILED_RULE
        FROM DQ_CHECK
        WHERE CONFIG_ID = ?
        ORDER BY CHECK_ID
        """,
        params=[CONFIG_ID],
    ).collect()

    total_checks = 0

    for row in checks:
        total_checks += 1
        data = row.as_dict()
        check_id = data.get("CHECK_ID")
        check_type = (data.get("CHECK_TYPE") or "").strip()
        table_fqn = data.get("TABLE_FQN")
        column_name = data.get("COLUMN_NAME")
        rule_expr_raw = (data.get("RULE_EXPR") or "").strip()
        rule_code = (data.get("RULE_CODE") or check_type or "").strip()
        rule_category = rule_categories.get(rule_code)
        params = (
            _safe_json_loads(data.get("RULE_PARAMS"))
            or _safe_json_loads(data.get("PARAMS_JSON"))
            or {}
        )
        compiled_rule = data.get("COMPILED_RULE")
        rule_expr_upper = rule_expr_raw.upper()

        ok = False
        failures = 0
        error_msg = None
        detail = {
            "rule_code": rule_code,
            "rule_category": rule_category,
            "parameters": params,
            "observed": None,
            "expected_min": None,
            "expected_max": None,
            "mean": None,
            "stddev": None,
            "samples": [],
        }

        agg_sql = None
        failure_sql = None
        try:
            is_agg = rule_expr_upper.startswith(AGG_PREFIX) or check_type.upper().startswith("AGG")
            if is_agg:
                agg_raw = rule_expr_raw[len(AGG_PREFIX):] if rule_expr_upper.startswith(AGG_PREFIX) else rule_expr_raw
                agg_sql = (agg_raw or "").strip()
                if agg_sql.endswith(';'):
                    agg_sql = agg_sql[:-1]
                agg_sql = agg_sql.strip()
                upper_sql = agg_sql.upper()
                if 'FROM ' in upper_sql and agg_sql.strip().upper().endswith('FROM'):
                    raise ValueError(f"Incomplete SQL (ends after FROM). SQL[:200]={agg_sql[:200]}")
                if agg_sql.count('"') % 2 != 0:
                    raise ValueError(f"Unbalanced double quotes in AGG SQL. SQL[:200]={agg_sql[:200]}")
                result_rows = session.sql(agg_sql).collect()
                first_value = result_rows[0][0] if result_rows else False
                ok = _normalize_bool(first_value)
                failures = 0 if ok else 1
            elif rule_code.upper() == "TABLE_ROWCOUNT_ANOMALY":
                stats = _rowcount_anomaly_stats(session, table_fqn, params)
                if stats:
                    detail.update({
                        "observed": stats.get("observed"),
                        "expected_min": stats.get("expected_min"),
                        "expected_max": stats.get("expected_max"),
                        "mean": stats.get("mean"),
                        "stddev": stats.get("stddev"),
                        "history_points": stats.get("history_points"),
                    })
                    ok = stats.get("ok") if stats.get("ok") is not None else False
                    failures = 0 if ok else 1
                else:
                    failures = 1
                    ok = False
            else:
                if not table_fqn:
                    raise ValueError("TABLE_FQN is required for row checks")
                predicate = _extract_predicate(compiled_rule, rule_expr_raw)
                if not predicate:
                    raise ValueError("Predicate is required for row checks")
                failure_sql = f"SELECT COUNT(*) AS FAILURES FROM {table_fqn} AS T WHERE NOT ({predicate})"
                failure_count = session.sql(failure_sql).collect()[0][0]
                failures = int(failure_count or 0)
                ok = failures == 0
                detail["observed"] = failures
                detail["expected_min"] = 0
                detail["expected_max"] = 0
                if failures > 0 and column_name:
                    try:
                        sample_sql = f"SELECT {column_name} FROM {table_fqn} AS T WHERE NOT ({predicate}) LIMIT 5"
                        sample_rows = session.sql(sample_sql).collect()
                        detail["samples"] = [r[0] for r in sample_rows]
                    except Exception:
                        detail["samples"] = []
        except Exception as exc:
            ok = False
            failures = 0
            context_sql = agg_sql or failure_sql
            if context_sql:
                error_msg = f"{exc} | SQL[:200]={context_sql[:200]}"
            else:
                error_msg = str(exc)

        if not ok and not error_msg:
            context_sql = agg_sql or failure_sql or rule_expr_raw
            detail_msg = context_sql[:200] if context_sql else ""
            error_msg = (
                f"{check_type or 'Check'} evaluated to FALSE"
                + (f" | SQL[:200]={detail_msg}" if detail_msg else "")
            )

        session.sql(
            """
            INSERT INTO DQ_RUN_RESULTS (RUN_ID, CONFIG_ID, CHECK_ID, CHECK_TYPE, RUN_TS, FAILURES, OK, ERROR_MSG)
            SELECT ?, ?, ?, ?, CURRENT_TIMESTAMP(), ?, ?, ?
            """,
            params=[run_id, CONFIG_ID, check_id, check_type, failures, ok, error_msg],
        ).collect()

        context_sql = agg_sql or failure_sql or rule_expr_raw
        status_text = "PASS" if ok else "FAIL"
        try:
            session.sql(
                """
                INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RUN_RESULT_DETAIL (
                    RUN_ID, CONFIG_ID, TABLE_FQN, COLUMN_NAME, RULE_CODE, RULE_CATEGORY, STATUS, FAILURE_COUNT, DETAIL, EXECUTED_AT, SQL_SNIPPET
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), CURRENT_TIMESTAMP(), ?
                """,
                params=[
                    run_id,
                    CONFIG_ID,
                    table_fqn,
                    column_name,
                    rule_code,
                    rule_category,
                    status_text,
                    failures,
                    json.dumps(detail, default=str),
                    context_sql[:500] if context_sql else None,
                ],
            ).collect()
        except Exception:
            # Diagnostics should not break the main run log
            pass

    return f"OK run_id={run_id} checks={total_checks}"
$$;

CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.SP_DQ_MANAGE_TASK(
    TARGET_DB STRING,
    TARGET_SCHEMA STRING,
    TASK_WAREHOUSE STRING,
    CONFIG_ID STRING,
    PROC_NAME STRING,
    CRON STRING,
    TZ STRING,
    ENABLE BOOLEAN
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
DECLARE
    v_db            STRING   DEFAULT COALESCE(TARGET_DB, '');
    v_schema        STRING   DEFAULT COALESCE(TARGET_SCHEMA, '');
    v_wh            STRING   DEFAULT COALESCE(TASK_WAREHOUSE, '');
    v_config        STRING   DEFAULT COALESCE(CONFIG_ID, '');
    v_proc          STRING   DEFAULT COALESCE(PROC_NAME, '');
    v_cron          STRING   DEFAULT COALESCE(CRON, '');
    v_tz            STRING   DEFAULT COALESCE(TZ, '');
    v_enable        BOOLEAN  DEFAULT COALESCE(ENABLE, FALSE);
    v_safe_config   STRING;
    v_task_name     STRING;
    v_db_ident      STRING;
    v_schema_ident  STRING;
    v_wh_ident      STRING;
    v_task_fqn      STRING;
    v_proc_fqn      STRING;
    v_sched         STRING;
    v_comment       STRING;
BEGIN
    v_db := TRIM(v_db);
    v_schema := TRIM(v_schema);
    v_wh := TRIM(v_wh);
    v_proc := TRIM(v_proc);
    v_cron := TRIM(v_cron);
    v_tz := TRIM(v_tz);

    IF (v_db = '') THEN
        RETURN 'ERROR: TARGET_DB is required';
    END IF;
    IF (v_schema = '') THEN
        RETURN 'ERROR: TARGET_SCHEMA is required';
    END IF;
    IF (v_wh = '') THEN
        RETURN 'ERROR: TASK_WAREHOUSE is required';
    END IF;
    IF (v_proc = '') THEN
        RETURN 'ERROR: PROC_NAME is required';
    END IF;
    IF (v_cron = '') THEN
        RETURN 'ERROR: CRON is required';
    END IF;
    IF (v_tz = '') THEN
        RETURN 'ERROR: TZ is required';
    END IF;

    v_db_ident := '"' || REPLACE(v_db, '"', '""') || '"';
    v_schema_ident := '"' || REPLACE(v_schema, '"', '""') || '"';
    v_wh_ident := '"' || REPLACE(v_wh, '"', '""') || '"';

    v_safe_config := REGEXP_REPLACE(UPPER(v_config), '[^A-Z0-9_]', '_');
    v_safe_config := REGEXP_REPLACE(v_safe_config, '_+', '_');
    v_safe_config := TRIM(v_safe_config, '_');
    IF (v_safe_config = '') THEN
        v_safe_config := 'X';
    END IF;
    v_task_name := 'DQ_TASK_' || v_safe_config;

    v_task_fqn := v_db_ident || '.' || v_schema_ident || '."' || REPLACE(v_task_name, '"', '""') || '"';
    v_proc_fqn := v_db_ident || '.' || v_schema_ident || '."' || REPLACE(v_proc, '"', '""') || '"';

    v_sched := 'USING CRON ' || v_cron || ' ' || v_tz;
    v_comment := 'Auto task for DQ config ' || v_config;

    EXECUTE IMMEDIATE
        'CREATE TASK IF NOT EXISTS ' || v_task_fqn || CHR(10) ||
        '  WAREHOUSE = ' || v_wh_ident || CHR(10) ||
        '  SCHEDULE  = ' || '''' || REPLACE(v_sched, '''', '''''') || '''' || CHR(10) ||
        '  COMMENT   = ' || '''' || REPLACE(v_comment, '''', '''''') || '''' || CHR(10) ||
        'AS CALL ' || v_proc_fqn || '(''' || REPLACE(v_config, '''', '''''') || ''')';

    EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' SUSPEND';

    EXECUTE IMMEDIATE
        'ALTER TASK ' || v_task_fqn || ' SET ' ||
        'WAREHOUSE = ' || v_wh_ident || ', ' ||
        'SCHEDULE  = ' || '''' || REPLACE(v_sched, '''', '''''') || '''' || ', ' ||
        'COMMENT   = ' || '''' || REPLACE(v_comment, '''', '''''') || '''';

    IF (v_enable) THEN
        EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' RESUME';
    END IF;

    RETURN 'OK: TASK ' || v_task_fqn || ' UPDATED';
END;
$$;

-- Grant execute permissions on the task management procedure to the application role.
-- GRANT EXECUTE ON PROCEDURE "ZEUS_ANALYTICS_SIMU"."DISCOVERY"."SP_DQ_MANAGE_TASK"(STRING, STRING, STRING, STRING, STRING, STRING, STRING, BOOLEAN) TO ROLE <app_role>;
