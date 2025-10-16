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

CREATE OR REPLACE PROCEDURE DQ_RUN_CONFIG(CONFIG_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_dq_config'
AS
$$
import uuid
from snowflake.snowpark import Session

AGG_PREFIX = "AGG:"


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


def run_dq_config(session: Session, CONFIG_ID: str) -> str:
    run_id = str(uuid.uuid4())
    checks = session.sql(
        """
        SELECT CONFIG_ID, CHECK_ID, CHECK_TYPE, TABLE_FQN, RULE_EXPR
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
        rule_expr_raw = (data.get("RULE_EXPR") or "").strip()
        rule_expr_upper = rule_expr_raw.upper()

        ok = False
        failures = None
        error_msg = None

        try:
            is_agg = rule_expr_upper.startswith(AGG_PREFIX) or check_type.upper().startswith("AGG")
            if is_agg:
                agg_sql = rule_expr_raw[len(AGG_PREFIX):].strip() if rule_expr_upper.startswith(AGG_PREFIX) else rule_expr_raw
                result_rows = session.sql(agg_sql).collect()
                first_value = result_rows[0][0] if result_rows else False
                ok = _normalize_bool(first_value)
                failures = 0 if ok else 1
            else:
                if not table_fqn:
                    raise ValueError("TABLE_FQN is required for row checks")
                predicate = rule_expr_raw
                if not predicate:
                    raise ValueError("RULE_EXPR is required for row checks")
                failure_sql = f"SELECT COUNT(*) AS FAILURES FROM {table_fqn} WHERE NOT ({predicate})"
                failure_count = session.sql(failure_sql).collect()[0][0]
                failures = int(failure_count or 0)
                ok = failures == 0
        except Exception as exc:
            ok = False
            failures = None
            error_msg = str(exc)

        session.sql(
            """
            INSERT INTO DQ_RUN_RESULTS (RUN_ID, CONFIG_ID, CHECK_ID, CHECK_TYPE, RUN_TS, FAILURES, OK, ERROR_MSG)
            SELECT ?, ?, ?, ?, CURRENT_TIMESTAMP(), ?, ?, ?
            """,
            params=[run_id, CONFIG_ID, check_id, check_type, failures, ok, error_msg],
        ).collect()

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
EXECUTE AS OWNER
AS
$$
BEGIN
    LET v_db STRING := COALESCE(TARGET_DB, '');
    LET v_schema STRING := COALESCE(TARGET_SCHEMA, '');
    LET v_wh STRING := COALESCE(TASK_WAREHOUSE, '');
    LET v_config STRING := COALESCE(CONFIG_ID, '');
    LET v_proc STRING := COALESCE(PROC_NAME, '');
    LET v_cron STRING := COALESCE(CRON, '');
    LET v_tz STRING := COALESCE(TZ, '');
    LET v_enable BOOLEAN := COALESCE(ENABLE, FALSE);
    LET v_safe_config STRING := NULL;
    LET v_task_name STRING := NULL;
    LET v_task_fqn STRING := NULL;
    LET v_proc_fqn STRING := NULL;
    LET v_sched_text STRING := NULL;
    LET v_sched_literal STRING := NULL;
    LET v_comment_text STRING := NULL;
    LET v_comment_literal STRING := NULL;
    LET v_config_literal STRING := NULL;
    LET v_db_ident STRING := NULL;
    LET v_schema_ident STRING := NULL;
    LET v_wh_ident STRING := NULL;
    v_db := TRIM(v_db);
    v_schema := TRIM(v_schema);
    v_wh := TRIM(v_wh);
    v_proc := TRIM(v_proc);
    v_cron := TRIM(v_cron);
    v_tz := TRIM(v_tz);
    IF v_db = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'TARGET_DB is required';
    END IF;
    IF v_schema = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'TARGET_SCHEMA is required';
    END IF;
    IF v_wh = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'TASK_WAREHOUSE is required';
    END IF;
    IF v_proc = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'PROC_NAME is required';
    END IF;
    IF v_cron = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'CRON schedule is required';
    END IF;
    IF v_tz = '' THEN
        RAISE STATEMENT_ERROR WITH MESSAGE = 'Time zone is required';
    END IF;

    v_safe_config := REGEXP_REPLACE(UPPER(v_config), '[^A-Z0-9_]', '_');
    v_safe_config := REGEXP_REPLACE(v_safe_config, '_+', '_');
    v_safe_config := TRIM(BOTH '_' FROM v_safe_config);
    IF v_safe_config = '' THEN
        v_safe_config := 'X';
    END IF;

    v_task_name := 'DQ_TASK_' || v_safe_config;

    v_db_ident := '"' || REPLACE(v_db, '"', '""') || '"';
    v_schema_ident := '"' || REPLACE(v_schema, '"', '""') || '"';
    v_wh_ident := '"' || REPLACE(v_wh, '"', '""') || '"';

    v_task_fqn := v_db_ident || '.' || v_schema_ident || '.' || '"' || REPLACE(v_task_name, '"', '""') || '"';
    v_proc_fqn := v_db_ident || '.' || v_schema_ident || '.' || '"' || REPLACE(v_proc, '"', '""') || '"';

    v_sched_text := 'USING CRON ' || v_cron || ' ' || v_tz;
    v_sched_literal := '''' || REPLACE(v_sched_text, '''', '''''') || '''';
    v_comment_text := 'Auto task for DQ config ' || v_config;
    v_comment_literal := '''' || REPLACE(v_comment_text, '''', '''''') || '''';
    v_config_literal := '''' || REPLACE(v_config, '''', '''''') || '''';

    EXECUTE IMMEDIATE 'USE DATABASE ' || v_db_ident;
    EXECUTE IMMEDIATE 'USE SCHEMA ' || v_schema_ident;
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || v_wh_ident;

    EXECUTE IMMEDIATE
        'CREATE TASK IF NOT EXISTS ' || v_task_fqn || CHR(10) ||
        '  WAREHOUSE = ' || v_wh_ident || CHR(10) ||
        '  SCHEDULE  = ' || v_sched_literal || CHR(10) ||
        '  COMMENT   = ' || v_comment_literal || CHR(10) ||
        'AS' || CHR(10) ||
        '  CALL ' || v_proc_fqn || '(' || v_config_literal || ')';

    EXECUTE IMMEDIATE
        'ALTER TASK ' || v_task_fqn || ' SET' || CHR(10) ||
        '  WAREHOUSE = ' || v_wh_ident || ',' || CHR(10) ||
        '  SCHEDULE  = ' || v_sched_literal || ',' || CHR(10) ||
        '  COMMENT   = ' || v_comment_literal;

    IF v_enable THEN
        EXECUTE IMMEDIATE 'ALTER TASK ' || v_task_fqn || ' RESUME';
    END IF;

    RETURN 'TASK ' || v_task_fqn || ' UPDATED';
END;
$$;

-- Grant execute permissions on the task management procedure to the application role.
-- GRANT EXECUTE ON PROCEDURE "ZEUS_ANALYTICS_SIMU"."DISCOVERY"."SP_DQ_MANAGE_TASK"(STRING, STRING, STRING, STRING, STRING, STRING, STRING, BOOLEAN) TO ROLE <app_role>;
