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

    EXECUTE IMMEDIATE 'USE DATABASE ' || v_db_ident;
    EXECUTE IMMEDIATE 'USE SCHEMA ' || v_schema_ident;
    EXECUTE IMMEDIATE 'USE WAREHOUSE ' || v_wh_ident;

    EXECUTE IMMEDIATE
        'CREATE TASK IF NOT EXISTS ' || v_task_fqn || CHR(10) ||
        '  WAREHOUSE = ' || v_wh_ident || CHR(10) ||
        '  SCHEDULE  = ' || '''' || REPLACE(v_sched, '''', '''''') || '''' || CHR(10) ||
        '  COMMENT   = ' || '''' || REPLACE(v_comment, '''', '''''') || '''' || CHR(10) ||
        'AS CALL ' || v_proc_fqn || '(''' || REPLACE(v_config, '''', '''''') || ''')';

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
