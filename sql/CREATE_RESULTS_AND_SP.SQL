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
