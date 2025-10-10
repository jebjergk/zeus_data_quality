CREATE OR REPLACE PROCEDURE SP_RUN_DQ_CONFIG(CONFIG_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from uuid import uuid4

def _first_bool(row):
    if hasattr(row, "asDict"):
        v = list(row.asDict().values())[0]
    else:
        v = row[0]
    return bool(v)

def run(session, CONFIG_ID: str) -> str:
    DQ_CONFIG_TBL = "DQ_CONFIG"
    DQ_CHECK_TBL  = "DQ_CHECK"
    DQ_RUN_RESULTS_TBL = "DQ_RUN_RESULTS"

    cfg = session.sql(
        f'SELECT CONFIG_ID, TARGET_TABLE_FQN FROM "{DQ_CONFIG_TBL}" WHERE CONFIG_ID = ?',
        params=[CONFIG_ID]
    ).collect()
    if not cfg:
        return f"CONFIG {CONFIG_ID} not found"

    rows = session.sql(f'''
        SELECT CHECK_ID, TABLE_FQN, RULE_EXPR, CHECK_TYPE
        FROM "{DQ_CHECK_TBL}"
        WHERE CONFIG_ID = ?
        ORDER BY CHECK_ID
    ''', params=[CONFIG_ID]).collect()

    run_id = str(uuid4())
    inserted = 0

    for r in rows:
        check_id   = r[0]
        table_fqn  = r[1]
        rule_expr  = (r[2] or '').strip()
        check_type = r[3]

        ok = None
        failures = None
        err = None
        try:
            if rule_expr.upper().startswith('AGG:'):
                sql = rule_expr[4:].strip()
                out = session.sql(sql).collect()
                ok = _first_bool(out[0]) if out else False
                failures = 0 if ok else 1
            else:
                cnt = session.sql(
                    f'SELECT COUNT(*) FROM {table_fqn} WHERE NOT ({rule_expr})'
                ).collect()[0][0]
                failures = int(cnt)
                ok = (failures == 0)
        except Exception as e:
            ok = False
            failures = None
            err = str(e)[:1000]

        session.sql(f'''
            INSERT INTO "{DQ_RUN_RESULTS_TBL}"
            (RUN_ID, CONFIG_ID, CHECK_ID, CHECK_TYPE, RUN_TS, FAILURES, OK, ERROR_MSG)
            SELECT ?,        ?,         ?,        ?,         CURRENT_TIMESTAMP(), ?,        ?,  ?
        ''', params=[run_id, CONFIG_ID, check_id, check_type, failures, ok, err]).collect()
        inserted += 1

    return f"OK run_id={run_id} checks={inserted}"
$$;
