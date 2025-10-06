from typing import Any, Dict
from utils.meta import _q

def ensure_task_for_config(session, config) -> Dict[str, Any]:
    task_name = f"DQ_TASK_{config.config_id}"
    sql_body = f"CALL RUN_DQ_CONFIG('{config.config_id}')"
    try:
        session.sql(f"""
            CREATE OR REPLACE TASK {_q(task_name)}
            WAREHOUSE = IDENTIFIER(CURRENT_WAREHOUSE())
            SCHEDULE = USING CRON 0 8 * * * Europe/Berlin
            AS {sql_body}
        """).collect()
        session.sql(f"ALTER TASK {_q(task_name)} RESUME").collect()
        return {"status":"TASK_CREATED","task":task_name}
    except Exception as e:
        return {"status":"FALLBACK","reason":str(e),"task":task_name}
