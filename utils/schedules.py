from typing import Any, Dict, Optional

from utils.meta import _q


def _current_warehouse(session) -> Optional[str]:
    """Return the currently active warehouse name, if any."""
    if not session:
        return None
    try:
        warehouse = session.get_current_warehouse()
        if warehouse:
            return warehouse
    except AttributeError:
        # Older Snowpark versions might not expose the helper; fall back to SQL.
        pass
    try:
        rows = session.sql("SELECT CURRENT_WAREHOUSE()").collect()
        if rows:
            # rows[0][0] works for Row / dict Row
            warehouse = rows[0][0] if not hasattr(rows[0], "asDict") else rows[0].asDict().get("CURRENT_WAREHOUSE()")
            return warehouse
    except Exception:
        return None
    return None


def ensure_task_for_config(session, config) -> Dict[str, Any]:
    task_name = f"DQ_TASK_{config.config_id}"
    warehouse = _current_warehouse(session)
    if not warehouse:
        return {"status": "NO_WAREHOUSE", "task": task_name}

    sql_body = f"CALL RUN_DQ_CONFIG('{config.config_id}')"
    try:
        session.sql(f"""
            CREATE OR REPLACE TASK {_q(task_name)}
            WAREHOUSE = {_q(warehouse)}
            SCHEDULE = USING CRON 0 8 * * * Europe/Berlin
            AS {sql_body}
        """).collect()
        session.sql(f"ALTER TASK {_q(task_name)} RESUME").collect()
        return {"status": "TASK_CREATED", "task": task_name}
    except Exception as e:
        return {"status": "FALLBACK", "reason": str(e), "task": task_name, "warehouse": warehouse}
