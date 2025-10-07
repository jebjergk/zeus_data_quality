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
    schedule_enabled = getattr(config, "schedule_enabled", True)
    if not schedule_enabled:
        return {"status": "SCHEDULE_DISABLED", "task": task_name}

    cron = (getattr(config, "schedule_cron", None) or "0 8 * * *").strip()
    timezone = (getattr(config, "schedule_timezone", None) or "Europe/Berlin").strip()
    if not cron:
        cron = "0 8 * * *"
    if not timezone:
        timezone = "Europe/Berlin"

    for value in (cron, timezone):
        if any(ch in value for ch in "'\";\n\r"):
            return {"status": "INVALID_SCHEDULE", "task": task_name, "reason": "Unsafe schedule characters"}

    warehouse = _current_warehouse(session)
    if not warehouse:
        return {"status": "NO_WAREHOUSE", "task": task_name}

    sql_body = f"CALL RUN_DQ_CONFIG('{config.config_id}')"
    try:
        session.sql(f"""
            CREATE OR REPLACE TASK {_q(task_name)}
            WAREHOUSE = {_q(warehouse)}
            SCHEDULE = USING CRON {cron} {timezone}
            AS {sql_body}
        """).collect()
        session.sql(f"ALTER TASK {_q(task_name)} RESUME").collect()
        return {"status": "TASK_CREATED", "task": task_name}
    except Exception as e:
        return {"status": "FALLBACK", "reason": str(e), "task": task_name, "warehouse": warehouse}
