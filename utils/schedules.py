# utils/schedules.py
from typing import Any, Dict, Optional, Tuple

from utils.meta import _q


def _target_db_schema(config) -> Optional[Tuple[str, str]]:
    """Extract database and schema from the config's target table."""
    target = getattr(config, "target_table_fqn", None)
    if not target:
        return None
    parts = [p.strip().strip('"') for p in str(target).split('.') if p.strip()]
    if len(parts) >= 3:
        return parts[-3], parts[-2]
    return None


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
    base_task_name = f"DQ_TASK_{config.config_id}"
    target_db_schema = _target_db_schema(config)
    if not target_db_schema:
        return {
            "status": "INVALID_TARGET",
            "task": base_task_name,
            "reason": "Target table must include database and schema",
        }
    db, schema = target_db_schema
    if not db or not schema:
        return {
            "status": "INVALID_TARGET",
            "task": base_task_name,
            "reason": "Target table must include database and schema",
        }
    task_identifier = f"{db}.{schema}.{base_task_name}"
    quoted_task_name = _q(task_identifier)
    schedule_enabled = getattr(config, "schedule_enabled", True)
    if not schedule_enabled:
        return {"status": "SCHEDULE_DISABLED", "task": quoted_task_name}

    cron = (getattr(config, "schedule_cron", None) or "0 8 * * *").strip()
    timezone = (getattr(config, "schedule_timezone", None) or "Europe/Berlin").strip()
    if not cron:
        cron = "0 8 * * *"
    if not timezone:
        timezone = "Europe/Berlin"

    for value in (cron, timezone):
        if any(ch in value for ch in "'\";\n\r"):
            return {"status": "INVALID_SCHEDULE", "task": quoted_task_name, "reason": "Unsafe schedule characters"}

    warehouse = _current_warehouse(session)
    if not warehouse:
        return {"status": "NO_WAREHOUSE", "task": quoted_task_name}

    stored_proc = _q(f"{db}.{schema}.SP_RUN_DQ_CONFIG")
    sql_body = f"CALL {stored_proc}('{config.config_id}')"
    schedule_expression = f"USING CRON {cron} {timezone}"
    try:
        session.sql(f"""
            CREATE OR REPLACE TASK {quoted_task_name}
            WAREHOUSE = {_q(warehouse)}
            SCHEDULE = '{schedule_expression}'
            AS {sql_body}
        """).collect()
        session.sql(f"ALTER TASK {quoted_task_name} RESUME").collect()
        return {"status": "TASK_CREATED", "task": quoted_task_name}
    except Exception as e:
        return {"status": "FALLBACK", "reason": str(e), "task": quoted_task_name, "warehouse": warehouse}
