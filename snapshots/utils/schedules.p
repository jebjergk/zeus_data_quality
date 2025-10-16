from typing import Any, Dict

from utils.configs import get_metadata_namespace, get_proc_name
from utils.dmfs import DEFAULT_WAREHOUSE, task_name_for_config, _q
from utils.meta import _parse_relation_name

PROC_NAME = get_proc_name()


def ensure_task_for_config(session, cfg) -> Dict[str, Any]:
    base_task_name = task_name_for_config(cfg.config_id)

    if not session:
        return {"status": "FALLBACK", "reason": "Missing session", "task": base_task_name}

    if not getattr(cfg, "schedule_enabled", True):
        return {"status": "SCHEDULE_DISABLED", "task": base_task_name}

    target_fqn = getattr(cfg, "target_table_fqn", "")
    try:
        db, schema, _ = _parse_relation_name(target_fqn)
    except Exception as exc:  # pragma: no cover - defensive branch
        return {
            "status": "FALLBACK",
            "reason": f"Invalid target table FQN: {exc}",
            "task": base_task_name,
        }

    if not db or not schema:
        return {
            "status": "FALLBACK",
            "reason": "Target table must include database and schema",
            "task": base_task_name,
        }

    warehouse = DEFAULT_WAREHOUSE

    schedule_cron = (getattr(cfg, "schedule_cron", None) or "0 8 * * *").strip() or "0 8 * * *"
    schedule_tz = (getattr(cfg, "schedule_timezone", None) or "Europe/Berlin").strip() or "Europe/Berlin"

    meta_db, meta_schema = get_metadata_namespace()

    task_fqn = _q(meta_db, meta_schema, base_task_name)

    try:
        result = session.sql(
            f'CALL {_q(meta_db, meta_schema, "SP_DQ_MANAGE_TASK")}(?, ?, ?, ?, ?, ?, ?, ?)',
            params=[
                meta_db,
                meta_schema,
                warehouse,
                str(cfg.config_id),
                PROC_NAME,
                schedule_cron,
                schedule_tz,
                True,
            ],
        ).collect()
    except Exception as exc:
        return {"status": "FALLBACK", "reason": str(exc), "task": task_fqn}

    if not result:
        return {
            "status": "FALLBACK",
            "reason": "Task management procedure returned no result",
            "task": task_fqn,
        }

    message = str(result[0][0])
    if message.upper().startswith("ERROR:"):
        return {"status": "FALLBACK", "reason": message, "task": task_fqn}

    return {"status": "TASK_CREATED", "task": task_fqn}
