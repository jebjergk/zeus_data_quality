from typing import Any, Dict

from utils.dmfs import create_or_update_task, task_name_for_config
from utils.meta import _parse_relation_name, metadata_db_schema


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

    try:
        warehouse = session.get_current_warehouse()
    except Exception:  # pragma: no cover - defensive branch
        warehouse = None

    if not warehouse:
        return {"status": "NO_WAREHOUSE", "task": base_task_name}

    schedule_cron = (getattr(cfg, "schedule_cron", None) or "0 8 * * *").strip() or "0 8 * * *"
    schedule_tz = (getattr(cfg, "schedule_timezone", None) or "Europe/Berlin").strip() or "Europe/Berlin"

    try:
        meta_db, meta_schema = metadata_db_schema(session)
    except Exception as exc:
        return {
            "status": "FALLBACK",
            "reason": f"Unable to determine metadata schema: {exc}",
            "task": base_task_name,
        }

    try:
        create_or_update_task(
            session,
            meta_db,
            meta_schema,
            warehouse,
            cfg.config_id,
            schedule_cron=schedule_cron,
            tz=schedule_tz,
        )
    except Exception as exc:
        return {
            "status": "FALLBACK",
            "reason": str(exc),
            "task": f"{meta_db}.{meta_schema}.{base_task_name}",
        }

    return {
        "status": "TASK_CREATED",
        "task": f"{meta_db}.{meta_schema}.{base_task_name}",
    }
