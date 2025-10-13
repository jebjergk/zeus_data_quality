from typing import Any, Dict

from utils.meta import _parse_relation_name, _q


def ensure_task_for_config(session, cfg) -> Dict[str, Any]:
    base_task_name = f"DQ_TASK_{cfg.config_id}"

    if not session:
        return {"status": "FALLBACK", "reason": "Missing session", "task": base_task_name}

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

    fq_task_identifier = f"{_q(db)}.{_q(schema)}.{_q(base_task_name)}"
    schedule_expression = "USING CRON 0 8 * * * Europe/Berlin"
    call_statement = f"CALL {_q(db)}.{_q(schema)}.SP_RUN_DQ_CONFIG('{cfg.config_id}')"

    try:
        session.sql(
            f"""
            CREATE OR REPLACE TASK {fq_task_identifier}
            WAREHOUSE = IDENTIFIER(CURRENT_WAREHOUSE())
            SCHEDULE = '{schedule_expression}'
            AS {call_statement}
            """
        ).collect()
        session.sql(f"ALTER TASK {fq_task_identifier} RESUME").collect()
        return {
            "status": "TASK_CREATED",
            "task": f"{db}.{schema}.{base_task_name}",
        }
    except Exception as exc:
        return {
            "status": "FALLBACK",
            "reason": str(exc),
            "task": f"{db}.{schema}.{base_task_name}",
        }
