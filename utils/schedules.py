"""Snowflake task scheduling helpers used across the Streamlit app."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from utils.meta import _q

DEFAULT_CRON_EXPRESSION = "0 8 * * *"
DEFAULT_TIMEZONE = "Europe/Berlin"


@dataclass
class TaskInfo:
    """Lightweight representation of a Snowflake task."""

    name: str
    state: Optional[str] = None
    schedule: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None


def build_task_name(config_id: str) -> str:
    """Return the deterministic task name for a configuration."""

    return f"DQ_TASK_{config_id}"


def build_cron_schedule(
    cron_expression: str = DEFAULT_CRON_EXPRESSION,
    timezone: str = DEFAULT_TIMEZONE,
) -> str:
    """Construct a ``USING CRON`` clause suitable for Snowflake tasks.

    Snowflake is strict about whitespace in cron expressions, therefore we
    collapse consecutive spaces to a single one before composing the final
    clause.
    """

    normalized = " ".join(cron_expression.split())
    return f"USING CRON {normalized} {timezone}"


def ensure_task_for_config(
    session,
    config,
    *,
    cron_expression: Optional[str] = None,
    timezone: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> Dict[str, Any]:
    """Create (or replace) the Snowflake task backing the supplied config.

    Parameters
    ----------
    session:
        Active Snowpark session used to issue SQL statements.
    config:
        ``DQConfig`` instance returned from ``utils.meta``.
    cron_expression / timezone:
        Optional overrides for the default schedule. When omitted, the module
        level defaults (08:00 Europe/Berlin) are applied.
    warehouse:
        Optionally pin the task to a specific warehouse. When omitted the
        current session warehouse is re-used via ``IDENTIFIER(CURRENT_WAREHOUSE())``.
    """

    if not session:
        return {"status": "NO_SESSION"}

    task_name = build_task_name(config.config_id)
    schedule_clause = build_cron_schedule(
        cron_expression or DEFAULT_CRON_EXPRESSION,
        timezone or DEFAULT_TIMEZONE,
    )
    sql_body = f"CALL RUN_DQ_CONFIG('{config.config_id}')"

    warehouse_clause = (
        f"WAREHOUSE = {_q(warehouse)}"
        if warehouse
        else "WAREHOUSE = IDENTIFIER(CURRENT_WAREHOUSE())"
    )

    try:
        session.sql(
            f"""
            CREATE OR REPLACE TASK {_q(task_name)}
            {warehouse_clause}
            SCHEDULE = '{schedule_clause}'
            AS {sql_body}
        """
        ).collect()
        session.sql(f"ALTER TASK {_q(task_name)} RESUME").collect()
        return {"status": "TASK_CREATED", "task": task_name, "schedule": schedule_clause}
    except Exception as exc:  # pragma: no cover - relies on Snowflake errors
        return {
            "status": "FALLBACK",
            "reason": str(exc),
            "task": task_name,
            "schedule": schedule_clause,
        }


def fetch_task(session, task_name: str) -> Optional[TaskInfo]:
    """Return information about a task if it exists."""

    if not session:
        return None

    like_pattern = task_name.replace("'", "''")
    rows = session.sql(f"SHOW TASKS LIKE '{like_pattern}'").collect()
    if not rows:
        return None

    row = rows[0]
    state = getattr(row, "state", None) or getattr(row, "condition", None)
    schedule = getattr(row, "schedule", None) or getattr(row, "cron", None)
    owner = getattr(row, "owner", None)
    comment = getattr(row, "comment", None)
    return TaskInfo(name=task_name, state=state, schedule=schedule, owner=owner, comment=comment)


def drop_task_for_config(session, config_id: str) -> Dict[str, Any]:
    """Drop the task associated with a configuration, if it exists."""

    if not session:
        return {"status": "NO_SESSION"}

    task_name = build_task_name(config_id)
    session.sql(f"DROP TASK IF EXISTS {_q(task_name)}").collect()
    return {"status": "TASK_DROPPED", "task": task_name}


def pause_task(session, config_id: str) -> Dict[str, Any]:
    """Suspend an existing task."""

    if not session:
        return {"status": "NO_SESSION"}

    task_name = build_task_name(config_id)
    session.sql(f"ALTER TASK {_q(task_name)} SUSPEND").collect()
    return {"status": "TASK_SUSPENDED", "task": task_name}


def resume_task(session, config_id: str) -> Dict[str, Any]:
    """Resume a suspended task."""

    if not session:
        return {"status": "NO_SESSION"}

    task_name = build_task_name(config_id)
    session.sql(f"ALTER TASK {_q(task_name)} RESUME").collect()
    return {"status": "TASK_RESUMED", "task": task_name}
