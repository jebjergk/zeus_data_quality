"""Convenience helpers for attaching and detaching Data Monitoring Framework views."""

import re
from typing import Any, List, Set, Tuple
from utils.meta import (
    DQConfig,
    DQCheck,
    _q as _q_meta,
    DQ_CONFIG_TBL,
    DQ_CHECK_TBL,
    metadata_db_schema,
)

AGG_PREFIX = "AGG:"

__all__ = [
    "AGG_PREFIX",
    "attach_dmfs",
    "detach_dmfs_safe",
    "_safe_ident",
    "task_name_for_config",
    "_q",
    "create_or_update_task",
    "run_task_now",
]

def _split_fqn(fqn: str) -> Tuple[str, str, str]:
    parts: List[str] = []
    current: List[str] = []
    in_quotes = False
    i = 0

    while i < len(fqn):
        ch = fqn[i]
        if ch == '"':
            next_char = fqn[i + 1] if i + 1 < len(fqn) else None
            if in_quotes and next_char == '"':
                current.append('"')
                i += 1  # skip escaped quote
            else:
                in_quotes = not in_quotes
        elif ch == '.' and not in_quotes:
            parts.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1

    parts.append(''.join(current).strip())

    if in_quotes or len(parts) != 3 or any(part == '' for part in parts):
        raise ValueError("Need DB.SCHEMA.TABLE")

    return tuple(part.strip('"') for part in parts)  # type: ignore

def _view_name(config_id: str, check_id: str) -> str:
    raw = f"DQ_{config_id}_{check_id}_FAILS".upper()
    return re.sub(r"[^A-Z0-9_]", "_", raw)

def attach_dmfs(session, config: DQConfig, checks: List[DQCheck]) -> List[str]:
    created: List[str] = []
    meta_db, meta_schema = metadata_db_schema(session)
    for chk in checks:
        rule = (chk.rule_expr or "").strip()
        if rule.upper().startswith(AGG_PREFIX):  # skip aggregates
            continue
        db, sch, tbl = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        predicate = rule[:-1] if rule.endswith(";") else rule
        session.sql(f"""
            CREATE OR REPLACE VIEW {_q_meta(meta_db)}.{_q_meta(meta_schema)}.{_q_meta(vname)} AS
            SELECT * FROM {_q_meta(db)}.{_q_meta(sch)}.{_q_meta(tbl)} WHERE NOT ({predicate})
        """).collect()
        created.append(f"{meta_db}.{meta_schema}.{vname}")
    return created

def _active_configs_using_table(session, table_fqn: str) -> Set[str]:
    df = session.sql(f"""
        SELECT DISTINCT k.CONFIG_ID
        FROM {_q_meta(DQ_CONFIG_TBL)} c JOIN {_q_meta(DQ_CHECK_TBL)} k USING(CONFIG_ID)
        WHERE UPPER(c.STATUS)='ACTIVE' AND k.TABLE_FQN = ?
    """, params=[table_fqn])
    out: Set[str] = set()
    for r in df.collect():
        out.add(r[0] if not hasattr(r, "asDict") else r.asDict().get("CONFIG_ID"))
    return out

def detach_dmfs_safe(session, config_id: str, checks: List[DQCheck]) -> List[str]:
    dropped: List[str] = []
    meta_db, meta_schema = metadata_db_schema(session)
    row_checks = [c for c in checks if not (c.rule_expr or "").upper().startswith(AGG_PREFIX)]
    tables = {c.table_fqn for c in row_checks}
    shared = {t for t in tables if len(_active_configs_using_table(session, t) - {config_id}) > 0}
    for chk in row_checks:
        if chk.table_fqn in shared: continue
        vname = _view_name(chk.config_id, chk.check_id)
        fqn = f"{_q_meta(meta_db)}.{_q_meta(meta_schema)}.{_q_meta(vname)}"
        session.sql(f"DROP VIEW IF EXISTS {fqn}").collect()
        dropped.append(fqn)
    return dropped


def _safe_ident(value: Any) -> str:
    """Return a Snowflake-safe identifier based on *value*."""

    text = "" if value is None else str(value)
    text = text.upper()
    text = re.sub(r"[^A-Z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.strip("_")
    if not text:
        text = "X"
    if text[0].isdigit():
        text = f"X_{text}"
    return text


def task_name_for_config(config_id: Any) -> str:
    """Return the canonical Snowflake task name for a configuration."""

    return f"DQ_TASK_{_safe_ident(config_id)}"


def _quote(part: Any) -> str:
    text = "" if part is None else str(part)
    text = text.strip('"')
    text = text.replace('"', '""')
    return f'"{text}"'


def _q(database: Any, schema: Any, object_name: Any) -> str:
    """Return a fully qualified, quoted Snowflake identifier."""

    return f"{_quote(database)}.{_quote(schema)}.{_quote(object_name)}"


def create_or_update_task(
    session,
    database: Any,
    schema: Any,
    warehouse: Any,
    config_id: Any,
    schedule_cron: str = "0 8 * * *",
    tz: str = "Europe/Berlin",
) -> None:
    """Create or update the Snowflake task for the given configuration."""

    name = task_name_for_config(config_id)
    fqn = _q(database, schema, name)
    schedule_expr = f"USING CRON {schedule_cron} {tz}"
    comment = f"Auto task for DQ config {config_id}"

    procedure_fqn = _q(database, schema, "SP_RUN_DQ_CONFIG")

    session.sql(
        f"""
         CREATE TASK IF NOT EXISTS {fqn}
           WAREHOUSE = ?
           SCHEDULE = ?
           COMMENT = ?
         AS
           CALL {procedure_fqn}(?);
        """,
        params=[warehouse, schedule_expr, comment, config_id],
    ).collect()

    session.sql(
        f"ALTER TASK {fqn} SET WAREHOUSE = ?, SCHEDULE = ?, COMMENT = ?",
        params=[warehouse, schedule_expr, comment],
    ).collect()

    session.sql(f"ALTER TASK {fqn} RESUME").collect()


def run_task_now(session, database: Any, schema: Any, config_id: Any):
    """Force-run the Snowflake task for the given configuration."""

    fqn = _q(database, schema, task_name_for_config(config_id))
    return session.sql(
        "SELECT SYSTEM$TASK_FORCE_RUN(?) AS REQUEST_ID",
        params=[fqn],
    ).to_pandas()
