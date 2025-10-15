"""Convenience helpers for attaching and detaching Data Monitoring Framework views."""

import re
from typing import Any, List, Optional, Set, Tuple

try:  # pragma: no cover - optional dependency in some environments
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - defensive guard when Streamlit absent
    st = None  # type: ignore

from utils.configs import get_proc_name
from utils.meta import (
    DQConfig,
    DQCheck,
    _q as _q_meta,
    DQ_CONFIG_TBL,
    DQ_CHECK_TBL,
)

AGG_PREFIX = "AGG:"
PROC_NAME = get_proc_name()

__all__ = [
    "AGG_PREFIX",
    "attach_dmfs",
    "detach_dmfs_safe",
    "_safe_ident",
    "task_name_for_config",
    "_q",
    "ensure_session_context",
    "session_snapshot",
    "preflight_requirements",
    "create_or_update_task",
    "run_task_now",
]


def _qi(name: str) -> str:
    """Quote an identifier for Snowflake."""

    return '"' + str(name).replace('"', '""') + '"'


def _q(db: str, sch: str, obj: str) -> str:
    """Return a fully qualified, quoted Snowflake identifier."""

    return f"{_qi(db)}.{_qi(sch)}.{_qi(obj)}"


def _ql(value: str) -> str:
    """Return a properly quoted Snowflake string literal."""

    return "'" + value.replace("'", "''") + "'"


def _is_missing_object_error(exc: Exception) -> bool:
    """Return ``True`` if *exc* matches a Snowflake "object missing" error."""

    message = str(exc).lower()
    return (
        "does not exist" in message
        or "object does not exist" in message
        or "002043" in message
        or "2043" in message
    )


def ensure_session_context(session, role: str, warehouse: str, db: str, schema: str):
    """Validate that the active Snowflake session matches expected context."""

    def _normalize(value: Optional[str]) -> str:
        return (value or "").strip().strip('"').upper()

    issues: List[str] = []

    current_role: Optional[str] = None
    if role:
        try:
            current_role = getattr(session, "get_current_role")()
        except Exception:
            current_role = None
        if not current_role:
            issues.append(
                f"Active role could not be verified. Switch to {_qi(role)} before continuing."
            )
        elif _normalize(current_role) != _normalize(role):
            issues.append(
                f"Active role {_qi(current_role)} does not match required role {_qi(role)}."
            )

    if db and "." in db and not schema:
        raise ValueError(
            "Metadata database value appears to include a schema. "
            "Set DQ_METADATA_DB and DQ_METADATA_SCHEMA separately."
        )

    if issues:
        raise ValueError(" ".join(issues))


if st is not None:  # pragma: no cover - decorator depends on Streamlit runtime

    @st.cache_data(show_spinner=False, ttl=60)
    def session_snapshot(session):
        return session.sql(
            """
              SELECT
                CURRENT_ACCOUNT()       AS ACCOUNT,
                CURRENT_REGION()        AS REGION,
                CURRENT_ORGANIZATION_NAME() AS ORG,
                CURRENT_ROLE()          AS ROLE,
                CURRENT_SECONDARY_ROLES() AS SECONDARY_ROLES,
                CURRENT_WAREHOUSE()     AS WAREHOUSE,
                CURRENT_DATABASE()      AS DB,
                CURRENT_SCHEMA()        AS SCHEMA
            """
        ).to_pandas()

else:  # pragma: no cover - Streamlit unavailable

    def session_snapshot(session):
        return session.sql(
            """
              SELECT
                CURRENT_ACCOUNT()       AS ACCOUNT,
                CURRENT_REGION()        AS REGION,
                CURRENT_ORGANIZATION_NAME() AS ORG,
                CURRENT_ROLE()          AS ROLE,
                CURRENT_SECONDARY_ROLES() AS SECONDARY_ROLES,
                CURRENT_WAREHOUSE()     AS WAREHOUSE,
                CURRENT_DATABASE()      AS DB,
                CURRENT_SCHEMA()        AS SCHEMA
            """
        ).to_pandas()


def _normalize_signature(signature: str) -> str:
    """Return a normalized version of a Snowflake argument signature."""

    sig = re.sub(r"\s+", " ", (signature or "").strip().upper())
    if not sig:
        return sig

    if not sig.startswith("(") or not sig.endswith(")"):
        return sig

    inner = sig[1:-1].strip()
    if not inner:
        return "()"

    args: List[str] = []
    depth = 0
    start = 0
    for index, ch in enumerate(inner):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            args.append(inner[start:index].strip())
            start = index + 1
    args.append(inner[start:].strip())

    cleaned: List[str] = []
    for arg in args:
        if not arg:
            continue
        tokens = arg.split()
        tokens = [tok for tok in tokens if tok not in {"IN", "OUT", "INOUT"}]
        if len(tokens) > 1:
            tokens = tokens[1:]
        cleaned.append(" ".join(tokens))

    return "(" + ", ".join(cleaned) + ")"


def preflight_requirements(
    session,
    db: str,
    schema: str,
    warehouse: str,
    proc_name: str = PROC_NAME,
    arg_sig: str = "(VARCHAR)",
):
    """Validate that the required schema and procedure are usable."""

    db_name = str(db or "").strip()
    if not db_name:
        raise ValueError("Database is required for preflight checks")

    schema_name = str(schema or "").strip()
    proc_identifier = str(proc_name or "").strip()

    if not schema_name:
        raise ValueError("Schema name is required for preflight checks")

    schema_rows = session.sql(
        f"""
          SELECT 1
          FROM {_q(db_name, "INFORMATION_SCHEMA", "SCHEMATA")}
          WHERE UPPER("SCHEMA_NAME") = UPPER({_ql(schema_name)})
        """
    ).collect()
    if not schema_rows:
        raise ValueError(
            f"Schema not found or accessible: {_qi(db_name)}.{_qi(schema_name)}"
        )

    rows = session.sql(
        f"""
          SELECT "ARGUMENT_SIGNATURE"
          FROM {_q(db_name, "INFORMATION_SCHEMA", "PROCEDURES")}
          WHERE UPPER("PROCEDURE_SCHEMA") = UPPER({_ql(schema_name)})
            AND UPPER("PROCEDURE_NAME") = UPPER({_ql(proc_identifier)})
        """
    ).collect()
    if not rows:
        raise ValueError(f"Procedure not found: {_q(db_name, schema_name, proc_identifier)}{arg_sig}")

    expected_signature = _normalize_signature(arg_sig)

    def _extract_signature(row: Any) -> str:
        if isinstance(row, dict):
            value = row.get("ARGUMENT_SIGNATURE")
        else:
            try:
                value = row["ARGUMENT_SIGNATURE"]  # type: ignore[index]
            except Exception:  # pragma: no cover - Snowpark rows index differently
                value = row[0]
        return "" if value is None else str(value)

    normalized_rows = []
    for row in rows:
        signature_value = _extract_signature(row)
        normalized_rows.append((_normalize_signature(signature_value), signature_value))
    if expected_signature not in {norm for norm, _ in normalized_rows}:
        available = ", ".join(original for _, original in normalized_rows)
        raise ValueError(
            "Procedure signature mismatch: "
            f"expected {arg_sig}, found [{available}] on "
            f"{_q(db_name, schema_name, proc_identifier)}"
        )

    # Warehouse visibility checks are intentionally omitted. Any missing or
    # unauthorized warehouse references will surface during DDL execution.


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

def attach_dmfs(
    session,
    config: DQConfig,
    checks: List[DQCheck],
    *,
    db: str,
    schema: str,
) -> List[str]:
    created: List[str] = []
    for chk in checks:
        rule = (chk.rule_expr or "").strip()
        if rule.upper().startswith(AGG_PREFIX):  # skip aggregates
            continue
        src_db, src_schema, src_tbl = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        predicate = rule[:-1] if rule.endswith(";") else rule
        session.sql(f"""
            CREATE OR REPLACE VIEW {_q_meta(db)}.{_q_meta(schema)}.{_q_meta(vname)} AS
            SELECT * FROM {_q_meta(src_db)}.{_q_meta(src_schema)}.{_q_meta(src_tbl)} WHERE NOT ({predicate})
        """).collect()
        created.append(f"{db}.{schema}.{vname}")
    return created

def _active_configs_using_table(session, table_fqn: str) -> Set[str]:
    table_literal = _ql(str(table_fqn or ""))
    df = session.sql(f"""
        SELECT DISTINCT k.CONFIG_ID
        FROM {_q_meta(DQ_CONFIG_TBL)} c JOIN {_q_meta(DQ_CHECK_TBL)} k USING(CONFIG_ID)
        WHERE UPPER(c.STATUS)='ACTIVE' AND k.TABLE_FQN = {table_literal}
    """)
    out: Set[str] = set()
    for r in df.collect():
        out.add(r[0] if not hasattr(r, "asDict") else r.asDict().get("CONFIG_ID"))
    return out

def detach_dmfs_safe(
    session,
    config_id: str,
    checks: List[DQCheck],
    *,
    db: str,
    schema: str,
) -> List[str]:
    dropped: List[str] = []
    row_checks = [c for c in checks if not (c.rule_expr or "").upper().startswith(AGG_PREFIX)]
    tables = {c.table_fqn for c in row_checks}
    shared = {t for t in tables if len(_active_configs_using_table(session, t) - {config_id}) > 0}
    for chk in row_checks:
        if chk.table_fqn in shared: continue
        vname = _view_name(chk.config_id, chk.check_id)
        fqn = f"{_q_meta(db)}.{_q_meta(schema)}.{_q_meta(vname)}"
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
    return text


def task_name_for_config(config_id: Any) -> str:
    """Return the canonical Snowflake task name for a configuration."""

    return f"DQ_TASK_{_safe_ident(config_id)}"


def create_or_update_task(
    session,
    db: Any,
    schema: Any,
    warehouse: Any,
    config_id: Any,
    *,
    schedule_cron: str = "0 8 * * *",
    tz: str = "Europe/Berlin",
    run_role: Any = None,
    proc_name: str = "DQ_RUN_CONFIG",
) -> None:
    """Create or update the Snowflake task for the given configuration."""

    db_name = "" if db is None else str(db)
    schema_name = "" if schema is None else str(schema)

    if not db_name or not schema_name:
        raise ValueError("Database and schema are required to manage tasks")

    name = task_name_for_config(config_id)
    fqn = _q(db_name, schema_name, name)
    proc = _q(db_name, schema_name, proc_name)
    sched = f"USING CRON {schedule_cron} {tz}"
    comment = f"Auto task for DQ config {config_id}"

    meta_location = f"{db_name}.{schema_name}".strip(".")

    def _handle_task_error(exc: Exception) -> Exception:
        message = str(exc)
        lowered = message.lower()
        if "nonexistent warehouse" in lowered or "091083" in lowered:
            return ValueError(
                "The current session warehouse is missing or inaccessible. "
                "Pick an existing warehouse (e.g. by running `USE WAREHOUSE <name>` in Snowflake) "
                "before enabling schedules."
            )
        if "002043" in message or "object does not exist" in lowered:
            hint = (
                " Snowflake could not find one of the required objects. "
                "Confirm that the metadata schema"
            )
            if meta_location:
                hint += f" `{meta_location}`"
            hint += (
                " exists, that the `DQ_RUN_CONFIG(VARCHAR)` stored procedure is deployed "
                "there, and that your role has privileges to use it."
            )
            return ValueError(message + "." + hint)
        return exc

    if warehouse is None or str(warehouse).strip() == "":
        raise ValueError(
            "A warehouse is required to schedule tasks. "
            "Select a warehouse in the app or configure a default in Snowflake before enabling schedules."
        )

    warehouse_name = str(warehouse).strip()
    run_role_name = str(run_role).strip() if run_role is not None else ""

    try:
        ensure_session_context(session, run_role_name, warehouse_name, db_name, schema_name)
        preflight_requirements(
            session,
            db_name,
            schema_name,
            warehouse_name,
            proc_name=proc_name,
            arg_sig="(VARCHAR)",
        )

        session.sql(
            f"""
        CREATE TASK IF NOT EXISTS {fqn}
          WAREHOUSE = {_qi(warehouse_name)}
          SCHEDULE  = ?
          COMMENT   = ?
        AS
          CALL {proc}(?);
        """,
            params=[sched, comment, config_id],
        ).collect()

        session.sql(
            f"""
        ALTER TASK {fqn} SET
          WAREHOUSE = {_qi(warehouse_name)},
          SCHEDULE  = ?,
          COMMENT   = ?
        """,
            params=[sched, comment],
        ).collect()

        session.sql(f"ALTER TASK {fqn} RESUME").collect()
    except Exception as exc:  # pragma: no cover - Snowflake specific
        raise _handle_task_error(exc)


def run_task_now(session, db: Any, schema: Any, config_id: Any):
    """Force-run the Snowflake task for the given configuration."""

    db_name = "" if db is None else str(db).strip()
    schema_name = "" if schema is None else str(schema).strip()
    if not db_name or not schema_name:
        raise ValueError("Database and schema are required to trigger a task run")

    fqn = _q(db_name, schema_name, task_name_for_config(config_id))
    return session.sql(
        f"SELECT SYSTEM$TASK_FORCE_RUN({_ql(fqn)}) AS REQUEST_ID"
    ).to_pandas()
