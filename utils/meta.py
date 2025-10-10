"""Utility helpers and data models for interacting with DQ metadata tables.

The functions in this module intentionally avoid depending on Snowpark at
import time so they can be reused in environments where the Snowpark Python
client is not installed.  Snowflake objects are loaded lazily via duck typing.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from snowflake.snowpark import Session
except Exception:
    Session = Any  # type: ignore

# Override with fully-qualified names if desired (e.g., "DB.SCHEMA.DQ_CONFIG")
DQ_CONFIG_TBL: str = "DQ_CONFIG"
DQ_CHECK_TBL: str = "DQ_CHECK"

__all__ = [
    "DQ_CONFIG_TBL",
    "DQ_CHECK_TBL",
    "DQConfig",
    "DQCheck",
    "_q",
    "fq_table",
    "metadata_db_schema",
    "ensure_meta_tables",
    "upsert_config",
    "list_configs",
    "get_config",
    "delete_config",
    "upsert_checks",
    "get_checks",
    "list_databases",
    "list_schemas",
    "list_tables",
    "list_columns",
]

# ---------- Models ----------
@dataclass
class DQConfig:
    config_id: str
    name: str
    description: Optional[str]
    target_table_fqn: str
    run_as_role: Optional[str]
    dmf_role: Optional[str]
    status: str
    owner: Optional[str]
    schedule_cron: Optional[str] = None
    schedule_timezone: Optional[str] = None
    schedule_enabled: bool = True

@dataclass
class DQCheck:
    config_id: str
    check_id: str
    table_fqn: str
    column_name: Optional[str]
    rule_expr: str
    severity: str
    sample_rows: int = 0
    check_type: Optional[str] = None
    params_json: Optional[str] = None

# ---------- Helpers ----------
def _q(ident: str) -> str:
    parts = [p.strip('"') for p in ident.split('.')]
    return '.'.join([f'"{p}"' for p in parts])

def fq_table(database: str, schema: str, table: str) -> str:
    return f'{_q(database.upper())}.{_q(schema.upper())}.{_q(table.upper())}'

def _normalize_row(row) -> Dict[str, Any]:
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {str(k).lower(): v for k, v in d.items()}

def _parse_relation_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    parts = [p.strip('"') for p in name.split('.') if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, None, parts[0]
    raise ValueError("Invalid relation name")

def _current_db_schema(session: Session) -> Tuple[Optional[str], Optional[str]]:
    current_db: Optional[str] = None
    current_schema: Optional[str] = None
    if session:
        for attr, holder in (("get_current_database", "db"), ("get_current_schema", "schema")):
            try:
                getter = getattr(session, attr)
                value = getter()
                if holder == "db" and value:
                    current_db = value
                elif holder == "schema" and value:
                    current_schema = value
            except Exception:
                continue
        if not (current_db and current_schema):
            try:
                row = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
                if hasattr(row, "asDict"):
                    d = row.asDict()
                    current_db = current_db or d.get("CURRENT_DATABASE()") or d.get("CURRENT_DATABASE")
                    current_schema = current_schema or d.get("CURRENT_SCHEMA()") or d.get("CURRENT_SCHEMA")
                else:
                    current_db = current_db or row[0]
                    current_schema = current_schema or row[1]
            except Exception:
                pass
    return current_db, current_schema

def metadata_db_schema(session: Session) -> Tuple[str, str]:
    cfg_db, cfg_schema, _ = _parse_relation_name(DQ_CONFIG_TBL)
    chk_db, chk_schema, _ = _parse_relation_name(DQ_CHECK_TBL)
    current_db, current_schema = _current_db_schema(session)

    db = (cfg_db or chk_db or current_db)
    schema = (cfg_schema or chk_schema or current_schema)

    if not db or not schema:
        raise ValueError("Unable to determine metadata schema for DQ views")

    return db, schema

def ensure_meta_tables(session: Session):
    if not session: return
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {_q(DQ_CONFIG_TBL)} (
          CONFIG_ID STRING PRIMARY KEY,
          NAME STRING,
          DESCRIPTION STRING,
          TARGET_TABLE_FQN STRING,
          RUN_AS_ROLE STRING,
          DMF_ROLE STRING,
          STATUS STRING,
          OWNER STRING,
          SCHEDULE_CRON STRING,
          SCHEDULE_TIMEZONE STRING,
          SCHEDULE_ENABLED BOOLEAN,
          CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
          UPDATED_AT TIMESTAMP_LTZ
        )
    """).collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_CRON STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_TIMEZONE STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CONFIG_TBL)} ADD COLUMN IF NOT EXISTS SCHEDULE_ENABLED BOOLEAN").collect()
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {_q(DQ_CHECK_TBL)} (
          CONFIG_ID STRING,
          CHECK_ID STRING,
          TABLE_FQN STRING,
          COLUMN_NAME STRING,
          RULE_EXPR STRING,
          SEVERITY STRING,
          SAMPLE_ROWS NUMBER DEFAULT 0,
          CHECK_TYPE STRING,
          PARAMS_JSON STRING,
          PRIMARY KEY (CONFIG_ID, CHECK_ID)
        )
    """).collect()

# ---------- CRUD ----------
def upsert_config(session: Session, cfg: DQConfig):
    ensure_meta_tables(session)
    session.sql(f"""
        MERGE INTO {_q(DQ_CONFIG_TBL)} t
        USING (SELECT ? as CONFIG_ID, ? as NAME, ? as DESCRIPTION, ? as TARGET_TABLE_FQN,
                      ? as RUN_AS_ROLE, ? as DMF_ROLE, ? as STATUS, ? as OWNER,
                      ? as SCHEDULE_CRON, ? as SCHEDULE_TIMEZONE, ? as SCHEDULE_ENABLED,
                      CURRENT_TIMESTAMP() as UPDATED_AT) s
        ON t.CONFIG_ID = s.CONFIG_ID
        WHEN MATCHED THEN UPDATE SET
          NAME = s.NAME, DESCRIPTION = s.DESCRIPTION, TARGET_TABLE_FQN = s.TARGET_TABLE_FQN,
          RUN_AS_ROLE = s.RUN_AS_ROLE, DMF_ROLE = s.DMF_ROLE, STATUS = s.STATUS,
          OWNER = s.OWNER, SCHEDULE_CRON = s.SCHEDULE_CRON,
          SCHEDULE_TIMEZONE = s.SCHEDULE_TIMEZONE,
          SCHEDULE_ENABLED = s.SCHEDULE_ENABLED,
          UPDATED_AT = s.UPDATED_AT
        WHEN NOT MATCHED THEN INSERT (CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
                                      SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED, UPDATED_AT)
        VALUES (s.CONFIG_ID, s.NAME, s.DESCRIPTION, s.TARGET_TABLE_FQN, s.RUN_AS_ROLE, s.DMF_ROLE, s.STATUS, s.OWNER,
                s.SCHEDULE_CRON, s.SCHEDULE_TIMEZONE, s.SCHEDULE_ENABLED, s.UPDATED_AT)
    """, params=[
        cfg.config_id, cfg.name, cfg.description, cfg.target_table_fqn,
        cfg.run_as_role, cfg.dmf_role, cfg.status, cfg.owner,
        cfg.schedule_cron, cfg.schedule_timezone, cfg.schedule_enabled
    ]).collect()

def list_configs(session: Session) -> List[DQConfig]:
    if not session: return []
    ensure_meta_tables(session)
    df = session.sql(
        f"""
        SELECT CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
               SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED
        FROM {_q(DQ_CONFIG_TBL)}
        ORDER BY STATUS DESC, NAME
        """
    )
    out: List[DQConfig] = []
    for r in df.collect():
        d = _normalize_row(r)
        schedule_enabled_raw = d.get("schedule_enabled")
        if schedule_enabled_raw is None:
            schedule_enabled = True
        elif isinstance(schedule_enabled_raw, str):
            schedule_enabled = schedule_enabled_raw.strip().upper() in {"TRUE", "T", "YES", "Y", "1"}
        else:
            schedule_enabled = bool(schedule_enabled_raw)
        out.append(DQConfig(
            config_id=d["config_id"], name=d["name"], description=d.get("description"),
            target_table_fqn=d["target_table_fqn"], run_as_role=d.get("run_as_role"),
            dmf_role=d.get("dmf_role"), status=d.get("status") or "DRAFT", owner=d.get("owner"),
            schedule_cron=d.get("schedule_cron"),
            schedule_timezone=d.get("schedule_timezone"),
            schedule_enabled=schedule_enabled
        ))
    return out

def get_config(session: Session, config_id: str) -> Optional[DQConfig]:
    if not session: return None
    df = session.sql(
        f"""
        SELECT CONFIG_ID, NAME, DESCRIPTION, TARGET_TABLE_FQN, RUN_AS_ROLE, DMF_ROLE, STATUS, OWNER,
               SCHEDULE_CRON, SCHEDULE_TIMEZONE, SCHEDULE_ENABLED
        FROM {_q(DQ_CONFIG_TBL)}
        WHERE CONFIG_ID = ?
        """,
        params=[config_id],
    )
    rows = df.collect()
    if not rows: return None
    d = _normalize_row(rows[0])
    schedule_enabled_raw = d.get("schedule_enabled")
    if schedule_enabled_raw is None:
        schedule_enabled = True
    elif isinstance(schedule_enabled_raw, str):
        schedule_enabled = schedule_enabled_raw.strip().upper() in {"TRUE", "T", "YES", "Y", "1"}
    else:
        schedule_enabled = bool(schedule_enabled_raw)
    return DQConfig(
        config_id=d["config_id"], name=d["name"], description=d.get("description"),
        target_table_fqn=d["target_table_fqn"], run_as_role=d.get("run_as_role"),
        dmf_role=d.get("dmf_role"), status=d.get("status") or "DRAFT", owner=d.get("owner"),
        schedule_cron=d.get("schedule_cron"),
        schedule_timezone=d.get("schedule_timezone"),
        schedule_enabled=schedule_enabled
    )

def delete_config(session: Session, config_id: str):
    if not session: return
    session.sql(f"DELETE FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ?", params=[config_id]).collect()
    session.sql(f"DELETE FROM {_q(DQ_CONFIG_TBL)} WHERE CONFIG_ID = ?", params=[config_id]).collect()

def upsert_checks(session: Session, checks: List[DQCheck]):
    if not session or not checks: return
    ensure_meta_tables(session)
    cfg_id = checks[0].config_id
    session.sql(f"DELETE FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ?", params=[cfg_id]).collect()
    for c in checks:
        session.sql(f"""
            INSERT INTO {_q(DQ_CHECK_TBL)} (CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY, SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON)
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
        """, params=[c.config_id, c.check_id, c.table_fqn, c.column_name, c.rule_expr, c.severity, int(c.sample_rows), c.check_type, c.params_json]).collect()

def get_checks(session: Session, config_id: str) -> List[DQCheck]:
    if not session: return []
    df = session.sql(f"SELECT CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY, SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON FROM {_q(DQ_CHECK_TBL)} WHERE CONFIG_ID = ? ORDER BY CHECK_ID", params=[config_id])
    out: List[DQCheck] = []
    for r in df.collect():
        d = _normalize_row(r)
        out.append(DQCheck(
            config_id=d["config_id"], check_id=d["check_id"], table_fqn=d["table_fqn"],
            column_name=d.get("column_name"), rule_expr=d["rule_expr"], severity=d.get("severity") or "ERROR",
            sample_rows=int(d.get("sample_rows") or 0), check_type=d.get("check_type"), params_json=d.get("params_json")
        ))
    return out

# ---------- Discovery (INFO_SCHEMA with safe fallbacks) ----------
def list_databases(session: Session) -> List[str]:
    if not session: return []
    try:
        df = session.sql("SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY 1")
        return [r[0] for r in df.collect()]
    except Exception:
        return []

def list_schemas(session: Session, database: str) -> List[str]:
    if not session or not database: return []
    try:
        df = session.sql(f'SELECT SCHEMA_NAME FROM {_q(database)}.INFORMATION_SCHEMA.SCHEMATA ORDER BY 1')
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f'SHOW SCHEMAS IN DATABASE {_q(database)}')
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []

def list_tables(session: Session, database: str, schema: str) -> List[str]:
    if not session or not (database and schema): return []
    try:
        df = session.sql(f"SELECT TABLE_NAME FROM {_q(database)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE='BASE TABLE' ORDER BY 1", params=[schema.upper()])
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"SHOW TABLES IN SCHEMA {_q(database)}.{_q(schema)}")
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []

def list_columns(session: Session, database: str, schema: str, table: str) -> List[str]:
    if not session or not (database and schema and table): return []
    try:
        df = session.sql(f"SELECT COLUMN_NAME FROM {_q(database)}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", params=[schema.upper(), table.upper()])
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"DESC TABLE {_q(database)}.{_q(schema)}.{_q(table)}")
            return [r[0] for r in df.collect()]  # NAME
        except Exception:
            return []
