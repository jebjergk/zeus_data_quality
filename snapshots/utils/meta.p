"""Utility helpers and data models for interacting with DQ metadata tables.

The functions in this module intentionally avoid depending on Snowpark at
import time so they can be reused in environments where the Snowpark Python
client is not installed.  Snowflake objects are loaded lazily via duck typing.
"""

from __future__ import annotations
import importlib
import json
from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

try:
    from snowflake.snowpark import Session
except Exception:
    Session = Any  # type: ignore

from utils.configs import get_metadata_namespace

_streamlit_spec = importlib.util.find_spec("streamlit")
if _streamlit_spec is None:

    class _StreamlitCacheStub:
        def cache_data(self, **_kwargs):
            def decorator(func):
                return func

            return decorator

    st = _StreamlitCacheStub()
else:
    import streamlit as st  # type: ignore


def _session_hash(session: Any) -> Any:
    """Return a stable hash for a Snowpark session."""

    if session is None:
        return None

    for attr in ("session_id", "get_session_id"):
        if hasattr(session, attr):
            value = getattr(session, attr)
            try:
                result = value() if callable(value) else value
            except Exception:
                result = None
            if result is not None:
                return result

    return id(session)


CACHE_HASH_FUNCS = {
    "snowflake.snowpark.session.Session": _session_hash,
}

if isinstance(Session, type):
    CACHE_HASH_FUNCS[Session] = _session_hash

METADATA_DB, METADATA_SCHEMA = get_metadata_namespace()

# Override with fully-qualified names if desired (e.g., "DB.SCHEMA.DQ_CONFIG")
DQ_CONFIG_TBL: str = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CONFIG"
DQ_CHECK_TBL: str = f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_CHECK"

__all__ = [
    "DQ_CONFIG_TBL",
    "DQ_CHECK_TBL",
    "DQConfig",
    "DQCheck",
    "_q",
    "fq_table",
    "ensure_meta_tables",
    "upsert_config",
    "list_configs",
    "get_config",
    "delete_config",
    "get_library_checks",
    "update_library_check",
    "insert_library_check",
    "delete_check_by_id",
    "upsert_checks",
    "get_checks",
    "list_databases",
    "list_schemas",
    "list_tables",
    "get_table_row_count",
    "list_columns",
    "list_columns_with_types",
    "compute_confidence_pct",
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
    rule_code: Optional[str] = None
    rule_params: Optional[str] = None
    rule_version: Optional[str] = None
    compiled_rule: Optional[str] = None

# ---------- Helpers ----------
def _q(ident: str) -> str:
    parts = [p.strip('"') for p in ident.split('.')]
    return '.'.join([f'"{p}"' for p in parts])

def fq_table(database: str, schema: str, table: str) -> str:
    return f'{_q(database.upper())}.{_q(schema.upper())}.{_q(table.upper())}'

def _coerce_float(value: Any) -> Optional[float]:
    """Best-effort conversion of a value to float with NaN/Inf rejection."""

    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except Exception:
            return None

    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def compute_confidence_pct(
    *,
    raw_confidence: Optional[Any] = None,
    numerator: Optional[Any] = None,
    denominator: Optional[Any] = None,
    sample_size: Optional[Any] = None,
) -> Optional[float]:
    """Return a sanitized confidence percentage bounded to [0, 100].

    The helper accepts either a raw confidence value (ratio or percentage) or an
    explicit numerator / denominator pair.  When sampling applies, a non-positive
    ``sample_size`` will short-circuit to ``0.0`` to avoid division-by-zero or
    NaN propagation.
    """

    sample_value = _coerce_float(sample_size)
    if sample_value is not None and sample_value <= 0:
        return 0.0

    ratio: Optional[float] = None

    if numerator is not None and denominator is not None:
        num_value = _coerce_float(numerator)
        den_value = _coerce_float(denominator)
        if den_value and den_value > 0:
            if num_value is None:
                num_value = 0.0
            ratio = max(0.0, min(num_value / den_value, 1.0))

    if ratio is None and raw_confidence is not None:
        confidence_value = _coerce_float(raw_confidence)
        if confidence_value is not None:
            if confidence_value > 1.0:
                ratio = confidence_value / 100.0
            else:
                ratio = confidence_value

    if ratio is None:
        return 0.0 if sample_value == 0 else None

    clamped_ratio = max(0.0, min(ratio, 1.0))
    percentage = clamped_ratio * 100.0
    return round(percentage, 3)


def _normalize_row(row) -> Dict[str, Any]:
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    normalized = {str(k).lower(): v for k, v in d.items()}

    if "confidence" in normalized:
        normalized["confidence"] = compute_confidence_pct(
            raw_confidence=normalized.get("confidence"),
            numerator=normalized.get("confidence_numerator"),
            denominator=normalized.get("confidence_denominator"),
            sample_size=normalized.get("sample_size"),
        )

    return normalized

def _parse_relation_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    parts = [p.strip('"') for p in name.split('.') if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 0:
        return None, None, ""
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
          RULE_CODE STRING,
          RULE_PARAMS STRING,
          RULE_VERSION STRING,
          COMPILED_RULE STRING,
          UPDATED_AT TIMESTAMP_LTZ,
          PRIMARY KEY (CONFIG_ID, CHECK_ID)
        )
    """).collect()
    session.sql(f"ALTER TABLE {_q(DQ_CHECK_TBL)} ADD COLUMN IF NOT EXISTS RULE_CODE STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CHECK_TBL)} ADD COLUMN IF NOT EXISTS RULE_PARAMS STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CHECK_TBL)} ADD COLUMN IF NOT EXISTS RULE_VERSION STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CHECK_TBL)} ADD COLUMN IF NOT EXISTS COMPILED_RULE STRING").collect()
    session.sql(f"ALTER TABLE {_q(DQ_CHECK_TBL)} ADD COLUMN IF NOT EXISTS UPDATED_AT TIMESTAMP_LTZ").collect()

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


def get_library_checks(
    session: Session, config_id: str, *, scope: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Load DQ_CHECK rows joined to the rule library for a config.

    Optionally filters by rule scope (TABLE/COLUMN) to keep table-level
    checks out of the rule grid while still allowing dedicated loading for
    the configuration header.
    """

    if not session or not config_id:
        return []

    rule_table = _q(f"{METADATA_DB}.{METADATA_SCHEMA}.DQ_RULE_LIBRARY")
    scope_clause = ""
    params = [config_id]
    if scope:
        scope_clause = " AND COALESCE(UPPER(r.SCOPE), '') = UPPER(?)"
        params.append(scope)

    df = session.sql(
        f"""
        SELECT
          c.CONFIG_ID,
          c.CHECK_ID,
          c.TABLE_FQN,
          c.COLUMN_NAME,
          c.RULE_EXPR,
          c.SEVERITY,
          c.SAMPLE_ROWS,
          c.CHECK_TYPE,
          c.PARAMS_JSON,
          c.RULE_CODE,
          c.RULE_PARAMS,
          c.RULE_VERSION,
          c.COMPILED_RULE,
          r.RULE_ID,
          r.CATEGORY,
          r.SEVERITY AS RULE_SEVERITY,
          r.PARAM_SCHEMA,
          r.DEFAULT_PARAMS,
          r.VERSION,
          r.SCOPE
        FROM {_q(DQ_CHECK_TBL)} c
        LEFT JOIN {rule_table} r
          ON c.RULE_CODE = r.RULE_CODE
        WHERE c.CONFIG_ID = ?{scope_clause}
        """,
        params=params,
    )
    out: List[Dict[str, Any]] = []
    for row in df.collect():
        out.append(_normalize_row(row))
    return out


def update_library_check(
    session: Session,
    *,
    check_id: str,
    rule_params: Dict[str, Any],
    rule_version: Optional[str],
    compiled_rule: Optional[str],
    rule_expr: Optional[str],
    severity: Optional[str] = None,
):
    if not session or not check_id:
        return
    serialized_params = json.dumps(rule_params, default=str) if rule_params is not None else None
    session.sql(
        f"""
        UPDATE {_q(DQ_CHECK_TBL)}
        SET RULE_PARAMS = :1,
            RULE_VERSION = :2,
            COMPILED_RULE = :3,
            RULE_EXPR = :4,
            SEVERITY = COALESCE(:5, SEVERITY),
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE CHECK_ID = :6
        """,
        params=[serialized_params, rule_version, compiled_rule, rule_expr, severity, check_id],
    ).collect()


def insert_library_check(
    session: Session,
    *,
    config_id: str,
    table_fqn: str,
    column_name: str,
    rule_code: str,
    rule_id: str,
    rule_params: Dict[str, Any],
    rule_version: Optional[str],
    compiled_rule: Optional[str],
    severity: Optional[str],
    sample_rows: int = 0,
) -> str:
    if not session:
        return ""

    ensure_meta_tables(session)
    check_id = str(uuid4())
    serialized_params = json.dumps(rule_params, default=str) if rule_params is not None else None
    session.sql(
        f"""
        INSERT INTO {_q(DQ_CHECK_TBL)} (
          CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY,
          SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON, RULE_CODE, RULE_PARAMS,
          RULE_VERSION, COMPILED_RULE, UPDATED_AT
        )
        SELECT :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, CURRENT_TIMESTAMP()
        """,
        params=[
            config_id,
            check_id,
            table_fqn,
            column_name,
            compiled_rule,
            severity,
            int(sample_rows),
            rule_id,
            serialized_params,
            rule_code,
            serialized_params,
            rule_version,
            compiled_rule,
        ],
    ).collect()
    return check_id


def delete_check_by_id(session: Session, check_id: str):
    if not session or not check_id:
        return
    session.sql(
        f"DELETE FROM {_q(DQ_CHECK_TBL)} WHERE CHECK_ID = :1",
        params=[check_id],
    ).collect()

def upsert_checks(session: Session, checks: List[DQCheck]):
    if not session or not checks:
        return

    ensure_meta_tables(session)
    cfg_id = checks[0].config_id

    existing_df = session.sql(
        f"""
        SELECT CHECK_ID, COLUMN_NAME, RULE_CODE, CHECK_TYPE
        FROM {_q(DQ_CHECK_TBL)}
        WHERE CONFIG_ID = ?
        """,
        params=[cfg_id],
    )
    existing_map: Dict[Tuple[str, str], str] = {}
    for row in existing_df.collect():
        norm_col = (row["COLUMN_NAME"] or "").upper()
        norm_rule = (row["RULE_CODE"] or row["CHECK_TYPE"] or "").upper()
        existing_map[(norm_col, norm_rule)] = row["CHECK_ID"]

    seen_ids: set[str] = set()

    for c in checks:
        norm_col = (c.column_name or "").upper()
        norm_rule = (c.rule_code or c.check_type or "").upper()
        serialized_params = c.rule_params
        if isinstance(serialized_params, dict):
            serialized_params = json.dumps(serialized_params, default=str)
        params_json = c.params_json
        if isinstance(params_json, dict):
            params_json = json.dumps(params_json, default=str)
        serialized_params = serialized_params or params_json

        existing_id = existing_map.get((norm_col, norm_rule))
        check_id = existing_id or c.check_id or str(uuid4())
        seen_ids.add(check_id)

        if existing_id:
            session.sql(
                f"""
                UPDATE {_q(DQ_CHECK_TBL)}
                SET TABLE_FQN = :1,
                    RULE_EXPR = :2,
                    SEVERITY = :3,
                    SAMPLE_ROWS = :4,
                    CHECK_TYPE = :5,
                    PARAMS_JSON = :6,
                    RULE_CODE = :7,
                    RULE_PARAMS = :8,
                    RULE_VERSION = :9,
                    COMPILED_RULE = :10,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE CHECK_ID = :11
                  AND CONFIG_ID = :12
                """,
                params=[
                    c.table_fqn,
                    c.rule_expr,
                    c.severity,
                    int(c.sample_rows),
                    c.check_type,
                    params_json,
                    c.rule_code,
                    serialized_params,
                    c.rule_version,
                    c.compiled_rule,
                    check_id,
                    cfg_id,
                ],
            ).collect()
        else:
            session.sql(
                f"""
                INSERT INTO {_q(DQ_CHECK_TBL)} (
                  CONFIG_ID, CHECK_ID, TABLE_FQN, COLUMN_NAME, RULE_EXPR, SEVERITY,
                  SAMPLE_ROWS, CHECK_TYPE, PARAMS_JSON, RULE_CODE, RULE_PARAMS,
                  RULE_VERSION, COMPILED_RULE, UPDATED_AT
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP()
                """,
                params=[
                    c.config_id,
                    check_id,
                    c.table_fqn,
                    c.column_name,
                    c.rule_expr,
                    c.severity,
                    int(c.sample_rows),
                    c.check_type,
                    params_json,
                    c.rule_code,
                    serialized_params,
                    c.rule_version,
                    c.compiled_rule,
                ],
            ).collect()

    if seen_ids:
        session.sql(
            f"""
            DELETE FROM {_q(DQ_CHECK_TBL)}
            WHERE CONFIG_ID = :1
              AND CHECK_ID NOT IN ({', '.join(['?' for _ in seen_ids])})
            """,
            params=[cfg_id, *seen_ids],
        ).collect()

def get_checks(session: Session, config_id: str) -> List[DQCheck]:
    if not session: return []
    df = session.sql(
        f"""
        SELECT
          CONFIG_ID,
          CHECK_ID,
          TABLE_FQN,
          COLUMN_NAME,
          RULE_EXPR,
          SEVERITY,
          SAMPLE_ROWS,
          CHECK_TYPE,
          PARAMS_JSON,
          RULE_CODE,
          RULE_PARAMS,
          RULE_VERSION,
          COMPILED_RULE
        FROM {_q(DQ_CHECK_TBL)}
        WHERE CONFIG_ID = ?
        ORDER BY CHECK_ID
        """,
        params=[config_id],
    )
    out: List[DQCheck] = []
    for r in df.collect():
        d = _normalize_row(r)
        out.append(DQCheck(
            config_id=d["config_id"], check_id=d["check_id"], table_fqn=d["table_fqn"],
            column_name=d.get("column_name"), rule_expr=d["rule_expr"], severity=d.get("severity") or "ERROR",
            sample_rows=int(d.get("sample_rows") or 0), check_type=d.get("check_type"), params_json=d.get("params_json"),
            rule_code=d.get("rule_code"), rule_params=d.get("rule_params"),
            rule_version=d.get("rule_version"), compiled_rule=d.get("compiled_rule")
        ))
    return out

# ---------- Discovery (INFO_SCHEMA with safe fallbacks) ----------
def list_databases(session: Session, *, editor_target_fqn: Optional[str] = None) -> List[str]:
    if not session:
        return []
    cache_key = editor_target_fqn or "GLOBAL::DATABASES"
    return _list_databases_cached(cache_key, session)


@st.cache_data(ttl=120, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _list_databases_cached(editor_target_fqn: str, session: Session) -> List[str]:
    del editor_target_fqn  # key only
    try:
        df = session.sql("SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY 1")
        return [r[0] for r in df.collect()]
    except Exception:
        return []


def list_schemas(
    session: Session,
    database: str,
    *,
    editor_target_fqn: Optional[str] = None,
) -> List[str]:
    if not session or not database:
        return []
    cache_key = editor_target_fqn or f"{database.upper()}::SCHEMAS"
    return _list_schemas_cached(cache_key, session, database)


@st.cache_data(ttl=120, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _list_schemas_cached(editor_target_fqn: str, session: Session, database: str) -> List[str]:
    del editor_target_fqn
    try:
        df = session.sql(f'SELECT SCHEMA_NAME FROM {_q(database)}.INFORMATION_SCHEMA.SCHEMATA ORDER BY 1')
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f'SHOW SCHEMAS IN DATABASE {_q(database)}')
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []


def list_tables(
    session: Session,
    database: str,
    schema: str,
    *,
    editor_target_fqn: Optional[str] = None,
) -> List[str]:
    if not session or not (database and schema):
        return []
    cache_key = editor_target_fqn or f"{database.upper()}.{schema.upper()}::TABLES"
    return _list_tables_cached(cache_key, session, database, schema)


@st.cache_data(ttl=120, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _list_tables_cached(editor_target_fqn: str, session: Session, database: str, schema: str) -> List[str]:
    del editor_target_fqn
    try:
        df = session.sql(f"SELECT TABLE_NAME FROM {_q(database)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE='BASE TABLE' ORDER BY 1", params=[schema.upper()])
        return [r[0] for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"SHOW TABLES IN SCHEMA {_q(database)}.{_q(schema)}")
            return [r[1] for r in df.collect()]  # NAME
        except Exception:
            return []


def get_table_row_count(
    session: Session,
    database: str,
    schema: str,
    table: str,
    *,
    editor_target_fqn: Optional[str] = None,
) -> Optional[int]:
    """Return the row count reported by Snowflake metadata for a table."""

    if not session or not (database and schema and table):
        return None

    cache_key = editor_target_fqn or fq_table(database, schema, table)
    return _get_table_row_count_cached(cache_key, session, database, schema, table)


@st.cache_data(ttl=120, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _get_table_row_count_cached(
    editor_target_fqn: str,
    session: Session,
    database: str,
    schema: str,
    table: str,
) -> Optional[int]:
    del editor_target_fqn

    try:
        df = session.sql(
            f"SELECT ROW_COUNT FROM {_q(database)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            params=[schema.upper(), table.upper()],
        )
        rows = df.collect()
        if rows:
            value = rows[0][0]
            return int(value) if value is not None else None
    except Exception:
        pass

    try:
        df = session.sql(
            f"SHOW TABLES LIKE ? IN SCHEMA {_q(database)}.{_q(schema)}",
            params=[table],
        )
        for row in df.collect():
            if hasattr(row, "asDict"):
                data = row.asDict()
                value = data.get("rows") or data.get("ROW_COUNT")
                if value is not None:
                    return int(value)
            else:
                if len(row) >= 8:
                    value = row[7]
                    if value is not None:
                        return int(value)
    except Exception:
        pass

    return None


def list_columns(
    session: Session,
    database: str,
    schema: str,
    table: str,
    *,
    editor_target_fqn: Optional[str] = None,
) -> List[str]:
    """Return column names for the given table."""

    return [name for name, _ in list_columns_with_types(session, database, schema, table, editor_target_fqn=editor_target_fqn)]


def list_columns_with_types(
    session: Session,
    database: str,
    schema: str,
    table: str,
    *,
    editor_target_fqn: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Return column names paired with their Snowflake data types.

    Falls back to ``DESC TABLE`` if INFORMATION_SCHEMA is unavailable.
    """

    if not session or not (database and schema and table):
        return []

    cache_key = editor_target_fqn or fq_table(database, schema, table)
    return _list_columns_with_types_cached(cache_key, session, database, schema, table)


@st.cache_data(ttl=120, show_spinner=False, hash_funcs=CACHE_HASH_FUNCS)
def _list_columns_with_types_cached(
    editor_target_fqn: str,
    session: Session,
    database: str,
    schema: str,
    table: str,
) -> List[Tuple[str, str]]:
    del editor_target_fqn
    try:
        df = session.sql(
            f"SELECT COLUMN_NAME, DATA_TYPE FROM {_q(database)}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            params=[schema.upper(), table.upper()],
        )
        return [(r[0], r[1]) for r in df.collect()]
    except Exception:
        try:
            df = session.sql(f"DESC TABLE {_q(database)}.{_q(schema)}.{_q(table)}")
            return [(r[0], r[1]) for r in df.collect()]  # NAME, TYPE
        except Exception:
            return []
