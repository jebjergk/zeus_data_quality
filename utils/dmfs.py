import re
from typing import List, Optional, Set, Tuple
from utils.meta import DQConfig, DQCheck, _q, DQ_CONFIG_TBL, DQ_CHECK_TBL

AGG_PREFIX = "AGG:"

def _split_fqn(fqn: str) -> Tuple[str,str,str]:
    parts = fqn.split(".")
    if len(parts) != 3: raise ValueError("Need DB.SCHEMA.TABLE")
    return tuple(p.strip('"') for p in parts)  # type: ignore

def _parse_relation_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    parts = [p.strip('"') for p in name.split('.') if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, None, parts[0]
    raise ValueError("Invalid relation name")

def _current_db_schema(session) -> Tuple[Optional[str], Optional[str]]:
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

def _meta_db_schema(session) -> Tuple[str, str]:
    cfg_db, cfg_schema, _ = _parse_relation_name(DQ_CONFIG_TBL)
    chk_db, chk_schema, _ = _parse_relation_name(DQ_CHECK_TBL)
    current_db, current_schema = _current_db_schema(session)

    db = (cfg_db or chk_db or current_db)
    schema = (cfg_schema or chk_schema or current_schema)

    if not db or not schema:
        raise ValueError("Unable to determine metadata schema for DQ views")

    return db, schema

def _view_name(config_id: str, check_id: str) -> str:
    raw = f"DQ_{config_id}_{check_id}_FAILS".upper()
    return re.sub(r"[^A-Z0-9_]", "_", raw)

def attach_dmfs(session, config: DQConfig, checks: List[DQCheck]) -> List[str]:
    created: List[str] = []
    meta_db, meta_schema = _meta_db_schema(session)
    for chk in checks:
        rule = (chk.rule_expr or "").strip()
        if rule.upper().startswith(AGG_PREFIX):  # skip aggregates
            continue
        db, sch, tbl = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        predicate = rule[:-1] if rule.endswith(";") else rule
        session.sql(f"""
            CREATE OR REPLACE VIEW {_q(meta_db)}.{_q(meta_schema)}.{_q(vname)} AS
            SELECT * FROM {_q(db)}.{_q(sch)}.{_q(tbl)} WHERE NOT ({predicate})
        """).collect()
        created.append(f"{meta_db}.{meta_schema}.{vname}")
    return created

def _active_configs_using_table(session, table_fqn: str) -> Set[str]:
    df = session.sql(f"""
        SELECT DISTINCT k.CONFIG_ID
        FROM {_q(DQ_CONFIG_TBL)} c JOIN {_q(DQ_CHECK_TBL)} k USING(CONFIG_ID)
        WHERE UPPER(c.STATUS)='ACTIVE' AND k.TABLE_FQN = ?
    """, params=[table_fqn])
    out: Set[str] = set()
    for r in df.collect():
        out.add(r[0] if not hasattr(r, "asDict") else r.asDict().get("CONFIG_ID"))
    return out

def detach_dmfs_safe(session, config_id: str, checks: List[DQCheck]) -> List[str]:
    dropped: List[str] = []
    meta_db, meta_schema = _meta_db_schema(session)
    row_checks = [c for c in checks if not (c.rule_expr or "").upper().startswith(AGG_PREFIX)]
    tables = {c.table_fqn for c in row_checks}
    shared = {t for t in tables if len(_active_configs_using_table(session, t) - {config_id}) > 0}
    for chk in row_checks:
        if chk.table_fqn in shared: continue
        vname = _view_name(chk.config_id, chk.check_id)
        fqn = f"{_q(meta_db)}.{_q(meta_schema)}.{_q(vname)}"
        session.sql(f"DROP VIEW IF EXISTS {fqn}").collect()
        dropped.append(fqn)
    return dropped
