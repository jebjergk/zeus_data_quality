from typing import List, Set, Tuple
from utils.meta import DQConfig, DQCheck, _q, DQ_CONFIG_TBL, DQ_CHECK_TBL

AGG_PREFIX = "AGG:"

def _split_fqn(fqn: str) -> Tuple[str,str,str]:
    parts = fqn.split(".")
    if len(parts) != 3: raise ValueError("Need DB.SCHEMA.TABLE")
    return tuple(p.strip('"') for p in parts)  # type: ignore

def _view_name(config_id: str, check_id: str) -> str:
    return f"DQ_{config_id}_{check_id}_FAILS".upper()

def attach_dmfs(session, config: DQConfig, checks: List[DQCheck]) -> List[str]:
    created: List[str] = []
    for chk in checks:
        rule = (chk.rule_expr or "").strip()
        if rule.upper().startswith(AGG_PREFIX):  # skip aggregates
            continue
        db, sch, tbl = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        predicate = rule[:-1] if rule.endswith(";") else rule
        session.sql(f"""
            CREATE OR REPLACE VIEW {_q(db)}.{_q(sch)}.{_q(vname)} AS
            SELECT * FROM {_q(db)}.{_q(sch)}.{_q(tbl)} WHERE NOT ({predicate})
        """).collect()
        created.append(f"{db}.{sch}.{vname}")
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
    row_checks = [c for c in checks if not (c.rule_expr or "").upper().startswith(AGG_PREFIX)]
    tables = {c.table_fqn for c in row_checks}
    shared = {t for t in tables if len(_active_configs_using_table(session, t) - {config_id}) > 0}
    for chk in row_checks:
        if chk.table_fqn in shared: continue
        db, sch, _ = _split_fqn(chk.table_fqn)
        vname = _view_name(chk.config_id, chk.check_id)
        fqn = f"{_q(db)}.{_q(sch)}.{_q(vname)}"
        session.sql(f"DROP VIEW IF EXISTS {fqn}").collect()
        dropped.append(fqn)
    return dropped
