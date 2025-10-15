# services/configs.py
from contextlib import contextmanager
from typing import Dict, Any, List

try:
    from snowflake.snowpark import Session
except Exception:
    Session = Any  # type: ignore

from utils import dmfs, meta
from utils.configs import get_metadata_namespace

@contextmanager
def transaction(session: Session):
    try:
        session.sql("BEGIN").collect()
        yield
        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

def save_config_and_checks(session: Session, cfg: meta.DQConfig, checks: List[meta.DQCheck], apply_now: bool) -> Dict[str, Any]:
    with transaction(session):
        meta.upsert_config(session, cfg)
        meta.upsert_checks(session, checks)
        result: Dict[str, Any] = {"config_id": cfg.config_id, "status": cfg.status}
        if apply_now:
            meta_db, meta_schema = get_metadata_namespace()
            created = dmfs.attach_dmfs(session, cfg, checks, db=meta_db, schema=meta_schema)
            result["dmfs_attached"] = created
    return result

def delete_config_full(session: Session, config_id: str) -> Dict[str, Any]:
    checks = meta.get_checks(session, config_id)
    meta_db, meta_schema = get_metadata_namespace()
    dropped = dmfs.detach_dmfs_safe(
        session,
        config_id,
        checks,
        db=meta_db,
        schema=meta_schema,
    )
    meta.delete_config(session, config_id)
    return {"config_id": config_id, "dmfs_dropped": dropped}
