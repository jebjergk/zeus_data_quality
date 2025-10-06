from contextlib import contextmanager
from typing import Any, Dict, List
from utils.meta import DQConfig, DQCheck
from utils import dmfs, meta

@contextmanager
def transaction(session):
    session.sql("BEGIN").collect()
    try:
        yield
        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

def save_config_and_checks(session, cfg: DQConfig, checks: List[DQCheck], apply_now: bool) -> Dict[str, Any]:
    with transaction(session):
        meta.upsert_config(session, cfg)
        meta.upsert_checks(session, checks)
        result: Dict[str, Any] = {"config_id": cfg.config_id, "status": cfg.status}
        if apply_now:
            created = dmfs.attach_dmfs(session, cfg, checks)
            result["dmfs_attached"] = created
    return result

def delete_config_full(session, config_id: str) -> Dict[str, Any]:
    checks = meta.get_checks(session, config_id)
    dropped = dmfs.detach_dmfs_safe(session, config_id, checks)
    meta.delete_config(session, config_id)
    return {"config_id": config_id, "dmfs_dropped": dropped}
