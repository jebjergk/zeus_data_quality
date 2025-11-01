from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from utils.meta import _q

RUNS_TBL = "DQ_PROFILE_RUN"
COLS_TBL = "DQ_PROFILE_COLUMN"


def _tbl(meta_db: str, meta_schema: str, name: str) -> str:
    return f"{_q(meta_db)}.{_q(meta_schema)}.{_q(name)}"


def list_saved_profiles(session, meta_db: str, meta_schema: str, limit: int = 50) -> List[Dict[str, Any]]:
    try:
        sql = f"""
            SELECT RUN_ID, RUN_AT, SUMMARY
            FROM {_tbl(meta_db, meta_schema, RUNS_TBL)}
            ORDER BY RUN_AT DESC
            LIMIT {int(limit)}
        """
        rows = session.sql(sql).collect()
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for r in rows:
        if hasattr(r, "asDict"):
            d = r.asDict()
        else:
            try:
                d = {
                    "RUN_ID": r[0],
                    "RUN_AT": r[1] if len(r) > 1 else None,
                    "SUMMARY": r[2] if len(r) > 2 else None,
                }
            except Exception:
                d = {}
        out.append({
            "run_id": d.get("RUN_ID") or d.get("run_id"),
            "run_at": d.get("RUN_AT") or d.get("run_at"),
            "summary": d.get("SUMMARY") or d.get("summary"),
        })
    return out


def load_saved_profile_run(
    session, meta_db: str, meta_schema: str, run_id: str
) -> Tuple[Optional[Any], List[Dict[str, Any]]]:
    try:
        summary_sql = f"""
            SELECT SUMMARY
            FROM {_tbl(meta_db, meta_schema, RUNS_TBL)}
            WHERE RUN_ID = ?
        """
        rows = session.sql(summary_sql, params=[run_id]).collect()
        if rows:
            if hasattr(rows[0], "asDict"):
                summary = rows[0].asDict().get("SUMMARY")
            else:
                try:
                    summary = rows[0][0]
                except Exception:
                    summary = None
        else:
            summary = None
    except Exception:
        summary = None

    try:
        cols_sql = f"""
            SELECT COLUMN_NAME, PROFILE, SEMANTIC_TYPE, CONFIDENCE, RATIONALE, SIGNALS, SUGGESTED_CHECKS
            FROM {_tbl(meta_db, meta_schema, COLS_TBL)}
            WHERE RUN_ID = ?
            ORDER BY COLUMN_NAME
        """
        rows = session.sql(cols_sql, params=[run_id]).collect()
        columns: List[Dict[str, Any]] = []
        for r in rows:
            if hasattr(r, "asDict"):
                d = r.asDict()
            else:
                try:
                    d = {
                        "COLUMN_NAME": r[0],
                        "PROFILE": r[1] if len(r) > 1 else None,
                        "SEMANTIC_TYPE": r[2] if len(r) > 2 else None,
                        "CONFIDENCE": r[3] if len(r) > 3 else None,
                        "RATIONALE": r[4] if len(r) > 4 else None,
                        "SIGNALS": r[5] if len(r) > 5 else None,
                        "SUGGESTED_CHECKS": r[6] if len(r) > 6 else None,
                    }
                except Exception:
                    d = {}
            columns.append(
                {
                    "column_name": d.get("COLUMN_NAME") or d.get("column_name"),
                    "profile": d.get("PROFILE") or d.get("profile"),
                    "semantic_type": d.get("SEMANTIC_TYPE") or d.get("semantic_type"),
                    "confidence": d.get("CONFIDENCE") or d.get("confidence"),
                    "rationale": d.get("RATIONALE") or d.get("rationale"),
                    "signals": d.get("SIGNALS") or d.get("signals"),
                    "suggested_checks": d.get("SUGGESTED_CHECKS")
                    or d.get("suggested_checks"),
                }
            )
    except Exception:
        columns = []

    return summary, columns


def delete_saved_profile_run(session, meta_db: str, meta_schema: str, run_id: str) -> bool:
    try:
        session.sql(
            f"DELETE FROM {_tbl(meta_db, meta_schema, COLS_TBL)} WHERE RUN_ID = ?",
            params=[run_id],
        ).collect()
        session.sql(
            f"DELETE FROM {_tbl(meta_db, meta_schema, RUNS_TBL)} WHERE RUN_ID = ?",
            params=[run_id],
        ).collect()
        return True
    except Exception:
        return False
