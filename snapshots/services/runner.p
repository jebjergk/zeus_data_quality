# services/runner.py
from typing import Dict, Any, List
from utils.meta import DQCheck, DQConfig

AGG_PREFIX = "AGG:"

def run_now(session, cfg: DQConfig, checks: List[DQCheck]) -> Dict[str, Any]:
    results: Dict[str, Any] = {"config_id": cfg.config_id, "checks": []}
    for chk in checks:
        rule = (chk.rule_expr or '').strip()
        if rule.upper().startswith(AGG_PREFIX):
            sql = rule[len(AGG_PREFIX):].strip()
            if sql:
                # Remove any wrapping quotes that may surround the SQL text.
                while len(sql) >= 2 and sql[0] == sql[-1] and sql[0] in {'"', "'"}:
                    sql = sql[1:-1].strip()
                # Snowflake can surface statements such as `'SELECT ...''` when
                # values were stored with escaped quotes. Strip any leading or
                # trailing quote characters that remain so we execute the raw
                # SQL statement.
                sql = sql.lstrip()
                while sql and sql[0] in {'"', "'"}:
                    sql = sql[1:].lstrip()
                sql = sql.rstrip()
                while sql and sql[-1] in {'"', "'"}:
                    sql = sql[:-1].rstrip()
            try:
                df = session.sql(sql)
            except Exception as exc:
                raise RuntimeError(f"Failed to execute aggregate check SQL: {exc}\nSQL:\n{sql}") from exc
            r = df.collect()[0]
            ok = bool((r[0] if not hasattr(r, 'asDict') else list(r.asDict().values())[0]))
            failures = 0 if ok else 1
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": True,
                "ok": ok,
                "failures": failures,
                "sample": []
            })
        else:
            failure_sql = f"SELECT COUNT(*) AS FAILURES FROM {chk.table_fqn} WHERE NOT ({rule})"
            try:
                df = session.sql(failure_sql)
            except Exception as exc:
                raise RuntimeError(f"Failed to execute row check SQL: {exc}\nSQL:\n{failure_sql}") from exc
            failures = int(df.collect()[0][0])
            sample = []
            if chk.sample_rows and failures:
                sample_sql = (
                    f"SELECT * FROM {chk.table_fqn} WHERE NOT ({rule}) LIMIT {int(chk.sample_rows)}"
                )
                try:
                    s_df = session.sql(sample_sql)
                except Exception as exc:
                    raise RuntimeError(f"Failed to fetch sample rows using SQL: {exc}\nSQL:\n{sample_sql}") from exc
                sample = [r.asDict() if hasattr(r, 'asDict') else dict(r) for r in s_df.collect()]
            results["checks"].append({
                "check_id": chk.check_id,
                "type": chk.check_type,
                "aggregate": False,
                "failures": failures,
                "sample": sample,
            })
    return results
