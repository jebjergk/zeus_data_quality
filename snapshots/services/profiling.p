"""Service helpers for table profiling and automated DQ suggestions."""

from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

from services.profile import _is_numeric, _is_temporal, _stringify
from utils.meta import _q

__all__ = [
    "list_columns",
    "run_table_profile",
    "suggest_checks_from_profile",
    "save_profile_results",
]


def _split_fqn(fqn: str) -> Tuple[str, str, str]:
    """Split a fully-qualified name into database, schema, and object components."""

    parts: List[str] = []
    current: List[str] = []
    in_quotes = False
    for ch in fqn or "":
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
            continue
        if ch == "." and not in_quotes:
            piece = "".join(current).strip()
            if piece:
                parts.append(piece.strip('"'))
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail.strip('"'))
    if len(parts) != 3:
        raise ValueError(f"Expected fully qualified name in the form DB.SCHEMA.TABLE, got: {fqn}")
    return parts[0], parts[1], parts[2]


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _is_string_type(data_type: str) -> bool:
    upper = (data_type or "").upper()
    return any(token in upper for token in ("CHAR", "STRING", "TEXT", "VARCHAR"))


def list_columns(session, db: str, schema: str, table: str) -> List[Dict[str, Any]]:
    """Return column metadata for the specified table."""

    if not session or not (db and schema and table):
        return []
    info_schema = f"{_q(db)}.INFORMATION_SCHEMA.COLUMNS"
    sql = (
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT, ORDINAL_POSITION "
        "FROM {table} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    ).format(table=info_schema)
    try:
        rows = session.sql(sql, params=[schema.upper(), table.upper()]).collect()
    except Exception:
        try:
            desc_sql = f"DESC TABLE {_q(db)}.{_q(schema)}.{_q(table)}"
            rows = session.sql(desc_sql).collect()
        except Exception:
            return []
        out: List[Dict[str, Any]] = []
        for row in rows:
            if hasattr(row, "asDict"):
                data = row.asDict()
                name = data.get("name")
                dtype = data.get("type")
                nullable = data.get("null?", data.get("nullable"))
                comment = data.get("comment")
                ordinal = data.get("ordinal_position", data.get("sequence"))
            else:
                try:
                    name = row[0]
                    dtype = row[1]
                    nullable = row[2] if len(row) > 2 else None
                    comment = row[3] if len(row) > 3 else None
                except Exception:
                    name = dtype = nullable = comment = None
                ordinal = None
            if not name:
                continue
            out.append(
                {
                    "column_name": str(name),
                    "data_type": str(dtype or ""),
                    "is_nullable": bool((nullable or "").upper().startswith("Y")) if isinstance(nullable, str) else None,
                    "comment": comment,
                    "ordinal_position": ordinal,
                }
            )
        return out

    cols: List[Dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "asDict"):
            data = row.asDict()
        else:
            data = {
                "COLUMN_NAME": row[0],
                "DATA_TYPE": row[1],
                "IS_NULLABLE": row[2] if len(row) > 2 else None,
                "COMMENT": row[6] if len(row) > 6 else None,
                "ORDINAL_POSITION": row[3] if len(row) > 3 else None,
            }
        cols.append(
            {
                "column_name": str(data.get("COLUMN_NAME")),
                "data_type": str(data.get("DATA_TYPE") or ""),
                "is_nullable": (data.get("IS_NULLABLE") or "").upper() == "YES",
                "comment": data.get("COMMENT"),
                "ordinal_position": data.get("ORDINAL_POSITION"),
            }
        )
    return cols


def _collect_single_row(session, sql: str, params: Optional[Sequence[Any]] = None):
    result = session.sql(sql, params=params).collect()
    return result[0] if result else None


def _extract_row_value(row, key: str, default=None):
    if row is None:
        return default
    if hasattr(row, "asDict"):
        data = row.asDict()
        for k in (key, key.upper(), key.lower()):
            if k in data:
                return data[k]
        return next(iter(data.values()), default)
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return default


def run_table_profile(
    session,
    fqn: str,
    sample_pct: Optional[float] = 10.0,
    top_n: int = 10,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Profile a table and return summary plus per-column metrics."""

    if not session or not fqn:
        return {}, []

    db, schema, table = _split_fqn(fqn)
    columns = list_columns(session, db, schema, table)
    if not columns:
        return {}, []

    pct: Optional[float]
    if sample_pct is None:
        pct = None
    else:
        pct = max(0.0, min(float(sample_pct), 100.0))
        if pct <= 0 or math.isclose(pct, 100.0, abs_tol=1e-6):
            pct = None

    sample_clause = "" if pct is None else f" SAMPLE BERNOULLI({pct})"

    table_ref = f"{_q(db)}.{_q(schema)}.{_q(table)}"
    sampled_ref = table_ref + sample_clause

    total_row_row = _collect_single_row(session, f"SELECT COUNT(*) AS CNT FROM {table_ref}")
    total_rows_raw = _extract_row_value(total_row_row, "CNT", 0)
    try:
        total_rows = int(total_rows_raw)
    except Exception:
        total_rows = 0

    profiled_row = _collect_single_row(session, f"SELECT COUNT(*) AS CNT FROM {sampled_ref}")
    profiled_raw = _extract_row_value(profiled_row, "CNT", 0)
    try:
        rows_profiled = int(profiled_raw)
    except Exception:
        rows_profiled = 0

    per_column: List[Dict[str, Any]] = []
    approx_threshold = 100000

    top_n_clamped = max(0, min(int(top_n), 10))

    for meta in columns:
        name = meta.get("column_name")
        if not name:
            continue
        dtype = meta.get("data_type") or ""
        qcol = _quote_identifier(name)
        distinct_expr = "APPROX_COUNT_DISTINCT({col})" if rows_profiled > approx_threshold else "COUNT(DISTINCT {col})"
        metrics_sql = [
            f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS NULLS",
            f"{distinct_expr.format(col=qcol)} AS DISTINCTS",
        ]
        if _is_numeric(dtype) or _is_temporal(dtype):
            metrics_sql.extend(
                [
                    f"MIN({qcol}) AS MIN_VAL",
                    f"MAX({qcol}) AS MAX_VAL",
                ]
            )
        else:
            metrics_sql.extend(["NULL AS MIN_VAL", "NULL AS MAX_VAL"])
        if _is_string_type(dtype):
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
            )
        else:
            metrics_sql.append("0 AS WHITESPACE_ROWS")
        metrics_sql.append(f"AVG(LENGTH({qcol}::STRING)) AS AVG_LEN")

        sql = "SELECT " + ", ".join(metrics_sql) + f" FROM {sampled_ref}"
        try:
            row = _collect_single_row(session, sql)
        except Exception:
            row = None

        nulls = _extract_row_value(row, "NULLS", 0)
        try:
            nulls_int = int(nulls)
        except Exception:
            nulls_int = 0

        distincts = _extract_row_value(row, "DISTINCTS")
        try:
            distincts_int = int(distincts) if distincts is not None else None
        except Exception:
            try:
                distincts_int = int(float(distincts)) if distincts is not None else None
            except Exception:
                distincts_int = None

        min_val = _extract_row_value(row, "MIN_VAL")
        max_val = _extract_row_value(row, "MAX_VAL")
        whitespace_rows = _extract_row_value(row, "WHITESPACE_ROWS", 0)
        avg_len_raw = _extract_row_value(row, "AVG_LEN")
        try:
            avg_len = float(avg_len_raw) if avg_len_raw is not None else None
        except Exception:
            avg_len = None
        try:
            whitespace_rows_int = int(whitespace_rows)
        except Exception:
            whitespace_rows_int = 0

        null_pct = (float(nulls_int) / rows_profiled * 100.0) if rows_profiled else 0.0
        non_nulls = max(rows_profiled - nulls_int, 0)
        if distincts_int is not None and non_nulls:
            distinct_pct = float(distincts_int) / float(non_nulls) * 100.0
        else:
            distinct_pct = None
        whitespace_pct = (float(whitespace_rows_int) / rows_profiled * 100.0) if rows_profiled else 0.0

        top_values: List[Dict[str, Any]] = []
        top_coverage = 0
        if top_n_clamped > 0 and rows_profiled:
            top_sql = (
                f"SELECT {qcol} AS VALUE, COUNT(*) AS CNT FROM {sampled_ref} "
                f"WHERE {qcol} IS NOT NULL GROUP BY 1 ORDER BY CNT DESC LIMIT {top_n_clamped}"
            )
            try:
                for item in session.sql(top_sql).collect():
                    if hasattr(item, "asDict"):
                        data = item.asDict()
                        value = data.get("VALUE") if "VALUE" in data else data.get("value")
                        count_raw = data.get("CNT") if "CNT" in data else data.get("cnt")
                    else:
                        value = item[0]
                        count_raw = item[1] if len(item) > 1 else 0
                    try:
                        count_int = int(count_raw)
                    except Exception:
                        count_int = 0
                    top_coverage += count_int
                    pct = (float(count_int) / non_nulls * 100.0) if non_nulls else 0.0
                    top_values.append({"value": value, "count": count_int, "pct": pct})
            except Exception:
                top_values = []
                top_coverage = 0
        coverage_pct = (float(top_coverage) / non_nulls * 100.0) if non_nulls else 0.0

        per_column.append(
            {
                "name": name,
                "column_name": name,
                "data_type": dtype,
                "nulls": nulls_int,
                "null_pct": null_pct,
                "distincts": distincts_int,
                "distinct_pct": distinct_pct,
                "min_val": min_val if (min_val is not None) else None,
                "max_val": max_val if (max_val is not None) else None,
                "avg_len": avg_len,
                "whitespace_pct": whitespace_pct,
                "top_values": top_values,
                "top_coverage_pct": coverage_pct,
                "rows_profiled": rows_profiled,
                "non_nulls": non_nulls,
                "error": None,
            }
        )

    summary = {
        "table": fqn,
        "database": db,
        "schema": schema,
        "table_name": table,
        "rowcount": total_rows,
        "rows_profiled": rows_profiled,
        "sample_pct": pct,
    }

    return summary, per_column


def _pattern_match_ratio(values: Iterable[Dict[str, Any]], regex: str) -> float:
    import re

    compiled = re.compile(regex)
    total = 0
    matched = 0
    for entry in values:
        count = entry.get("count", 0) or 0
        value = entry.get("value")
        if value is None:
            continue
        total += int(count)
        text = str(value)
        if compiled.fullmatch(text):
            matched += int(count)
    if total == 0:
        return 0.0
    return float(matched) / float(total)


def _detect_format(top_values: Sequence[Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    if not top_values:
        return None
    patterns = [
        (r"[^@\s]+@[^@\s]+\.[^@\s]+", "EMAIL"),
        (r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "UUID"),
        (r"\d{4}-\d{2}-\d{2}", "ISO_DATE"),
    ]
    for regex, label in patterns:
        ratio = _pattern_match_ratio(top_values, regex)
        if ratio >= 0.9:
            return regex, label
    return None


def suggest_checks_from_profile(
    per_col: List[Dict[str, Any]],
    recent_rowcount: Optional[int] = None,
) -> Dict[str, Any]:
    """Generate heuristic data quality checks from profile information."""

    suggestions: Dict[str, Any] = {"columns": {}, "table": {}}
    if not per_col:
        return suggestions

    rows_profiled = max(int(col.get("rows_profiled") or 0) for col in per_col)
    baseline_rows = recent_rowcount if recent_rowcount is not None else rows_profiled
    baseline_rows = max(baseline_rows, rows_profiled)

    best_ts_col: Optional[str] = None
    best_ts_score = -1.0

    for col in per_col:
        name = col.get("column_name") or col.get("name")
        if not name:
            continue
        data_type = col.get("data_type") or ""
        null_pct = float(col.get("null_pct") or 0.0)
        distinct_pct = col.get("distinct_pct")
        distincts = col.get("distincts")
        min_val = col.get("min_val")
        max_val = col.get("max_val")
        whitespace_pct = float(col.get("whitespace_pct") or 0.0)
        top_values = col.get("top_values") or []
        coverage_pct = float(col.get("top_coverage_pct") or 0.0)
        rows_profiled_col = int(col.get("rows_profiled") or rows_profiled)
        non_nulls = int(col.get("non_nulls") or max(rows_profiled_col - int(col.get("nulls") or 0), 0))

        checks: Dict[str, Dict[str, Any]] = {}

        if distinct_pct is not None and distincts is not None:
            if distinct_pct >= 99.9 and null_pct <= 1.0 and distincts >= non_nulls:
                checks["UNIQUE"] = {"severity": "ERROR", "params": {"ignore_nulls": True}}

        if null_pct > 0:
            severity = "ERROR" if null_pct >= 1.0 else "WARN"
            max_nulls = int(math.ceil((baseline_rows or 0) * (null_pct / 100.0)))
            checks["NULL_COUNT"] = {"severity": severity, "params": {"max_nulls": max_nulls}}

        if whitespace_pct >= 5.0 and _is_string_type(data_type):
            checks["WHITESPACE"] = {"severity": "WARN", "params": {"mode": "NO_LEADING_TRAILING"}}

        if (min_val is not None and max_val is not None) and (_is_numeric(data_type) or _is_temporal(data_type)):
            checks["MIN_MAX"] = {
                "severity": "WARN",
                "params": {
                    "min": _stringify(min_val),
                    "max": _stringify(max_val),
                },
            }

        if distincts is not None and distincts <= 20 and coverage_pct >= 90.0 and top_values:
            allowed = []
            total_counts = 0
            for entry in top_values:
                value = entry.get("value")
                count = entry.get("count") or 0
                if value is None:
                    continue
                allowed.append(_stringify(value))
                total_counts += int(count)
            if allowed and total_counts:
                checks["VALUE_DISTRIBUTION"] = {
                    "severity": "WARN",
                    "params": {
                        "allowed_values_csv": ", ".join(allowed[:20]),
                        "min_match_ratio": 0.9,
                    },
                }

        fmt_match = _detect_format(top_values)
        if fmt_match and _is_string_type(data_type):
            regex, label = fmt_match
            checks["FORMAT_DISTRIBUTION"] = {
                "severity": "WARN",
                "params": {"regex": regex, "label": label},
            }

        if checks:
            suggestions["columns"][name] = {
                "checks": checks,
                "data_type": data_type,
                "sample_rows": 25,
            }

        if _is_temporal(data_type):
            name_upper = name.upper()
            score = 100.0 - null_pct
            for boost, keyword in (
                (50, "UPDATE"),
                (45, "MODIFIED"),
                (40, "LOAD"),
                (35, "CREATE"),
                (30, "EVENT"),
                (25, "TIME"),
            ):
                if keyword in name_upper:
                    score += boost
            if score > best_ts_score:
                best_ts_score = score
                best_ts_col = name

    if best_ts_col:
        suggestions["table"]["FRESHNESS"] = {
            "severity": "WARN",
            "params": {
                "timestamp_column": best_ts_col,
                "max_age_minutes": 1440,
            },
        }
        suggestions["table"]["ROW_COUNT_ANOMALY"] = {
            "severity": "WARN",
            "params": {
                "timestamp_column": best_ts_col,
                "lookback_days": 28,
                "sensitivity": 3.0,
                "min_history_days": 7,
            },
        }

    return suggestions


def save_profile_results(
    session,
    meta_db: str,
    meta_schema: str,
    run_info: Dict[str, Any],
    rows: List[Dict[str, Any]],
) -> str:
    """Persist profile results into metadata tables and return the run identifier."""

    if not session:
        raise ValueError("Session is required")
    if not (meta_db and meta_schema):
        raise ValueError("Metadata database and schema are required")

    run_id = run_info.get("run_id") or uuid4().hex
    summary_json = json.dumps({**run_info, "run_id": run_id})

    runs_tbl = f"{_q(meta_db)}.{_q(meta_schema)}.DQ_PROFILE_RUN"
    cols_tbl = f"{_q(meta_db)}.{_q(meta_schema)}.DQ_PROFILE_COLUMN"

    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {runs_tbl} (
            RUN_ID STRING,
            RUN_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SUMMARY VARIANT,
            PRIMARY KEY (RUN_ID)
        )
        """
    ).collect()
    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cols_tbl} (
            RUN_ID STRING,
            COLUMN_NAME STRING,
            PROFILE VARIANT,
            PRIMARY KEY (RUN_ID, COLUMN_NAME)
        )
        """
    ).collect()

    session.sql(f"DELETE FROM {runs_tbl} WHERE RUN_ID = ?", params=[run_id]).collect()
    session.sql(f"DELETE FROM {cols_tbl} WHERE RUN_ID = ?", params=[run_id]).collect()

    session.sql(
        f"INSERT INTO {runs_tbl} (RUN_ID, SUMMARY) SELECT ?, PARSE_JSON(?)",
        params=[run_id, summary_json],
    ).collect()

    for row_data in rows:
        column_name = row_data.get("column_name") or row_data.get("name")
        if not column_name:
            continue
        serialized = json.dumps({**row_data, "column_name": column_name})
        session.sql(
            f"INSERT INTO {cols_tbl} (RUN_ID, COLUMN_NAME, PROFILE) SELECT ?, ?, PARSE_JSON(?)",
            params=[run_id, column_name, serialized],
        ).collect()

    return str(run_id)
