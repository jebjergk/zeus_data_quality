from __future__ import annotations

from typing import Any, Dict, List, Optional


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _is_numeric(data_type: str) -> bool:
    upper = data_type.upper()
    return any(token in upper for token in ("NUMBER", "INT", "DECIMAL", "FLOAT", "DOUBLE", "REAL"))


def _is_temporal(data_type: str) -> bool:
    upper = data_type.upper()
    return any(token in upper for token in ("DATE", "TIME", "TIMESTAMP"))


def build_profile_suggestion(profile_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Generate heuristic DQ suggestions from a profile result."""

    if not profile_result:
        return None

    columns: List[Dict[str, Any]] = profile_result.get("columns") or []
    summary: Dict[str, Any] = profile_result.get("summary") or {}
    rows_profiled = int(summary.get("rows_profiled") or 0)

    suggestion_columns: Dict[str, Dict[str, Any]] = {}
    for column in columns:
        name = column.get("name") or column.get("column_name")
        if not name:
            continue
        data_type = str(column.get("data_type") or "")
        nulls = column.get("nulls") or 0
        null_pct = float(column.get("null_pct") or 0.0)
        distincts = column.get("distincts")
        whitespace_pct = float(column.get("whitespace_pct") or 0.0)
        min_val = column.get("min_val")
        max_val = column.get("max_val")
        top_values = column.get("top_values") or []

        checks: Dict[str, Dict[str, Any]] = {}
        sample_rows = 25

        if rows_profiled > 0:
            if nulls == 0:
                checks["NULL_COUNT"] = {"severity": "ERROR", "params": {"max_nulls": 0}}
            else:
                severity = "ERROR" if null_pct >= 5 else "WARN"
                checks["NULL_COUNT"] = {"severity": severity, "params": {"max_nulls": int(nulls)}}

        if rows_profiled > 0 and distincts is not None:
            if nulls == 0 and distincts >= max(rows_profiled - 1, 1):
                checks["UNIQUE"] = {"severity": "ERROR", "params": {"ignore_nulls": True}}
            elif distincts <= max(10, rows_profiled * 0.05):
                allowed = []
                total_top = 0
                for entry in top_values:
                    value = entry.get("value")
                    count = entry.get("count") or 0
                    if value is None:
                        continue
                    allowed.append(_stringify(value))
                    total_top += int(count)
                coverage = (total_top / rows_profiled) if rows_profiled else 0
                if allowed and coverage >= 0.8:
                    checks["VALUE_DISTRIBUTION"] = {
                        "severity": "WARN",
                        "params": {
                            "allowed_values_csv": ", ".join(allowed[:20]),
                            "min_match_ratio": 0.8,
                        },
                    }

        if whitespace_pct >= 5:
            checks["WHITESPACE"] = {"severity": "WARN", "params": {"mode": "NO_LEADING_TRAILING"}}

        if (min_val is not None and max_val is not None) and (_is_numeric(data_type) or _is_temporal(data_type)):
            checks["MIN_MAX"] = {
                "severity": "WARN",
                "params": {"min": _stringify(min_val), "max": _stringify(max_val)},
            }

        if not checks:
            continue

        suggestion_columns[name] = {
            "sample_rows": sample_rows,
            "checks": checks,
            "data_type": data_type,
        }

    suggestion_table: Dict[str, Dict[str, Any]] = {}
    best_ts_col: Optional[str] = None
    best_score = -1.0
    for column in columns:
        name = column.get("name") or column.get("column_name")
        if not name:
            continue
        data_type = str(column.get("data_type") or "")
        if not _is_temporal(data_type):
            continue
        null_pct = float(column.get("null_pct") or 0.0)
        score = 100.0 - null_pct
        name_upper = name.upper()
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
        if score > best_score:
            best_score = score
            best_ts_col = name

    if best_ts_col:
        suggestion_table["FRESHNESS"] = {
            "severity": "WARN",
            "params": {"timestamp_column": best_ts_col, "max_age_minutes": 1440},
        }
        suggestion_table["ROW_COUNT_ANOMALY"] = {
            "severity": "WARN",
            "params": {
                "timestamp_column": best_ts_col,
                "lookback_days": 28,
                "sensitivity": 3.0,
                "min_history_days": 7,
            },
        }

    if not suggestion_columns and not suggestion_table:
        return None

    return {
        "target_table": profile_result.get("target_table"),
        "summary": summary,
        "columns": suggestion_columns,
        "table": suggestion_table,
    }
