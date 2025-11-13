"""Helpers for Profiling v2 metadata interactions."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)

DISCOVERY_DB = "ZEUS_ANALYTICS_SIMU"
DISCOVERY_SCHEMA = "DISCOVERY"
DISCOVERY_NAMESPACE = f"{DISCOVERY_DB}.{DISCOVERY_SCHEMA}"

PROFILE_PROC = f"{DISCOVERY_NAMESPACE}.DQ_PROFILE_FULL"
TABLE_SUMMARY_VIEW = f"{DISCOVERY_NAMESPACE}.DQ_TABLE_PROFILE_SUMMARY"
COLUMN_FEATURES_TABLE = f"{DISCOVERY_NAMESPACE}.DQ_COLUMN_FEATURES"
COLUMN_CLASSIFICATION_TABLE = f"{DISCOVERY_NAMESPACE}.DQ_COLUMN_CLASSIFICATION"
SUGGESTED_CHECKS_TABLE = f"{DISCOVERY_NAMESPACE}.DQ_SUGGESTED_CHECKS"
PROFILE_RUN_TABLE = f"{DISCOVERY_NAMESPACE}.DQ_PROFILE_RUN"


class ProfilingError(RuntimeError):
    """Raised when profiling metadata cannot be loaded or executed."""


def _normalize_table_fqn(table_fqn: Optional[str]) -> str:
    if not table_fqn:
        return ""
    cleaned = str(table_fqn).strip()
    if not cleaned:
        return ""
    cleaned = cleaned.strip('"')
    return cleaned.upper()


def _require_session(session: Any) -> Any:
    if session is None:
        raise ProfilingError("Snowpark session is required")
    return session


def _execute_sql(session: Any, sql: str, params: Optional[Iterable[Any]] = None):
    stmt = _require_session(session).sql(sql, params=params)
    return stmt


def run_full_profile(session: Any, table_fqn: str) -> None:
    """Invoke the DQ profiling stored procedure for the provided table."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        raise ProfilingError("Fully-qualified table name is required")

    LOGGER.info("profiling_v2:call proc target=%s", normalized)
    try:
        _execute_sql(session, f"CALL {PROFILE_PROC}(?)", params=[normalized]).collect()
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:proc_failed target=%s", normalized)
        raise ProfilingError(str(exc)) from exc


def _sort_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    ordering = [col for col in ("PROFILED_AT", "UPDATED_AT", "RUN_TS") if col in df.columns]
    if ordering:
        return df.sort_values(by=ordering, ascending=False)
    return df


def fetch_table_summary(session: Any, table_fqn: str) -> Dict[str, Any]:
    """Return the most recent table-level profiling summary."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return {}

    sql = f"SELECT * FROM {TABLE_SUMMARY_VIEW} WHERE TABLE_FQN = ?"
    df = _execute_sql(session, sql, params=[normalized]).to_pandas()
    if df.empty:
        return {}
    latest = _sort_summary_frame(df).iloc[0]
    return latest.to_dict()


def fetch_column_features(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return per-column statistics from DQ_COLUMN_FEATURES."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT
            COLUMN_NAME,
            PHYSICAL_TYPE,
            NULL_RATIO,
            DISTINCT_RATIO,
            MIN_VALUE,
            MAX_VALUE,
            AVG_LENGTH,
            WHITESPACE_RATIO,
            SAMPLE_PATTERNS,
            PROFILED_AT
        FROM {COLUMN_FEATURES_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY COALESCE(ORDINAL_POSITION, 0), COLUMN_NAME
    """
    return _execute_sql(session, sql, params=[normalized]).to_pandas()


def fetch_column_classifications(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return semantic tags per column from DQ_COLUMN_CLASSIFICATION."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT
            COLUMN_NAME,
            CONTENT_TYPE,
            SEMANTIC_ROLE,
            SOURCE,
            CONFIDENCE,
            UPDATED_AT
        FROM {COLUMN_CLASSIFICATION_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY CONFIDENCE DESC NULLS LAST, COLUMN_NAME
    """
    return _execute_sql(session, sql, params=[normalized]).to_pandas()


def fetch_suggested_checks(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return suggested DQ checks surfaced by the profiling engine."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT
            COLUMN_NAME,
            CHECK_TYPE,
            PARAMETERS,
            RATIONALE,
            PRIORITY,
            CREATED_AT
        FROM {SUGGESTED_CHECKS_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY PRIORITY, COLUMN_NAME
    """
    return _execute_sql(session, sql, params=[normalized]).to_pandas()


def fetch_recent_runs(session: Any, table_fqn: str, limit: int = 10) -> pd.DataFrame:
    """Return recent profiling runs for the table."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT
            RUN_ID,
            TABLE_FQN,
            PROFILED_AT,
            ROW_COUNT,
            SAMPLE_PERCENT,
            STATUS,
            DURATION_SECONDS
        FROM {PROFILE_RUN_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY PROFILED_AT DESC
        LIMIT {max(1, limit)}
    """
    return _execute_sql(session, sql, params=[normalized]).to_pandas()
