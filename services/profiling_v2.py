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
    """Return the table FQN when it is a clean DB.SCHEMA.TABLE string."""

    if table_fqn is None:
        return ""

    value = str(table_fqn)
    if not value:
        return ""

    # Reject inputs with leading/trailing whitespace to avoid mutating the FQN.
    if value != value.strip():
        return ""

    parts = value.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        return ""

    return value


def _require_session(session: Any) -> Any:
    if session is None:
        raise ProfilingError("Snowpark session is required")
    return session


def _execute_sql(session: Any, sql: str, params: Optional[Iterable[Any]] = None):
    stmt = _require_session(session).sql(sql, params=params)
    return stmt


def _friendly_error_message(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        return exc.__class__.__name__
    return message


def run_profiling_v2(session: Any, table_fqn: str) -> None:
    """Execute the Profiling v2 stored procedure for *table_fqn*."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        raise ProfilingError("Fully-qualified table name is required")

    LOGGER.info("profiling_v2:call proc target=%s", normalized)
    try:
        _execute_sql(session, f"CALL {PROFILE_PROC}(?)", params=[normalized]).collect()
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        message = _friendly_error_message(exc)
        LOGGER.exception("profiling_v2:proc_failed target=%s", normalized)
        raise ProfilingError(f"Profiling run failed: {message}") from exc


# Backwards compatibility for earlier callers/tests.
run_full_profile = run_profiling_v2


def _fetch_dataframe(session: Any, sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    return _execute_sql(session, sql, params=params).to_pandas()


def _sort_summary_frame(df: pd.DataFrame) -> pd.DataFrame:
    ordering = [col for col in ("PROFILED_AT", "UPDATED_AT", "RUN_TS") if col in df.columns]
    if ordering:
        return df.sort_values(by=ordering, ascending=False)
    return df


def get_table_profile_summary(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return all profiling summary rows for the table."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"SELECT * FROM {TABLE_SUMMARY_VIEW} WHERE TABLE_FQN = ?"
    try:
        return _fetch_dataframe(session, sql, params=[normalized])
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:summary_fetch_failed target=%s", normalized)
        return pd.DataFrame()


def fetch_table_summary(session: Any, table_fqn: str) -> Dict[str, Any]:
    """Return the most recent table-level profiling summary."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return {}

    df = get_table_profile_summary(session, normalized)
    if df.empty:
        return {}
    latest = _sort_summary_frame(df).iloc[0]
    return latest.to_dict()


def get_column_features(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return profiling features for each column on *table_fqn*."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT *
        FROM {COLUMN_FEATURES_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY COLUMN_NAME
    """
    try:
        df = _fetch_dataframe(session, sql, params=[normalized])
        if not df.empty:
            for column in ("MIN_VALUE", "MAX_VALUE"):
                if column in df.columns:
                    df[column] = df[column].apply(
                        lambda value: None if pd.isna(value) else str(value)
                    )
        return df
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:column_features_failed target=%s", normalized)
        return pd.DataFrame()


def fetch_column_features(session: Any, table_fqn: str) -> pd.DataFrame:
    """Backwards-compatible wrapper for :func:`get_column_features`."""

    return get_column_features(session, table_fqn)


def get_column_classification(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return semantic classification rows ordered for UI rendering."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT *
        FROM {COLUMN_CLASSIFICATION_TABLE}
        WHERE TABLE_FQN = :table_fqn
        ORDER BY COLUMN_NAME, SOURCE DESC, CLASSIFIED_AT DESC
    """
    try:
        return _fetch_dataframe(session, sql, params={"table_fqn": normalized})
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception(
            "profiling_v2:column_classification_failed target=%s", normalized
        )
        return pd.DataFrame()


def fetch_column_classifications(session: Any, table_fqn: str) -> pd.DataFrame:
    """Backwards-compatible wrapper for :func:`get_column_classification`."""

    return get_column_classification(session, table_fqn)


def get_effective_classification(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return the latest classification per column for rendering."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT *
        FROM {COLUMN_CLASSIFICATION_TABLE}
        WHERE TABLE_FQN = :table_fqn
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TABLE_FQN, COLUMN_NAME
            ORDER BY CLASSIFIED_AT DESC
        ) = 1
        ORDER BY COLUMN_NAME
    """
    try:
        return _fetch_dataframe(session, sql, params={"table_fqn": normalized})
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception(
            "profiling_v2:effective_column_classification_failed target=%s",
            normalized,
        )
        return pd.DataFrame()


def save_manual_classification(
    session: Any,
    table_fqn: str,
    column_name: str,
    content_type: Optional[str],
    semantic_role: Optional[str],
    actor: Optional[str] = None,
) -> None:
    """Persist a manual classification override for *column_name*."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        raise ProfilingError("Fully-qualified table name is required")

    column = str(column_name or "").strip()
    if not column:
        raise ProfilingError("Column name is required for manual classification")

    if actor:
        LOGGER.info(
            "profiling_v2:manual_override actor=%s table=%s column=%s",
            actor,
            normalized,
            column,
        )

    sql = f"""
        INSERT INTO {COLUMN_CLASSIFICATION_TABLE} (
            TABLE_FQN,
            COLUMN_NAME,
            CONTENT_TYPE,
            SEMANTIC_ROLE,
            SOURCE,
            CONFIDENCE,
            CLASSIFIED_AT
        ) VALUES (
            :table_fqn,
            :column_name,
            :content_type,
            :semantic_role,
            'MANUAL',
            1.0,
            CURRENT_TIMESTAMP()
        )
    """
    params = {
        "table_fqn": normalized,
        "column_name": column,
        "content_type": (content_type or None),
        "semantic_role": (semantic_role or None),
    }
    try:
        _execute_sql(session, sql, params=params).collect()
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception(
            "profiling_v2:manual_classification_failed target=%s column=%s",
            normalized,
            column,
        )
        raise ProfilingError("Failed to save manual classification") from exc


def get_suggested_checks(session: Any, table_fqn: str) -> pd.DataFrame:
    """Return suggested DQ checks for each column."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT *
        FROM {SUGGESTED_CHECKS_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY COLUMN_NAME, RULE_ID
    """
    try:
        return _fetch_dataframe(session, sql, params=[normalized])
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:suggested_checks_failed target=%s", normalized)
        return pd.DataFrame()


def fetch_suggested_checks(session: Any, table_fqn: str) -> pd.DataFrame:
    """Backwards-compatible wrapper for :func:`get_suggested_checks`."""

    return get_suggested_checks(session, table_fqn)


def fetch_recent_runs(session: Any, table_fqn: str, limit: int = 10) -> pd.DataFrame:
    """Return recent profiling runs for the table."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        SELECT
            PROFILE_RUN_ID AS RUN_ID,
            TABLE_FQN,
            STARTED_AT,
            FINISHED_AT,
            STATUS,
            DETAILS
        FROM {PROFILE_RUN_TABLE}
        WHERE TABLE_FQN = ?
        ORDER BY STARTED_AT DESC
        LIMIT {max(1, limit)}
    """
    try:
        return _fetch_dataframe(session, sql, params=[normalized])
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:recent_runs_failed target=%s", normalized)
        return pd.DataFrame()
