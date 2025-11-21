"""Helpers for Profiling v2 metadata interactions."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from snowflake.snowpark import Session

LOGGER = logging.getLogger(__name__)

DISCOVERY_DB = "ZEUS_ANALYTICS_SIMU"
DISCOVERY_SCHEMA = "DISCOVERY"
DISCOVERY_NAMESPACE = f"{DISCOVERY_DB}.{DISCOVERY_SCHEMA}"

PROFILE_PROC = f"{DISCOVERY_NAMESPACE}.DQ_PROFILE_FULL"
CLASSIFY_PROC = f"{DISCOVERY_NAMESPACE}.DQ_CLASSIFY_COLUMNS_HEURISTIC"
SUGGESTIONS_PROC = f"{DISCOVERY_NAMESPACE}.DQ_APPLY_RULES"
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


def _run_single_stage(
    session: Any,
    table_fqn: str,
    proc_name: str,
    failure_message: str,
) -> None:
    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        raise ProfilingError("Fully-qualified table name is required")

    LOGGER.info("profiling_v2:call proc target=%s proc=%s", normalized, proc_name)
    try:
        sql = f"CALL {proc_name}(:table_fqn)"
        _execute_sql(session, sql, params={"table_fqn": normalized}).collect()
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        message = _friendly_error_message(exc)
        LOGGER.exception(
            "profiling_v2:proc_failed target=%s proc=%s", normalized, proc_name
        )
        raise ProfilingError(f"{failure_message}: {message}") from exc


def run_classification_only(session: Any, table_fqn: str) -> None:
    """Re-run only the column classification stage for *table_fqn*."""

    _run_single_stage(
        session,
        table_fqn,
        CLASSIFY_PROC,
        "Classification run failed",
    )


def run_suggestions_only(session: Any, table_fqn: str) -> None:
    """Execute only the DQ_APPLY_RULES stage for *table_fqn*."""

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        raise ProfilingError("Fully-qualified table name is required")

    LOGGER.info("profiling_v2:call apply_rules target=%s", normalized)
    try:
        sql = f"CALL {SUGGESTIONS_PROC}(?)"
        _execute_sql(session, sql, params=[normalized]).collect()
        LOGGER.info("profiling_v2:apply_rules_complete target=%s", normalized)
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        message = _friendly_error_message(exc)
        LOGGER.exception("profiling_v2:apply_rules_failed target=%s", normalized)
        raise ProfilingError(f"Suggestions run failed: {message}") from exc


def _fetch_dataframe(session: Any, sql: str, params: Optional[Iterable[Any]] = None) -> pd.DataFrame:
    return _execute_sql(session, sql, params=params).to_pandas()


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=list(columns))
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df


def _normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    renamed = {
        column: str(column or "").strip().upper()
        for column in df.columns
    }
    return df.rename(columns=renamed)


def _latest_partition(
    df: pd.DataFrame,
    partition_cols: Iterable[str],
    order_candidates: Iterable[str],
) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    working = df.copy()
    order_cols = [col for col in order_candidates if col in working.columns]
    if order_cols:
        working = working.sort_values(by=order_cols, ascending=[False] * len(order_cols))
    subset = [col for col in partition_cols if col in working.columns]
    if not subset:
        return working
    return working.drop_duplicates(subset=subset, keep="first")


def _format_count_ratio(count: Any, ratio: Any) -> str:
    def _format_value(value: Any) -> str:
        if pd.isna(value):
            return "-"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    count_text = _format_value(count)
    if pd.isna(ratio):
        return count_text
    return f"{count_text} ({float(ratio) * 100:.2f}%)"


def _format_length_triplet(min_length: Any, max_length: Any, avg_length: Any) -> str:
    def _format_value(value: Any) -> str:
        if pd.isna(value):
            return "-"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    return " / ".join(
        [
            _format_value(min_length),
            _format_value(max_length),
            _format_value(avg_length),
        ]
    )


def _normalize_column_name(column_name: Optional[str]) -> str:
    value = str(column_name or "").strip()
    return value


def _casefolded_column(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.casefold()


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


def get_effective_classification(session: Session, table_fqn: str) -> pd.DataFrame:
    """
    Return one row per column for the latest classification (manual or heuristic).
    Used by the column editors in the Profiling v2 UI.
    """

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame()

    sql = f"""
        WITH ranked AS (
            SELECT
                TABLE_FQN,
                COLUMN_NAME,
                CONTENT_TYPE,
                SEMANTIC_ROLE,
                SOURCE,
                CONFIDENCE,
                CLASSIFIED_AT,
                ROW_NUMBER() OVER (
                    PARTITION BY TABLE_FQN, COLUMN_NAME
                    ORDER BY CLASSIFIED_AT DESC
                ) AS RN
            FROM {COLUMN_CLASSIFICATION_TABLE}
            WHERE TABLE_FQN = :1
        )
        SELECT
            TABLE_FQN,
            COLUMN_NAME,
            CONTENT_TYPE,
            SEMANTIC_ROLE,
            SOURCE,
            CONFIDENCE,
            CLASSIFIED_AT
        FROM ranked
        WHERE RN = 1
    """
    try:
        return session.sql(sql, params=[normalized]).to_pandas()
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


def get_overview_grid(session: Session, table_fqn: str) -> pd.DataFrame:
    """
    Unified overview grid for Profiling v2:
    - One row per column
    - Features from DQ_COLUMN_FEATURES
    - ALL suggestions per column aggregated via LISTAGG from DQ_SUGGESTED_CHECKS
    - Latest classification CONFIDENCE from DQ_COLUMN_CLASSIFICATION
    """

    overview_columns = [
        "include_in_dq_config",
        "column_name",
        "data_type",
        "null_info",
        "distinct_info",
        "min_value",
        "max_value",
        "length_info",
        "rule_id",
        "check_type",
        "severity",
        "rationale",
        "confidence",
        "has_suggestion",
    ]

    normalized = _normalize_table_fqn(table_fqn)
    if not normalized:
        return pd.DataFrame(columns=overview_columns)

    sql = f"""
        WITH features AS (
            SELECT
                TABLE_FQN,
                COLUMN_NAME,
                DATA_TYPE,
                ROW_COUNT,
                NULL_COUNT,
                NULL_RATIO,
                DISTINCT_COUNT,
                DISTINCT_RATIO,
                MIN_VALUE,
                MAX_VALUE
            FROM {COLUMN_FEATURES_TABLE}
            WHERE TABLE_FQN = :1
        ),
        suggestions AS (
            SELECT
                TABLE_FQN,
                COLUMN_NAME,
                LISTAGG(RULE_ID, ', ')    WITHIN GROUP (ORDER BY SUGGESTED_AT DESC, RULE_ID) AS RULE_ID,
                LISTAGG(CHECK_TYPE, '; ') WITHIN GROUP (ORDER BY SUGGESTED_AT DESC, RULE_ID) AS CHECK_TYPE,
                LISTAGG(SEVERITY, ', ')   WITHIN GROUP (ORDER BY SUGGESTED_AT DESC, RULE_ID) AS SEVERITY,
                LISTAGG(RATIONALE, ' | ') WITHIN GROUP (ORDER BY SUGGESTED_AT DESC, RULE_ID) AS RATIONALE
            FROM {SUGGESTED_CHECKS_TABLE}
            WHERE TABLE_FQN = :1
            GROUP BY TABLE_FQN, COLUMN_NAME
        ),
        classification AS (
            SELECT
                TABLE_FQN,
                COLUMN_NAME,
                CONFIDENCE
            FROM (
                SELECT
                    TABLE_FQN,
                    COLUMN_NAME,
                    CONFIDENCE,
                    CLASSIFIED_AT,
                    ROW_NUMBER() OVER (
                        PARTITION BY TABLE_FQN, COLUMN_NAME
                        ORDER BY CLASSIFIED_AT DESC
                    ) AS RN
                FROM {COLUMN_CLASSIFICATION_TABLE}
                WHERE TABLE_FQN = :1
            )
            WHERE RN = 1
        )
        SELECT
            f.TABLE_FQN,
            f.COLUMN_NAME,
            f.DATA_TYPE,
            f.ROW_COUNT,
            f.NULL_COUNT,
            f.NULL_RATIO,
            f.DISTINCT_COUNT,
            f.DISTINCT_RATIO,
            f.MIN_VALUE,
            f.MAX_VALUE,
            s.RULE_ID,
            s.CHECK_TYPE,
            s.SEVERITY,
            s.RATIONALE,
            c.CONFIDENCE
        FROM features f
        LEFT JOIN suggestions s
          ON s.TABLE_FQN = f.TABLE_FQN
         AND s.COLUMN_NAME = f.COLUMN_NAME
        LEFT JOIN classification c
          ON c.TABLE_FQN = f.TABLE_FQN
         AND c.COLUMN_NAME = f.COLUMN_NAME
    """

    try:
        df = session.sql(sql, params=[normalized]).to_pandas()
    except Exception as exc:  # pragma: no cover - Snowflake specific failures
        LOGGER.exception("profiling_v2:overview_fetch_failed target=%s", normalized)
        return pd.DataFrame(columns=overview_columns)

    if df.empty:
        return pd.DataFrame(columns=overview_columns)

    df.columns = [str(column).lower() for column in df.columns]

    def fmt_ratio(count: Any, ratio: Any) -> str:
        if pd.isna(count) and pd.isna(ratio):
            return "-"
        if pd.isna(ratio):
            return str(int(count)) if pd.notna(count) else "0"
        try:
            pct = float(ratio) * 100.0
        except Exception:
            return str(count)
        count_str = str(int(count)) if pd.notna(count) else "0"
        return f"{count_str} ({pct:.2f}%)"

    df["null_info"] = df.apply(
        lambda row: fmt_ratio(row.get("null_count"), row.get("null_ratio")),
        axis=1,
    )
    df["distinct_info"] = df.apply(
        lambda row: fmt_ratio(row.get("distinct_count"), row.get("distinct_ratio")),
        axis=1,
    )

    df["min_value"] = df.get("min_value")
    df["max_value"] = df.get("max_value")
    df["length_info"] = "- / - / -"
    df["has_suggestion"] = df["rule_id"].notna()
    df["include_in_dq_config"] = df["has_suggestion"].astype(bool)

    def norm_conf(value: Any) -> str:
        if value is None or pd.isna(value):
            return "-"
        try:
            return f"{float(value):.2f}"
        except Exception:
            return str(value)

    df["confidence"] = df.get("confidence").map(norm_conf)

    for column in ("rule_id", "check_type", "severity", "rationale"):
        if column not in df.columns:
            df[column] = None
        df[column] = df[column].astype(object).where(df[column].notna(), "-")

    overview = pd.DataFrame()
    overview["include_in_dq_config"] = df["include_in_dq_config"].astype(bool)
    overview["column_name"] = df["column_name"].astype(str)
    overview["data_type"] = df.get("data_type", "").astype(str)
    overview["null_info"] = df["null_info"].astype(str)
    overview["distinct_info"] = df["distinct_info"].astype(str)
    overview["min_value"] = df["min_value"].fillna("").astype(str)
    overview["max_value"] = df["max_value"].fillna("").astype(str)
    overview["length_info"] = df["length_info"].astype(str)
    overview["rule_id"] = df["rule_id"]
    overview["check_type"] = df["check_type"]
    overview["severity"] = df["severity"]
    overview["rationale"] = df["rationale"]
    overview["confidence"] = df["confidence"]
    overview["has_suggestion"] = df["has_suggestion"].astype(bool)

    return overview[overview_columns]


def fetch_suggested_checks(session: Any, table_fqn: str) -> pd.DataFrame:
    """Backwards-compatible wrapper for :func:`get_suggested_checks`."""

    return get_suggested_checks(session, table_fqn)


def _filter_column_records(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    if "COLUMN_NAME" not in df.columns:
        return pd.DataFrame()
    normalized = _normalize_column_name(column_name)
    if not normalized:
        return pd.DataFrame()
    folded = _casefolded_column(df["COLUMN_NAME"])
    mask = folded == normalized.casefold()
    matches = df.loc[mask]
    if matches.empty:
        return pd.DataFrame()
    return matches


def _first_column_record(df: pd.DataFrame, column_name: str) -> Dict[str, Any]:
    matches = _filter_column_records(df, column_name)
    if matches.empty:
        return {}
    return matches.iloc[0].to_dict()


def _suggested_checks_for_column(df: pd.DataFrame, column_name: str) -> List[Dict[str, Any]]:
    matches = _filter_column_records(df, column_name)
    if matches.empty:
        return []
    return matches.to_dict("records")


def _pluck_fields(record: Dict[str, Any], fields: Iterable[str]) -> Dict[str, Any]:
    if not record:
        return {}
    result: Dict[str, Any] = {}
    for field in fields:
        if field in record:
            result[field] = record.get(field)
    return result


def get_column_detail(session: Any, table_fqn: str, column_name: str) -> Dict[str, Any]:
    """Return profiling, classification, and suggestion metadata for one column."""

    normalized = _normalize_table_fqn(table_fqn)
    column = _normalize_column_name(column_name)
    if not normalized or not column:
        return {}

    features = get_column_features(session, normalized)
    classification = get_effective_classification(session, normalized)
    suggestions = get_suggested_checks(session, normalized)
    feature_record = _first_column_record(features, column)
    classification_record = _first_column_record(classification, column)
    suggestion_records = _suggested_checks_for_column(suggestions, column)
    feature_fields = (
        "DATA_TYPE",
        "ROW_COUNT",
        "NULL_COUNT",
        "NULL_RATIO",
        "DISTINCT_COUNT",
        "DISTINCT_RATIO",
        "MIN_VALUE",
        "MAX_VALUE",
        "AVG_LENGTH",
        "MAX_LENGTH",
    )
    classification_fields = (
        "CONTENT_TYPE",
        "SEMANTIC_ROLE",
        "SOURCE",
        "CONFIDENCE",
        "CLASSIFIED_AT",
    )
    detail = {
        "table_fqn": normalized,
        "column_name": column,
        "features": _pluck_fields(feature_record, feature_fields),
        "classification": _pluck_fields(classification_record, classification_fields),
        "suggested_checks": [
            {
                "RULE_ID": record.get("RULE_ID"),
                "CHECK_TYPE": record.get("CHECK_TYPE"),
                "SEVERITY": record.get("SEVERITY"),
                "PARAMETERS": record.get("PARAMS", record.get("PARAMETERS")),
                "RATIONALE": record.get("RATIONALE"),
            }
            for record in suggestion_records
        ],
    }
    return detail


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
            DETAILS,
            ROW_COUNT,
            SAMPLE_MODE,
            SAMPLE_PERCENT,
            SAMPLE_EST_ROWS
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
