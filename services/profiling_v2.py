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


def _truncate_details(value: Any, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "\u2026"


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


def _latest_classifications(class_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if not isinstance(class_df, pd.DataFrame) or class_df.empty:
        return {}

    working = class_df.copy()
    sort_by: List[str] = []
    ascending: List[bool] = []
    if "COLUMN_NAME" in working.columns:
        sort_by.append("COLUMN_NAME")
        ascending.append(True)
    for column in ("SOURCE", "CLASSIFIED_AT"):
        if column in working.columns:
            sort_by.append(column)
            ascending.append(False)

    if sort_by:
        working = working.sort_values(by=sort_by, ascending=ascending)

    if "COLUMN_NAME" not in working.columns:
        return {}

    deduped = working.drop_duplicates(subset=["COLUMN_NAME"], keep="first")
    result: Dict[str, Dict[str, Any]] = {}
    for record in deduped.to_dict("records"):
        column = record.get("COLUMN_NAME")
        if column is not None:
            result[str(column)] = record
    return result


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

    def _stringify(value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        return str(value)

    def norm_conf(value: Any) -> str:
        if value is None or pd.isna(value):
            return "-"
        try:
            return f"{float(value):.2f}"
        except Exception:
            return str(value)

    features = _normalize_dataframe_columns(get_column_features(session, normalized))
    if features.empty:
        return pd.DataFrame(columns=overview_columns)

    suggestions = _normalize_dataframe_columns(get_suggested_checks(session, normalized))
    classification = _normalize_dataframe_columns(
        get_column_classification(session, normalized)
    )

    suggestion_lookup: Dict[str, Dict[str, Any]] = {}
    if not suggestions.empty and "COLUMN_NAME" in suggestions.columns:
        ordering: List[str] = []
        ascending: List[bool] = []
        if "SUGGESTED_AT" in suggestions.columns:
            ordering.append("SUGGESTED_AT")
            ascending.append(False)
        if "RULE_ID" in suggestions.columns:
            ordering.append("RULE_ID")
            ascending.append(True)
        working_suggestions = suggestions.copy()
        if ordering:
            working_suggestions = working_suggestions.sort_values(
                by=ordering, ascending=ascending
            )

        for column, group in working_suggestions.groupby("COLUMN_NAME"):
            def _join(field: str, sep: str) -> Optional[str]:
                if field not in group.columns:
                    return None
                values = [value for value in group[field].tolist() if pd.notna(value)]
                if not values:
                    return None
                return sep.join(str(value) for value in values)

            suggestion_lookup[str(column)] = {
                "rule_id": _join("RULE_ID", ", ") or "-",
                "check_type": _join("CHECK_TYPE", "; ") or "-",
                "severity": _join("SEVERITY", ", ") or "-",
                "rationale": _join("RATIONALE", " | ") or "-",
                "has_suggestion": bool(len(group)),
            }

    latest_classifications = _latest_classifications(classification)

    overview_rows: List[Dict[str, Any]] = []
    for _, row in features.iterrows():
        column_name = _normalize_column_name(row.get("COLUMN_NAME"))
        column_suggestions = suggestion_lookup.get(column_name, {})
        classification_row = latest_classifications.get(column_name, {})

        overview_rows.append(
            {
                "include_in_dq_config": bool(column_suggestions.get("has_suggestion", False)),
                "column_name": column_name,
                "data_type": _stringify(row.get("DATA_TYPE")),
                "null_info": _format_count_ratio(
                    row.get("NULL_COUNT"), row.get("NULL_RATIO")
                ),
                "distinct_info": _format_count_ratio(
                    row.get("DISTINCT_COUNT"), row.get("DISTINCT_RATIO")
                ),
                "min_value": _stringify(row.get("MIN_VALUE")),
                "max_value": _stringify(row.get("MAX_VALUE")),
                "length_info": _format_length_triplet(
                    row.get("MIN_LENGTH"),
                    row.get("MAX_LENGTH"),
                    row.get("AVG_LENGTH"),
                ),
                "rule_id": column_suggestions.get("rule_id", "-"),
                "check_type": column_suggestions.get("check_type", "-"),
                "severity": column_suggestions.get("severity", "-"),
                "rationale": _truncate_details(
                    column_suggestions.get("rationale", "-"), max_length=500
                ),
                "confidence": norm_conf(classification_row.get("CONFIDENCE")),
                "has_suggestion": bool(column_suggestions.get("has_suggestion", False)),
            }
        )

    overview = pd.DataFrame.from_records(overview_rows, columns=overview_columns)
    return overview


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
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            TABLE_FQN,
            STARTED_AT,
            FINISHED_AT,
            STATUS,
            DETAILS,
            ROW_COUNT,
            SAMPLE_MODE,
            SAMPLE_PERCENT,
            SAMPLE_EST_ROWS,
            CREATED_AT,
            UPDATED_AT
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
