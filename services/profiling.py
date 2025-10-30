"""Service helpers for table profiling and automated DQ suggestions."""

from __future__ import annotations

import json
import logging
import math
import os
import random
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from uuid import uuid4

from services.profile import _is_numeric, _is_temporal, _stringify
from services.semantics import clamp_confidence, truncate_note
from utils.meta import _q

__all__ = [
    "list_columns",
    "run_table_profile",
    "suggest_checks_from_profile",
    "save_profile_results",
    "normalize_profile_row",
]


logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


PROFILE_DEBUG = _env_flag("PROFILE_DEBUG", False)


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


SEMANTIC_REGEX_PATTERNS: Dict[str, str] = {
    "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
    "iban": r"^[A-Z]{2}[0-9A-Z]{13,32}$",
    "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
    "bic": r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "url": r"^(https?|ftp)://[^\s/$.?#].[^\s]*$",
    "ipv4": r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(?!$)|$)){4}$",
    "phone_e164": r"^\+[1-9][0-9]{1,14}$",
}


CHAR_CLASS_PATTERNS: Dict[str, str] = {
    "digit": r"^[0-9]+$",
    "alpha": r"^[A-Za-z]+$",
    "alnum": r"^[0-9A-Za-z]+$",
    "whitespace": r".*\s.*",
}


DATE_PARSE_CONFIGS: Tuple[Dict[str, str], ...] = (
    {
        "key": "iso",
        "format": "YYYY-MM-DD",
        "label": "YYYY-MM-DD",
        "regex": r"^\d{4}-\d{2}-\d{2}$",
    },
    {
        "key": "iso_slash",
        "format": "YYYY/MM/DD",
        "label": "YYYY/MM/DD",
        "regex": r"^\d{4}/\d{2}/\d{2}$",
    },
    {
        "key": "iso_dot",
        "format": "YYYY.MM.DD",
        "label": "YYYY.MM.DD",
        "regex": r"^\d{4}\.\d{2}\.\d{2}$",
    },
    {
        "key": "yyyymmdd",
        "format": "YYYYMMDD",
        "label": "YYYYMMDD",
        "regex": r"^\d{8}$",
    },
    {
        "key": "ddmmyyyy",
        "format": "DDMMYYYY",
        "label": "DDMMYYYY",
        "regex": r"^\d{8}$",
    },
    {
        "key": "dd_mm_yyyy",
        "format": "DD-MM-YYYY",
        "label": "DD-MM-YYYY",
        "regex": r"^\d{1,2}-\d{1,2}-\d{4}$",
    },
    {
        "key": "dd_slash_mm",
        "format": "DD/MM/YYYY",
        "label": "DD/MM/YYYY",
        "regex": r"^\d{1,2}/\d{1,2}/\d{4}$",
    },
    {
        "key": "mm_dd_yyyy",
        "format": "MM-DD-YYYY",
        "label": "MM-DD-YYYY",
        "regex": r"^\d{1,2}-\d{1,2}-\d{4}$",
    },
    {
        "key": "mm_slash_dd",
        "format": "MM/DD/YYYY",
        "label": "MM/DD/YYYY",
        "regex": r"^\d{1,2}/\d{1,2}/\d{4}$",
    },
    {
        "key": "dd_mon_yyyy",
        "format": "DD-MON-YYYY",
        "label": "DD-MON-YYYY",
        "regex": r"^\d{1,2}-[A-Za-z]{3}-\d{4}$",
        "transform": "UPPER({col}::STRING)",
    },
    {
        "key": "mon_dd_yyyy",
        "format": "MON-DD-YYYY",
        "label": "MON-DD-YYYY",
        "regex": r"^[A-Za-z]{3}-\d{1,2}-\d{4}$",
        "transform": "UPPER({col}::STRING)",
    },
)


DATE_PARSE_LABELS: Dict[str, str] = {
    config["key"]: config["label"] for config in DATE_PARSE_CONFIGS
}


DATE_PARSE_CONFIG_MAP: Dict[str, Dict[str, str]] = {
    config["key"]: config for config in DATE_PARSE_CONFIGS
}


FALLBACK_COUNTRY_CODES: Set[str] = {
    "US",
    "DE",
    "FR",
    "GB",
    "CA",
    "CH",
    "JP",
}


FALLBACK_COUNTRY_NAMES: Set[str] = {
    "UNITED STATES",
    "GERMANY",
    "FRANCE",
    "UNITED KINGDOM",
    "CANADA",
    "SWITZERLAND",
    "JAPAN",
}


FALLBACK_CURRENCY_CODES: Set[str] = {"USD", "EUR", "GBP", "CHF", "JPY", "CAD"}


FALLBACK_EXCHANGE_CODES: Set[str] = {"XETR", "GETTEX", "FWB"}


REFERENCE_SIGNAL_NAMES: Dict[str, str] = {
    "country_codes": "reference_country_code",
    "country_names": "reference_country_name",
    "currency_codes": "reference_currency_code",
    "exchange_codes": "reference_exchange_code",
}


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_in_clause(values: Set[str]) -> Optional[str]:
    if not values:
        return None
    parts = [f"'{_escape_sql_literal(val)}'" for val in sorted({v for v in values if v})]
    if not parts:
        return None
    return ", ".join(parts)


def _load_reference_sets(session, db: str, schema: str) -> Dict[str, Set[str]]:
    references: Dict[str, Set[str]] = {
        "country_codes": set(FALLBACK_COUNTRY_CODES),
        "country_names": set(FALLBACK_COUNTRY_NAMES),
        "currency_codes": set(FALLBACK_CURRENCY_CODES),
        "exchange_codes": set(FALLBACK_EXCHANGE_CODES),
    }
    candidates: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = {
        "country_codes": [
            ("ISO_COUNTRIES", ("CODE", "ALPHA2", "ALPHA3")),
            ("COUNTRIES", ("COUNTRY_CODE", "CODE", "ISO_CODE")),
        ],
        "country_names": [
            ("ISO_COUNTRIES", ("NAME", "COUNTRY_NAME")),
            ("COUNTRIES", ("COUNTRY_NAME", "NAME")),
        ],
        "currency_codes": [
            ("ISO_CURRENCIES", ("CODE", "CURRENCY_CODE")),
            ("CURRENCIES", ("CURRENCY_CODE", "CODE")),
        ],
        "exchange_codes": [
            ("EXCHANGES", ("EXCHANGE_CODE", "CODE")),
            ("MARKET_CODES", ("CODE",)),
        ],
    }

    for ref_key, table_candidates in candidates.items():
        loaded = False
        for table_name, columns in table_candidates:
            table_ref = f"{_q(db)}.{_q(schema)}.{_q(table_name)}"
            select_cols = []
            for col in columns:
                select_cols.append(_quote_identifier(col))
            sql = f"SELECT {', '.join(select_cols)} FROM {table_ref}"
            try:
                rows = session.sql(sql).collect()
            except Exception:
                continue
            values: Set[str] = set()
            for row in rows:
                if hasattr(row, "asDict"):
                    data = row.asDict()
                    for col in columns:
                        candidate = (
                            data.get(col)
                            or data.get(col.lower())
                            or data.get(col.upper())
                        )
                        if candidate is None:
                            continue
                        text = str(candidate).strip().upper()
                        if text:
                            values.add(text)
                else:
                    for idx, col in enumerate(columns):
                        try:
                            candidate = row[idx]
                        except Exception:
                            continue
                        if candidate is None:
                            continue
                        text = str(candidate).strip().upper()
                        if text:
                            values.add(text)
            if values:
                references[ref_key] = values
                loaded = True
                break
        if not loaded and ref_key in ("country_codes", "country_names"):
            # ensure consistency between country code/name sets if only one loads
            references[ref_key] = set(v.upper() for v in references.get(ref_key, set()))

    return references


def _derive_name_hints(column_name: str) -> Dict[str, bool]:
    lowered = (column_name or "").lower()
    tokens = {
        "email": {"email"},
        "iban": {"iban"},
        "isin": {"isin"},
        "bic": {"bic", "swift"},
        "uuid": {"uuid", "guid"},
        "url": {"url", "uri", "link"},
        "ipv4": {"ip", "ipv4"},
        "phone": {"phone", "mobile", "msisdn", "tel"},
        "country": {"country", "nation"},
        "currency": {"currency", "ccy"},
        "exchange": {"exchange", "venue", "market"},
        "account": {"account", "acct"},
        "order": {"order"},
        "trade": {"trade"},
        "ticker": {"ticker", "symbol"},
        "status": {"status", "state"},
        "enum": {"type", "class", "category"},
        "amount": {"amount", "amt", "value"},
        "price": {"price", "rate"},
        "quantity": {"qty", "quantity", "volume"},
        "timestamp": {"timestamp", "datetime", "created", "updated", "date"},
        "boolean": {"flag"},
        "code": {"code", "lookup"},
    }
    hints: Dict[str, bool] = {}
    for key, keywords in tokens.items():
        hints[key] = any(token in lowered for token in keywords)
    hints["boolean"] = hints.get("boolean", False) or lowered.startswith("is_") or lowered.startswith("has_")
    hints["id"] = "id" in lowered or lowered.endswith("_id")
    return hints


SEMANTIC_TYPE_CANDIDATES: Tuple[str, ...] = (
    "EMAIL",
    "IBAN",
    "ISIN",
    "BIC",
    "ACCOUNT_ID",
    "ORDER_ID",
    "TRADE_ID",
    "TICKER/SYMBOL",
    "CURRENCY_CODE",
    "COUNTRY_CODE/NAME",
    "PRICE/AMOUNT/QUANTITY",
    "ENUM/STATUS",
    "BOOLEAN",
    "TIMESTAMP/DATE",
    "UUID",
    "URL",
    "PHONE",
    "REF_CODE",
    "DATE_IN_TEXT",
)


BOOLEAN_TRUE_VALUES = {"1", "Y", "YES", "TRUE", "T"}
BOOLEAN_FALSE_VALUES = {"0", "N", "NO", "FALSE", "F"}


def _is_boolean_type(data_type: str) -> bool:
    upper = (data_type or "").upper()
    return "BOOL" in upper or "BOOLEAN" in upper


def _infer_semantic_type(
    column_entry: Dict[str, Any]
) -> Tuple[str, float, str, Optional[Any], Optional[Any]]:
    signals = column_entry.get("signals", {}) or {}
    regex = signals.get("regex", {}) or {}
    char_classes = signals.get("character_classes", {}) or {}
    references = signals.get("reference_matches", {}) or {}
    hints = signals.get("hints", {}) or {}
    length = signals.get("length", {}) or {}
    string_stats = signals.get("string_stats", {}) or {}
    date_patterns = signals.get("date_patterns", {}) or {}
    date_parse = signals.get("date_parse", {}) or {}
    num_date = signals.get("date_parse_numeric") or {}

    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _as_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                return None

    length_min = _as_float(length.get("min"))
    length_max = _as_float(length.get("max"))
    length_avg = _as_float(length.get("avg"))
    length_stddev = _as_float(length.get("stddev"))
    length_spread = _as_float(length.get("spread"))
    distinct_ratio_signal = _as_float(string_stats.get("distinct_ratio"))
    top1_ratio_signal = _as_float(string_stats.get("top1_ratio"))
    top3_ratio_signal = _as_float(string_stats.get("top3_ratio"))
    numeric_like_ratio_signal = _as_float(string_stats.get("numeric_like_ratio"))
    parse_format_ratios = date_parse.get("formats")
    if isinstance(parse_format_ratios, dict):
        format_ratios = {
            str(key): _as_float(value)
            for key, value in parse_format_ratios.items()
        }
    else:
        format_ratios = {}
        for key, value in date_parse.items():
            if key.endswith("_ratio"):
                format_ratios[str(key[:-6])] = _as_float(value)
    best_date_ratio = _as_float(date_parse.get("best_ratio"))
    overall_success_ratio = _as_float(date_parse.get("success_ratio"))
    date_success_ratio_signal = _as_float(date_parse.get("date_success_ratio"))
    best_date_format = date_parse.get("best_format")
    parsed_date_min = date_parse.get("parsed_min")
    parsed_date_max = date_parse.get("parsed_max")
    date_valid_count_raw = date_parse.get("valid_count")
    try:
        date_valid_count = int(date_valid_count_raw) if date_valid_count_raw is not None else 0
    except Exception:
        try:
            date_valid_count = int(float(date_valid_count_raw)) if date_valid_count_raw is not None else 0
        except Exception:
            date_valid_count = 0
    pattern_values: List[float] = []
    pattern_ratio_lookup: Dict[str, Optional[float]] = {}
    if isinstance(date_patterns, dict):
        for key, value in date_patterns.items():
            normalized_key = str(key or "").lower()
            val_float = _as_float(value)
            if val_float is not None:
                pattern_values.append(val_float)
            pattern_ratio_lookup[normalized_key] = val_float
            if normalized_key.endswith("_ratio") and normalized_key[:-6] not in pattern_ratio_lookup:
                pattern_ratio_lookup[normalized_key[:-6]] = val_float
    pattern_values.extend(val for val in format_ratios.values() if val is not None)
    yyyymmdd_pattern_ratio = pattern_ratio_lookup.get("yyyymmdd")
    ddmmyyyy_pattern_ratio = pattern_ratio_lookup.get("ddmmyyyy")
    iso_pattern_ratio = pattern_ratio_lookup.get("iso") or pattern_ratio_lookup.get("iso_ymd")
    pattern_ratio_fallback = max(pattern_values) if pattern_values else 0.0
    column_name_lower = (column_entry.get("column_name") or "").lower()

    distinct_pct_raw = column_entry.get("distinct_pct")
    distinct_pct = float(distinct_pct_raw) if distinct_pct_raw is not None else None
    null_pct = float(column_entry.get("null_pct") or 0.0)
    data_type = column_entry.get("data_type") or ""
    top_values = column_entry.get("top_values") or []
    distincts = column_entry.get("distincts")
    non_nulls = int(column_entry.get("non_nulls") or 0)
    rows_profiled = int(column_entry.get("rows_profiled") or 0)
    profile_min: Optional[Any] = column_entry.get("min_val")
    profile_max: Optional[Any] = column_entry.get("max_val")

    def _ratio(mapping: Dict[str, Any], key: str) -> float:
        value = mapping.get(key)
        try:
            return float(value)
        except Exception:
            return 0.0

    def _boost(target: str, amount: float, reason: Optional[str] = None) -> None:
        if amount <= 0:
            return
        scores[target] = scores.get(target, 0.0) + amount
        if reason:
            rationales.setdefault(target, []).append(reason)

    scores: Dict[str, float] = {candidate: 0.0 for candidate in SEMANTIC_TYPE_CANDIDATES}
    rationales: Dict[str, List[str]] = {candidate: [] for candidate in SEMANTIC_TYPE_CANDIDATES}
    forced_type: Optional[str] = None
    forced_confidence: Optional[float] = None
    forced_rationale_parts: List[str] = []

    if distinct_ratio_signal is not None:
        resolved_distinct_ratio = float(distinct_ratio_signal)
    else:
        distinct_count = column_entry.get("distincts")
        non_null_count = column_entry.get("non_nulls")
        try:
            resolved_distinct_ratio = (
                float(distinct_count) / float(non_null_count)
                if (distinct_count is not None and non_null_count)
                else 0.0
            )
        except Exception:
            resolved_distinct_ratio = 0.0

    email_ratio = _ratio(regex, "email")
    if email_ratio > 0:
        _boost("EMAIL", 85.0 * min(email_ratio, 1.0), f"{email_ratio:.0%} values match email format")
    if hints.get("email"):
        _boost("EMAIL", 20.0, "column name references email")

    iban_ratio = _ratio(regex, "iban")
    if iban_ratio > 0:
        _boost("IBAN", 90.0 * min(iban_ratio, 1.0), f"{iban_ratio:.0%} values look like IBANs")
    if hints.get("iban"):
        _boost("IBAN", 20.0, "column name references IBAN")
    if length_min is not None and length_max is not None and 15 <= length_min <= 34 and length_max <= 34:
        _boost("IBAN", 10.0, "length range matches IBAN expectation")

    isin_ratio = _ratio(regex, "isin")
    if isin_ratio > 0:
        _boost("ISIN", 88.0 * min(isin_ratio, 1.0), f"{isin_ratio:.0%} values match ISIN structure")
    if hints.get("isin"):
        _boost("ISIN", 18.0, "column name references ISIN")
    if length_min is not None and length_max is not None and abs(length_min - 12.0) <= 1 and abs(length_max - 12.0) <= 1:
        _boost("ISIN", 8.0, "length aligns with ISIN standard")

    bic_ratio = _ratio(regex, "bic")
    if bic_ratio > 0:
        _boost("BIC", 80.0 * min(bic_ratio, 1.0), f"{bic_ratio:.0%} values match BIC/SWIFT format")
    if hints.get("bic"):
        _boost("BIC", 18.0, "column name references BIC/SWIFT")
    if length_min is not None and length_max is not None and 8 <= length_min <= length_max <= 11:
        _boost("BIC", 6.0, "length aligns with BIC expectation")

    uuid_ratio = _ratio(regex, "uuid")
    if uuid_ratio > 0:
        _boost("UUID", 85.0 * min(uuid_ratio, 1.0), f"{uuid_ratio:.0%} values match UUID format")

    url_ratio = _ratio(regex, "url")
    if url_ratio > 0:
        _boost("URL", 75.0 * min(url_ratio, 1.0), f"{url_ratio:.0%} values look like URLs")
    if hints.get("url"):
        _boost("URL", 15.0, "column name references URL/URI")

    phone_ratio = _ratio(regex, "phone_e164")
    if phone_ratio > 0:
        _boost("PHONE", 70.0 * min(phone_ratio, 1.0), f"{phone_ratio:.0%} values match phone pattern")
    if hints.get("phone"):
        _boost("PHONE", 12.0, "column name references phone")

    date_success_ratio = (
        date_success_ratio_signal
        if date_success_ratio_signal is not None
        else (
            best_date_ratio
            if best_date_ratio is not None
            else overall_success_ratio
        )
    )
    targeted_patterns = {
        "yyyymmdd",
        "ddmmyyyy",
        "iso",
        "iso_slash",
        "iso_dot",
        "dd_mm_yyyy",
        "dd_slash_mm",
    }
    pattern_max_ratio = 0.0
    for key in targeted_patterns:
        ratio_val = pattern_ratio_lookup.get(key)
        if ratio_val is not None and ratio_val > pattern_max_ratio:
            pattern_max_ratio = ratio_val
    if (
        date_success_ratio is not None
        and date_success_ratio >= 0.6
        and date_valid_count > 0
    ):
        format_key = str(best_date_format or "").lower()
        label = DATE_PARSE_LABELS.get(
            format_key,
            str(best_date_format or "text date").upper(),
        )
        _boost(
            "DATE_IN_TEXT",
            85.0 * min(date_success_ratio, 1.0),
            f"{date_success_ratio:.0%} of values parse as {label}",
        )
        if parsed_date_min or parsed_date_max:
            _boost(
                "DATE_IN_TEXT",
                8.0,
                "parsed range {start} → {end}".format(
                    start=parsed_date_min or "?",
                    end=parsed_date_max or "?",
                ),
            )
    else:
        pattern_ratio = max(
            iso_pattern_ratio or 0.0,
            yyyymmdd_pattern_ratio or 0.0,
            ddmmyyyy_pattern_ratio or 0.0,
            pattern_ratio_fallback,
        )
        if pattern_ratio >= 0.7:
            _boost(
                "DATE_IN_TEXT",
                40.0 * pattern_ratio,
                f"{pattern_ratio:.0%} of values resemble date patterns",
            )
    if "date" in column_name_lower and not _is_temporal(data_type):
        _boost("DATE_IN_TEXT", 10.0, "column name references date but stored as string")

    def _parse_date_value(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except Exception:
            try:
                return datetime.strptime(text[:10], "%Y-%m-%d")
            except Exception:
                return None

    parsed_min_dt = _parse_date_value(parsed_date_min)
    parsed_max_dt = _parse_date_value(parsed_date_max)
    date_span_years: Optional[float] = None
    if parsed_min_dt and parsed_max_dt:
        try:
            span_days = abs((parsed_max_dt - parsed_min_dt).days)
            date_span_years = span_days / 365.25
        except Exception:
            date_span_years = None

    sentinel_zero_count = _as_int(column_entry.get("date_sentinel_count")) or 0
    best_success_ratio = 0.0
    for candidate in (
        date_success_ratio,
        best_date_ratio,
        overall_success_ratio,
    ):
        if candidate is not None and candidate > best_success_ratio:
            best_success_ratio = float(candidate)
    best_success_ratio = max(0.0, min(best_success_ratio, 1.0))

    if (
        not forced_type
        and best_success_ratio >= 0.8
        and pattern_max_ratio >= 0.8
        and date_valid_count > 0
    ):
        format_key = str(best_date_format or "").lower()
        label = DATE_PARSE_LABELS.get(
            format_key,
            str(best_date_format or "text date").upper(),
        )
        high_confidence = (
            best_success_ratio >= 0.95
            or (
                best_success_ratio >= 0.9
                and (date_span_years or 0.0) >= 5.0
            )
        )
        if high_confidence:
            forced_confidence = round(min(1.0, max(best_success_ratio, 0.9)), 3)
        else:
            forced_confidence = round(min(0.89, max(0.75, best_success_ratio)), 3)
        pct_value = best_success_ratio * 100.0
        if pct_value < 10.0:
            pct_text = f"{pct_value:.1f}%".rstrip("0").rstrip(".")
        else:
            pct_text = f"{pct_value:.0f}%"
        note_parts: List[str] = [f"Parsed as {label} ({pct_text})"]
        if sentinel_zero_count > 0:
            note_parts.append("ignored sentinel zeros")
        if date_span_years is not None and date_span_years >= 1.0 and high_confidence:
            note_parts.append(f"span {date_span_years:.1f}y")
        note_text = "; ".join(part for part in note_parts if part)
        forced_type = "DATE_IN_TEXT"
        forced_rationale_parts = [truncate_note(note_text)]

    if (
        not forced_type
        and _is_numeric(data_type)
        and float(num_date.get("yyyymmdd_ratio") or 0.0) >= 0.6
    ):
        forced_type = "DATE_IN_TEXT"
        forced_confidence = clamp_confidence(0.8, 0.75, 0.95)
        forced_rationale_parts.append(
            "numeric column parses as YYYYMMDD ≥ 60%"
        )
        payload_min = num_date.get("parsed_min")
        payload_max = num_date.get("parsed_max")
        if payload_min or payload_max:
            profile_min = payload_min or profile_min
            profile_max = payload_max or profile_max

    if not forced_type:
        row_cnt_value = _as_int(column_entry.get("row_cnt"))
        null_cnt_value = _as_int(column_entry.get("null_cnt")) or 0
        if row_cnt_value is None:
            row_cnt_value = _as_int(column_entry.get("rows_profiled"))
        non_null_count = non_nulls or 0
        if row_cnt_value is not None:
            non_null_count = max(row_cnt_value - null_cnt_value, non_null_count)
        distinct_count_value = _as_int(column_entry.get("distincts"))
        top3_ratio_value = top3_ratio_signal
        if top3_ratio_value is None and non_null_count:
            try:
                top3_total = sum(
                    int(top_values[idx].get("count") or 0)
                    for idx in range(min(3, len(top_values)))
                )
                top3_ratio_value = (
                    float(top3_total) / float(non_null_count)
                ) if non_null_count else None
            except Exception:
                top3_ratio_value = None
        if distinct_ratio_signal is not None:
            distinct_ratio_value = float(distinct_ratio_signal)
        elif distinct_count_value is not None and non_null_count:
            distinct_ratio_value = float(distinct_count_value) / float(non_null_count)
        else:
            distinct_ratio_value = None

        column_name_upper = column_name_lower.upper()
        ref_hint_tokens = (
            "_CODE",
            "_TYPE",
            "_STATUS",
            "LAND",
            "COUNTRY",
            "XREF",
            "CLASS",
            "CAT",
            "SEGMENT",
        )
        ref_name_hint = any(token in column_name_upper for token in ref_hint_tokens)

        distinct_pct_display = None
        if distinct_ratio_value is not None:
            distinct_pct_display = f"{distinct_ratio_value:.0%} distinct"
        top3_display = None
        if top3_ratio_value is not None:
            top3_display = f"top3 coverage {top3_ratio_value:.0%}"

        len_spread_value = length_spread
        if len_spread_value is None and length_min is not None and length_max is not None:
            len_spread_value = float(length_max) - float(length_min)
        spread_ratio = None
        if len_spread_value is not None and length_avg not in (None, 0.0):
            spread_ratio = len_spread_value / max(length_avg or 1.0, 1e-6)

        near_unique = False
        if distinct_ratio_value is not None and distinct_ratio_value >= 0.6:
            near_unique = True
        elif (
            distinct_count_value is not None
            and row_cnt_value is not None
            and row_cnt_value > 0
        ):
            threshold = min(int(row_cnt_value * 0.6), 10000)
            if distinct_count_value >= threshold:
                near_unique = True

        top3_ok_for_account = top3_ratio_value is None or top3_ratio_value <= 0.30
        entropy_ok = False
        if length_avg is not None and length_avg >= 8.0:
            entropy_ok = True
        elif length_stddev is not None and length_stddev >= 2.5:
            entropy_ok = True
        account_condition = near_unique and top3_ok_for_account and entropy_ok

        tight_length_spread = False
        if len_spread_value is not None:
            if len_spread_value <= 2.0:
                tight_length_spread = True
            elif spread_ratio is not None and spread_ratio <= 0.25:
                tight_length_spread = True

        ref_cardinality = False
        if (
            distinct_ratio_value is not None
            and top3_ratio_value is not None
            and distinct_ratio_value <= 0.30
            and top3_ratio_value >= 0.80
        ):
            ref_cardinality = True
        elif (
            distinct_count_value is not None
            and row_cnt_value is not None
            and row_cnt_value >= 5000
            and distinct_count_value <= 200
        ):
            ref_cardinality = True

        ref_condition = ref_cardinality and tight_length_spread

        preliminary_best_type: Optional[str] = None
        if scores:
            try:
                preliminary_best_type = max(scores, key=scores.get)
            except ValueError:
                preliminary_best_type = None

        name_hint_bonus = 0.05 if ref_name_hint else 0.0
        if hints.get("account") or hints.get("id"):
            name_hint_bonus = max(name_hint_bonus, 0.05)

        free_text_penalty = 0.0
        if (
            spread_ratio is not None
            and spread_ratio >= 1.0
            and (distinct_ratio_value or 0.0) >= 0.30
        ):
            free_text_penalty = 0.1

        if account_condition and (not ref_condition or ref_cardinality is False):
            account_confidence = 0.82
            if distinct_ratio_value is not None:
                if distinct_ratio_value >= 0.8:
                    account_confidence += 0.08
                elif distinct_ratio_value >= 0.7:
                    account_confidence += 0.05
            if top3_ratio_value is not None:
                if top3_ratio_value <= 0.15:
                    account_confidence += 0.05
                elif top3_ratio_value <= 0.25:
                    account_confidence += 0.03
            if length_avg is not None and length_avg >= 12.0:
                account_confidence += 0.04
            if hints.get("account") or hints.get("id"):
                account_confidence += 0.05
            account_confidence = max(0.0, account_confidence - free_text_penalty)
            forced_type = "ACCOUNT_ID"
            account_confidence = clamp_confidence(account_confidence, 0.75, 0.99) or 0.0
            forced_confidence = round(account_confidence, 3)
            rationale_parts: List[str] = []
            if distinct_pct_display:
                rationale_parts.append(distinct_pct_display)
            if top3_display:
                rationale_parts.append(top3_display)
            if length_avg is not None:
                rationale_parts.append(f"avg len {length_avg:.1f}")
            if distinct_count_value is not None and row_cnt_value:
                rationale_parts.append(
                    f"{distinct_count_value} distinct / {row_cnt_value} rows"
                )
            if free_text_penalty > 0:
                rationale_parts.append("wide length spread")
            if not rationale_parts:
                rationale_parts.append("near-unique identifier pattern")
            forced_rationale_parts = [truncate_note("; ".join(rationale_parts))]
        elif ref_condition:
            ref_confidence = 0.8
            if distinct_ratio_value is not None and distinct_ratio_value <= 0.2:
                ref_confidence += 0.05
            elif distinct_ratio_value is not None and distinct_ratio_value <= 0.3:
                ref_confidence += 0.03
            if top3_ratio_value is not None and top3_ratio_value >= 0.9:
                ref_confidence += 0.05
            elif top3_ratio_value is not None and top3_ratio_value >= 0.8:
                ref_confidence += 0.03
            if distinct_count_value is not None and distinct_count_value <= 50:
                ref_confidence += 0.05
            elif distinct_count_value is not None and distinct_count_value <= 200:
                ref_confidence += 0.03
            ref_confidence += name_hint_bonus
            ref_confidence = max(0.0, ref_confidence - free_text_penalty)
            forced_type = "REF_CODE"
            ref_confidence = clamp_confidence(ref_confidence, 0.7, 0.95) or 0.0
            forced_confidence = round(ref_confidence, 3)
            note_parts = []
            if top3_display:
                note_parts.append(top3_display)
            if len_spread_value is not None:
                if len_spread_value.is_integer():
                    spread_text = f"len spread {int(len_spread_value)}"
                else:
                    spread_text = f"len spread {len_spread_value:.1f}".rstrip("0").rstrip(".")
                note_parts.append(spread_text)
            if tight_length_spread:
                note_parts.append("tight length spread")
            if distinct_count_value is not None and row_cnt_value:
                note_parts.append(
                    f"{distinct_count_value} distinct / {row_cnt_value} rows"
                )
            if free_text_penalty > 0:
                note_parts.append("wide length spread")
            if not note_parts:
                note_parts.append("reference code distribution")
            forced_rationale_parts = [truncate_note("; ".join(note_parts))]
        elif (
            preliminary_best_type in {"ACCOUNT_ID", "REF_CODE"}
            and distinct_count_value is not None
            and distinct_count_value <= 200
            and (top3_ratio_value or 0.0) >= 0.6
        ):
            forced_type = "ENUM/STATUS"
            forced_confidence = 0.6
            note_parts = []
            if top3_display:
                note_parts.append(top3_display)
            note_parts.append(f"{distinct_count_value} frequent values")
            forced_rationale_parts = [truncate_note("; ".join(note_parts))]

    currency_ref = _ratio(references, "reference_currency_code")
    if currency_ref > 0:
        _boost("CURRENCY_CODE", 90.0 * min(currency_ref, 1.0), f"{currency_ref:.0%} values match known currency codes")
    if hints.get("currency"):
        _boost("CURRENCY_CODE", 20.0, "column name references currency")
    if length_min is not None and length_max is not None and 2 <= length_min <= 3 <= length_max <= 4:
        _boost("CURRENCY_CODE", 6.0, "length compatible with currency codes")

    country_code_ref = _ratio(references, "reference_country_code")
    if country_code_ref > 0:
        _boost("COUNTRY_CODE/NAME", 80.0 * min(country_code_ref, 1.0), f"{country_code_ref:.0%} values match ISO country codes")
    country_name_ref = _ratio(references, "reference_country_name")
    if country_name_ref > 0:
        _boost("COUNTRY_CODE/NAME", 70.0 * min(country_name_ref, 1.0), f"{country_name_ref:.0%} values match known country names")
    if hints.get("country"):
        _boost("COUNTRY_CODE/NAME", 15.0, "column name references country")

    exchange_ref = _ratio(references, "reference_exchange_code")
    if exchange_ref > 0:
        _boost("TICKER/SYMBOL", 40.0 * min(exchange_ref, 1.0), "values overlap with known exchange codes")

    char_alpha = _ratio(char_classes, "alpha")
    char_digit = _ratio(char_classes, "digit")
    char_alnum = _ratio(char_classes, "alnum")

    uppercase_matches = 0
    total_matches = 0
    boolean_candidates: Set[str] = set()
    for entry in top_values:
        value = entry.get("value")
        if value is None:
            continue
        text = str(value).strip()
        count = entry.get("count") or 0
        try:
            count_int = int(count)
        except Exception:
            count_int = 0
        if count_int <= 0:
            count_int = 1
        if text:
            total_matches += count_int
            if text.upper() == text and any(c.isalpha() for c in text):
                uppercase_matches += count_int
            boolean_candidates.add(text.upper())

    uppercase_ratio = (float(uppercase_matches) / float(total_matches)) if total_matches else 0.0

    if hints.get("ticker"):
        _boost("TICKER/SYMBOL", 30.0, "column name references ticker/symbol")
    if uppercase_ratio >= 0.6 and (length_max is None or length_max <= 6):
        _boost("TICKER/SYMBOL", 45.0 * uppercase_ratio, "top values predominantly uppercase and short")
    if char_alpha >= 0.6 and (length_avg is None or length_avg <= 6.5):
        _boost("TICKER/SYMBOL", 10.0, "values mainly alphabetic with short length")
    if distinct_pct is not None and distinct_pct >= 0.60:
        _boost("TICKER/SYMBOL", 6.0, "high uniqueness typical for tickers")

    if _is_numeric(data_type):
        _boost("PRICE/AMOUNT/QUANTITY", 45.0, "numeric data type")
    if hints.get("amount") or hints.get("price") or hints.get("quantity"):
        _boost("PRICE/AMOUNT/QUANTITY", 35.0, "column name references financial amounts")
    if char_digit >= 0.8 and length_avg is not None and length_avg >= 3.0:
        _boost("PRICE/AMOUNT/QUANTITY", 8.0, "values primarily numeric")

    if hints.get("status") or hints.get("enum"):
        _boost("ENUM/STATUS", 30.0, "column name references status/type")
    if distincts is not None and distincts <= 20 and non_nulls:
        coverage = float(sum(int((entry.get("count") or 0)) for entry in top_values)) / float(non_nulls) if non_nulls else 0.0
        if distinct_pct is not None and distinct_pct <= 0.40:
            _boost("ENUM/STATUS", 25.0, "low cardinality suggests enum")
        if coverage >= 0.8:
            _boost("ENUM/STATUS", 10.0, "few values cover majority of rows")

    if _is_boolean_type(data_type):
        _boost("BOOLEAN", 85.0, "boolean data type")
    if boolean_candidates and boolean_candidates <= (BOOLEAN_TRUE_VALUES | BOOLEAN_FALSE_VALUES):
        _boost("BOOLEAN", 50.0, "values align with boolean vocabulary")
    if hints.get("boolean"):
        _boost("BOOLEAN", 12.0, "column name suggests boolean flag")

    if _is_temporal(data_type):
        _boost("TIMESTAMP/DATE", 90.0, "temporal data type")
    if hints.get("timestamp"):
        _boost("TIMESTAMP/DATE", 15.0, "column name references time/date")

    if hints.get("order"):
        _boost("ORDER_ID", 35.0, "column name references order")
    if hints.get("trade"):
        _boost("TRADE_ID", 35.0, "column name references trade")
    if hints.get("account"):
        _boost("ACCOUNT_ID", 35.0, "column name references account")
    if hints.get("id"):
        _boost("ACCOUNT_ID", 8.0, "generic identifier naming")
        _boost("ORDER_ID", 8.0)
        _boost("TRADE_ID", 8.0)

    consistent_length = False
    if length_min is not None and length_max is not None:
        consistent_length = math.isclose(length_min, length_max, rel_tol=0.0, abs_tol=1.0)

    if distinct_ratio_signal is not None and distinct_ratio_signal >= 0.25:
        dominance_ok = (top1_ratio_signal is None or top1_ratio_signal <= 0.25) and (
            top3_ratio_signal is None or top3_ratio_signal <= 0.5
        )
        if dominance_ok:
            reason = (
                f"{distinct_ratio_signal:.0%} of values unique with low mode dominance"
            )
            _boost("REF_CODE", 35.0 + min(distinct_ratio_signal, 1.0) * 40.0, reason)
            if hints.get("code") or "code" in column_name_lower or hints.get("reference"):
                _boost("REF_CODE", 12.0, "column name implies reference code")
            if consistent_length and length_min is not None:
                _boost("REF_CODE", 8.0, f"values share consistent length ≈ {length_min:.0f}")
            if 0.05 <= (numeric_like_ratio_signal or 0.0) <= 0.95:
                _boost("REF_CODE", 6.0, "mix of digits suggests coded identifiers")
            if char_alnum >= 0.6:
                _boost("REF_CODE", 4.0, "alphanumeric composition typical of codes")
            if uppercase_ratio >= 0.5 and consistent_length:
                _boost("REF_CODE", 5.0, "values mostly uppercase with tight length range")
    if (
        distinct_ratio_signal is not None
        and distinct_ratio_signal <= 0.2
        and (top3_ratio_signal or 0.0) >= 0.6
        and (numeric_like_ratio_signal or 0.0) >= 0.2
    ):
        _boost(
            "REF_CODE",
            18.0,
            "low diversity but multiple repeated codes",
        )
        if consistent_length and length_min is not None:
            _boost("REF_CODE", 6.0, f"values align around length ≈ {length_min:.0f}")
        if uppercase_ratio >= 0.5:
            _boost("REF_CODE", 4.0, "mostly uppercase values")

    distinct_ratio_value_raw = float(resolved_distinct_ratio)
    distinct_ratio_value = max(0.0, min(1.0, distinct_ratio_value_raw))
    top3_ratio_value = max(0.0, min(1.0, float(top3_ratio_signal or 0.0)))
    row_count_value = int(column_entry.get("non_nulls") or 0)
    distinct_count_value = column_entry.get("distincts")
    cond_low_cardinality = 0.01 <= distinct_ratio_value <= 0.30 and top3_ratio_value >= 0.80
    cond_limited_unique = (
        isinstance(distinct_count_value, (int, float))
        and float(distinct_count_value) <= 200
        and row_count_value >= 10000
    )
    if forced_type is None and (cond_low_cardinality or cond_limited_unique):
        forced_type = "REF_CODE"
        uniqueness_component = max(0.0, min(1.0, 1.0 - distinct_ratio_value))
        coverage_component = max(0.0, min(1.0, top3_ratio_value))
        name_hint = 0.0
        if hints.get("code") or "code" in column_name_lower or "xref" in column_name_lower or "land" in column_name_lower:
            name_hint = 1.0
            forced_rationale_parts.append("column name suggests reference code")
        confidence_calc = 0.5 * uniqueness_component + 0.35 * coverage_component + 0.15 * name_hint
        forced_confidence = round(min(1.0, confidence_calc), 3)
        if cond_low_cardinality:
            forced_rationale_parts.append(
                "low cardinality ({distinct:.0%} distinct) with top 3 covering {top3:.0%}".format(
                    distinct=distinct_ratio_value,
                    top3=top3_ratio_value,
                )
            )
        if cond_limited_unique:
            forced_rationale_parts.append(
                f"{distinct_count_value} distinct values across {row_count_value} rows"
            )
        if consistent_length and length_min is not None:
            forced_rationale_parts.append(
                f"values cluster around length {length_min:.0f}"
            )

    if distinct_pct is not None and distinct_pct >= 0.70:
        _boost("ACCOUNT_ID", 12.0, "high uniqueness typical for identifiers")
        _boost("ORDER_ID", 12.0)
        _boost("TRADE_ID", 12.0)

    if char_alnum >= 0.5 and length_avg is not None and length_avg >= 6.0:
        _boost("ACCOUNT_ID", 8.0, "alphanumeric mix resembles identifiers")
        if hints.get("order"):
            _boost("ORDER_ID", 5.0)
        if hints.get("trade"):
            _boost("TRADE_ID", 5.0)

    if non_nulls and rows_profiled and (non_nulls / rows_profiled) < 0.5:
        reduction = 1.0 - ((non_nulls / rows_profiled) * 0.5)
        for key in scores:
            scores[key] *= max(0.0, 1.0 - reduction)
            if reduction > 0.0:
                rationales.setdefault(key, [])

    best_type = max(scores, key=scores.get)
    best_score = scores.get(best_type, 0.0)

    if best_score <= 0.0:
        if _is_temporal(data_type):
            best_type = "TIMESTAMP/DATE"
            best_score = 20.0
            rationales.setdefault(best_type, []).append("temporal data type without stronger signal")
        elif _is_numeric(data_type):
            best_type = "PRICE/AMOUNT/QUANTITY"
            best_score = 15.0
            rationales.setdefault(best_type, []).append("numeric column with no specific pattern match")
        elif _is_boolean_type(data_type):
            best_type = "BOOLEAN"
            best_score = 15.0
            rationales.setdefault(best_type, []).append("boolean-like column by data type")
        else:
            best_type = "ACCOUNT_ID"
            best_score = 10.0
            rationales.setdefault(best_type, []).append("defaulting to generic identifier due to lack of stronger signals")

    forced_rationale = "; ".join(part for part in forced_rationale_parts if part)
    if best_type == "DATE_IN_TEXT":
        profile_min = parsed_date_min or profile_min
        profile_max = parsed_date_max or profile_max
    elif _is_temporal(data_type):
        profile_min = column_entry.get("min_val") or profile_min
        profile_max = column_entry.get("max_val") or profile_max
    elif best_type == "REF_CODE":
        profile_min = column_entry.get("numeric_min") or profile_min
        profile_max = column_entry.get("numeric_max") or profile_max
    elif _is_numeric(data_type):
        profile_min = column_entry.get("min_val")
        profile_max = column_entry.get("max_val")

    if forced_type:
        best_type = forced_type
        if forced_confidence is not None:
            confidence = forced_confidence
        else:
            confidence = round(min(1.0, max(best_score, 0.0) / 100.0), 3)
        if not forced_rationale:
            if forced_type == "DATE_IN_TEXT" and date_success_ratio >= 0.9:
                forced_rationale = f"{date_success_ratio:.0%} of values parse successfully"
            else:
                forced_rationale = "rule-based semantic classification"
        return best_type, confidence, forced_rationale, profile_min, profile_max

    confidence = min(1.0, max(best_score, 0.0) / 100.0)
    confidence = round(confidence, 3)

    explanations = rationales.get(best_type, [])
    if not explanations:
        if null_pct >= 0.5:
            explanations = ["limited matches because column is mostly null"]
        else:
            explanations = ["limited heuristic support but selected best available type"]

    rationale = "; ".join(explanations[:3])

    return best_type, confidence, rationale, profile_min, profile_max


def normalize_profile_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Return a normalized copy of a per-column profile row."""

    payload = dict(row or {})
    name_value = payload.get("column_name") or payload.get("name") or ""
    name_text = str(name_value)
    payload["column_name"] = name_text
    payload["name"] = name_text
    payload["data_type"] = str(payload.get("data_type") or "")

    top_values_raw = payload.get("top_values") or []
    if isinstance(top_values_raw, list):
        normalized_top_values: List[Dict[str, Any]] = []
        for entry in top_values_raw:
            if isinstance(entry, dict):
                normalized_top_values.append(dict(entry))
            else:
                normalized_top_values.append({"value": entry})
        payload["top_values"] = normalized_top_values
    else:
        payload["top_values"] = []

    return payload


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

    sample_seed: Optional[int] = None
    if pct is not None:
        # Use a deterministic seed across all queries in this profiling run so that
        # row counts and per-column metrics reference the same sampled subset.
        sample_seed = random.randrange(0, 2**31)

    sample_clause = ""
    if pct is not None:
        seed_clause = f" SEED ({sample_seed})" if sample_seed is not None else ""
        sample_clause = f" SAMPLE BERNOULLI({pct}){seed_clause}"

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

    reference_sets = _load_reference_sets(session, db, schema)

    for meta in columns:
        name = meta.get("column_name")
        if not name:
            continue
        dtype = meta.get("data_type") or ""
        qcol = _quote_identifier(name)
        distinct_expr = "APPROX_COUNT_DISTINCT({col})" if rows_profiled > approx_threshold else "COUNT(DISTINCT {col})"
        metrics_sql = [
            "COUNT(*) AS ROW_CNT",
            f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS NULLS",
            f"{distinct_expr.format(col=qcol)} AS DISTINCTS",
            f"SUM(CASE WHEN {qcol} IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULLS_COUNT",
        ]
        num_date_matches_alias: Optional[str] = None
        num_date_min_alias: Optional[str] = None
        num_date_max_alias: Optional[str] = None
        is_numeric = _is_numeric(dtype)
        if is_numeric:
            metrics_sql.extend(
                [
                    f"MIN({qcol}) AS MIN_VAL",
                    f"MAX({qcol}) AS MAX_VAL",
                ]
            )
            num_date_matches_alias = "NUM_YYYYMMDD_MATCHES"
            num_date_min_alias = "NUM_YYYYMMDD_MIN"
            num_date_max_alias = "NUM_YYYYMMDD_MAX"
            metrics_sql.extend(
                [
                    (
                        "SUM(CASE WHEN TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD') IS NOT NULL "
                        "THEN 1 ELSE 0 END) AS {alias}"
                    ).format(col=qcol, alias=num_date_matches_alias),
                    (
                        "MIN(TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD')) AS {alias}"
                    ).format(col=qcol, alias=num_date_min_alias),
                    (
                        "MAX(TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD')) AS {alias}"
                    ).format(col=qcol, alias=num_date_max_alias),
                ]
            )
        elif _is_temporal(dtype):
            metrics_sql.extend(
                [
                    f"MIN({qcol}) AS MIN_VAL",
                    f"MAX({qcol}) AS MAX_VAL",
                ]
            )
        else:
            metrics_sql.extend(
                [
                    f"MIN(TO_VARCHAR({qcol})) AS MIN_VAL",
                    f"MAX(TO_VARCHAR({qcol})) AS MAX_VAL",
                ]
            )
        is_string = _is_string_type(dtype)
        if is_string:
            trimmed_expr = f"TRIM({qcol}::STRING)"
            sentinel_values = ("'0'", "'00000000'", "'0000-00-00'", "'0000/00/00'")
            guarded_numeric_expr = (
                "CASE WHEN {trim} IN ({sentinels}) THEN NULL ELSE TO_VARCHAR({col}) END"
            ).format(trim=trimmed_expr, sentinels=", ".join(sentinel_values), col=qcol)
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol}::STRING = '' THEN 1 ELSE 0 END) AS EMPTY_STR_ROWS"
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s+$') THEN 1 ELSE 0 END) AS WS_ONLY_ROWS"
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND {qcol}::STRING != TRIM({qcol}::STRING) THEN 1 ELSE 0 END) AS LEAD_TRAIL_WS_ROWS"
            )
            num_date_matches_alias = "NUM_YYYYMMDD_MATCHES"
            num_date_min_alias = "NUM_YYYYMMDD_MIN"
            num_date_max_alias = "NUM_YYYYMMDD_MAX"
            metrics_sql.extend(
                [
                    (
                        "SUM(CASE WHEN TRY_TO_DATE({expr}, 'YYYYMMDD') IS NOT NULL THEN 1 ELSE 0 END) AS {alias}"
                    ).format(expr=guarded_numeric_expr, alias=num_date_matches_alias),
                    (
                        "MIN(TRY_TO_DATE({expr}, 'YYYYMMDD')) AS {alias}"
                    ).format(expr=guarded_numeric_expr, alias=num_date_min_alias),
                    (
                        "MAX(TRY_TO_DATE({expr}, 'YYYYMMDD')) AS {alias}"
                    ).format(expr=guarded_numeric_expr, alias=num_date_max_alias),
                ]
            )
        else:
            metrics_sql.append("0 AS EMPTY_STR_ROWS")
            metrics_sql.append("0 AS WS_ONLY_ROWS")
            metrics_sql.append("0 AS WHITESPACE_ROWS")
            metrics_sql.append("0 AS LEAD_TRAIL_WS_ROWS")
            num_date_matches_alias = None
            num_date_min_alias = None
            num_date_max_alias = None
        length_expr: Optional[str]
        if is_string:
            length_expr = f"LENGTH({qcol}::STRING)"
        elif is_numeric:
            length_expr = f"LENGTH(TO_VARCHAR({qcol}))"
        else:
            length_expr = None
        if length_expr:
            metrics_sql.append(
                f"AVG(CASE WHEN {qcol} IS NOT NULL THEN {length_expr} END) AS AVG_LEN"
            )
        else:
            metrics_sql.append("NULL AS AVG_LEN")

        char_pattern_aliases: Dict[str, str] = {}
        regex_aliases: Dict[str, str] = {}
        ref_match_aliases: Dict[str, str] = {}
        date_pattern_aliases: Dict[str, str] = {}
        date_parse_aliases: Dict[str, str] = {}
        date_parse_min_aliases: Dict[str, str] = {}
        date_parse_max_aliases: Dict[str, str] = {}
        date_pattern_seen: Dict[str, str] = {}
        any_parse_alias: Optional[str] = None
        any_parse_min_alias: Optional[str] = None
        any_parse_max_alias: Optional[str] = None
        numeric_like_alias: Optional[str] = None
        sentinel_alias: Optional[str] = None
        len_stddev_alias: Optional[str] = None
        if length_expr:
            metrics_sql.extend(
                [
                    f"MIN({length_expr}) AS LEN_MIN",
                    f"MAX({length_expr}) AS LEN_MAX",
                ]
            )
            len_stddev_alias = "LEN_STDDEV"
            metrics_sql.append(
                f"STDDEV_SAMP({length_expr}) AS {len_stddev_alias}"
            )
        if is_string:
            for key, pattern in SEMANTIC_REGEX_PATTERNS.items():
                alias = f"REGEX_{key.upper()}_MATCHES"
                pattern_sql = pattern.replace("\\", "\\\\")
                metrics_sql.append(
                    f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
                )
                regex_aliases[key] = alias
            trimmed_expr = f"TRIM({qcol}::STRING)"
            sentinel_alias = "STRING_DATE_SENTINELS"
            sentinel_values = ("'0'", "'00000000'", "'0000-00-00'", "'0000/00/00'")
            metrics_sql.append(
                "SUM(CASE WHEN {expr} IN ({sentinels}) THEN 1 ELSE 0 END) AS {alias}".format(
                    expr=trimmed_expr,
                    sentinels=", ".join(sentinel_values),
                    alias=sentinel_alias,
                )
            )
            for key, pattern in CHAR_CLASS_PATTERNS.items():
                alias = f"CHAR_{key.upper()}_MATCHES"
                pattern_sql = pattern.replace("\\", "\\\\")
                metrics_sql.append(
                    f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
                )
                char_pattern_aliases[key] = alias
            for ref_key, values in (
                ("country_codes", reference_sets.get("country_codes", set())),
                ("country_names", reference_sets.get("country_names", set())),
                ("currency_codes", reference_sets.get("currency_codes", set())),
                ("exchange_codes", reference_sets.get("exchange_codes", set())),
            ):
                clause = _build_in_clause(values)
                if not clause:
                    continue
                alias = f"REF_{ref_key.upper()}_MATCHES"
                metrics_sql.append(
                    "SUM(CASE WHEN {col} IS NOT NULL AND UPPER({col}::STRING) IN ({clause}) "
                    "THEN 1 ELSE 0 END) AS {alias}".format(col=qcol, clause=clause, alias=alias)
                )
                signal_key = REFERENCE_SIGNAL_NAMES.get(ref_key, ref_key)
                ref_match_aliases[signal_key] = alias

            numeric_like_alias = "STRING_NUMERIC_LIKE_ROWS"
            numeric_like_pattern = r"^\d+(\.\d+)?$".replace("\\", "\\\\")
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{numeric_like_pattern}') THEN 1 ELSE 0 END) AS {numeric_like_alias}"
            )

            coalesce_terms: List[str] = []
            for config in DATE_PARSE_CONFIGS:
                key = config["key"]
                fmt = config["format"]
                alias_key = (
                    key.upper()
                    .replace("-", "_")
                    .replace("/", "_")
                    .replace(".", "_")
                )
                parse_alias = f"STRING_DATE_PARSE_{alias_key}"
                template = config.get("transform") or "{col}::STRING"
                input_expr = template.format(col=qcol)
                guarded_expr = (
                    "CASE WHEN {trim} IN ({sentinels}) THEN NULL ELSE {expr} END".format(
                        trim=trimmed_expr,
                        sentinels=", ".join(sentinel_values),
                        expr=input_expr,
                    )
                )
                metrics_sql.append(
                    f"SUM(CASE WHEN TRY_TO_DATE({guarded_expr}, '{fmt}') IS NOT NULL THEN 1 ELSE 0 END) AS {parse_alias}"
                )
                date_parse_aliases[key] = parse_alias
                min_alias = f"STRING_DATE_MIN_{alias_key}"
                max_alias = f"STRING_DATE_MAX_{alias_key}"
                date_parse_min_aliases[key] = min_alias
                date_parse_max_aliases[key] = max_alias
                metrics_sql.append(
                    f"MIN(TRY_TO_DATE({guarded_expr}, '{fmt}')) AS {min_alias}"
                )
                metrics_sql.append(
                    f"MAX(TRY_TO_DATE({guarded_expr}, '{fmt}')) AS {max_alias}"
                )
                coalesce_terms.append(f"TRY_TO_DATE({guarded_expr}, '{fmt}')")
                regex_pattern = config.get("regex")
                if regex_pattern:
                    alias = date_pattern_seen.get(regex_pattern)
                    if not alias:
                        alias = f"STRING_DATE_PATTERN_{alias_key}"
                        pattern_sql = regex_pattern.replace("\\", "\\\\")
                        metrics_sql.append(
                            f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
                        )
                        date_pattern_seen[regex_pattern] = alias
                    date_pattern_aliases[key] = alias
            if coalesce_terms:
                coalesce_expr = f"COALESCE({', '.join(coalesce_terms)})"
                any_parse_alias = "STRING_DATE_PARSE_ANY"
                any_parse_min_alias = "STRING_DATE_PARSE_ANY_MIN"
                any_parse_max_alias = "STRING_DATE_PARSE_ANY_MAX"
                metrics_sql.append(
                    f"SUM(CASE WHEN {coalesce_expr} IS NOT NULL THEN 1 ELSE 0 END) AS {any_parse_alias}"
                )
                metrics_sql.append(f"MIN({coalesce_expr}) AS {any_parse_min_alias}")
                metrics_sql.append(f"MAX({coalesce_expr}) AS {any_parse_max_alias}")
        else:
            char_pattern_aliases = {}
            regex_aliases = {}
            ref_match_aliases = {}
            date_pattern_aliases = {}
            date_parse_aliases = {}
            date_parse_min_aliases = {}
            date_parse_max_aliases = {}
            date_pattern_seen = {}
            any_parse_alias = None
            any_parse_min_alias = None
            any_parse_max_alias = None
            numeric_like_alias = None
            trimmed_expr = None
            sentinel_values = ()

        sql = "SELECT " + ", ".join(metrics_sql) + f" FROM {sampled_ref}"
        error_message: Optional[str] = None
        try:
            row = _collect_single_row(session, sql)
        except Exception as exc:
            error_message = f"{exc.__class__.__name__}: {exc}"
            logger.warning("Profile query failed for column %s: %s", name, error_message)
            minimal_metrics = [
                "COUNT(*) AS ROW_CNT",
                f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS NULLS",
                f"{distinct_expr.format(col=qcol)} AS DISTINCTS",
                f"SUM(CASE WHEN {qcol} IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULLS_COUNT",
            ]
            if is_numeric:
                minimal_metrics.extend(
                    [
                        f"MIN({qcol}) AS MIN_VAL",
                        f"MAX({qcol}) AS MAX_VAL",
                    ]
                )
            elif _is_temporal(dtype):
                minimal_metrics.extend(
                    [
                        f"MIN({qcol}) AS MIN_VAL",
                        f"MAX({qcol}) AS MAX_VAL",
                    ]
                )
            else:
                minimal_metrics.extend(
                    [
                        f"MIN(TO_VARCHAR({qcol})) AS MIN_VAL",
                        f"MAX(TO_VARCHAR({qcol})) AS MAX_VAL",
                    ]
                )
            if is_string:
                minimal_metrics.extend(
                    [
                        f"SUM(CASE WHEN {qcol}::STRING = '' THEN 1 ELSE 0 END) AS EMPTY_STR_ROWS",
                        f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s+$') THEN 1 ELSE 0 END) AS WS_ONLY_ROWS",
                        f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS",
                        f"SUM(CASE WHEN {qcol} IS NOT NULL AND {qcol}::STRING != TRIM({qcol}::STRING) THEN 1 ELSE 0 END) AS LEAD_TRAIL_WS_ROWS",
                    ]
                )
            else:
                minimal_metrics.extend(
                    [
                        "0 AS EMPTY_STR_ROWS",
                        "0 AS WS_ONLY_ROWS",
                        "0 AS WHITESPACE_ROWS",
                        "0 AS LEAD_TRAIL_WS_ROWS",
                    ]
                )
            if num_date_matches_alias:
                if is_string:
                    minimal_metrics.extend(
                        [
                            (
                                "SUM(CASE WHEN TRY_TO_DATE({expr}, 'YYYYMMDD') IS NOT NULL THEN 1 ELSE 0 END) AS {alias}"
                            ).format(expr=guarded_numeric_expr, alias=num_date_matches_alias),
                            (
                                "MIN(TRY_TO_DATE({expr}, 'YYYYMMDD')) AS {alias}"
                            ).format(expr=guarded_numeric_expr, alias=num_date_min_alias),
                            (
                                "MAX(TRY_TO_DATE({expr}, 'YYYYMMDD')) AS {alias}"
                            ).format(expr=guarded_numeric_expr, alias=num_date_max_alias),
                        ]
                    )
                else:
                    minimal_metrics.extend(
                        [
                            (
                                "SUM(CASE WHEN TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD') IS NOT NULL THEN 1 ELSE 0 END) AS {alias}"
                            ).format(col=qcol, alias=num_date_matches_alias),
                            (
                                "MIN(TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD')) AS {alias}"
                            ).format(col=qcol, alias=num_date_min_alias),
                            (
                                "MAX(TRY_TO_DATE(TO_VARCHAR({col}), 'YYYYMMDD')) AS {alias}"
                            ).format(col=qcol, alias=num_date_max_alias),
                        ]
                    )
            if length_expr:
                minimal_metrics.extend(
                    [
                        f"AVG(CASE WHEN {qcol} IS NOT NULL THEN {length_expr} END) AS AVG_LEN",
                        f"MIN({length_expr}) AS LEN_MIN",
                        f"MAX({length_expr}) AS LEN_MAX",
                        f"STDDEV_SAMP({length_expr}) AS LEN_STDDEV",
                    ]
                )
            else:
                minimal_metrics.extend(
                    [
                        "NULL AS AVG_LEN",
                        "NULL AS LEN_MIN",
                        "NULL AS LEN_MAX",
                        "NULL AS LEN_STDDEV",
                    ]
                )
            minimal_sql = "SELECT " + ", ".join(minimal_metrics) + f" FROM {sampled_ref}"
            try:
                row = _collect_single_row(session, minimal_sql)
            except Exception as fallback_exc:
                fallback_message = f"{fallback_exc.__class__.__name__}: {fallback_exc}"
                logger.warning(
                    "Minimal profile query failed for column %s: %s",
                    name,
                    fallback_message,
                )
                if error_message:
                    error_message = f"{error_message}; fallback failed: {fallback_message}"
                else:
                    error_message = f"fallback failed: {fallback_message}"
                row = None

        row_cnt_raw = _extract_row_value(row, "ROW_CNT", rows_profiled)
        try:
            row_cnt = int(row_cnt_raw)
        except Exception:
            try:
                row_cnt = int(float(row_cnt_raw))
            except Exception:
                row_cnt = int(rows_profiled)

        nulls_raw = _extract_row_value(row, "NULLS", 0)
        try:
            nulls_int = int(nulls_raw or 0)
        except Exception:
            try:
                nulls_int = int(float(nulls_raw)) if nulls_raw is not None else 0
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

        min_val_raw = _extract_row_value(row, "MIN_VAL")
        max_val_raw = _extract_row_value(row, "MAX_VAL")
        if is_string:
            min_val = None if min_val_raw is None else str(min_val_raw)
            max_val = None if max_val_raw is None else str(max_val_raw)
        else:
            min_val = _stringify(min_val_raw) if min_val_raw is not None else None
            max_val = _stringify(max_val_raw) if max_val_raw is not None else None

        empty_str_rows = _extract_row_value(row, "EMPTY_STR_ROWS", 0)
        whitespace_rows = _extract_row_value(row, "WHITESPACE_ROWS", 0)
        try:
            empty_str_rows_int = int(empty_str_rows)
        except Exception:
            try:
                empty_str_rows_int = int(float(empty_str_rows))
            except Exception:
                empty_str_rows_int = 0

        lead_trail_ws_rows = _extract_row_value(row, "LEAD_TRAIL_WS_ROWS", 0)
        ws_only_rows = _extract_row_value(row, "WS_ONLY_ROWS", 0)
        try:
            whitespace_rows_int = int(whitespace_rows)
        except Exception:
            try:
                whitespace_rows_int = int(float(whitespace_rows))
            except Exception:
                whitespace_rows_int = 0

        try:
            lead_trail_ws_rows_int = int(lead_trail_ws_rows)
        except Exception:
            try:
                lead_trail_ws_rows_int = int(float(lead_trail_ws_rows))
            except Exception:
                lead_trail_ws_rows_int = 0

        try:
            ws_only_rows_int = int(ws_only_rows)
        except Exception:
            try:
                ws_only_rows_int = int(float(ws_only_rows))
            except Exception:
                ws_only_rows_int = 0

        num_date_cnt_raw = _extract_row_value(row, num_date_matches_alias or "", 0) if num_date_matches_alias else 0
        try:
            num_date_cnt = int(num_date_cnt_raw)
        except Exception:
            try:
                num_date_cnt = int(float(num_date_cnt_raw))
            except Exception:
                num_date_cnt = 0
        num_date_min_raw = _extract_row_value(row, num_date_min_alias or "") if num_date_min_alias else None
        num_date_max_raw = _extract_row_value(row, num_date_max_alias or "") if num_date_max_alias else None

        avg_len_raw = _extract_row_value(row, "AVG_LEN")
        try:
            avg_len_value = float(avg_len_raw) if avg_len_raw is not None else None
        except Exception:
            avg_len_value = None

        non_nulls = max(row_cnt - nulls_int, 0)
        null_pct = (float(nulls_int) / float(row_cnt)) if row_cnt else 0.0
        if distincts_int is not None and non_nulls:
            distinct_pct = float(distincts_int) / float(non_nulls)
        else:
            distinct_pct = None
        whitespace_ratio = (float(whitespace_rows_int) / float(non_nulls)) if non_nulls else 0.0
        lead_trail_ws_ratio = (float(lead_trail_ws_rows_int) / float(non_nulls)) if non_nulls else 0.0
        only_ws_ratio = (float(ws_only_rows_int) / float(non_nulls)) if non_nulls else 0.0
        num_date_ratio = (float(num_date_cnt) / float(non_nulls)) if non_nulls else 0.0
        num_date_min_value = str(num_date_min_raw) if num_date_min_raw is not None else None
        num_date_max_value = str(num_date_max_raw) if num_date_max_raw is not None else None
        whitespace_pct = whitespace_ratio * 100.0

        avg_len_debug_note: Optional[str] = None
        if is_string:
            if avg_len_value is None:
                if non_nulls > 0:
                    logger.warning("avg_len metric missing for column %s", name)
                    if PROFILE_DEBUG:
                        avg_len_debug_note = "avg_len missing; replaced FILTER with CASE"
                avg_len_final: Optional[float] = 0.0
            else:
                avg_len_final = float(avg_len_value)
                if (
                    PROFILE_DEBUG
                    and non_nulls > 0
                    and math.isclose(avg_len_final, 0.0, rel_tol=0.0, abs_tol=1e-9)
                ):
                    logger.warning("avg_len value zero for column %s", name)
                    avg_len_debug_note = "avg_len missing; replaced FILTER with CASE"
        elif is_numeric:
            avg_len_final = float(avg_len_value) if avg_len_value is not None else None
        else:
            avg_len_final = None

        len_min_val: Optional[float] = None
        len_max_val: Optional[float] = None
        len_stddev_val: Optional[float] = None
        supports_length_stats = length_expr is not None
        if supports_length_stats:
            len_min_raw = _extract_row_value(row, "LEN_MIN")
            len_max_raw = _extract_row_value(row, "LEN_MAX")
            try:
                len_min_val = float(len_min_raw) if len_min_raw is not None else None
            except Exception:
                len_min_val = None
            try:
                len_max_val = float(len_max_raw) if len_max_raw is not None else None
            except Exception:
                len_max_val = None
            if len_stddev_alias:
                len_stddev_raw = _extract_row_value(row, len_stddev_alias)
                try:
                    len_stddev_val = float(len_stddev_raw) if len_stddev_raw is not None else None
                except Exception:
                    len_stddev_val = None

        regex_ratios: Dict[str, Optional[float]] = {}
        for key, alias in regex_aliases.items():
            matches_raw = _extract_row_value(row, alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            ratio = (float(matches) / float(non_nulls)) if non_nulls else 0.0
            regex_ratios[key] = ratio

        char_ratios: Dict[str, Optional[float]] = {}
        for key, alias in char_pattern_aliases.items():
            matches_raw = _extract_row_value(row, alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            ratio = (float(matches) / float(non_nulls)) if non_nulls else 0.0
            char_ratios[key] = ratio

        reference_ratios: Dict[str, Optional[float]] = {}
        for key, alias in ref_match_aliases.items():
            matches_raw = _extract_row_value(row, alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            ratio = (float(matches) / float(non_nulls)) if non_nulls else 0.0
            reference_ratios[key] = ratio

        numeric_like_ratio: Optional[float] = None
        if numeric_like_alias:
            matches_raw = _extract_row_value(row, numeric_like_alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            numeric_like_ratio = (float(matches) / float(non_nulls)) if non_nulls else 0.0

        sentinel_count: int = 0
        if sentinel_alias:
            sentinel_raw = _extract_row_value(row, sentinel_alias, 0)
            try:
                sentinel_count = int(sentinel_raw)
            except Exception:
                try:
                    sentinel_count = int(float(sentinel_raw))
                except Exception:
                    sentinel_count = 0
        valid_string_count = max((non_nulls or 0) - sentinel_count, 0)

        date_pattern_ratios: Dict[str, Optional[float]] = {}
        for key, alias in date_pattern_aliases.items():
            matches_raw = _extract_row_value(row, alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            denominator = float(valid_string_count) if valid_string_count else float(non_nulls or 0)
            if denominator:
                date_pattern_ratios[key] = float(matches) / denominator
            else:
                date_pattern_ratios[key] = 0.0

        date_parse_counts: Dict[str, int] = {}
        date_parse_ratios: Dict[str, Optional[float]] = {}
        parsed_min_values: Dict[str, Optional[Any]] = {}
        parsed_max_values: Dict[str, Optional[Any]] = {}
        for key, alias in date_parse_aliases.items():
            matches_raw = _extract_row_value(row, alias, 0)
            try:
                matches = int(matches_raw)
            except Exception:
                matches = 0
            date_parse_counts[key] = matches
            denominator = float(valid_string_count) if valid_string_count else float(non_nulls or 0)
            if denominator:
                date_parse_ratios[key] = float(matches) / denominator
            else:
                date_parse_ratios[key] = 0.0
            parsed_min_values[key] = _extract_row_value(row, date_parse_min_aliases.get(key, ""))
            parsed_max_values[key] = _extract_row_value(row, date_parse_max_aliases.get(key, ""))

        any_parse_ratio: Optional[float] = None
        any_parsed_min: Optional[Any] = None
        any_parsed_max: Optional[Any] = None
        if any_parse_alias:
            any_matches_raw = _extract_row_value(row, any_parse_alias, 0)
            try:
                any_matches = int(any_matches_raw)
            except Exception:
                any_matches = 0
            denominator = float(valid_string_count) if valid_string_count else float(non_nulls or 0)
            if denominator:
                any_parse_ratio = float(any_matches) / denominator
            else:
                any_parse_ratio = 0.0
            any_parsed_min = _extract_row_value(row, any_parse_min_alias or "")
            any_parsed_max = _extract_row_value(row, any_parse_max_alias or "")

        best_date_key: Optional[str] = None
        best_date_ratio = 0.0
        for key, ratio in date_parse_ratios.items():
            ratio_val = float(ratio or 0.0)
            if ratio_val >= best_date_ratio and date_parse_counts.get(key, 0) > 0:
                # prefer earlier key ordering when ratios equal
                if not math.isclose(ratio_val, best_date_ratio) or best_date_key is None:
                    best_date_ratio = ratio_val
                    best_date_key = key

        parsed_date_min: Optional[str] = None
        parsed_date_max: Optional[str] = None
        if any_parsed_min is not None or any_parsed_max is not None:
            parsed_date_min = str(any_parsed_min) if any_parsed_min is not None else None
            parsed_date_max = str(any_parsed_max) if any_parsed_max is not None else None
        elif best_date_key:
            best_min = parsed_min_values.get(best_date_key)
            best_max = parsed_max_values.get(best_date_key)
            parsed_date_min = str(best_min) if best_min is not None else None
            parsed_date_max = str(best_max) if best_max is not None else None

        hints = _derive_name_hints(name)

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

        whitespace_length_counts: List[Tuple[int, int]] = []
        if is_string and ws_only_rows_int > 0 and rows_profiled:
            ws_len_sql = (
                f"SELECT LENGTH({qcol}::STRING) AS WS_LEN, COUNT(*) AS CNT FROM {sampled_ref} "
                f"WHERE {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s+$') "
                "GROUP BY 1 ORDER BY 1"
            )
            try:
                for item in session.sql(ws_len_sql).collect():
                    if hasattr(item, "asDict"):
                        data = item.asDict()
                        length_raw = data.get("WS_LEN") if "WS_LEN" in data else data.get("ws_len")
                        count_raw = data.get("CNT") if "CNT" in data else data.get("cnt")
                    else:
                        length_raw = item[0]
                        count_raw = item[1] if len(item) > 1 else 0
                    try:
                        length_int = int(length_raw) if length_raw is not None else None
                    except Exception:
                        try:
                            length_int = int(float(length_raw)) if length_raw is not None else None
                        except Exception:
                            length_int = None
                    try:
                        count_int = int(count_raw)
                    except Exception:
                        try:
                            count_int = int(float(count_raw))
                        except Exception:
                            count_int = 0
                    if length_int is None:
                        continue
                    whitespace_length_counts.append((length_int, count_int))
            except Exception:
                whitespace_length_counts = []

        if nulls_int > 0:
            pct = (float(nulls_int) / float(row_cnt) * 100.0) if row_cnt else 0.0
            top_values.append({"value": None, "count": nulls_int, "pct": pct})
        if is_string and empty_str_rows_int > 0:
            pct = (float(empty_str_rows_int) / float(non_nulls) * 100.0) if non_nulls else 0.0
            top_values.append({"value": "", "count": empty_str_rows_int, "pct": pct})
        if is_string and whitespace_length_counts and non_nulls:
            for length_int, count_int in whitespace_length_counts:
                pct = (float(count_int) / float(non_nulls) * 100.0) if non_nulls else 0.0
                top_values.append({"value": f"__WS_LEN__:{length_int}", "count": count_int, "pct": pct})

        coverage_pct = (float(top_coverage) / non_nulls * 100.0) if non_nulls else 0.0

        top1_ratio = None
        top3_ratio = None
        if top_values and non_nulls:
            top1_ratio = float(top_values[0].get("count") or 0) / float(non_nulls)
            top3_ratio = (
                float(sum((top_values[idx].get("count") or 0) for idx in range(min(3, len(top_values)))))
                / float(non_nulls)
            )

        column_entry: Dict[str, Any] = {
            "name": name,
            "column_name": name,
            "data_type": dtype,
            "nulls": nulls_int,
            "null_pct": float(null_pct),
            "distincts": distincts_int,
            "distinct_pct": float(distinct_pct) if distinct_pct is not None else None,
            "min_val": min_val,
            "max_val": max_val,
            "avg_len": avg_len_final if supports_length_stats else None,
            "whitespace_pct": whitespace_pct,
            "whitespace_only_pct": only_ws_ratio * 100.0,
            "top_values": top_values,
            "top_coverage_pct": coverage_pct,
            "rows_profiled": row_cnt,
            "row_cnt": row_cnt,
            "non_nulls": non_nulls,
            "null_cnt": nulls_int,
            "error": error_message,
        }
        if avg_len_debug_note:
            existing_note = column_entry.get("note")
            if existing_note:
                column_entry["note"] = f"{existing_note}; {avg_len_debug_note}"
            else:
                column_entry["note"] = avg_len_debug_note

        column_entry["len_min"] = len_min_val if supports_length_stats else None
        column_entry["len_max"] = len_max_val if supports_length_stats else None
        if supports_length_stats and len_min_val is not None and len_max_val is not None:
            column_entry["len_spread"] = float(len_max_val) - float(len_min_val)
        else:
            column_entry["len_spread"] = None

        if is_string:
            column_entry.update(
                {
                    "distinct_ratio": (float(distincts_int) / float(non_nulls)) if (distincts_int is not None and non_nulls) else None,
                    "top1_ratio": top1_ratio,
                    "top3_ratio": top3_ratio,
                    "numeric_like_ratio": numeric_like_ratio,
                    "date_sentinel_count": sentinel_count,
                    "len_stddev": len_stddev_val,
                    "date_pattern_yyyymmdd_ratio": date_pattern_ratios.get("yyyymmdd"),
                    "date_pattern_ddmmyyyy_ratio": date_pattern_ratios.get("ddmmyyyy"),
                    "date_pattern_iso_ymd_ratio": date_pattern_ratios.get("iso"),
                    "date_parse_ratio_yyyymmdd": date_parse_ratios.get("yyyymmdd"),
                    "date_parse_ratio_ddmmyyyy": date_parse_ratios.get("ddmmyyyy"),
                    "date_parse_ratio_iso": date_parse_ratios.get("iso"),
                    "date_parse_ratio_best": best_date_ratio if best_date_key else None,
                    "date_parse_best_format": best_date_key,
                    "date_parse_success_ratio": any_parse_ratio,
                    "date_success_ratio": best_date_ratio if best_date_key else any_parse_ratio,
                    "date_format_detected": best_date_key,
                    "date_valid_count": valid_string_count,
                    "parsed_date_min": parsed_date_min,
                    "parsed_date_max": parsed_date_max,
                }
            )

        signals: Dict[str, Any] = {
            "null_pct": null_pct,
            "distinct_pct": distinct_pct,
            "regex": regex_ratios,
            "character_classes": char_ratios,
            "reference_matches": reference_ratios,
            "hints": hints,
        }
        signals["date_parse_numeric"] = {
            "yyyymmdd_ratio": num_date_ratio,
            "parsed_min": num_date_min_value,
            "parsed_max": num_date_max_value,
        }
        if supports_length_stats:
            signals["length"] = {
                "min": len_min_val,
                "max": len_max_val,
                "avg": avg_len_final,
                "stddev": len_stddev_val,
                "spread": column_entry.get("len_spread"),
            }
        else:
            signals["length"] = {"min": None, "max": None, "avg": None, "spread": None}
        if is_string:
            if "whitespace" in char_ratios:
                signals["whitespace_ratio"] = char_ratios.get("whitespace")
            signals["string_stats"] = {
                "row_cnt": non_nulls,
                "distinct_ratio": column_entry.get("distinct_ratio"),
                "top1_ratio": top1_ratio,
                "top3_ratio": top3_ratio,
                "numeric_like_ratio": numeric_like_ratio,
                "sentinel_count": sentinel_count,
                "len_spread": column_entry.get("len_spread"),
                "avg_len": avg_len_final,
                "whitespace_ratio": whitespace_ratio,
                "lead_trail_ws_ratio": lead_trail_ws_ratio,
                "only_ws_ratio": only_ws_ratio,
                "empty_str_count": empty_str_rows_int,
                "whitespace_only_count": ws_only_rows_int,
            }
            signals["date_patterns"] = {str(key): date_pattern_ratios.get(key) for key in date_pattern_ratios}
            signals["date_parse"] = {
                "formats": {str(key): date_parse_ratios.get(key) for key in date_parse_ratios},
                "best_ratio": best_date_ratio if best_date_key else None,
                "best_format": best_date_key,
                "success_ratio": any_parse_ratio,
                "date_success_ratio": best_date_ratio if best_date_key else any_parse_ratio,
                "format_detected": best_date_key,
                "valid_count": valid_string_count,
                "parsed_min": parsed_date_min,
                "parsed_max": parsed_date_max,
            }

        column_entry["signals"] = signals

        (
            semantic_type,
            confidence,
            rationale,
            profile_min,
            profile_max,
        ) = _infer_semantic_type(column_entry)
        column_entry["semantic_type"] = semantic_type
        column_entry["confidence"] = confidence
        column_entry["rationale"] = rationale
        column_entry["profile_min"] = profile_min
        column_entry["profile_max"] = profile_max

        if (
            semantic_type == "REF_CODE"
            and is_string
            and (numeric_like_ratio or 0.0) >= 0.9
        ):
            try:
                bounds_row = _collect_single_row(
                    session,
                    (
                        "SELECT MIN(TRY_TO_NUMBER({col}::STRING)) AS NUMERIC_MIN, "
                        "MAX(TRY_TO_NUMBER({col}::STRING)) AS NUMERIC_MAX FROM {table} "
                        "WHERE TRY_TO_NUMBER({col}::STRING) IS NOT NULL"
                    ).format(col=qcol, table=sampled_ref),
                )
            except Exception:
                bounds_row = None
            numeric_min_raw = _extract_row_value(bounds_row, "NUMERIC_MIN")
            numeric_max_raw = _extract_row_value(bounds_row, "NUMERIC_MAX")

            def _to_float(value: Any) -> Optional[float]:
                if value is None:
                    return None
                try:
                    return float(value)
                except Exception:
                    try:
                        return float(str(value))
                    except Exception:
                        return None

            numeric_min_val = _to_float(numeric_min_raw)
            numeric_max_val = _to_float(numeric_max_raw)
            if numeric_min_val is not None:
                column_entry["numeric_min"] = numeric_min_val
                column_entry["profile_min"] = numeric_min_val
            elif numeric_min_raw is not None:
                column_entry["numeric_min"] = str(numeric_min_raw)
                if column_entry.get("profile_min") is None:
                    column_entry["profile_min"] = str(numeric_min_raw)
            if numeric_max_val is not None:
                column_entry["numeric_max"] = numeric_max_val
                column_entry["profile_max"] = numeric_max_val
            elif numeric_max_raw is not None:
                column_entry["numeric_max"] = str(numeric_max_raw)
                if column_entry.get("profile_max") is None:
                    column_entry["profile_max"] = str(numeric_max_raw)

        dq_checks: Dict[str, Dict[str, Any]] = {}
        dq_reason = ""

        distinct_ratio_value = float(distinct_pct) if distinct_pct is not None else None
        null_pct_value = float(null_pct)
        only_ws_ratio_value = float(only_ws_ratio)
        whitespace_ratio_value = float(whitespace_ratio)

        guardrail_skip = False
        if non_nulls == 0:
            guardrail_skip = True
        elif only_ws_ratio_value >= 0.80:
            guardrail_skip = True
        elif null_pct_value >= 0.90 and (distincts_int in (None, 0)):
            guardrail_skip = True

        if not guardrail_skip:
            if (
                distinct_ratio_value is None
                and distincts_int is not None
                and non_nulls
            ):
                distinct_ratio_value = float(distincts_int) / float(non_nulls)

            if (
                distinct_ratio_value is not None
                and distinct_ratio_value >= 0.60
                and nulls_int == 0
            ):
                dq_reason = f"near-unique; {int(round(null_pct_value * 100))}% nulls"
                dq_checks["UNIQUE"] = {
                    "severity": "ERROR",
                    "params": {"ignore_nulls": True},
                }
            elif (
                distincts_int is not None
                and distincts_int <= 50
                and (top3_ratio or 0.0) >= 0.80
            ):
                coverage_pct_val = int(round((top3_ratio or 0.0) * 100))
                dq_reason = f"enum-like; top3 {coverage_pct_val}%"
                allowed_values: List[str] = []
                for entry in top_values:
                    value = entry.get("value")
                    if value is None:
                        continue
                    allowed_values.append(_stringify(value))
                    if len(allowed_values) >= 20:
                        break
                dq_checks["VALUE_DISTRIBUTION"] = {
                    "severity": "WARN",
                    "params": {
                        "allowed_values_csv": ", ".join(allowed_values),
                        "min_match_ratio": 0.9,
                    },
                }
            elif _is_temporal(dtype) and min_val is not None and max_val is not None:
                dq_reason = f"temporal range {min_val}–{max_val}"
                dq_checks["MIN_MAX"] = {
                    "severity": "WARN",
                    "params": {
                        "min": _stringify(min_val) if min_val is not None else None,
                        "max": _stringify(max_val) if max_val is not None else None,
                    },
                }
            elif (
                semantic_type == "DATE_IN_TEXT"
                and (
                    float(any_parse_ratio or 0.0)
                    if any_parse_ratio not in (None, 0)
                    else float(num_date_ratio or 0.0)
                )
                >= 0.6
            ):
                success_ratio = (
                    float(any_parse_ratio)
                    if any_parse_ratio not in (None, 0)
                    else float(num_date_ratio or 0.0)
                )
                success_pct = int(round(success_ratio * 100))
                dq_reason = f"dates in text; {success_pct}% parse success"
                check_min = parsed_date_min or num_date_min_value
                check_max = parsed_date_max or num_date_max_value
                dq_checks["MIN_MAX"] = {
                    "severity": "WARN",
                    "params": {"min": check_min, "max": check_max},
                }
                best_format_key = best_date_key
                if best_format_key:
                    config = DATE_PARSE_CONFIG_MAP.get(best_format_key, {})
                    format_label = DATE_PARSE_LABELS.get(best_format_key, best_format_key)
                    regex_value = config.get("regex") or ""
                    dq_checks["FORMAT_DISTRIBUTION"] = {
                        "severity": "WARN",
                        "params": {"label": format_label, "regex": regex_value},
                    }
            elif is_numeric and min_val is not None and max_val is not None:
                dq_reason = f"numeric range {min_val}–{max_val}"
                dq_checks["MIN_MAX"] = {
                    "severity": "WARN",
                    "params": {
                        "min": _stringify(min_val) if min_val is not None else None,
                        "max": _stringify(max_val) if max_val is not None else None,
                    },
                }

        if dq_checks:
            if null_pct_value > 0.0:
                max_nulls_allowed = int(math.ceil(float(row_cnt) * null_pct_value))
                null_severity = "WARN" if null_pct_value < 0.01 else "ERROR"
                dq_checks["NULL_COUNT"] = {
                    "severity": null_severity,
                    "params": {"max_nulls": max_nulls_allowed},
                }
            if is_string and (
                whitespace_ratio_value >= 0.05 or only_ws_ratio_value > 0.0
            ):
                dq_checks["WHITESPACE"] = {
                    "severity": "WARN",
                    "params": {"mode": "NO_LEADING_TRAILING"},
                }

        column_entry["dq_selected"] = bool(dq_checks)
        column_entry["dq_reason"] = dq_reason
        column_entry["dq_checks"] = dq_checks or None

        per_column.append(column_entry)

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
            if distinct_pct >= 0.999 and null_pct <= 0.01 and distincts >= non_nulls:
                checks["UNIQUE"] = {"severity": "ERROR", "params": {"ignore_nulls": True}}

        if null_pct > 0:
            severity = "ERROR" if null_pct >= 0.01 else "WARN"
            max_nulls = int(math.ceil((baseline_rows or 0) * null_pct))
            checks["NULL_COUNT"] = {"severity": severity, "params": {"max_nulls": max_nulls}}

        if whitespace_pct >= 5.0 and _is_string_type(data_type):
            checks["WHITESPACE"] = {"severity": "WARN", "params": {"mode": "NO_LEADING_TRAILING"}}

        if (min_val is not None and max_val is not None) and (
            _is_numeric(data_type) or _is_temporal(data_type) or _is_string_type(data_type)
        ):
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
            score = 100.0 - (null_pct * 100.0)
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
            SEMANTIC_TYPE STRING,
            CONFIDENCE FLOAT,
            RATIONALE STRING,
            SIGNALS VARIANT,
            SUGGESTED_CHECKS VARIANT,
            PRIMARY KEY (RUN_ID, COLUMN_NAME)
        )
        """
    ).collect()

    for column_name, column_type in (
        ("SEMANTIC_TYPE", "STRING"),
        ("CONFIDENCE", "FLOAT"),
        ("RATIONALE", "STRING"),
        ("SIGNALS", "VARIANT"),
        ("SUGGESTED_CHECKS", "VARIANT"),
    ):
        session.sql(
            f"ALTER TABLE {cols_tbl} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        ).collect()

    session.sql(f"DELETE FROM {runs_tbl} WHERE RUN_ID = ?", params=[run_id]).collect()
    session.sql(f"DELETE FROM {cols_tbl} WHERE RUN_ID = ?", params=[run_id]).collect()

    session.sql(
        f"INSERT INTO {runs_tbl} (RUN_ID, SUMMARY) SELECT ?, PARSE_JSON(?)",
        params=[run_id, summary_json],
    ).collect()

    for row_data in rows:
        normalized_row = normalize_profile_row(row_data)
        column_name = normalized_row.get("column_name")
        if not column_name:
            continue
        serialized = json.dumps(normalized_row)
        semantic_type = normalized_row.get("semantic_type")
        confidence_raw = normalized_row.get("confidence")
        rationale = normalized_row.get("rationale")
        signals_value = normalized_row.get("signals")
        suggested_checks_value = normalized_row.get("suggested_checks")

        if (
            semantic_type is None
            or confidence_raw is None
            or (isinstance(rationale, str) and not rationale.strip())
        ):
            try:
                (
                    inferred_type,
                    inferred_conf,
                    inferred_rationale,
                    inferred_min,
                    inferred_max,
                ) = _infer_semantic_type(normalized_row)
            except Exception:
                inferred_type = inferred_conf = inferred_rationale = None
                inferred_min = inferred_max = None
            else:
                if semantic_type is None:
                    semantic_type = inferred_type
                if confidence_raw is None:
                    confidence_raw = inferred_conf
                if (isinstance(rationale, str) and not rationale.strip()) or rationale is None:
                    rationale = inferred_rationale
                if normalized_row.get("profile_min") is None and inferred_min is not None:
                    normalized_row["profile_min"] = inferred_min
                if normalized_row.get("profile_max") is None and inferred_max is not None:
                    normalized_row["profile_max"] = inferred_max

        try:
            confidence = float(confidence_raw) if confidence_raw is not None else None
        except Exception:
            confidence = None

        def _json_or_null(value: Any) -> str:
            if value is None:
                return "null"
            try:
                return json.dumps(value)
            except Exception:
                return "null"

        signals_json = _json_or_null(signals_value)
        suggested_checks_json = _json_or_null(suggested_checks_value)

        session.sql(
            f"""
            INSERT INTO {cols_tbl} (
                RUN_ID,
                COLUMN_NAME,
                PROFILE,
                SEMANTIC_TYPE,
                CONFIDENCE,
                RATIONALE,
                SIGNALS,
                SUGGESTED_CHECKS
            )
            SELECT ?, ?, PARSE_JSON(?), ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?)
            """,
            params=[
                run_id,
                column_name,
                serialized,
                semantic_type,
                confidence,
                rationale,
                signals_json,
                suggested_checks_json,
            ],
        ).collect()

    return str(run_id)
