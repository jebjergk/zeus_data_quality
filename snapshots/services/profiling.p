"""Service helpers for table profiling and automated DQ suggestions."""

from __future__ import annotations

import json
import logging
import math
import os
import random
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from uuid import uuid4

from services.profile import _is_numeric, _is_temporal, _stringify
from services.semantics import clamp_confidence, truncate_note
from utils.meta import _q

__all__ = [
    "list_columns",
    "list_saved_profiles",
    "load_profile_run",
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
    "iban": r"^[A-Z]{2}\d{2}[0-9A-Z]{11,30}$",
    "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
    "bic": r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "url": r"^(https?|ftp)://[^\s/$.?#].[^\s]*$",
    "ipv4": r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(?!$)|$)){4}$",
    "phone_e164": r"^\+[1-9][0-9]{1,14}$",
}

_IBAN_REGEX = re.compile(SEMANTIC_REGEX_PATTERNS["iban"])


CHAR_CLASS_PATTERNS: Dict[str, str] = {
    "digit": r"^[0-9]+$",
    "alpha": r"^[A-Za-z]+$",
    "alnum": r"^[0-9A-Za-z]+$",
    "whitespace": r".*\s.*",
}


def _normalize_iban_candidate(value: Any) -> str:
    """Return an uppercase alphanumeric IBAN candidate without whitespace."""

    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    cleaned = "".join(ch for ch in text if ch.isalnum())
    return cleaned.upper()


def _iban_mod97(value: str) -> bool:
    """Validate an IBAN using the ISO 13616 mod-97 checksum."""

    if not value:
        return False
    if len(value) < 15 or len(value) > 34:
        return False
    rearranged = value[4:] + value[:4]
    remainder = 0
    for ch in rearranged:
        if ch.isdigit():
            digit_seq = ch
        elif ch.isalpha():
            digit_seq = str(ord(ch.upper()) - 55)
        else:
            return False
        for digit in digit_seq:
            try:
                remainder = (remainder * 10 + int(digit)) % 97
            except Exception:
                return False
    return remainder == 1


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
        valid_string_count = int(date_valid_count_raw) if date_valid_count_raw is not None else 0
    except Exception:
        try:
            valid_string_count = int(float(date_valid_count_raw)) if date_valid_count_raw is not None else 0
        except Exception:
            valid_string_count = 0
    date_valid_count = valid_string_count
    best_count_signal = _as_int(date_parse.get("best_count")) or 0
    text_success_count_signal = _as_int(date_parse.get("success_count")) or 0
    best_source_signal = str(date_parse.get("best_source") or "").lower()
    numeric_date_ratio_signal = _as_float(num_date.get("numdate_ratio") or num_date.get("yyyymmdd_ratio"))
    numeric_date_count_signal = _as_int(num_date.get("numdate_count") or num_date.get("count")) or 0
    numeric_date_min_signal = (
        num_date.get("numdate_min")
        or date_parse.get("numdate_min")
        or num_date.get("parsed_min")
    )
    numeric_date_max_signal = (
        num_date.get("numdate_max")
        or date_parse.get("numdate_max")
        or num_date.get("parsed_max")
    )
    if parsed_date_min is None and numeric_date_min_signal is not None:
        parsed_date_min = numeric_date_min_signal
    if parsed_date_max is None and numeric_date_max_signal is not None:
        parsed_date_max = numeric_date_max_signal
    if numeric_date_ratio_signal is not None:
        format_ratios.setdefault("yyyymmdd_numeric", numeric_date_ratio_signal)
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

    text_ratio_candidates: List[float] = []
    for key, value in format_ratios.items():
        if str(key or "").lower().endswith("_numeric"):
            continue
        val = _as_float(value)
        if val is not None:
            text_ratio_candidates.append(val)
    for candidate in (
        date_success_ratio_signal,
        overall_success_ratio,
    ):
        if candidate is not None:
            text_ratio_candidates.append(float(candidate))
    if best_date_ratio is not None and best_source_signal != "numeric":
        text_ratio_candidates.append(float(best_date_ratio))
    text_best_ratio = max(text_ratio_candidates) if text_ratio_candidates else 0.0
    text_success_count = max(best_count_signal, text_success_count_signal, 0)
    if text_success_count <= 0 and text_best_ratio > 0 and valid_string_count > 0:
        text_success_count = int(round(text_best_ratio * float(valid_string_count)))
    if text_success_count <= 0 and text_best_ratio > 0 and non_nulls > 0:
        text_success_count = int(round(text_best_ratio * float(non_nulls)))
    numeric_ratio = float(numeric_date_ratio_signal or 0.0)
    numeric_success_count = max(numeric_date_count_signal, 0)
    best_success_ratio = text_best_ratio
    best_success_source = "text"
    best_success_count = max(text_success_count, 0)
    if numeric_ratio > 0 and numeric_success_count > 0:
        if numeric_ratio > best_success_ratio + 1e-6 or (
            math.isclose(numeric_ratio, best_success_ratio, rel_tol=1e-6, abs_tol=1e-9)
            and numeric_success_count > best_success_count
        ):
            best_success_ratio = numeric_ratio
            best_success_source = "numeric"
            best_success_count = numeric_success_count
    if best_success_source == "numeric":
        if best_date_format not in {"yyyymmdd", "YYYYMMDD"}:
            best_date_format = "yyyymmdd"
        best_date_ratio = numeric_ratio
        if numeric_date_min_signal is not None:
            parsed_date_min = numeric_date_min_signal
        if numeric_date_max_signal is not None:
            parsed_date_max = numeric_date_max_signal
    elif best_date_ratio is None and text_best_ratio > 0:
        best_date_ratio = text_best_ratio
    date_valid_count = best_success_count

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

    date_success_ratio = best_success_ratio if best_success_ratio > 0 else None
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
        and date_success_ratio >= 0.8
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
        if pattern_ratio >= 0.8 and (parsed_date_min or parsed_date_max):
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
    min_year_ok = parsed_min_dt.year >= 1950 and parsed_min_dt.year <= 2100 if parsed_min_dt else False
    max_year_ok = parsed_max_dt.year >= 1950 and parsed_max_dt.year <= 2100 if parsed_max_dt else False
    at_least_one_year_ok = min_year_ok or max_year_ok
    span_ok = True
    if parsed_min_dt and parsed_max_dt and date_span_years is not None:
        span_ok = date_span_years <= 200
    date_range_ok = at_least_one_year_ok and span_ok

    sentinel_zero_count = _as_int(column_entry.get("date_sentinel_count")) or 0
    numeric_zero_signal = _as_int(column_entry.get("numeric_zero_count")) or 0
    best_success_ratio = max(0.0, min(best_success_ratio, 1.0))
    ratio_threshold = 0.9 if best_success_source == "numeric" else 0.8
    ratio_ok = (
        not forced_type
        and best_success_ratio >= ratio_threshold
        and best_success_count > 0
        and date_range_ok
    )
    pattern_ratio = max(
        iso_pattern_ratio or 0.0,
        yyyymmdd_pattern_ratio or 0.0,
        ddmmyyyy_pattern_ratio or 0.0,
        pattern_ratio_fallback,
    )
    pattern_ok = (
        not forced_type
        and not ratio_ok
        and pattern_ratio >= 0.8
        and parsed_min_dt is not None
        and parsed_max_dt is not None
        and date_range_ok
    )
    if ratio_ok or pattern_ok:
        forced_type = "DATE_IN_TEXT"
        confidence_basis = best_success_ratio if ratio_ok else pattern_ratio
        forced_confidence = round(min(max(confidence_basis, 0.8), 0.99), 3)
        note_parts: List[str] = []
        if ratio_ok:
            if best_success_source == "numeric":
                note_parts.append(f"numeric YYYYMMDD parse {best_success_ratio:.0%}")
            else:
                format_key = str(best_date_format or "").lower()
                label = DATE_PARSE_LABELS.get(
                    format_key,
                    str(best_date_format or "text date").upper(),
                )
                note_parts.append(f"Parsed as {label} ({best_success_ratio:.0%})")
        if pattern_ok:
            note_parts.append(f"{pattern_ratio:.0%} date pattern match")
        if sentinel_zero_count > 0:
            note_parts.append("ignored sentinel zeros")
        if numeric_zero_signal > 0 and best_success_source == "numeric":
            note_parts.append("ignored numeric zeros")
        if date_span_years is not None and date_span_years >= 1.0:
            note_parts.append(f"span {date_span_years:.1f}y")
        if parsed_date_min or parsed_date_max:
            note_parts.append(
                "range {start} → {end}".format(
                    start=parsed_date_min or "?",
                    end=parsed_date_max or "?",
                )
            )
        forced_rationale_parts = [
            truncate_note("; ".join(part for part in note_parts if part))
        ]
        profile_min = parsed_date_min or profile_min
        profile_max = parsed_date_max or profile_max
    elif best_success_ratio >= 0.7 and not forced_type:
        scores["DATE_IN_TEXT"] = scores.get("DATE_IN_TEXT", 0.0) * 0.5

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
    country_name_ref = _ratio(references, "reference_country_name")
    country_name_hint = bool(hints.get("country"))
    country_selected_mode: Optional[str] = None
    country_selected_overlap: float = 0.0
    country_shape_mismatch = False

    exchange_ref = _ratio(references, "reference_exchange_code")
    if exchange_ref > 0:
        _boost("TICKER/SYMBOL", 40.0 * min(exchange_ref, 1.0), "values overlap with known exchange codes")

    char_alpha = _ratio(char_classes, "alpha")
    char_digit = _ratio(char_classes, "digit")
    char_alnum = _ratio(char_classes, "alnum")

    distinct_ratio_value = max(0.0, min(1.0, float(resolved_distinct_ratio)))
    numeric_like_ratio = float(numeric_like_ratio_signal or 0.0)
    length_min_val = length_min if length_min is not None else None
    length_max_val = length_max if length_max is not None else None
    length_avg_val = length_avg if length_avg is not None else None
    code_shape_ok = (
        country_code_ref >= 0.80
        and (char_alpha or 0.0) >= 0.95
        and length_min_val is not None
        and length_max_val is not None
        and length_min_val >= 2.0
        and length_max_val <= 3.0
        and distinct_ratio_value <= 0.5
    )
    name_shape_ok = (
        country_name_ref >= 0.70
        and (char_alpha or 0.0) >= 0.60
        and length_avg_val is not None
        and length_avg_val >= 4.0
        and distinct_ratio_value <= 0.8
        and numeric_like_ratio < 0.2
    )
    code_overlap = max(0.0, min(1.0, country_code_ref))
    name_overlap = max(0.0, min(1.0, country_name_ref))
    code_score = 80.0 * code_overlap if code_shape_ok else 0.0
    name_score = 70.0 * name_overlap if name_shape_ok else 0.0
    country_rationale_parts: List[str] = []
    if code_shape_ok and code_score >= name_score:
        country_selected_mode = "code"
        country_selected_overlap = code_overlap
        country_score = code_score
        country_rationale_parts.append(
            "ref overlap {overlap:.0%}; len 2–3; alpha {alpha:.0%}".format(
                overlap=code_overlap,
                alpha=char_alpha or 0.0,
            )
        )
    elif name_shape_ok and name_score > 0:
        country_selected_mode = "name"
        country_selected_overlap = name_overlap
        country_score = name_score
        avg_hint = f"; avg len {length_avg_val:.0f}" if length_avg_val is not None else ""
        country_rationale_parts.append(
            "ref overlap {overlap:.0%}; alpha {alpha:.0%}{extra}".format(
                overlap=name_overlap,
                alpha=char_alpha or 0.0,
                extra=avg_hint,
            )
        )
    else:
        country_score = 0.0
        if country_code_ref >= 0.5 or country_name_ref >= 0.5:
            country_shape_mismatch = True

    if country_score > 0 and country_name_hint:
        country_score += 10.0
        country_rationale_parts.append("name hint 'country'")

    country_penalty = False
    if (
        length_max_val is not None
        and length_max_val > 15.0
        and (char_alpha or 0.0) < 0.6
    ) or distinct_ratio_value >= 0.6:
        country_penalty = True

    if country_penalty and country_score > 0:
        country_score = min(country_score, 5.0)
        country_shape_mismatch = True
        country_rationale_parts.append("shape mismatch")

    if country_score > 0:
        scores["COUNTRY_CODE/NAME"] = country_score
        rationales["COUNTRY_CODE/NAME"] = [
            truncate_note(part) for part in country_rationale_parts if part
        ]
    elif country_shape_mismatch:
        scores["COUNTRY_CODE/NAME"] = min(scores.get("COUNTRY_CODE/NAME", 0.0), 1.0)
        rationales.setdefault("COUNTRY_CODE/NAME", []).append("shape mismatch")

    uppercase_matches = 0
    total_matches = 0
    boolean_candidates: Set[str] = set()
    iban_candidate_samples: List[str] = []
    iban_sample_seen: Set[str] = set()
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
        iban_candidate = _normalize_iban_candidate(value)
        if (
            iban_candidate
            and len(iban_candidate_samples) < 20
            and _IBAN_REGEX.fullmatch(iban_candidate)
            and iban_candidate not in iban_sample_seen
        ):
            iban_candidate_samples.append(iban_candidate)
            iban_sample_seen.add(iban_candidate)

    uppercase_ratio = (float(uppercase_matches) / float(total_matches)) if total_matches else 0.0

    iban_checksum_checked = False
    iban_checksum_ok = False
    iban_confidence_cap = 1.0
    iban_notes: List[str] = []
    if iban_candidate_samples:
        iban_checksum_checked = True
        for candidate in iban_candidate_samples:
            if _iban_mod97(candidate):
                iban_checksum_ok = True
                break
    elif iban_ratio >= 0.9:
        iban_confidence_cap = min(iban_confidence_cap, 0.5)
        iban_notes.append("insufficient IBAN samples for checksum validation")

    iban_negative_prior = False
    top3_ratio_value = (
        float(top3_ratio_signal)
        if top3_ratio_signal is not None
        else None
    )
    if (
        (top3_ratio_value is not None and top3_ratio_value >= 0.9)
        or (length_spread is not None and length_spread > 4.0)
    ):
        iban_negative_prior = True

    iban_length_ok = (
        length_min is not None
        and length_max is not None
        and length_min >= 15.0
        and length_max <= 34.0
    )
    if hints.get("iban"):
        _boost("IBAN", 10.0, "column name references IBAN")

    if (
        not iban_negative_prior
        and iban_length_ok
        and uppercase_ratio >= 0.6
    ):
        if iban_ratio >= 0.9:
            if iban_checksum_ok:
                _boost(
                    "IBAN",
                    95.0 * min(iban_ratio, 1.0),
                    f"{iban_ratio:.0%} values look like IBANs; checksum ok",
                )
                iban_notes.append("checksum verified on sampled values")
            else:
                if iban_checksum_checked:
                    _boost(
                        "IBAN",
                        30.0 * min(iban_ratio, 1.0),
                        "IBAN regex matches but checksum failed",
                    )
                    iban_confidence_cap = min(iban_confidence_cap, 0.5)
                    iban_notes.append("checksum mismatch on sampled values")
                else:
                    _boost(
                        "IBAN",
                        35.0 * min(iban_ratio, 1.0),
                        "IBAN regex matches; checksum not verified",
                    )
                    iban_confidence_cap = min(iban_confidence_cap, 0.7)
                    iban_notes.append("checksum not verified")
        elif iban_ratio >= 0.8:
            _boost(
                "IBAN",
                25.0 * min(iban_ratio, 1.0),
                f"{iban_ratio:.0%} values resemble IBAN structure",
            )
            iban_confidence_cap = min(iban_confidence_cap, 0.7)

    if iban_notes:
        existing_notes = rationales.setdefault("IBAN", [])
        for note in iban_notes:
            if note not in existing_notes:
                existing_notes.append(note)

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

    if not forced_type:
        current_confidence_estimate = min(1.0, max(best_score, 0.0) / 100.0)
        top3_ratio_raw = top3_ratio_signal
        top3_ratio_eval = float(top3_ratio_raw) if top3_ratio_raw is not None else None
        distinct_count_numeric = (
            float(distinct_count_value) if isinstance(distinct_count_value, (int, float)) else None
        )
        distincts_high = distinct_ratio_value >= 0.7
        if (
            not distincts_high
            and distinct_count_numeric is not None
            and non_nulls > 0
        ):
            distincts_high = distinct_count_numeric >= (0.7 * float(non_nulls))
        length_condition = length_avg is None or length_avg >= 8.0
        top3_condition = (
            top3_ratio_eval is not None and float(top3_ratio_eval) <= 0.30
        )
        if (
            distincts_high
            and top3_condition
            and length_condition
            and current_confidence_estimate <= 0.85
        ):
            account_confidence = max(0.8, min(0.9, 0.75 + distinct_ratio_value * 0.25))
            account_score = account_confidence * 100.0
            if account_score > scores.get("ACCOUNT_ID", 0.0):
                scores["ACCOUNT_ID"] = account_score
            best_type = "ACCOUNT_ID"
            best_score = scores.get("ACCOUNT_ID", account_score)
            rationale_parts = [f"{distinct_ratio_value:.0%} distinct"]
            if top3_ratio_eval is not None:
                rationale_parts.append(f"top3 {top3_ratio_eval:.0%}")
            if length_avg is not None:
                rationale_parts.append(f"avg len {length_avg:.1f}")
            rationales.setdefault("ACCOUNT_ID", []).append(
                truncate_note("; ".join(part for part in rationale_parts if part))
            )

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
        if best_type == "IBAN":
            confidence = round(min(confidence, iban_confidence_cap), 3)
            if iban_confidence_cap < 1.0 and not iban_checksum_ok:
                cap_msg = "checksum mismatch on sampled values" if iban_checksum_checked else "checksum not verified"
                forced_rationale = truncate_note(
                    f"{forced_rationale}; {cap_msg}" if forced_rationale else cap_msg
                )
        if not forced_rationale:
            if forced_type == "DATE_IN_TEXT" and date_success_ratio >= 0.9:
                forced_rationale = f"{date_success_ratio:.0%} of values parse successfully"
            else:
                forced_rationale = "rule-based semantic classification"
        return best_type, confidence, forced_rationale, profile_min, profile_max

    confidence = min(1.0, max(best_score, 0.0) / 100.0)
    confidence = round(confidence, 3)

    if best_type == "IBAN":
        confidence = round(min(confidence, iban_confidence_cap), 3)
        if iban_confidence_cap < 1.0 and not iban_checksum_ok:
            msg = "checksum mismatch on sampled values" if iban_checksum_checked else "checksum not verified"
            existing_notes = rationales.setdefault("IBAN", [])
            if msg not in existing_notes:
                existing_notes.append(msg)

    if (
        best_type == "COUNTRY_CODE/NAME"
        and country_selected_overlap > 0
        and not country_shape_mismatch
    ):
        if country_selected_mode == "code":
            threshold = 0.80
        else:
            threshold = 0.70
        base_conf = 0.75
        target_conf = 0.9
        upper_overlap = 0.95
        if country_selected_overlap <= threshold:
            country_confidence = base_conf
        else:
            capped_overlap = min(country_selected_overlap, upper_overlap)
            if capped_overlap <= threshold:
                country_confidence = base_conf
            else:
                slope = (target_conf - base_conf) / (upper_overlap - threshold)
                country_confidence = base_conf + slope * (capped_overlap - threshold)
        if country_selected_overlap >= upper_overlap:
            country_confidence = target_conf
        confidence = round(min(0.95, max(base_conf, country_confidence)), 3)

    explanations = rationales.get(best_type, [])
    if not explanations:
        if null_pct >= 0.5:
            explanations = ["limited matches because column is mostly null"]
        else:
            explanations = ["limited heuristic support but selected best available type"]

    rationale = "; ".join(explanations[:3])

    return best_type, confidence, rationale, profile_min, profile_max


def normalize_profile_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Return a normalized copy of a per-column profile row.

    Legacy payloads may lack the newer whitespace, numeric, or length metrics, so this
    helper fills in defaults and coerces values to sensible Python primitives. This
    keeps downstream consumers aligned regardless of whether the source profile was
    generated before or after the expanded metrics rollout.
    """

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

    count_defaults = {
        "empty_string_count": 0,
        "whitespace_only_count": 0,
        "whitespace_row_count": 0,
        "lead_trail_whitespace_count": 0,
        "numeric_zero_count": 0,
    }
    for key, default in count_defaults.items():
        value = payload.get(key)
        if value is None:
            payload[key] = default
            continue
        try:
            payload[key] = int(value)
        except Exception:
            payload[key] = default

    metric_defaults = {
        "avg_len": None,
        "len_min": None,
        "len_max": None,
        "len_spread": None,
        "len_stddev": None,
        "top_coverage_pct": 0.0,
    }
    for key, default in metric_defaults.items():
        if key not in payload:
            payload[key] = default

    float_fields = (
        "avg_len",
        "len_min",
        "len_max",
        "len_spread",
        "len_stddev",
        "top_coverage_pct",
    )
    for key in float_fields:
        value = payload.get(key)
        if value is None:
            continue
        try:
            payload[key] = float(value)
        except Exception:
            if key == "top_coverage_pct":
                payload[key] = 0.0
            else:
                payload[key] = None

    if "date_valid_count" not in payload:
        payload["date_valid_count"] = payload.get("non_nulls") or 0
    try:
        payload["date_valid_count"] = int(payload.get("date_valid_count") or 0)
    except Exception:
        payload["date_valid_count"] = 0

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


def _coerce_variant_map(value: Any) -> Dict[str, Any]:
    """Best-effort conversion of a Snowflake VARIANT payload into a dict."""

    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "asDict"):
        try:
            data = value.asDict()
        except Exception:  # pragma: no cover - defensive fallback
            data = None
        if isinstance(data, dict):
            return dict(data)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
        return {}
    return {}


def _coerce_variant_value(value: Any) -> Any:
    """Return a Python value from a Snowflake VARIANT payload when possible."""

    if value is None:
        return None
    if hasattr(value, "asDict"):
        try:
            return value.asDict()
        except Exception:  # pragma: no cover - defensive fallback
            return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def run_table_profile(
    session,
    fqn: str,
    sample_pct: Optional[float] = 10.0,
    top_n: int = 10,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Profile a table and return summary plus per-column metrics.

    The per-column payload includes counts for nulls, empty strings, whitespace-only
    values, and numeric zeros alongside min/max/average length statistics. Top value
    coverage, semantic inference signals, and parsed date ranges are also returned so
    that downstream consumers can render consistent UI without additional queries.
    """
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
        string_expr = f"TO_VARCHAR({qcol})"
        length_expr = f"LENGTH({string_expr})"

        metrics_sql = [
            "COUNT(*) AS ROW_CNT",
            f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS NULLS",
            f"{distinct_expr.format(col=qcol)} AS DISTINCTS",
            f"SUM(CASE WHEN {qcol} IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULLS_COUNT",
            (
                "SUM(CASE WHEN {col} IS NOT NULL AND {str_expr} = '' THEN 1 ELSE 0 END)"
                " AS EMPTY_STRINGS"
            ).format(col=qcol, str_expr=string_expr),
            (
                "SUM(CASE WHEN {col} IS NOT NULL AND REGEXP_LIKE({str_expr}, '^[[:space:]]+$') THEN 1 ELSE 0 END)"
                " AS WHITESPACE_ONLY_ROWS"
            ).format(col=qcol, str_expr=string_expr),
        ]
        num_date_matches_alias: Optional[str] = "NUMDATE_PARSE_COUNT"
        num_date_min_alias: Optional[str] = "NUMDATE_MIN"
        num_date_max_alias: Optional[str] = "NUMDATE_MAX"
        num_date_expr_sql: Optional[str] = None
        guarded_numeric_expr: Optional[str] = None
        is_numeric = _is_numeric(dtype)
        if is_numeric:
            metrics_sql.extend(
                [
                    f"MIN({qcol}) AS MIN_VAL",
                    f"MAX({qcol}) AS MAX_VAL",
                ]
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} = 0 THEN 1 ELSE 0 END) AS NUMERIC_ZERO_ROWS"
            )
            numeric_string_expr = f"{qcol}::STRING"
            padded_expr = (
                "CASE WHEN LENGTH({base}) = 8 THEN {base} "
                "WHEN LENGTH({base}) < 8 THEN LPAD({base}, 8, '0') ELSE NULL END"
            ).format(base=numeric_string_expr)
            num_date_expr_sql = (
                "CASE WHEN {padded} IS NOT NULL AND {padded} <> '00000000' "
                "THEN TRY_TO_DATE({padded}, 'YYYYMMDD') ELSE NULL END"
            ).format(padded=padded_expr)
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
            metrics_sql.append("0 AS NUMERIC_ZERO_ROWS")
        is_string = _is_string_type(dtype)
        if is_string:
            trimmed_expr = f"TRIM({qcol}::STRING)"
            sentinel_values = ("'0'", "'00000000'", "'0000-00-00'", "'0000/00/00'")
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '^\\s|\\s$') THEN 1 ELSE 0 END) AS LEAD_TRAIL_WS_ROWS"
            )
            digits_only_expr = f"TRIM({qcol}::STRING)"
            guarded_numeric_expr = (
                "CASE WHEN REGEXP_LIKE({expr}, '^[0-9]{{8}}$') AND {expr} <> '00000000' "
                "THEN {expr} ELSE NULL END"
            ).format(expr=digits_only_expr)
            num_date_expr_sql = (
                "CASE WHEN REGEXP_LIKE({expr}, '^[0-9]{{8}}$') AND {expr} <> '00000000' "
                "THEN TRY_TO_DATE({expr}, 'YYYYMMDD') ELSE NULL END"
            ).format(expr=digits_only_expr)
        else:
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
            )
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '^\\s|\\s$') THEN 1 ELSE 0 END) AS LEAD_TRAIL_WS_ROWS"
            )

        if num_date_expr_sql and num_date_matches_alias and num_date_min_alias and num_date_max_alias:
            metrics_sql.extend(
                [
                    (
                        "SUM(CASE WHEN {expr} IS NOT NULL THEN 1 ELSE 0 END) AS {alias}"
                    ).format(expr=num_date_expr_sql, alias=num_date_matches_alias),
                    f"MIN({num_date_expr_sql}) AS {num_date_min_alias}",
                    f"MAX({num_date_expr_sql}) AS {num_date_max_alias}",
                ]
            )
        else:
            metrics_sql.append(
                f"0 AS {num_date_matches_alias or 'NUMDATE_PARSE_COUNT'}"
            )
            metrics_sql.append(
                f"NULL AS {num_date_min_alias or 'NUMDATE_MIN'}"
            )
            metrics_sql.append(
                f"NULL AS {num_date_max_alias or 'NUMDATE_MAX'}"
            )
        if is_string:
            length_expr: Optional[str] = f"LENGTH({qcol}::STRING)"
        else:
            length_expr = f"LENGTH(TO_VARCHAR({qcol}))"
        metrics_sql.append(
            f"AVG(CASE WHEN {qcol} IS NOT NULL THEN {length_expr} END) AS AVG_LEN"
        )

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
                    f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
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
                    "SUM(CASE WHEN {col} IS NOT NULL AND UPPER({string_expr}) IN ({clause}) "
                    "THEN 1 ELSE 0 END) AS {alias}".format(col=qcol, string_expr=string_expr, clause=clause, alias=alias)
                )
                signal_key = REFERENCE_SIGNAL_NAMES.get(ref_key, ref_key)
                ref_match_aliases[signal_key] = alias

            numeric_like_alias = "STRING_NUMERIC_LIKE_ROWS"
            numeric_like_pattern = r"^\d+(\.\d+)?$".replace("\\", "\\\\")
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({string_expr}, '{numeric_like_pattern}') THEN 1 ELSE 0 END) AS {numeric_like_alias}"
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
                        f"SUM(CASE WHEN {qcol} = 0 THEN 1 ELSE 0 END) AS NUMERIC_ZERO_ROWS",
                    ]
                )
            elif _is_temporal(dtype):
                minimal_metrics.extend(
                    [
                        f"MIN({qcol}) AS MIN_VAL",
                        f"MAX({qcol}) AS MAX_VAL",
                        "0 AS NUMERIC_ZERO_ROWS",
                    ]
                )
            else:
                minimal_metrics.extend(
                    [
                        f"MIN(TO_VARCHAR({qcol})) AS MIN_VAL",
                        f"MAX(TO_VARCHAR({qcol})) AS MAX_VAL",
                        "0 AS NUMERIC_ZERO_ROWS",
                    ]
                )
            minimal_metrics.extend(
                [
                    (
                        "SUM(CASE WHEN {col} IS NOT NULL AND {str_expr} = '' THEN 1 ELSE 0 END)"
                        " AS EMPTY_STRINGS"
                    ).format(col=qcol, str_expr=string_expr),
                    (
                        "SUM(CASE WHEN {col} IS NOT NULL AND REGEXP_LIKE({str_expr}, '^[[:space:]]+$') THEN 1 ELSE 0 END)"
                        " AS WHITESPACE_ONLY_ROWS"
                    ).format(col=qcol, str_expr=string_expr),
                    (
                        "SUM(CASE WHEN {col} IS NOT NULL AND REGEXP_LIKE({str_expr}, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END)"
                        " AS WHITESPACE_ROWS"
                    ).format(col=qcol, str_expr=string_expr),
                    (
                        "SUM(CASE WHEN {col} IS NOT NULL AND REGEXP_LIKE({str_expr}, '^\\s|\\s$') THEN 1 ELSE 0 END)"
                        " AS LEAD_TRAIL_WS_ROWS"
                    ).format(col=qcol, str_expr=string_expr),
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
            minimal_metrics.extend(
                [
                    f"AVG(CASE WHEN {qcol} IS NOT NULL THEN {length_expr} END) AS AVG_LEN",
                    f"MIN({length_expr}) AS LEN_MIN",
                    f"MAX({length_expr}) AS LEN_MAX",
                    f"STDDEV_SAMP({length_expr}) AS LEN_STDDEV",
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

        empty_string_raw = _extract_row_value(row, "EMPTY_STRINGS", 0)
        if empty_string_raw in (None, 0):
            empty_string_raw = _extract_row_value(row, "EMPTY_STR_ROWS", empty_string_raw)
        whitespace_rows = _extract_row_value(row, "WHITESPACE_ROWS", 0)
        whitespace_only_raw = _extract_row_value(row, "WHITESPACE_ONLY_ROWS", 0)
        if whitespace_only_raw in (None, 0):
            whitespace_only_raw = _extract_row_value(row, "WS_ONLY_ROWS", whitespace_only_raw)
        numeric_zero_raw = _extract_row_value(row, "NUMERIC_ZERO_ROWS", 0)
        try:
            empty_str_rows_int = int(empty_string_raw or 0)
        except Exception:
            try:
                empty_str_rows_int = int(float(empty_string_raw)) if empty_string_raw is not None else 0
            except Exception:
                empty_str_rows_int = 0

        lead_trail_ws_rows = _extract_row_value(row, "LEAD_TRAIL_WS_ROWS", 0)
        ws_only_rows = whitespace_only_raw
        try:
            numeric_zero_rows_int = int(numeric_zero_raw)
        except Exception:
            try:
                numeric_zero_rows_int = int(float(numeric_zero_raw))
            except Exception:
                numeric_zero_rows_int = 0
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
        num_date_ratio: float = 0.0
        num_date_min_value = str(num_date_min_raw) if num_date_min_raw is not None else None
        num_date_max_value = str(num_date_max_raw) if num_date_max_raw is not None else None
        whitespace_pct = whitespace_ratio * 100.0

        avg_len_debug_note: Optional[str] = None
        if avg_len_value is None:
            avg_len_final: Optional[float] = None
            if is_string and non_nulls > 0:
                logger.warning("avg_len metric missing for column %s", name)
                if PROFILE_DEBUG:
                    avg_len_debug_note = "avg_len missing; replaced FILTER with CASE"
        else:
            avg_len_final = float(avg_len_value)
            if (
                is_string
                and PROFILE_DEBUG
                and non_nulls > 0
                and math.isclose(avg_len_final, 0.0, rel_tol=0.0, abs_tol=1e-9)
            ):
                logger.warning("avg_len value zero for column %s", name)
                avg_len_debug_note = "avg_len missing; replaced FILTER with CASE"

        len_min_val: Optional[float] = None
        len_max_val: Optional[float] = None
        len_stddev_val: Optional[float] = None
        supports_length_stats = True
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
        valid_numeric_count = max((non_nulls or 0) - numeric_zero_rows_int, 0)
        if is_string:
            denominator = float(valid_string_count) if valid_string_count else 0.0
        elif is_numeric:
            denominator = float(valid_numeric_count) if valid_numeric_count else 0.0
        else:
            denominator = float(non_nulls)
        if denominator:
            num_date_ratio = float(num_date_cnt) / denominator
        else:
            num_date_ratio = 0.0

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
        any_parse_count: Optional[int] = None
        any_parsed_min: Optional[Any] = None
        any_parsed_max: Optional[Any] = None
        if any_parse_alias:
            any_matches_raw = _extract_row_value(row, any_parse_alias, 0)
            try:
                any_matches = int(any_matches_raw)
            except Exception:
                any_matches = 0
            any_parse_count = any_matches
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
        best_parse_count = date_parse_counts.get(best_date_key, 0) if best_date_key else 0
        textual_best_ratio = max(best_date_ratio, float(any_parse_ratio or 0.0))
        numeric_best = False
        if num_date_cnt > 0 and num_date_ratio >= textual_best_ratio:
            numeric_best = True
            best_date_key = "yyyymmdd"
            best_date_ratio = num_date_ratio
            parsed_date_min = num_date_min_value
            parsed_date_max = num_date_max_value
            best_parse_count = num_date_cnt
        else:
            if any_parsed_min is not None or any_parsed_max is not None:
                parsed_date_min = str(any_parsed_min) if any_parsed_min is not None else None
                parsed_date_max = str(any_parsed_max) if any_parsed_max is not None else None
                if any_parse_count is not None:
                    best_parse_count = any_parse_count
            elif best_date_key:
                best_min = parsed_min_values.get(best_date_key)
                best_max = parsed_max_values.get(best_date_key)
                parsed_date_min = str(best_min) if best_min is not None else None
                parsed_date_max = str(best_max) if best_max is not None else None
                best_parse_count = date_parse_counts.get(best_date_key, 0)

        hints = _derive_name_hints(name)

        top_values: List[Dict[str, Any]] = []
        top_coverage = 0
        denom_non_nulls = max(1, int(non_nulls or 0))
        if top_n_clamped > 0 and rows_profiled:
            if non_nulls <= 0:
                top_values = [
                    {"value": None, "count": int(rows_profiled), "pct": 100.0 if rows_profiled else 0.0}
                ]
            else:
                top_sql = (
                    f"SELECT {qcol} AS VALUE, COUNT(*) AS CNT "
                    f"FROM {sampled_ref} "
                    f"WHERE {qcol} IS NOT NULL "
                    f"GROUP BY 1 ORDER BY CNT DESC FETCH NEXT {top_n_clamped} ROWS ONLY"
                )
                try:
                    seen_value_keys: Set[Tuple[str, str]] = set()
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
                            try:
                                count_int = int(float(count_raw))
                            except Exception:
                                count_int = 0
                        if value is None:
                            continue
                        key = (type(value).__name__, repr(value))
                        if key in seen_value_keys:
                            continue
                        seen_value_keys.add(key)
                        top_coverage += count_int
                        pct = (float(count_int) / float(denom_non_nulls) * 100.0)
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

        if is_string and whitespace_length_counts and non_nulls:
            for length_int, count_int in whitespace_length_counts:
                pct = (float(count_int) / float(denom_non_nulls) * 100.0)
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
            "avg_len": avg_len_final,
            "whitespace_pct": whitespace_pct,
            "whitespace_only_pct": only_ws_ratio * 100.0,
            "empty_string_count": empty_str_rows_int,
            "whitespace_only_count": ws_only_rows_int,
            "whitespace_row_count": whitespace_rows_int,
            "lead_trail_whitespace_count": lead_trail_ws_rows_int,
            "numeric_zero_count": numeric_zero_rows_int,
            "top_values": top_values,
            "top_coverage_pct": coverage_pct,
            "rows_profiled": row_cnt,
            "row_cnt": row_cnt,
            "non_nulls": non_nulls,
            "null_cnt": nulls_int,
            "error": error_message,
        }
        if not is_string:
            column_entry["date_valid_count"] = (
                valid_numeric_count if is_numeric else non_nulls
            )
        if avg_len_debug_note:
            existing_note = column_entry.get("note")
            if existing_note:
                column_entry["note"] = f"{existing_note}; {avg_len_debug_note}"
            else:
                column_entry["note"] = avg_len_debug_note

        column_entry["len_min"] = len_min_val
        column_entry["len_max"] = len_max_val
        column_entry["len_stddev"] = len_stddev_val
        if len_min_val is not None and len_max_val is not None:
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
        elif parsed_date_min is not None or parsed_date_max is not None:
            column_entry["parsed_date_min"] = parsed_date_min
            column_entry["parsed_date_max"] = parsed_date_max

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
            "numdate_ratio": num_date_ratio,
            "count": num_date_cnt,
            "parsed_min": num_date_min_value,
            "parsed_max": num_date_max_value,
            "numdate_min": num_date_min_value,
            "numdate_max": num_date_max_value,
            "valid_count": valid_numeric_count,
            "numeric_zero_count": numeric_zero_rows_int,
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
                "empty_string_count": empty_str_rows_int,
                "whitespace_only_count": ws_only_rows_int,
            }
            signals["date_patterns"] = {str(key): date_pattern_ratios.get(key) for key in date_pattern_ratios}
        date_parse_formats: Dict[str, Optional[float]] = {
            str(key): date_parse_ratios.get(key) for key in date_parse_ratios
        }
        date_parse_formats["yyyymmdd_numeric"] = num_date_ratio if num_date_cnt > 0 else 0.0
        success_ratio_value: Optional[float]
        if any_parse_ratio is not None:
            success_ratio_value = any_parse_ratio
        elif num_date_cnt > 0:
            success_ratio_value = num_date_ratio
        else:
            success_ratio_value = None
        valid_base_count = (
            valid_string_count
            if is_string
            else valid_numeric_count if is_numeric else non_nulls
        )
        date_parse_payload: Dict[str, Any] = {
            "formats": date_parse_formats,
            "best_ratio": best_date_ratio if best_date_key else None,
            "best_format": best_date_key,
            "success_ratio": success_ratio_value,
            "date_success_ratio": best_date_ratio if best_date_key else success_ratio_value,
            "format_detected": best_date_key,
            "valid_count": valid_base_count,
            "parsed_min": parsed_date_min,
            "parsed_max": parsed_date_max,
            "success_count": any_parse_count if any_parse_count is not None else (num_date_cnt if num_date_cnt > 0 else None),
            "best_count": best_parse_count,
            "numdate_ratio": num_date_ratio,
            "numdate_min": num_date_min_value,
            "numdate_max": num_date_max_value,
            "numdate_count": num_date_cnt,
            "best_source": "numeric" if numeric_best else "text",
            "numeric_zero_count": numeric_zero_rows_int,
        }
        signals["date_parse"] = date_parse_payload
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


def list_saved_profiles(session, meta_db: str, meta_schema: str) -> List[Dict[str, Any]]:
    """Return recent saved profile runs from the metadata tables."""

    if not session or not (meta_db and meta_schema):
        return []

    runs_tbl = f"{_q(meta_db)}.{_q(meta_schema)}.DQ_PROFILE_RUN"
    sql = (
        f"SELECT RUN_ID, RUN_AT, SUMMARY FROM {runs_tbl} "
        "ORDER BY RUN_AT DESC LIMIT 25"
    )

    try:
        rows = session.sql(sql).collect()
    except Exception:  # pragma: no cover - Snowflake specific
        logger.debug("Unable to list saved profile runs", exc_info=True)
        return []

    results: List[Dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "asDict"):
            data = row.asDict()
        else:
            data = {}
            try:
                data["RUN_ID"] = row[0]
                data["RUN_AT"] = row[1] if len(row) > 1 else None
                data["SUMMARY"] = row[2] if len(row) > 2 else None
            except Exception:
                data = {}

        run_id_value = data.get("RUN_ID") or data.get("run_id")
        if not run_id_value:
            continue

        summary_payload = _coerce_variant_map(data.get("SUMMARY") or data.get("summary"))
        results.append(
            {
                "run_id": str(run_id_value),
                "run_at": data.get("RUN_AT") or data.get("run_at"),
                "summary": summary_payload,
            }
        )

    return results


def load_profile_run(
    session,
    meta_db: str,
    meta_schema: str,
    run_id: str,
) -> Dict[str, Any]:
    """Reconstruct a saved profile run from metadata tables."""

    if not session or not (meta_db and meta_schema) or not run_id:
        return {}

    runs_tbl = f"{_q(meta_db)}.{_q(meta_schema)}.DQ_PROFILE_RUN"
    cols_tbl = f"{_q(meta_db)}.{_q(meta_schema)}.DQ_PROFILE_COLUMN"

    try:
        run_rows = session.sql(
            f"SELECT RUN_ID, SUMMARY FROM {runs_tbl} WHERE RUN_ID = ?",
            params=[run_id],
        ).collect()
    except Exception:  # pragma: no cover - Snowflake specific
        logger.debug("Unable to load saved profile run %s", run_id, exc_info=True)
        return {}

    if not run_rows:
        return {}

    summary_payload = _coerce_variant_map(
        _extract_row_value(run_rows[0], "SUMMARY")
    )

    target_table = summary_payload.get("target_table") or summary_payload.get("table")

    def _coerce_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def _coerce_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except Exception:
            return None

    top_n: Optional[int]
    top_n = _coerce_int(summary_payload.get("top_n"))

    columns: List[Dict[str, Any]] = []
    try:
        column_rows = session.sql(
            f"""
            SELECT COLUMN_NAME, PROFILE, SEMANTIC_TYPE, CONFIDENCE, RATIONALE, SIGNALS, SUGGESTED_CHECKS
            FROM {cols_tbl}
            WHERE RUN_ID = ?
            ORDER BY COLUMN_NAME
            """,
            params=[run_id],
        ).collect()
    except Exception:  # pragma: no cover - Snowflake specific
        logger.debug("Unable to load saved profile columns for %s", run_id, exc_info=True)
        column_rows = []

    for row in column_rows:
        if hasattr(row, "asDict"):
            data = row.asDict()
        else:
            try:
                data = {
                    "COLUMN_NAME": row[0],
                    "PROFILE": row[1] if len(row) > 1 else None,
                    "SEMANTIC_TYPE": row[2] if len(row) > 2 else None,
                    "CONFIDENCE": row[3] if len(row) > 3 else None,
                    "RATIONALE": row[4] if len(row) > 4 else None,
                    "SIGNALS": row[5] if len(row) > 5 else None,
                    "SUGGESTED_CHECKS": row[6] if len(row) > 6 else None,
                }
            except Exception:
                data = {}

        profile_payload = _coerce_variant_map(data.get("PROFILE") or data.get("profile"))
        if not profile_payload:
            profile_payload = {}

        merged = dict(profile_payload)

        for key in ("semantic_type", "confidence", "rationale"):
            upper = key.upper()
            value = data.get(upper) if upper in data else data.get(key)
            if value is not None:
                merged[key] = value

        signals_value = data.get("SIGNALS") or data.get("signals")
        if signals_value is not None:
            merged["signals"] = _coerce_variant_value(signals_value)

        suggested_value = data.get("SUGGESTED_CHECKS") or data.get("suggested_checks")
        if suggested_value is not None:
            merged["suggested_checks"] = _coerce_variant_value(suggested_value)

        merged.setdefault("column_name", data.get("COLUMN_NAME") or data.get("column_name") or "")

        normalized = normalize_profile_row(merged)
        columns.append(normalized)

    rows_profiled_value = _coerce_int(summary_payload.get("rows_profiled"))
    if rows_profiled_value is None:
        rows_profiled_value = 0

    sample_pct_raw = summary_payload.get("sample_pct")
    if sample_pct_raw is None:
        sample_pct_value: Optional[float] = None
    else:
        sample_pct_value = _coerce_float(sample_pct_raw)
        if sample_pct_value is None:
            try:
                sample_pct_value = float(sample_pct_raw)
            except Exception:
                sample_pct_value = None

    duration_value = _coerce_float(summary_payload.get("duration_sec"))
    if duration_value is None:
        duration_value = 0.0

    columns_count = len(columns) if columns else _coerce_int(summary_payload.get("columns"))
    if columns_count is None:
        columns_count = len(columns)

    summary_out = {
        "rows_profiled": rows_profiled_value,
        "sample_pct": sample_pct_value,
        "duration_sec": duration_value,
        "columns": columns_count,
    }

    return {
        "target_table": target_table,
        "summary": summary_out,
        "columns": columns,
        "top_n": top_n,
    }
