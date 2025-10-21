"""Service helpers for table profiling and automated DQ suggestions."""

from __future__ import annotations

import json
import math
import random
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from uuid import uuid4

from services.profile import _is_numeric, _is_temporal, _stringify
from utils.meta import _q

__all__ = [
    "list_columns",
    "run_table_profile",
    "suggest_checks_from_profile",
    "save_profile_results",
    "normalize_profile_row",
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
        "timestamp": {"timestamp", "datetime", "created", "updated"},
        "boolean": {"flag"},
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
)


BOOLEAN_TRUE_VALUES = {"1", "Y", "YES", "TRUE", "T"}
BOOLEAN_FALSE_VALUES = {"0", "N", "NO", "FALSE", "F"}


def _is_boolean_type(data_type: str) -> bool:
    upper = (data_type or "").upper()
    return "BOOL" in upper or "BOOLEAN" in upper


def _infer_semantic_type(column_entry: Dict[str, Any]) -> Tuple[str, float, str]:
    signals = column_entry.get("signals", {}) or {}
    regex = signals.get("regex", {}) or {}
    char_classes = signals.get("character_classes", {}) or {}
    references = signals.get("reference_matches", {}) or {}
    hints = signals.get("hints", {}) or {}
    length = signals.get("length", {}) or {}

    def _as_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    length_min = _as_float(length.get("min"))
    length_max = _as_float(length.get("max"))
    length_avg = _as_float(length.get("avg"))

    distinct_pct_raw = column_entry.get("distinct_pct")
    distinct_pct = float(distinct_pct_raw) if distinct_pct_raw is not None else None
    null_pct = float(column_entry.get("null_pct") or 0.0)
    data_type = column_entry.get("data_type") or ""
    top_values = column_entry.get("top_values") or []
    distincts = column_entry.get("distincts")
    non_nulls = int(column_entry.get("non_nulls") or 0)
    rows_profiled = int(column_entry.get("rows_profiled") or 0)

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
    if distinct_pct is not None and distinct_pct >= 60.0:
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
        if distinct_pct is not None and distinct_pct <= 40.0:
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

    if distinct_pct is not None and distinct_pct >= 70.0:
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

    confidence = min(1.0, max(best_score, 0.0) / 100.0)
    confidence = round(confidence, 3)

    explanations = rationales.get(best_type, [])
    if not explanations:
        if null_pct >= 50.0:
            explanations = ["limited matches because column is mostly null"]
        else:
            explanations = ["limited heuristic support but selected best available type"]

    rationale = "; ".join(explanations[:3])

    return best_type, confidence, rationale


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
            f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS NULLS",
            f"{distinct_expr.format(col=qcol)} AS DISTINCTS",
            f"SUM(CASE WHEN {qcol} IS NOT NULL THEN 1 ELSE 0 END) AS NON_NULLS_COUNT",
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
        is_string = _is_string_type(dtype)
        if is_string:
            metrics_sql.append(
                f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '^\\s|\\s$|\\s{{2,}}') THEN 1 ELSE 0 END) AS WHITESPACE_ROWS"
            )
        else:
            metrics_sql.append("0 AS WHITESPACE_ROWS")
        metrics_sql.append(f"AVG(LENGTH({qcol}::STRING)) AS AVG_LEN")

        char_pattern_aliases: Dict[str, str] = {}
        regex_aliases: Dict[str, str] = {}
        if is_string:
            metrics_sql.extend(
                [
                    f"MIN(LENGTH({qcol}::STRING)) AS LEN_MIN",
                    f"MAX(LENGTH({qcol}::STRING)) AS LEN_MAX",
                ]
            )
            for key, pattern in SEMANTIC_REGEX_PATTERNS.items():
                alias = f"REGEX_{key.upper()}_MATCHES"
                pattern_sql = pattern.replace("\\", "\\\\")
                metrics_sql.append(
                    f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
                )
                regex_aliases[key] = alias
            for key, pattern in CHAR_CLASS_PATTERNS.items():
                alias = f"CHAR_{key.upper()}_MATCHES"
                pattern_sql = pattern.replace("\\", "\\\\")
                metrics_sql.append(
                    f"SUM(CASE WHEN {qcol} IS NOT NULL AND REGEXP_LIKE({qcol}::STRING, '{pattern_sql}') THEN 1 ELSE 0 END) AS {alias}"
                )
                char_pattern_aliases[key] = alias

            ref_match_aliases: Dict[str, str] = {}
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
        else:
            char_pattern_aliases = {}
            regex_aliases = {}
            ref_match_aliases = {}

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

        non_nulls_raw = _extract_row_value(row, "NON_NULLS_COUNT", None)
        try:
            non_nulls_count = int(non_nulls_raw) if non_nulls_raw is not None else None
        except Exception:
            non_nulls_count = None

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
        non_nulls = (
            int(non_nulls_count)
            if isinstance(non_nulls_count, int)
            else max(rows_profiled - nulls_int, 0)
        )
        if distincts_int is not None and non_nulls:
            distinct_pct = float(distincts_int) / float(non_nulls) * 100.0
        else:
            distinct_pct = None
        whitespace_pct = (float(whitespace_rows_int) / rows_profiled * 100.0) if rows_profiled else 0.0

        len_min_val: Optional[float] = None
        len_max_val: Optional[float] = None
        if is_string:
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
        coverage_pct = (float(top_coverage) / non_nulls * 100.0) if non_nulls else 0.0

        column_entry: Dict[str, Any] = {
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

        signals: Dict[str, Any] = {
            "null_pct": null_pct,
            "distinct_pct": distinct_pct,
            "regex": regex_ratios,
            "character_classes": char_ratios,
            "reference_matches": reference_ratios,
            "hints": hints,
        }
        if is_string:
            signals["length"] = {
                "min": len_min_val,
                "max": len_max_val,
                "avg": avg_len,
            }
            if "whitespace" in char_ratios:
                signals["whitespace_ratio"] = char_ratios.get("whitespace")
        else:
            signals["length"] = {"min": None, "max": None, "avg": None}

        for key, ratio in regex_ratios.items():
            column_entry[f"signal_{key}_ratio"] = ratio
        for key, ratio in char_ratios.items():
            suffix = "whitespace_ratio" if key == "whitespace" else f"{key}_ratio"
            column_entry[f"signal_{suffix}"] = ratio
        for key, ratio in reference_ratios.items():
            column_entry[f"signal_{key}_ratio"] = ratio
        column_entry["signal_len_min"] = len_min_val if is_string else None
        column_entry["signal_len_max"] = len_max_val if is_string else None
        column_entry["signal_len_avg"] = avg_len if (is_string and avg_len is not None) else None
        for key, value in hints.items():
            column_entry[f"signal_hint_{key}"] = bool(value)

        column_entry["signals"] = signals

        semantic_type, confidence, rationale = _infer_semantic_type(column_entry)
        column_entry["semantic_type"] = semantic_type
        column_entry["confidence"] = confidence
        column_entry["rationale"] = rationale

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
            if distinct_pct >= 99.9 and null_pct <= 1.0 and distincts >= non_nulls:
                checks["UNIQUE"] = {"severity": "ERROR", "params": {"ignore_nulls": True}}

        if null_pct > 0:
            severity = "ERROR" if null_pct >= 1.0 else "WARN"
            max_nulls = int(math.ceil((baseline_rows or 0) * (null_pct / 100.0)))
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
                inferred_type, inferred_conf, inferred_rationale = _infer_semantic_type(normalized_row)
            except Exception:
                inferred_type = inferred_conf = inferred_rationale = None
            else:
                if semantic_type is None:
                    semantic_type = inferred_type
                if confidence_raw is None:
                    confidence_raw = inferred_conf
                if (isinstance(rationale, str) and not rationale.strip()) or rationale is None:
                    rationale = inferred_rationale

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
