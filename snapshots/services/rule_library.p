"""Helpers for loading rule templates from DQ_RULE_LIBRARY."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional

try:  # pragma: no cover - Snowpark availability is environment-specific
    from snowflake.snowpark import Session
except Exception:  # pragma: no cover - fallback for type checking
    Session = Any  # type: ignore

from utils.meta import _q


@dataclass(frozen=True)
class RuleTemplate:
    rule_uid: str
    rule_id: str
    check_type: Optional[str]
    expression_template: Optional[str]
    param_schema: Any
    default_severity: Optional[str]
    description: Optional[str]
    active: bool


# Compatibility shim: map legacy check_type values stored in DQ_CHECK to
# their corresponding rule keys in DQ_RULE_LIBRARY. Keep this mapping small
# and focused on pre-library enums.
LEGACY_RULE_KEY_MAP: Dict[str, str] = {
    "ROW_COUNT": "ROW_COUNT",
    "ROW_COUNT_ANOMALY": "ROW_COUNT_ANOMALY",
    "FRESHNESS": "FRESHNESS",
    "UNIQUE": "UNIQUE",
    "NULL_COUNT": "NULL_COUNT",
    "MIN_MAX": "MIN_MAX",
    "WHITESPACE": "WHITESPACE",
    "FORMAT_DISTRIBUTION": "FORMAT_DISTRIBUTION",
    "VALUE_DISTRIBUTION": "VALUE_DISTRIBUTION",
}


def _parse_param_schema(raw_value: Any) -> Any:
    if raw_value is None:
        return []
    if isinstance(raw_value, (dict, list)):
        return raw_value
    text = str(raw_value).strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        return text


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict()
    try:
        return {
            "RULE_UID": row[0],
            "RULE_ID": row[1],
            "CHECK_TYPE": row[2],
            "EXPRESSION_TEMPLATE": row[3],
            "PARAM_SCHEMA": row[4],
            "DEFAULT_SEVERITY": row[5],
            "DESCRIPTION": row[6],
            "ACTIVE": row[7],
        }
    except Exception:
        return {}


def load_rule_library(
    session: Session,
    metadata_db: Optional[str] = None,
    metadata_schema: Optional[str] = None,
    *,
    include_inactive: bool = False,
) -> List[RuleTemplate]:
    if not session:
        return []
    table = _q(
        f"{metadata_db}.{metadata_schema}.DQ_RULE_LIBRARY"
        if metadata_db and metadata_schema
        else "DQ_RULE_LIBRARY"
    )
    where_clause = "" if include_inactive else "WHERE ACTIVE"
    sql = f"""
        SELECT
            RULE_UID,
            RULE_ID,
            CHECK_TYPE,
            EXPRESSION_TEMPLATE,
            PARAM_SCHEMA,
            DEFAULT_SEVERITY,
            DESCRIPTION,
            ACTIVE
        FROM {table}
        {where_clause}
    """
    try:
        rows = session.sql(sql).collect()
    except Exception as exc:  # pragma: no cover - surfacing Snowflake errors
        logging.error("Failed to load DQ_RULE_LIBRARY: %s", exc)
        return []

    templates: List[RuleTemplate] = []
    for row in rows:
        data = _row_to_dict(row)
        templates.append(
            RuleTemplate(
                rule_uid=str(data.get("RULE_UID", "")),
                rule_id=str(data.get("RULE_ID", "")),
                check_type=(data.get("CHECK_TYPE") or None),
                expression_template=(data.get("EXPRESSION_TEMPLATE") or None),
                param_schema=_parse_param_schema(data.get("PARAM_SCHEMA")),
                default_severity=(data.get("DEFAULT_SEVERITY") or None),
                description=(data.get("DESCRIPTION") or None),
                active=bool(data.get("ACTIVE", True)),
            )
        )
    return templates


def normalize_rule_key(
    raw_key: str,
    active_rules: Mapping[str, RuleTemplate],
    *,
    legacy_map: Mapping[str, str] = LEGACY_RULE_KEY_MAP,
) -> str:
    key = (raw_key or "").upper()
    if key in active_rules:
        return key
    compat_key = legacy_map.get(key, key)
    compat_key_upper = compat_key.upper()
    if compat_key_upper in active_rules:
        return compat_key_upper
    return key


def active_rule_map(rules: Iterable[RuleTemplate]) -> Dict[str, RuleTemplate]:
    return {r.rule_id.upper(): r for r in rules if r.active and r.rule_id}
