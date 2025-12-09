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
    rule_code: str
    rule_id: str
    scope: Optional[str]
    category: Optional[str]
    severity: Optional[str]
    engine_type: Optional[str]
    expression: Optional[str]
    param_schema: Any
    default_params: Any
    enabled: bool
    version: Optional[str]
    check_type: Optional[str]
    description: Optional[str]


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
            "RULE_CODE": row[1],
            "RULE_ID": row[2],
            "SCOPE": row[3],
            "CATEGORY": row[4],
            "SEVERITY": row[5],
            "ENGINE_TYPE": row[6],
            "EXPRESSION": row[7],
            "PARAM_SCHEMA": row[8],
            "DEFAULT_PARAMS": row[9],
            "ENABLED": row[10],
            "VERSION": row[11],
            "CHECK_TYPE": row[12],
            "DESCRIPTION": row[13],
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
    where_clause = "" if include_inactive else "WHERE ENABLED"
    sql = f"""
        SELECT
            RULE_UID,
            RULE_CODE,
            RULE_ID,
            SCOPE,
            CATEGORY,
            SEVERITY,
            ENGINE_TYPE,
            EXPRESSION,
            PARAM_SCHEMA,
            DEFAULT_PARAMS,
            ENABLED,
            VERSION,
            CHECK_TYPE,
            DESCRIPTION
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
                rule_code=str(data.get("RULE_CODE", "")),
                rule_id=str(data.get("RULE_ID", "")),
                scope=(data.get("SCOPE") or None),
                category=(data.get("CATEGORY") or None),
                severity=(data.get("SEVERITY") or None),
                engine_type=(data.get("ENGINE_TYPE") or None),
                expression=(data.get("EXPRESSION") or None),
                param_schema=_parse_param_schema(data.get("PARAM_SCHEMA")),
                default_params=_parse_param_schema(data.get("DEFAULT_PARAMS")),
                enabled=bool(data.get("ENABLED", True)),
                version=(data.get("VERSION") or None),
                check_type=(data.get("CHECK_TYPE") or None),
                description=(data.get("DESCRIPTION") or None),
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
    return {r.rule_id.upper(): r for r in rules if r.enabled and r.rule_id}


def load_active_rules_from_library(
    session: Session,
    metadata_db: Optional[str] = None,
    metadata_schema: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Load active rule entries from ``DQ_RULE_LIBRARY`` for UI dropdowns."""

    templates = load_rule_library(
        session,
        metadata_db,
        metadata_schema,
        include_inactive=False,
    )
    rules: List[Dict[str, Any]] = []
    for template in templates:
        rules.append(
            {
                "rule_key": template.rule_id.upper(),
                "label": template.rule_id,
                "check_type": template.check_type,
                "default_severity": template.severity,
                "description": template.description,
            }
        )
    return rules
