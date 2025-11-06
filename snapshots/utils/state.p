"""Session-state helpers for Streamlit views."""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple
from uuid import UUID

try:
    import numpy as _np
except Exception:  # pragma: no cover - numpy is optional at runtime
    _np = None

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - optional dependency for tests
    class _StreamlitStateStub:
        session_state: Dict[str, Any] = {}

    st = _StreamlitStateStub()  # type: ignore[assignment]

_INCLUDE_MAP = "profile_include_map"
SAVED_PROFILES_STATE = "profile_saved_profiles"


from snowflake.snowpark.context import get_active_session

from utils.config import PROFILES_TABLE_FQN


logger = logging.getLogger(__name__)


def _get_session():
    # Never shadow this name elsewhere
    return get_active_session()


class _ProfileListResult(dict):
    """Dictionary wrapper that behaves like a list for legacy callers."""

    def __iter__(self):  # type: ignore[override]
        return iter(self.get("items", []))

    def __len__(self) -> int:  # type: ignore[override]
        return len(self.get("items", []))

    def __bool__(self) -> bool:  # type: ignore[override]
        return bool(self.get("items", []))


def _json_default(value: Any) -> Any:
    """Serialize unsupported objects when dumping to JSON."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if _np is not None and isinstance(value, _np.generic):
        return value.item()
    if isinstance(value, UUID):
        return str(value)
    return str(value)


_PROFILE_TIMESTAMP_FIELDS = frozenset({"created_at", "createdTs", "created"})


def _normalize_timestamp_value(value: Any) -> str:
    """Return an ISO 8601 string for timestamp-like values."""

    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_profile_payload(value: Any) -> Any:
    """Recursively normalise timestamp fields within saved profile payloads."""

    if isinstance(value, MutableMapping):
        return {
            key: (
                _normalize_timestamp_value(inner)
                if key in _PROFILE_TIMESTAMP_FIELDS
                else _normalize_profile_payload(inner)
            )
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [_normalize_profile_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_profile_payload(item) for item in value)
    if isinstance(value, set):
        return {_normalize_profile_payload(item) for item in value}
    return value


def normalize_saved_profile(profile: Any) -> Dict[str, Any]:
    """Return a saved profile payload with timestamp fields serialised to strings."""

    if isinstance(profile, str):
        try:
            parsed = json.loads(profile)
        except Exception:
            return {}
        profile = parsed

    normalized = _normalize_profile_payload(profile)
    if isinstance(normalized, MutableMapping):
        return dict(normalized)
    return {}


def normalize_saved_profiles(profiles: Any) -> List[Dict[str, Any]]:
    """Return a normalised sequence of saved profile payloads."""

    if isinstance(profiles, str):
        try:
            parsed = json.loads(profiles)
        except Exception:
            return []
        profiles = parsed

    iterable: Iterable[Any]
    if isinstance(profiles, MutableMapping):
        container = profiles.get("runs")
        if isinstance(container, MutableMapping):
            iterable = container.values()
        elif isinstance(container, Sequence) and not isinstance(
            container, (str, bytes, bytearray)
        ):
            iterable = container
        elif container is not None:
            iterable = [container]
        else:
            container = profiles.get("items")
            if isinstance(container, MutableMapping):
                iterable = container.values()
            elif isinstance(container, Sequence) and not isinstance(
                container, (str, bytes, bytearray)
            ):
                iterable = container
            elif container is not None:
                iterable = [container]
            elif any(
                key in profiles
                for key in ("run_id", "summary", "created", "created_at", "createdTs")
            ):
                iterable = [profiles]
            else:
                iterable = profiles.values()
    elif isinstance(profiles, Sequence) and not isinstance(profiles, (str, bytes, bytearray)):
        iterable = profiles
    else:
        return []

    normalized_runs = []
    for item in iterable:
        normalized = normalize_saved_profile(item)
        if normalized:
            normalized_runs.append(normalized)
    return normalized_runs


def _clean_identifier(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"')


def _ensure_iso_timestamp(value: Any) -> str:
    """Return an ISO-8601-ish string from assorted timestamp inputs."""

    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).isoformat()
    text = str(value).strip()
    if not text:
        return ""

    candidate = text
    if "T" not in candidate and " " in candidate:
        candidate = candidate.replace(" ", "T", 1)
    if candidate.endswith("Z"):
        normalized = candidate[:-1] + "+00:00"
    else:
        normalized = candidate
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    iso_value = parsed.isoformat()
    if candidate.endswith("Z"):
        return iso_value.replace("+00:00", "Z")
    return iso_value


def _extract_schema_table(*candidates: Any) -> Tuple[str, str]:
    schema = ""
    table = ""
    for candidate in candidates:
        if not isinstance(candidate, MutableMapping):
            continue
        target = _clean_identifier(
            candidate.get("target_fqn")
            or candidate.get("target_table")
            or candidate.get("target")
        )
        if target:
            parts = [part.strip().strip('"') for part in target.split(".") if part.strip()]
            if len(parts) >= 2:
                return parts[-2], parts[-1]
        schema_value = _clean_identifier(
            candidate.get("schema")
            or candidate.get("schema_name")
            or candidate.get("schemaName")
            or candidate.get("SCH_NAME")
        )
        table_value = _clean_identifier(
            candidate.get("table_name")
            or candidate.get("table")
            or candidate.get("TABLE_NAME")
            or candidate.get("name")
        )
        if schema_value and table_value:
            return schema_value, table_value
    return schema, table


def _to_mapping(value: Any) -> MutableMapping[str, Any]:
    if isinstance(value, MutableMapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        if isinstance(parsed, MutableMapping):
            return parsed
    return {}


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return default


def _profile_summary_map(run: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    return _to_mapping(run.get("summary"))


def _extract_run_identity(
    run: MutableMapping[str, Any]
) -> Tuple[str, MutableMapping[str, Any]]:
    summary_map = _profile_summary_map(run)
    run_id = _first_text(
        run.get("run_id"),
        run.get("id"),
        summary_map.get("run_id"),
        summary_map.get("id"),
    ).strip()
    return run_id, summary_map


def _extract_table_identifiers(
    run: MutableMapping[str, Any], summary_map: MutableMapping[str, Any]
) -> Tuple[str, str, str]:
    schema, table = _extract_schema_table(run, summary_map)
    schema_clean = _clean_identifier(schema)
    table_clean = _clean_identifier(table)
    if not (schema_clean and table_clean):
        target = _clean_identifier(
            run.get("target_fqn")
            or summary_map.get("target_table")
            or summary_map.get("target")
        )
        if target:
            parts = [
                part.strip().strip('"')
                for part in target.split(".")
                if part.strip()
            ]
            if len(parts) >= 2:
                schema_clean = schema_clean or parts[-2]
                table_clean = table_clean or parts[-1]
    table_fqn = ""
    if schema_clean and table_clean:
        table_fqn = f"{schema_clean}.{table_clean}"
    return schema_clean, table_clean, table_fqn


def _profile_timestamp(
    run: MutableMapping[str, Any], summary_map: MutableMapping[str, Any]
) -> str:
    timestamp_value = _first_text(
        summary_map.get("saved_at"),
        run.get("saved_at"),
        summary_map.get("run_at"),
        run.get("run_at"),
        summary_map.get("run_at_str"),
        run.get("run_at_str"),
        summary_map.get("created_at"),
        run.get("created_at"),
        summary_map.get("created"),
        run.get("created"),
        summary_map.get("createdTs"),
        run.get("createdTs"),
    )
    return _ensure_iso_timestamp(timestamp_value)


def _profile_display_name(
    run: MutableMapping[str, Any], summary_map: MutableMapping[str, Any]
) -> str:
    return _first_text(
        summary_map.get("profile_name"),
        run.get("profile_name"),
        summary_map.get("name"),
        run.get("name"),
        summary_map.get("target_table"),
        run.get("target_fqn"),
        run.get("table_name"),
        run.get("table"),
        default="Unnamed",
    )


def _split_relation_parts(
    table_fqn: Optional[str],
) -> List[Tuple[str, bool]]:
    if not table_fqn:
        return []

    text = str(table_fqn).strip()
    if not text:
        return []

    parts: List[Tuple[str, bool]] = []
    current: List[str] = []
    current_quoted = False
    in_quotes = False
    i = 0

    while i < len(text):
        ch = text[i]
        if ch == '"':
            next_char = text[i + 1] if i + 1 < len(text) else ""
            if in_quotes and next_char == '"':
                current.append('"')
                i += 1
            else:
                in_quotes = not in_quotes
                if in_quotes:
                    current_quoted = True
        elif ch == '.' and not in_quotes:
            part = "".join(current).strip()
            if part or current_quoted:
                parts.append((part, current_quoted))
            current = []
            current_quoted = False
        else:
            current.append(ch)
        i += 1

    part = "".join(current).strip()
    if part or current_quoted:
        parts.append((part, current_quoted))

    return [(value, quoted) for value, quoted in parts if value]


def _canonicalize_identifier(value: str, quoted: bool) -> str:
    if not value:
        return ""
    return value if quoted else value.upper()


def _normalise_table_filter(table_fqn: Optional[str]) -> Tuple[str, str, str, str]:
    filter_db = ""
    filter_schema = ""
    filter_table = ""
    canonical = ""

    parts = _split_relation_parts(table_fqn)
    if parts:
        if len(parts) >= 3:
            db_part, schema_part, table_part = parts[-3:]
        elif len(parts) == 2:
            db_part = ("", False)
            schema_part, table_part = parts
        else:
            db_part = ("", False)
            schema_part = ("", False)
            table_part = parts[0]

        canonical_db = _canonicalize_identifier(db_part[0], db_part[1]) if db_part else ""
        canonical_schema = (
            _canonicalize_identifier(schema_part[0], schema_part[1]) if schema_part else ""
        )
        canonical_table = (
            _canonicalize_identifier(table_part[0], table_part[1]) if table_part else ""
        )

        canonical_parts = [
            part for part in (canonical_db, canonical_schema, canonical_table) if part
        ]
        canonical = ".".join(canonical_parts)

        filter_db = canonical_db.upper() if canonical_db else ""
        filter_schema = canonical_schema.upper() if canonical_schema else ""
        filter_table = canonical_table.upper() if canonical_table else ""

    return filter_db, filter_schema, filter_table, canonical


def _canon_fqn(fqn: str) -> str:
    """Return an uppercased canonical representation of a table FQN."""

    if not fqn:
        return ""

    text = str(fqn).strip()
    if not text:
        return ""

    parts = _split_relation_parts(text)
    if len(parts) >= 3:
        canonical_parts = []
        for value, _quoted in parts[-3:]:
            cleaned = _clean_identifier(value)
            if cleaned:
                canonical_parts.append(cleaned.upper())
        if canonical_parts:
            return ".".join(canonical_parts)

    if parts:
        cleaned_parts = []
        for value, _quoted in parts:
            cleaned = _clean_identifier(value)
            if cleaned:
                cleaned_parts.append(cleaned)
        if cleaned_parts:
            return ".".join(cleaned_parts).upper()

    cleaned_text = _clean_identifier(text)
    if cleaned_text:
        return cleaned_text.upper()
    return text.upper()


def _table_fqn_upper_variants(*values: Optional[str]) -> List[str]:
    """Return ordered uppercase variants for potential legacy table FQNs."""

    variants: List[str] = []
    seen = set()

    for value in values:
        if not value:
            continue

        text = str(value).strip()
        if not text:
            continue

        raw_upper = text.upper()
        if raw_upper and raw_upper not in seen:
            variants.append(raw_upper)
            seen.add(raw_upper)

        parts = _split_relation_parts(text)
        if not parts:
            continue

        cleaned_parts = []
        for part_value, _quoted in parts:
            cleaned = _clean_identifier(part_value)
            if cleaned:
                cleaned_parts.append(cleaned.upper())

        if cleaned_parts:
            cleaned_upper = ".".join(cleaned_parts)
            if cleaned_upper and cleaned_upper not in seen:
                variants.append(cleaned_upper)
                seen.add(cleaned_upper)

        quoted_parts = []
        for part_value, _quoted in parts:
            cleaned = _clean_identifier(part_value)
            if cleaned:
                quoted_parts.append(f'"{cleaned.upper()}"')

        if quoted_parts:
            quoted_upper = ".".join(quoted_parts)
            if quoted_upper and quoted_upper not in seen:
                variants.append(quoted_upper)
                seen.add(quoted_upper)

    return variants


_PROFILES_SESSION_KEYS = (
    "profiles_store_session",
    "profile_session",
    "snowpark_session",
    "snowflake_session",
    "session",
    "connection",
)


def _quote_identifier(identifier: str) -> str:
    cleaned = _clean_identifier(identifier)
    if not cleaned:
        return ""
    escaped = cleaned.replace('"', '""')
    return f'"{escaped}"'


def _store_fqn_parts() -> Tuple[Tuple[str, bool], Tuple[str, bool], Tuple[str, bool]]:
    parts = _split_relation_parts(PROFILES_TABLE_FQN)
    if len(parts) >= 3:
        db_part, schema_part, table_part = parts[-3], parts[-2], parts[-1]
    else:
        raw_parts = [
            part.strip()
            for part in str(PROFILES_TABLE_FQN or "").split(".")
            if part.strip()
        ]
        while len(raw_parts) < 3:
            raw_parts.insert(0, "")
        db_part = (raw_parts[-3], False)
        schema_part = (raw_parts[-2], False)
        table_part = (raw_parts[-1], False)
    return db_part, schema_part, table_part


def _info_schema_name(value: str, quoted: bool) -> str:
    cleaned = _clean_identifier(value)
    if not cleaned:
        return ""
    return cleaned if quoted else cleaned.upper()


def _resolve_profiles_session() -> Any:
    try:
        state = st.session_state
    except Exception:  # pragma: no cover - defensive
        state = {}

    getter = getattr(state, "get", None)
    for key in _PROFILES_SESSION_KEYS:
        if getter is not None:
            candidate = getter(key, None)
        else:
            candidate = state.get(key) if isinstance(state, dict) else None
        if candidate is not None:
            return candidate

    for attr in ("snowpark_session", "session", "connection"):
        candidate = getattr(st, attr, None)
        if candidate is not None:
            return candidate
    app_module = sys.modules.get("streamlit_app")
    if app_module is not None:
        candidate = getattr(app_module, "session", None)
        if candidate is not None:
            return candidate
    return None


def verify_profiles_store() -> Dict[str, Any]:
    """Check whether the saved profiles table exists in Snowflake."""

    parts = _split_relation_parts(PROFILES_TABLE_FQN)
    if len(parts) >= 3:
        db_part, schema_part, table_part = parts[-3], parts[-2], parts[-1]
    else:
        return {"ok": False, "fqn": PROFILES_TABLE_FQN, "err": "invalid_fqn"}

    db_name = _clean_identifier(db_part[0])
    schema_name = _clean_identifier(schema_part[0])
    table_name = _clean_identifier(table_part[0])

    if not (db_name and schema_name and table_name):
        return {"ok": False, "fqn": PROFILES_TABLE_FQN, "err": "invalid_fqn"}

    db_upper = db_name.upper()
    schema_upper = schema_name.upper()
    table_upper = table_name.upper()

    profiles_session = _resolve_profiles_session()
    if profiles_session is None:
        return {"ok": False, "fqn": PROFILES_TABLE_FQN, "err": "session_unavailable"}

    db_identifier = _quote_identifier(db_upper)
    if not db_identifier:
        return {"ok": False, "fqn": PROFILES_TABLE_FQN, "err": "invalid_database"}

    sql = (
        f"SELECT COUNT(*) AS c FROM {db_identifier}.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
    )
    params = [schema_upper, table_upper]

    try:
        rows = _get_session().sql(sql, params=params).collect()
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "ok": False,
            "fqn": PROFILES_TABLE_FQN,
            "err": str(exc) or "verification_failed",
        }

    count = 0
    if rows:
        row = rows[0]
        try:
            count = int(row[0])
        except Exception:  # pragma: no cover - defensive
            count = int(getattr(row, "c", 0) or getattr(row, "C", 0) or 0)

    logger.info(
        "Profiles store verification",
        extra={"db": db_upper, "schema": schema_upper, "table": table_upper, "count": count},
    )

    if count > 0:
        return {"ok": True, "fqn": PROFILES_TABLE_FQN}

    return {
        "ok": False,
        "fqn": PROFILES_TABLE_FQN,
        "err": "profiles table missing",
    }


def verify_profiles_store_select() -> Dict[str, Any]:
    """Ensure the current role can SELECT from the profiles store."""

    sql = f"SELECT 1 FROM {PROFILES_TABLE_FQN} LIMIT 1"
    try:
        _get_session().sql(sql).collect()
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc) or "profiles_select_failed"
        logger.error(
            "Profiles store SELECT check failed",
            extra={
                "where": "verify_profiles_store_select",
                "store_fqn": PROFILES_TABLE_FQN,
                "err": err_msg,
            },
            exc_info=True,
        )
        return {"ok": False, "err": err_msg, "fqn": PROFILES_TABLE_FQN}

    logger.info(
        "Profiles store SELECT check succeeded",
        extra={"where": "verify_profiles_store_select", "store_fqn": PROFILES_TABLE_FQN},
    )
    return {"ok": True, "fqn": PROFILES_TABLE_FQN}


def has_profiles_for_fqn(table_fqn: str) -> Dict[str, Any]:
    """Return a lightweight count of saved profiles for the given table FQN."""

    canon = _canon_fqn(table_fqn or "")
    context = {
        "where": "has_profiles_for_fqn",
        "table_fqn": table_fqn or "",
        "table_fqn_canon": canon,
        "store_fqn": PROFILES_TABLE_FQN,
        "canon": canon,
    }

    if not canon:
        logger.info(
            "Saved profile count lookup skipped due to empty canonical FQN",
            extra={**context, "count": 0, "reason": "empty_canon"},
        )
        return {"ok": True, "count": 0, "canon": canon}

    sql = (
        f"SELECT COUNT(*) AS c FROM {PROFILES_TABLE_FQN} "
        "WHERE UPPER(TABLE_FQN) = ?"
    )

    try:
        rows = _get_session().sql(sql, params=[canon]).collect()
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc) or "profile_count_failed"
        logger.error(
            "Failed to count saved profiles",
            extra={**context, "err": err_msg},
            exc_info=True,
        )
        return {"ok": False, "err": err_msg}

    count = 0
    if rows:
        row = rows[0]
        try:
            count = int(row[0])
        except Exception:  # pragma: no cover - defensive
            count = int(getattr(row, "c", 0) or getattr(row, "C", 0) or 0)

    logger.info(
        "Saved profile count lookup",
        extra={**context, "count": count},
    )
    return {"ok": True, "count": count, "canon": canon}


def save_profile(profile: Any) -> Dict[str, Any]:
    """Persist a saved profile payload into session state."""

    context = {"where": "save_profile", "store_fqn": PROFILES_TABLE_FQN}
    context_with_table = dict(context)
    try:
        normalized_profile = normalize_saved_profile(profile)
        if not normalized_profile:
            raise ValueError("Profile payload is empty or invalid")

        run_map = _to_mapping(normalized_profile)
        if not run_map:
            raise ValueError("Profile payload is not a mapping")

        run_id, summary_map = _extract_run_identity(run_map)
        if not run_id:
            raise ValueError("Profile payload is missing a run identifier")

        table_fqn_raw = _extract_table_identifiers(run_map, summary_map)[2]
        canonical_table_fqn = _canon_fqn(table_fqn_raw)

        if not canonical_table_fqn:
            for candidate in (
                run_map.get("target_fqn"),
                summary_map.get("target_fqn"),
                summary_map.get("target_table"),
                summary_map.get("target"),
                run_map.get("target"),
            ):
                if candidate is None:
                    continue
                candidate_text = str(candidate).strip()
                if not candidate_text:
                    continue
                candidate_canon = _canon_fqn(candidate_text)
                if candidate_canon:
                    table_fqn_raw = candidate_text
                    canonical_table_fqn = candidate_canon
                    break

        context_with_table = {
            **context,
            "table_fqn_raw": table_fqn_raw or "",
            "table_fqn_canonical": canonical_table_fqn,
            "table_fqn_canon": canonical_table_fqn,
        }

        if not canonical_table_fqn:
            logger.error(
                "Cannot save profile without a table FQN",
                extra={
                    **context_with_table,
                    "profile_id": run_id,
                    "reason": "empty_fqn",
                },
            )
            return {"ok": False, "err": "empty_fqn"}

        verification = verify_profiles_store()
        if not verification.get("ok"):
            logger.info(
                "Profiles store unavailable",
                extra={
                    **context_with_table,
                    "profile_id": run_id,
                    "verification_err": verification.get("err") or "",
                },
            )
            return {
                "ok": False,
                "err": "profiles table missing",
                "fqn": PROFILES_TABLE_FQN,
            }

        existing = normalize_saved_profiles(
            st.session_state.get(SAVED_PROFILES_STATE, [])
        )
        current_run_dict = dict(run_map)
        updated: List[Dict[str, Any]] = []
        replaced = False
        for run in existing:
            existing_run_dict = _to_mapping(run)
            existing_id, _ = _extract_run_identity(existing_run_dict)
            if existing_id == run_id:
                updated.append(current_run_dict)
                replaced = True
            else:
                updated.append(dict(existing_run_dict))
        if not replaced:
            updated.append(current_run_dict)

        st.session_state[SAVED_PROFILES_STATE] = updated

        current_run_dict["table_fqn"] = canonical_table_fqn

        serialized_profile = _json_dumps_safe(current_run_dict)

        logger.info(
            "Saved profile payload",
            extra={
                **context_with_table,
                "profile_id": run_id,
                "store_size": len(updated),
            },
        )
        return {"ok": True, "id": run_id, "item_json": serialized_profile}
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc)
        logger.error(
            "Failed to save profile",
            extra={**context_with_table, "err": err_msg},
            exc_info=True,
        )
        return {"ok": False, "err": err_msg}


def list_saved_profiles(table_fqn: Optional[str]) -> _ProfileListResult:
    """Return normalised saved profile entries for the dropdown."""

    canon_filter = _canon_fqn(table_fqn or "")
    if not (table_fqn and canon_filter):
        empty_context = {
            "where": "list_profiles",
            "table_fqn": table_fqn or "",
            "table_fqn_raw": table_fqn or "",
            "canonical_table_fqn": canon_filter,
            "table_fqn_canon": canon_filter,
            "filter_canon_fqn": canon_filter,
            "reason": "empty_fqn",
        }
        logger.info(
            "Skipping saved profile listing due to empty table FQN",
            extra=empty_context,
        )
        return _ProfileListResult(
            {"ok": True, "items": [], "debug": {"reason": "empty_fqn"}}
        )

    filter_db, filter_schema, filter_table, canonical_fqn = _normalise_table_filter(
        table_fqn
    )
    context = {
        "where": "list_profiles",
        "table_fqn": table_fqn or "",
        "table_fqn_raw": table_fqn or "",
        "canonical_table_fqn": canonical_fqn or canon_filter,
        "table_fqn_canon": canonical_fqn or canon_filter,
        "filter_canon_fqn": canon_filter,
        "filter_db": filter_db,
        "filter_schema": filter_schema,
        "filter_table": filter_table,
        "final_sql": "SESSION_STATE_FILTER(UPPER(table_fqn)=?)",
        "store_fqn": PROFILES_TABLE_FQN,
    }

    select_verification = verify_profiles_store_select()
    if not select_verification.get("ok"):
        err_msg = select_verification.get("err") or "profiles_select_failed"
        logger.error(
            "Profiles store SELECT check failed",
            extra={**context, "err": err_msg},
        )
        return _ProfileListResult(
            {
                "ok": False,
                "err": err_msg,
                "fqn": PROFILES_TABLE_FQN,
                "items": [],
            }
        )

    verification = verify_profiles_store()
    if not verification.get("ok"):
        logger.info(
            "Profiles store unavailable",
            extra={
                **context,
                "verification_err": verification.get("err") or "",
            },
        )
        return _ProfileListResult(
            {
                "ok": False,
                "err": "profiles table missing",
                "fqn": PROFILES_TABLE_FQN,
                "items": [],
            }
        )

    try:
        stored = st.session_state.get(SAVED_PROFILES_STATE, [])
        if not stored:
            logger.info(
                "Saved profile store missing or empty",
                extra={**context, "rowcount": 0, "reason": "store_missing"},
            )

        runs = normalize_saved_profiles(stored) if stored else []
        if stored and not runs:
            logger.info(
                "Saved profile store normalized to zero rows",
                extra={**context, "rowcount": 0, "reason": "normalised_empty"},
            )

        prepared_runs: List[Dict[str, Any]] = []
        missing_id = 0
        sample_values: List[str] = []
        sample_keys = set()

        for run in runs:
            run_map = _to_mapping(run)
            if not run_map:
                missing_id += 1
                continue

            run_id, summary_map = _extract_run_identity(run_map)
            if not run_id:
                missing_id += 1
                continue

            schema_clean, table_clean, table_name = _extract_table_identifiers(
                run_map, summary_map
            )

            fqn_candidates = [
                run_map.get("target_fqn"),
                summary_map.get("target_fqn"),
                summary_map.get("target_table"),
                summary_map.get("target"),
                run_map.get("target"),
            ]
            if table_name:
                fqn_candidates.append(table_name)

            stored_fqn_raw = ""
            for candidate in fqn_candidates:
                if candidate is None:
                    continue
                candidate_text = str(candidate).strip()
                if candidate_text:
                    stored_fqn_raw = candidate_text
                    break

            if not stored_fqn_raw and schema_clean and table_clean:
                stored_fqn_raw = f"{schema_clean}.{table_clean}"

            stored_canon = _canon_fqn(stored_fqn_raw)
            sample_key = stored_canon or stored_fqn_raw.upper()
            if sample_key and sample_key not in sample_keys:
                sample_keys.add(sample_key)
                if len(sample_values) < 5:
                    sample_values.append(stored_fqn_raw or stored_canon)

            timestamp_iso = _profile_timestamp(run_map, summary_map)
            display_name = _profile_display_name(run_map, summary_map)
            run_dict = dict(run_map)

            prepared_runs.append(
                {
                    "id": run_id,
                    "entry": {
                        "id": run_id,
                        "name": display_name,
                        "table_fqn": table_name,
                        "created_at_iso": timestamp_iso,
                        "timestamp": timestamp_iso,
                        "run": run_dict,
                        "run_json": _json_dumps_safe(run_dict),
                    },
                    "stored_canon": stored_canon,
                    "schema_key": schema_clean.upper() if schema_clean else "",
                    "table_key": table_clean.upper() if table_clean else "",
                    "legacy_keys": set(_table_fqn_upper_variants(stored_fqn_raw, stored_canon)),
                }
            )

        total_runs = len(runs)

        def _apply_filter(match_func):
            matched: List[Dict[str, Any]] = []
            counters = {"missing_target": 0, "filter_mismatch": 0}
            for info in prepared_runs:
                matched_flag, reason = match_func(info)
                if matched_flag:
                    matched.append(info)
                elif reason in counters:
                    counters[reason] += 1
            return matched, counters

        def _primary_match(info: Dict[str, Any]):
            if canon_filter:
                if info["stored_canon"] == canon_filter:
                    return True, None
                if filter_schema and filter_table:
                    if not (info["schema_key"] and info["table_key"]):
                        return False, "missing_target"
                    if (
                        info["schema_key"] == filter_schema
                        and info["table_key"] == filter_table
                    ):
                        return True, None
                    return False, "filter_mismatch"
                return False, "filter_mismatch"
            if filter_schema and filter_table:
                if not (info["schema_key"] and info["table_key"]):
                    return False, "missing_target"
                if (
                    info["schema_key"] == filter_schema
                    and info["table_key"] == filter_table
                ):
                    return True, None
                return False, "filter_mismatch"
            return True, None

        primary_matches, primary_counters = _apply_filter(_primary_match)

        entries_by_id: Dict[str, Dict[str, Any]] = {}
        final_counters = primary_counters
        final_matches = primary_matches
        fallback_used = False
        legacy_variant_keys: List[str] = []

        if not primary_matches and canon_filter:
            session_variants = [
                table_fqn,
                canonical_fqn,
                st.session_state.get("profile_target_fqn"),
                st.session_state.get("editor_target_fqn"),
            ]
            legacy_variant_keys = _table_fqn_upper_variants(*session_variants)
            legacy_key_set = {key for key in legacy_variant_keys if key}

            if legacy_key_set:

                def _legacy_match(info: Dict[str, Any]):
                    if info["legacy_keys"] & legacy_key_set:
                        return True, None
                    if filter_schema and filter_table:
                        if not (info["schema_key"] and info["table_key"]):
                            return False, "missing_target"
                        if (
                            info["schema_key"] == filter_schema
                            and info["table_key"] == filter_table
                        ):
                            return True, None
                    return False, "filter_mismatch"

                legacy_matches, legacy_counters = _apply_filter(_legacy_match)
                if legacy_matches:
                    final_matches = legacy_matches
                    final_counters = legacy_counters
                    fallback_used = True
                    context["final_sql"] = "SESSION_STATE_FILTER(UPPER(table_fqn) IN (?…))"
                else:
                    final_counters = legacy_counters

        for info in final_matches:
            entry = info["entry"]
            entry_id = entry.get("id")
            if entry_id and entry_id not in entries_by_id:
                entries_by_id[entry_id] = entry

        entries = list(entries_by_id.values())

        log_extra = {
            **context,
            "rowcount": len(entries),
            "total_saved_profiles": total_runs,
            "skipped_missing_id": missing_id,
            "skipped_missing_target": final_counters["missing_target"],
            "skipped_filter_mismatch": final_counters["filter_mismatch"],
        }
        if fallback_used:
            log_extra["fallback_variants"] = legacy_variant_keys

        if not entries:
            debug_payload = {
                "total_rows": total_runs,
                "samples": sample_values,
                "canon": canon_filter,
            }
            if legacy_variant_keys:
                debug_payload["legacy_variants"] = legacy_variant_keys
            logger.info(
                "Saved profile list empty after filtering",
                extra={**log_extra, "debug": debug_payload},
            )
            return _ProfileListResult({"ok": True, "items": [], "debug": debug_payload})

        logger.info("Listed saved profiles", extra=log_extra)

        return _ProfileListResult({"ok": True, "items": entries})
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc)
        logger.error(
            "Failed to list saved profiles",
            extra={**context, "err": err_msg},
            exc_info=True,
        )
        return _ProfileListResult({"ok": False, "err": err_msg, "items": []})


def load_profile_by_id(profile_id: str) -> Dict[str, Any]:
    """Load a saved profile payload from session state by identifier."""

    context = {
        "where": "load_profile",
        "profile_id": str(profile_id or ""),
        "store_fqn": PROFILES_TABLE_FQN,
    }

    try:
        if not profile_id:
            raise ValueError("Profile identifier is required")

        stored = st.session_state.get(SAVED_PROFILES_STATE, [])
        if not stored:
            logger.info(
                "Saved profile store missing while loading",
                extra={**context, "reason": "store_missing"},
            )
            return {"ok": False, "err": "Saved profile store is empty"}

        runs = normalize_saved_profiles(stored)
        for run in runs:
            run_map = _to_mapping(run)
            if not run_map:
                continue
            run_id, summary_map = _extract_run_identity(run_map)
            if run_id == str(profile_id).strip():
                table_fqn = _extract_table_identifiers(run_map, summary_map)[2]
                logger.info(
                    "Loaded saved profile",
                    extra={
                        **context,
                        "table_fqn": table_fqn,
                        "table_fqn_raw": table_fqn,
                        "table_fqn_canon": _canon_fqn(table_fqn),
                    },
                )
                item_dict = dict(run_map)
                return {
                    "ok": True,
                    "item": item_dict,
                    "item_json": _json_dumps_safe(item_dict),
                }

        logger.info(
            "Saved profile not found",
            extra={**context, "reason": "not_found"},
        )
        return {"ok": False, "err": f"Profile '{profile_id}' not found"}
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc)
        logger.error(
            "Failed to load saved profile",
            extra={**context, "err": err_msg},
            exc_info=True,
        )
        return {"ok": False, "err": err_msg}


_json_dumps_original = json.dumps


def _json_dumps_with_default(*args: Any, **kwargs: Any) -> str:
    """Proxy for :func:`json.dumps` that injects the profile serializer."""

    if kwargs.get("default") is None:
        kwargs["default"] = _json_default
    return _json_dumps_original(*args, **kwargs)


if _json_dumps_original is not _json_dumps_with_default:
    json.dumps = _json_dumps_with_default


def _json_dumps_safe(obj: Any) -> str:
    """Return a JSON string using the profile serializer and UTF-8 characters."""

    return _json_dumps_original(obj, default=_json_default, ensure_ascii=False)


def get_include_map() -> Dict[str, bool]:
    """Return a shallow copy of the current include map."""

    state = st.session_state.get(_INCLUDE_MAP, {})
    if isinstance(state, dict):
        return {str(key): bool(value) for key, value in state.items()}
    return {}


def set_include(column: str, include: bool) -> None:
    """Set the include flag for a single column."""

    include_map = get_include_map()
    include_map[str(column)] = bool(include)
    st.session_state[_INCLUDE_MAP] = include_map


def bulk_set_includes(columns: Iterable[str], include: bool) -> None:
    """Replace the include map with a bulk assignment."""

    st.session_state[_INCLUDE_MAP] = {
        str(column): bool(include) for column in columns
    }


def prune_includes(valid_keys: Iterable[str]) -> None:
    """Remove include entries not present in ``valid_keys``."""

    valid = {str(key) for key in valid_keys}
    include_map = get_include_map()
    st.session_state[_INCLUDE_MAP] = {
        key: value for key, value in include_map.items() if key in valid
    }
