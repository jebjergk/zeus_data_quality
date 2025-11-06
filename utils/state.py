"""Session-state helpers for Streamlit views."""

from __future__ import annotations

import json
import logging
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


logger = logging.getLogger(__name__)


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


def list_saved_profiles(table_fqn: Optional[str]) -> List[Dict[str, Any]]:
    """Return normalised saved profile entries for the dropdown."""

    try:
        stored = st.session_state.get(SAVED_PROFILES_STATE, [])
        runs = normalize_saved_profiles(stored)
        if not runs:
            return []

        filter_schema = ""
        filter_table = ""
        if table_fqn:
            parts = [
                part.strip().strip('"')
                for part in str(table_fqn).split(".")
                if part and str(part).strip()
            ]
            if len(parts) >= 2:
                filter_schema = parts[-2].lower()
                filter_table = parts[-1].lower()

        entries: List[Dict[str, Any]] = []
        for run in runs:
            summary_map = _to_mapping(run.get("summary"))
            run_id = _first_text(
                run.get("run_id"),
                run.get("id"),
                summary_map.get("run_id"),
                summary_map.get("id"),
            )
            if not run_id:
                continue

            schema, table = _extract_schema_table(run, summary_map)
            if filter_schema and filter_table:
                schema_clean = _clean_identifier(schema)
                table_clean = _clean_identifier(table)
                if not (schema_clean and table_clean):
                    target = _clean_identifier(
                        run.get("target_fqn") or summary_map.get("target_table")
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
                if not (schema_clean and table_clean):
                    continue
                if schema_clean.lower() != filter_schema or table_clean.lower() != filter_table:
                    continue

            name = _first_text(
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
            timestamp_iso = _ensure_iso_timestamp(timestamp_value)

            entries.append(
                {
                    "id": run_id,
                    "name": name,
                    "timestamp": timestamp_iso,
                    "run": run,
                }
            )
        return entries
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("Failed to list saved profiles: %s", exc, exc_info=True)
        return []


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
