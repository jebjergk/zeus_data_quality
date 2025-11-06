"""Session-state helpers for Streamlit views."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence
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

logger = logging.getLogger(__name__)


_INCLUDE_MAP = "profile_include_map"
_SAVED_PROFILES_KEY = "saved_profiles"


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


def _get_saved_profiles_container(*, create: bool = False) -> Dict[str, Dict[str, Any]]:
    """Return the saved profiles state container, optionally creating it."""

    existing = st.session_state.get(_SAVED_PROFILES_KEY)
    if isinstance(existing, dict):
        return existing
    if isinstance(existing, MutableMapping):
        materialized = dict(existing)
        st.session_state[_SAVED_PROFILES_KEY] = materialized
        return materialized
    if create:
        container: Dict[str, Dict[str, Any]] = {}
        st.session_state[_SAVED_PROFILES_KEY] = container
        return container
    return {}


def save_profile(table_fqn: str, profile_id: str, profile: Any) -> Dict[str, Any]:
    """Persist a profile payload into session state with structured logging."""

    table_key = (table_fqn or "").strip()
    profile_key = (profile_id or "").strip()

    try:
        if not table_key:
            raise ValueError("table_fqn is required")
        if not profile_key:
            raise ValueError("profile_id is required")

        container = _get_saved_profiles_container(create=True)
        bucket = container.get(table_key)
        if not isinstance(bucket, dict):
            bucket = {}
            container[table_key] = bucket

        normalized_profile = normalize_saved_profile(profile)
        bucket[profile_key] = normalized_profile
        st.session_state[_SAVED_PROFILES_KEY] = container

        logger.info(
            "save_profile ok table_fqn=%s profile_id=%s",
            table_key,
            profile_key,
        )
        return {
            "ok": True,
            "table_fqn": table_key,
            "profile_id": profile_key,
            "profile": normalized_profile,
        }
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error(
            "save_profile failed table_fqn=%s profile_id=%s error=%s",
            table_fqn,
            profile_id,
            exc,
            exc_info=True,
        )
        return {
            "ok": False,
            "err": str(exc),
            "where": "save_profile",
            "table_fqn": str(table_fqn),
            "profile_id": str(profile_id),
        }


def list_saved_profiles(table_fqn: Optional[str] = None) -> Dict[str, Any]:
    """Return saved profiles for a table with explicit success or failure."""

    table_key = (table_fqn or "").strip() if table_fqn is not None else None

    try:
        container = _get_saved_profiles_container(create=False)
        profiles_raw: List[Any]
        if table_key:
            bucket = container.get(table_key)
            profiles_raw = list(bucket.values()) if isinstance(bucket, dict) else []
        else:
            profiles_raw = []
            for bucket in container.values():
                if isinstance(bucket, dict):
                    profiles_raw.extend(bucket.values())

        profiles = normalize_saved_profiles(profiles_raw)
        logger.info(
            "list_saved_profiles ok table_fqn=%s count=%d",
            table_key or "*",
            len(profiles),
        )
        return {
            "ok": True,
            "table_fqn": table_key,
            "profiles": profiles,
        }
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error(
            "list_saved_profiles failed table_fqn=%s error=%s",
            table_fqn,
            exc,
            exc_info=True,
        )
        return {
            "ok": False,
            "err": str(exc),
            "where": "list_saved_profiles",
            "table_fqn": table_key,
        }


def load_profile_by_id(table_fqn: str, profile_id: str) -> Dict[str, Any]:
    """Return a saved profile payload for ``profile_id`` or a failure payload."""

    table_key = (table_fqn or "").strip()
    profile_key = (profile_id or "").strip()

    try:
        if not table_key:
            raise ValueError("table_fqn is required")
        if not profile_key:
            raise ValueError("profile_id is required")

        container = _get_saved_profiles_container(create=False)
        bucket = container.get(table_key)
        profile_raw: Optional[Any]
        if isinstance(bucket, dict):
            profile_raw = bucket.get(profile_key)
        else:
            profile_raw = None

        if profile_raw is None:
            logger.info(
                "load_profile_by_id miss table_fqn=%s profile_id=%s",
                table_key,
                profile_key,
            )
            return {
                "ok": False,
                "err": "profile not found",
                "where": "load_profile_by_id",
                "table_fqn": table_key,
                "profile_id": profile_key,
            }

        profile = normalize_saved_profile(profile_raw)
        logger.info(
            "load_profile_by_id ok table_fqn=%s profile_id=%s",
            table_key,
            profile_key,
        )
        return {
            "ok": True,
            "table_fqn": table_key,
            "profile_id": profile_key,
            "profile": profile,
        }
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error(
            "load_profile_by_id failed table_fqn=%s profile_id=%s error=%s",
            table_fqn,
            profile_id,
            exc,
            exc_info=True,
        )
        return {
            "ok": False,
            "err": str(exc),
            "where": "load_profile_by_id",
            "table_fqn": str(table_fqn),
            "profile_id": str(profile_id),
        }
