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


def _normalise_table_filter(table_fqn: Optional[str]) -> Tuple[str, str, str]:
    filter_schema = ""
    filter_table = ""
    normalized = ""
    if table_fqn:
        parts = [
            part.strip().strip('"')
            for part in str(table_fqn).split(".")
            if part and str(part).strip()
        ]
        if len(parts) >= 2:
            filter_schema = parts[-2].lower()
            filter_table = parts[-1].lower()
            normalized = f"{parts[-2]}.{parts[-1]}"
    return filter_schema, filter_table, normalized


def save_profile(profile: Any) -> Dict[str, Any]:
    """Persist a saved profile payload into session state."""

    context = {"where": "save_profile"}
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

        existing = normalize_saved_profiles(
            st.session_state.get(SAVED_PROFILES_STATE, [])
        )
        updated: List[Dict[str, Any]] = []
        replaced = False
        for run in existing:
            run_dict = _to_mapping(run)
            existing_id, _ = _extract_run_identity(run_dict)
            if existing_id == run_id:
                updated.append(dict(run_map))
                replaced = True
            else:
                updated.append(dict(run_dict))
        if not replaced:
            updated.append(dict(run_map))

        st.session_state[SAVED_PROFILES_STATE] = updated

        table_fqn = _extract_table_identifiers(run_map, summary_map)[2]
        logger.info(
            "Saved profile payload",
            extra={
                **context,
                "profile_id": run_id,
                "table_fqn": table_fqn,
                "store_size": len(updated),
            },
        )
        return {"ok": True, "id": run_id}
    except Exception as exc:  # pragma: no cover - defensive
        err_msg = str(exc)
        logger.error(
            "Failed to save profile",
            extra={**context, "err": err_msg},
            exc_info=True,
        )
        return {"ok": False, "err": err_msg}


def list_saved_profiles(table_fqn: Optional[str]) -> _ProfileListResult:
    """Return normalised saved profile entries for the dropdown."""

    filter_schema, filter_table, normalized_fqn = _normalise_table_filter(table_fqn)
    context = {
        "where": "list_profiles",
        "table_fqn": table_fqn or "",
        "normalized_table_fqn": normalized_fqn,
        "final_sql": "SESSION_STATE_FILTER(schema=?, table=?)",
    }

    try:
        stored = st.session_state.get(SAVED_PROFILES_STATE, [])
        if not stored:
            logger.info(
                "Saved profile store missing or empty",
                extra={**context, "rowcount": 0, "reason": "store_missing"},
            )
            return _ProfileListResult({"ok": True, "items": []})

        runs = normalize_saved_profiles(stored)
        if not runs:
            logger.info(
                "Saved profile store normalized to zero rows",
                extra={**context, "rowcount": 0, "reason": "normalised_empty"},
            )
            return _ProfileListResult({"ok": True, "items": []})

        entries: List[Dict[str, Any]] = []
        counters = {
            "missing_id": 0,
            "missing_target": 0,
            "filter_mismatch": 0,
        }

        for run in runs:
            run_map = _to_mapping(run)
            if not run_map:
                counters["missing_id"] += 1
                continue

            run_id, summary_map = _extract_run_identity(run_map)
            if not run_id:
                counters["missing_id"] += 1
                continue

            schema_clean, table_clean, table_name = _extract_table_identifiers(
                run_map, summary_map
            )
            if filter_schema and filter_table:
                if not (schema_clean and table_clean):
                    counters["missing_target"] += 1
                    continue
                if (
                    schema_clean.lower() != filter_schema
                    or table_clean.lower() != filter_table
                ):
                    counters["filter_mismatch"] += 1
                    continue

            timestamp_iso = _profile_timestamp(run_map, summary_map)
            display_name = _profile_display_name(run_map, summary_map)

            entries.append(
                {
                    "id": run_id,
                    "name": display_name,
                    "table_fqn": table_name,
                    "created_at_iso": timestamp_iso,
                    "timestamp": timestamp_iso,
                    "run": dict(run_map),
                }
            )

        logger.info(
            "Listed saved profiles",
            extra={
                **context,
                "rowcount": len(entries),
                "skipped_missing_id": counters["missing_id"],
                "skipped_missing_target": counters["missing_target"],
                "skipped_filter_mismatch": counters["filter_mismatch"],
            },
        )

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

    context = {"where": "load_profile", "profile_id": str(profile_id or "")}

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
                    extra={**context, "table_fqn": table_fqn},
                )
                return {"ok": True, "item": dict(run_map)}

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
