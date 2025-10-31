"""Environment-driven feature flags and kill-switches."""

from __future__ import annotations

import os


def flag(name: str, default: bool = False) -> bool:
    """Return the boolean value of an environment flag."""

    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "t", "yes", "y", "on"}


UI_CONTRACT_STRICT = flag("UI_CONTRACT_STRICT", False)
DEMO_LOCK = flag("DEMO_LOCK", False)
DEBUG_PROFILING = flag("DEBUG_PROFILING_PAYLOAD", False)
PROFILE_INLINE_SELECT = flag("PROFILE_INLINE_SELECT", True)
PROFILE_TOP_VALUES_NULLS = flag("PROFILE_TOP_VALUES_NULLS", True)
