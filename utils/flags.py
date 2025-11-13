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
DEBUG_PROFILING: bool = False
"""When True, Profiling v2 view shows a developer debug expander with diagnostics."""
