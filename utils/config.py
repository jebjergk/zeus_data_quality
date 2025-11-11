"""Utility configuration helpers."""

# Temporary shim: re-export from configs.py
from .configs import *  # noqa

PROFILES_TABLE_FQN = "ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_SAVED_PROFILES"
"""Central source of truth for saved profiles table."""

ALLOW_CREATE_PROFILES_TABLE = False
"""DDL is managed manually; do not auto-create the profiles table."""

PROFILES_TABLE_GRANT_SELECT_TO = None
"""No automatic grants are applied to the profiles table."""

__all__ = [
    *globals().get("__all__", []),
    "PROFILES_TABLE_FQN",
    "ALLOW_CREATE_PROFILES_TABLE",
    "PROFILES_TABLE_GRANT_SELECT_TO",
]
