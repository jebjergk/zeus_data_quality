"""Utility configuration helpers."""

# Temporary shim: re-export from configs.py
from .configs import *  # noqa

PROFILES_TABLE_FQN = "ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_SAVED_PROFILES"
"""Central source of truth for saved profiles table."""

__all__ = [*globals().get("__all__", []), "PROFILES_TABLE_FQN"]
