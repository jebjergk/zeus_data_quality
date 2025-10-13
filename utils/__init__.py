"""Utility package for Zeus Data Quality Streamlit application."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Dict

__all__ = [
    "checkdefs",
    "dmfs",
    "meta",
    "schedules",
    "ui",
]

_lazy_cache: Dict[str, ModuleType] = {}


def __getattr__(name: str) -> ModuleType:
    if name in __all__:
        if name not in _lazy_cache:
            _lazy_cache[name] = import_module(f"{__name__}.{name}")
        return _lazy_cache[name]
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
