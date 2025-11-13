"""Utility configuration helpers."""

# Temporary shim: re-export from configs.py
from .configs import *  # noqa

__all__ = [*globals().get("__all__", [])]
