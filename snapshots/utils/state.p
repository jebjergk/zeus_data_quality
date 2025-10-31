"""Session-state helpers for profile selections."""

from __future__ import annotations

from typing import Dict, List

import streamlit as st

_INCLUDE_MAP = "profile_include_map"


def get_include_map() -> Dict[str, bool]:
    """Return a copy of the current include map from session state."""

    value = st.session_state.get(_INCLUDE_MAP, {})
    if isinstance(value, dict):
        return {str(key): bool(val) for key, val in value.items()}
    return {}


def set_include(column: str, include: bool) -> None:
    """Set the include flag for a single column."""

    include_map = get_include_map()
    include_map[str(column)] = bool(include)
    st.session_state[_INCLUDE_MAP] = include_map


def bulk_set_includes(columns: List[str], include: bool) -> None:
    """Replace the include map with a uniform value for provided columns."""

    st.session_state[_INCLUDE_MAP] = {str(column): bool(include) for column in columns}
