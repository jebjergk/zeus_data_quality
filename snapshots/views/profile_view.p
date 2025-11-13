"""Temporary Profiling v2 placeholder view."""

from __future__ import annotations

from typing import Any, Optional

import streamlit as st

from ui import strings as ui_strings


def render_profile(
    session: Any,
    metadata_db: str,
    metadata_schema: str,
    profiling_helpers: Optional[Any] = None,
) -> None:
    """Render a placeholder until the Profiling v2 UI ships."""

    _ = session, metadata_db, metadata_schema  # placeholder for future use

    st.header(ui_strings.PROFILE_V2_HEADER_TITLE)
    st.caption(ui_strings.PROFILE_V2_HEADER_CAPTION)
    st.info(ui_strings.PROFILE_V2_PLACEHOLDER_MESSAGE)
    if profiling_helpers is not None:
        st.caption(
            f"Profiling metadata source: {profiling_helpers.DISCOVERY_NAMESPACE}"
        )
