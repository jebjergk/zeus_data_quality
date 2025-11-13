"""Temporary Profiling v2 placeholder view."""

from __future__ import annotations

import streamlit as st

from ui import strings as ui_strings


def render_profile(*_args, **_kwargs) -> None:
    """Render a placeholder until the Profiling v2 UI ships."""

    st.header(ui_strings.PROFILE_V2_HEADER_TITLE)
    st.caption(ui_strings.PROFILE_V2_HEADER_CAPTION)
    st.info(ui_strings.PROFILE_V2_PLACEHOLDER_MESSAGE)
