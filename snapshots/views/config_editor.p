"""Runtime guards for configuration editor views."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui import strings as ui_strings
from utils.flags import DEMO_LOCK, UI_CONTRACT_STRICT


def _contract_message(message: str) -> None:
    """Display a contract warning or elevate to an error when strict."""

    if UI_CONTRACT_STRICT or DEMO_LOCK:
        st.error(message)
    else:
        st.warning(message)


def render_row_count_preview(df: pd.DataFrame) -> None:
    """Render the row-count preview grid with UI contract validation."""

    if not isinstance(df, pd.DataFrame):
        _contract_message(ui_strings.CONFIG_PREVIEW_CONTRACT_VIOLATION_FRAME)
        return

    actual_columns = list(df.columns)
    expected_columns = list(ui_strings.CONFIG_PREVIEW_EXPECTED_COLUMNS)
    if actual_columns != expected_columns:
        _contract_message(
            ui_strings.CONFIG_PREVIEW_CONTRACT_VIOLATION_COLUMNS.format(
                expected=expected_columns, actual=actual_columns
            )
        )
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=ui_strings.CONFIG_PREVIEW_HEIGHT,
    )
