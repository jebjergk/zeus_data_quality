"""Runtime guards for configuration editor views."""

from __future__ import annotations

from typing import Sequence

import pandas as pd
import streamlit as st

from ui.strings import ConfigEditorStrings
from utils.flags import DEMO_LOCK, UI_CONTRACT_STRICT

_EXPECTED_PREVIEW_COLUMNS: Sequence[str] = ("day", "cnt")


def _contract_message(message: str) -> None:
    """Display a contract warning or elevate to an error when strict."""

    strict = UI_CONTRACT_STRICT or DEMO_LOCK
    if strict:
        st.error(message)
    else:
        st.warning(message)


def render_row_count_preview(df: pd.DataFrame) -> None:
    """Render the row-count preview grid with UI contract validation."""

    if not isinstance(df, pd.DataFrame):
        _contract_message(ConfigEditorStrings.CONTRACT_DATAFRAME_EXPECTED)
        return

    actual_columns = list(df.columns)
    expected_columns = list(_EXPECTED_PREVIEW_COLUMNS)
    if actual_columns != expected_columns:
        _contract_message(
            ConfigEditorStrings.CONTRACT_COLUMNS_MISMATCH.format(
                expected=expected_columns, actual=actual_columns
            )
        )
        return

    st.dataframe(df, use_container_width=True, hide_index=True, height=320)
