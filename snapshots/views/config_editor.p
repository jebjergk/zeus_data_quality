"""Runtime guards for configuration editor views."""

from __future__ import annotations

import os
from typing import Sequence

import pandas as pd
import streamlit as st

_CONTRACT_ENV_FLAG = "UI_CONTRACT_STRICT"
_EXPECTED_PREVIEW_COLUMNS: Sequence[str] = ("day", "cnt")


def _contract_message(message: str) -> None:
    """Display a contract warning or elevate to an error when strict."""

    strict = os.getenv(_CONTRACT_ENV_FLAG, "0") == "1"
    if strict:
        st.error(message)
    else:
        st.warning(message)


def render_row_count_preview(df: pd.DataFrame) -> None:
    """Render the row-count preview grid with UI contract validation."""

    if not isinstance(df, pd.DataFrame):
        _contract_message(
            "UI contract violation in config preview: expected a pandas DataFrame."
        )
        return

    actual_columns = list(df.columns)
    expected_columns = list(_EXPECTED_PREVIEW_COLUMNS)
    if actual_columns != expected_columns:
        _contract_message(
            "UI contract violation in config preview: expected columns "
            f"{expected_columns} but found {actual_columns}."
        )
        return

    st.dataframe(df, use_container_width=True, hide_index=True, height=320)
