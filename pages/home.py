"""Streamlit Overview page for Zeus Data Quality."""
from __future__ import annotations

import streamlit as st

from .ui_shared import page_header


def render_home(session, app_version: str | None = None) -> None:
    """Render the Zeus Data Quality overview page."""

    if app_version:
        st.session_state.setdefault("app_version", app_version)

    page_header(
        "Zeus Data Quality Overview",
        session=session,
        show_version=True,
        include_metadata=True,
    )

    st.markdown(
        """
        Zeus Data Quality (Zeus DQ) accelerates governance by centralising the
        definition, scheduling, and monitoring of data quality rules directly inside
        Snowflake. The application provides an opinionated workflow for analysts
        and engineers to capture the checks that matter, execute them reliably,
        and visualise the outcomes without leaving the warehouse.
        """
    )

    if not session:
        st.info(
            "No active Snowflake session detected. You can still explore the UI, "
            "but configuration data will not be loaded until a session is "
            "available."
        )

    st.subheader("Get started in a few steps")
    st.markdown(
        """
        1. **Configure** — Use the *Configurations* page to register a target table,
           describe the business intent, and add the validation checks you need.
        2. **Schedule & Run** — Choose how and when rules execute using built-in
           scheduling helpers or trigger ad-hoc runs directly from Snowflake.
        3. **Monitor** — Track historical performance and investigate failures on
           the *Monitor* page with trend charts and detailed run results.
        4. **Learn more** — Consult the *Documentation* section for rule syntax,
           onboarding instructions, and integration tips.
        """
    )

    st.subheader("What you can expect")
    st.markdown(
        """
        - **Unified metadata**: All configurations and results live in a shared
          Snowflake schema, keeping governance artefacts close to the data.
        - **Flexible checks**: Capture column-level and table-level validations
          using declarative rule expressions with optional sampling.
        - **Actionable insights**: Quickly identify failing rules, owners, and the
          severity of issues to drive remediation work.
        """
    )
