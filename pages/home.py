"""Streamlit Overview page for Zeus Data Quality."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import streamlit as st

try:
    from utils.meta import metadata_db_schema
except Exception:  # pragma: no cover - optional metadata helper
    metadata_db_schema = None  # type: ignore


def _build_caption(session) -> str:
    """Return a descriptive caption with build metadata and session context."""
    secrets = getattr(st, "secrets", {})

    build_version = secrets.get("build_version") or secrets.get("version") or secrets.get("release")
    build_timestamp = secrets.get("build_timestamp") or secrets.get("build_time")

    parts = []
    if build_version:
        parts.append(f"Build {build_version}")
    else:
        parts.append("Local development build")

    formatted_ts: Optional[str] = None
    if isinstance(build_timestamp, (int, float)):
        formatted_ts = datetime.fromtimestamp(build_timestamp).strftime("%Y-%m-%d %H:%M %Z")
    elif isinstance(build_timestamp, str) and build_timestamp.strip():
        formatted_ts = build_timestamp.strip()

    if formatted_ts:
        parts.append(f"generated {formatted_ts}")

    if session and metadata_db_schema:
        try:
            db, schema = metadata_db_schema(session)
            parts.append(f"metadata: {db}.{schema}")
        except Exception:
            parts.append("metadata: unknown")
    elif session:
        parts.append("connected to Snowflake session")

    return " · ".join(parts)


def render_home(session) -> None:
    """Render the Zeus Data Quality overview page."""
    st.title("Zeus Data Quality Overview")
    st.caption(_build_caption(session))

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
