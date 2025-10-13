"""Shared Streamlit UI helpers for Zeus Data Quality pages."""
from __future__ import annotations

import html
from datetime import datetime
from typing import Optional

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - fallback for doc builds/tests
    class _StreamlitStub:
        """Minimal stub so helpers can be imported without Streamlit."""

        secrets: dict[str, str] = {}
        session_state: dict[str, object] = {}

        def __getattr__(self, name):  # pragma: no cover - defensive guard
            def _missing(*_args, **_kwargs):
                raise RuntimeError(
                    "Streamlit is required for UI rendering; install streamlit to use "
                    f"`st.{name}()`"
                )

            return _missing

    st = _StreamlitStub()

try:  # pragma: no cover - optional metadata helper
    from utils.meta import metadata_db_schema
except Exception:  # pragma: no cover - optional metadata helper
    metadata_db_schema = None  # type: ignore


def build_caption(session=None, *, include_metadata: bool = True) -> str:
    """Return a build/version caption with optional metadata context."""
    secrets = getattr(st, "secrets", {})

    def _safe_secret_get(key: str) -> Optional[str]:
        """Return a secret value if available, otherwise ``None``.

        Streamlit raises ``StreamlitSecretNotFoundError`` when the secrets file
        is missing.  In local development we want to gracefully fall back to an
        empty configuration instead of surfacing an exception to the user.
        """

        if secrets is None:
            return None

        if isinstance(secrets, dict) and not secrets:
            return None

        getter = getattr(secrets, "get", None)
        if getter is None:
            return None

        try:
            return getter(key)
        except Exception:
            return None

    build_version = (
        _safe_secret_get("build_version")
        or _safe_secret_get("version")
        or _safe_secret_get("release")
    )
    build_timestamp = _safe_secret_get("build_timestamp") or _safe_secret_get(
        "build_time"
    )

    parts: list[str] = []
    if build_version:
        parts.append(f"Build {build_version}")
    else:
        parts.append("Local development build")

    formatted_ts: Optional[str] = None
    if isinstance(build_timestamp, (int, float)):
        formatted_ts = datetime.fromtimestamp(build_timestamp).strftime(
            "%Y-%m-%d %H:%M %Z"
        )
    elif isinstance(build_timestamp, str) and build_timestamp.strip():
        formatted_ts = build_timestamp.strip()

    if formatted_ts:
        parts.append(f"generated {formatted_ts}")

    if session:
        if include_metadata and metadata_db_schema:
            try:
                db, schema = metadata_db_schema(session)
                parts.append(f"metadata: {db}.{schema}")
            except Exception:  # pragma: no cover - depends on session
                parts.append("metadata: unknown")
        else:
            parts.append("connected to Snowflake session")

    return " · ".join(parts)


def page_header(
    title: str,
    subtitle: str | None = None,
    *,
    session=None,
    show_version: bool = False,
    include_metadata: bool = True,
) -> None:
    """Render a consistent page header with optional subtitle and version."""

    st.title(title)
    if subtitle:
        st.caption(subtitle)
    if show_version:
        st.caption(build_caption(session, include_metadata=include_metadata))


def divider(label: str | None = None) -> None:
    """Insert a subtle horizontal divider with optional caption."""

    st.markdown("<hr class='sf-hr' />", unsafe_allow_html=True)
    if label:
        st.caption(label)


_PILL_STYLES = {
    "info": ("#e5f6fd", "#cbeefb", "#055e86"),
    "success": ("#eafaf0", "#d4f2df", "#0a5c2b"),
    "warning": ("#fef3c7", "#fde68a", "#92400e"),
    "danger": ("#fee2e2", "#fecaca", "#b91c1c"),
}


def pill(text: str, *, tone: str = "info") -> str:
    """Return HTML for a rounded pill badge."""

    bg, border, color = _PILL_STYLES.get(tone, _PILL_STYLES["info"])
    safe_text = html.escape(text)
    return (
        "<span style=\""
        "display:inline-block;padding:.15rem .55rem;border-radius:999px;"
        "font-size:.75rem;font-weight:600;"
        f"background:{bg};border:1px solid {border};color:{color};"
        f"\">{safe_text}</span>"
    )


def danger_note(message: str, *, icon: str = "⚠️") -> None:
    """Render a prominent warning note with a red accent."""

    safe_message = html.escape(message)
    note_html = (
        "<div style=\""
        "border-radius:10px;border:1px solid #fecaca;background:#fef2f2;"
        "padding:.75rem 1rem;color:#991b1b;font-weight:500;"
        "display:flex;gap:.5rem;align-items:flex-start;"
        "\">"
        f"<span>{icon}</span>"
        f"<span>{safe_message}</span>"
        "</div>"
    )
    st.markdown(note_html, unsafe_allow_html=True)
