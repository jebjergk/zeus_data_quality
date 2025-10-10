"""Utilities for working with Snowflake SQL sessions.

This module previously existed in the project and downstream pages still
import :class:`SnowflakeSQLService`.  The class disappeared during a refactor
which leaves Streamlit unable to import the module at runtime.  The
implementation below restores the public surface area that other modules rely
on while keeping the logic lightweight so it can operate both inside and
outside a Snowflake-connected environment.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

try:  # pragma: no cover - optional dependency
    from snowflake.snowpark import Session
    from snowflake.snowpark.context import get_active_session
except Exception:  # pragma: no cover - executed when snowpark is unavailable
    Session = Any  # type: ignore

    def get_active_session() -> Optional[Any]:  # type: ignore
        """Return ``None`` when Snowpark is not installed."""

        return None


@dataclass
class SnowflakeSQLService:
    """Thin wrapper around the active Snowflake Snowpark session.

    The service is intentionally small: it only deals with acquiring and
    caching a session and offers a couple of helpers that the Streamlit pages
    can reuse.  When the application runs outside Snowflake (for example during
    local development) we gracefully fall back to ``None`` instead of raising
    an import error.  Callers can inspect :pyattr:`session` or use
    :py:meth:`ensure_session` to guard Snowflake specific paths.
    """

    session: Optional[Session] = field(default=None)

    def __post_init__(self) -> None:
        """Populate :pyattr:`session` if one is not supplied explicitly."""

        if self.session is None:
            try:
                self.session = get_active_session()
            except Exception:  # pragma: no cover - depends on Snowflake runtime
                self.session = None

    # Public helpers -----------------------------------------------------
    def has_session(self) -> bool:
        """Return ``True`` when a Snowflake session is available."""

        return self.session is not None

    def ensure_session(self) -> Session:
        """Return the active Snowflake session or raise a helpful error."""

        if self.session is None:
            raise RuntimeError("No active Snowflake session is available.")
        return self.session

    def run_sql(self, statement: str) -> Iterable[Any]:
        """Execute ``statement`` using the underlying Snowflake session.

        The helper mirrors the behaviour of ``Session.sql(...).collect()`` so
        pages can run lightweight queries without repeating boilerplate.  An
        empty iterator is returned when no session is active which keeps local
        development paths defensive.
        """

        if not statement:
            raise ValueError("SQL statement must be a non-empty string.")

        if self.session is None:
            return []

        df = self.session.sql(statement)
        try:
            return df.collect()
        except Exception:
            # Surface query issues to the caller while keeping the stack clean.
            raise

    def current_role(self) -> Optional[str]:
        """Attempt to fetch the current role from Snowflake.

        The method mirrors existing behaviour that UI components expect when
        rendering connection metadata.  ``None`` is returned if no active
        session is present or when the query fails.
        """

        if self.session is None:
            return None
        try:
            row = self.session.sql("SELECT CURRENT_ROLE()").collect()[0]
            if hasattr(row, "asDict"):
                data: Dict[str, Any] = row.asDict()
                return data.get("CURRENT_ROLE()") or data.get("CURRENT_ROLE")
            return row[0]
        except Exception:
            return None


__all__ = ["SnowflakeSQLService"]
