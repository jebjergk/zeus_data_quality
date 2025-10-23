"""Utilities for semantic inference helpers."""

from __future__ import annotations

from typing import Optional

__all__ = ["clamp_confidence", "truncate_note"]


def clamp_confidence(value: Optional[float], minimum: float, maximum: float = 0.98) -> Optional[float]:
    """Clamp a confidence score between minimum and maximum bounds."""

    if value is None:
        return None
    return max(minimum, min(maximum, value))


def truncate_note(text: Optional[str], max_length: int = 120) -> str:
    """Trim notes to a maximum length, appending ellipsis if needed."""

    value = (text or "").strip()
    if not value:
        return ""
    if len(value) <= max_length:
        return value
    if max_length <= 3:
        return value[:max_length]
    return value[: max_length - 3].rstrip() + "..."

