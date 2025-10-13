"""Snapshot tooling for producing .p mirrors of source files."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple
import sys

GLOBS: Tuple[str, ...] = (
    "streamlit_app.py",
    "pages/*.py",
    "utils/*.py",
    "services/*.py",
    "SQL/*.sql",
    "SQL/*.SQL",
)

DIVIDER_TEMPLATE = "===================== FILE: {path} =====================\n\n"


def _collect_files(root: Path) -> List[Path]:
    """Collect all files matching configured globs under the project root."""
    discovered = set()
    for pattern in GLOBS:
        for path in root.glob(pattern):
            if path.is_file():
                try:
                    discovered.add(path.resolve())
                except OSError:
                    continue
    return sorted(discovered, key=lambda p: p.relative_to(root).as_posix())


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError) as exc:
        print(f"Skipping non-text or unreadable file: {path} ({exc})", file=sys.stderr)
        return None


def _write_snapshot_file(root: Path, snapshot_root: Path, source_path: Path, content: str) -> None:
    relative_path = source_path.relative_to(root)
    snapshot_path = (snapshot_root / relative_path).with_suffix(".p")
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(content, encoding="utf-8")


def _write_aggregate(snapshot_root: Path, entries: Iterable[Tuple[Path, str]]) -> None:
    aggregate_path = snapshot_root / "SOURCE_SNAPSHOT.md"
    parts = []
    for relative_path, content in entries:
        parts.append(DIVIDER_TEMPLATE.format(path=relative_path.as_posix()))
        parts.append(content)
        if not content.endswith("\n"):
            parts.append("\n")
        parts.append("\n")
    aggregate_path.write_text("".join(parts), encoding="utf-8")


def create_snapshot(root: Path | None = None) -> Path:
    """Create snapshot mirrors for configured source files.

    Args:
        root: Optional project root path. Defaults to the repository root inferred from
            this file's location.

    Returns:
        Path to the snapshot directory that was written.
    """

    if root is None:
        root = Path(__file__).resolve().parent.parent

    snapshot_root = root / "snapshots"
    snapshot_root.mkdir(parents=True, exist_ok=True)

    entries: List[Tuple[Path, str]] = []
    for source_path in _collect_files(root):
        content = _read_text(source_path)
        if content is None:
            continue
        _write_snapshot_file(root, snapshot_root, source_path, content)
        entries.append((source_path.relative_to(root), content))

    _write_aggregate(snapshot_root, entries)
    return snapshot_root


__all__ = ["create_snapshot", "GLOBS"]
