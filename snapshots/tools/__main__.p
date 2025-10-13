"""Entry points for the ``tools`` module."""
from __future__ import annotations

import sys
from pathlib import Path

from . import snapshot_source


def _print_usage() -> None:
    print("Usage: python -m tools snapshot")


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    if not argv or argv[0] != "snapshot":
        _print_usage()
        raise SystemExit(1)

    root = Path(__file__).resolve().parent.parent
    snapshot_dir = snapshot_source.create_snapshot(root)
    relative_snapshot = snapshot_dir.relative_to(root)
    print(f"{relative_snapshot.as_posix()}/")


if __name__ == "__main__":
    main()
