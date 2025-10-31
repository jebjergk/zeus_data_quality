from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class Snapshotter:
    """Simple snapshot helper with opt-in updates via UPDATE_SNAPSHOTS."""

    def __init__(self, request: pytest.FixtureRequest) -> None:
        self._request = request
        self._dir = Path(__file__).parent / "__snapshots__"
        self._dir.mkdir(parents=True, exist_ok=True)

    def assert_match(self, data: Any, name: str = "snapshot") -> None:
        path = self._dir / f"{self._request.node.name}__{name}.json"
        serialized = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        if os.getenv("UPDATE_SNAPSHOTS"):
            path.write_text(serialized, encoding="utf-8")
            return
        if not path.exists():
            pytest.fail(
                "Snapshot does not exist. Run with UPDATE_SNAPSHOTS=1 to create it."
            )
        existing = path.read_text(encoding="utf-8")
        assert (
            existing == serialized
        ), "Snapshot mismatch. Run with UPDATE_SNAPSHOTS=1 to update intentionally."


@pytest.fixture
def snapshot(request: pytest.FixtureRequest) -> Snapshotter:
    return Snapshotter(request)
