"""Tests for helpers in :mod:`utils.meta`."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, List

import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import meta


class FakeRow(dict):
    """Dictionary-backed row providing a Snowpark-like ``asDict``."""

    def asDict(self):  # noqa: N802 - mimic Snowpark API casing
        return self


class FakeResult:
    """Object returning canned rows for ``collect``."""

    def __init__(self, rows: List[Any]):
        self._rows = rows

    def collect(self):
        return self._rows


class FakeSession:
    """Minimal stub of a Snowpark session for metadata queries."""

    def __init__(self, rows: List[Any]):
        self.rows = rows
        self.queries: list[tuple[str, Any]] = []

    def sql(self, statement: str, params: Any | None = None):
        self.queries.append((statement.strip(), params))
        if "SELECT" in statement and "FROM" in statement:
            return FakeResult(self.rows)
        return FakeResult([])


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        (True, True),
        (0, False),
        (1, True),
        ("yes", True),
        ("No", False),
        (" 1 ", True),
        ("0", False),
        (object(), True),
    ],
)
def test_coerce_bool(value, expected):
    assert meta._coerce_bool(value) == expected


def test_coerce_bool_with_default():
    assert meta._coerce_bool_with_default(None, True) is True
    assert meta._coerce_bool_with_default(None, False) is False
    assert meta._coerce_bool_with_default("YES", False) is True
    assert meta._coerce_bool_with_default("no", True) is False


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        (date(2024, 12, 31), date(2024, 12, 31)),
        (datetime(2024, 1, 2, 3, 4, 5), date(2024, 1, 2)),
        ("2024-05-01", date(2024, 5, 1)),
        ("2024-05-01T12:30:00", date(2024, 5, 1)),
        ("", None),
        ("not-a-date", None),
    ],
)
def test_coerce_date(value, expected):
    assert meta._coerce_date(value) == expected


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("DB.SCHEMA.TABLE", ("DB", "SCHEMA", "TABLE")),
        ("SCHEMA.TABLE", (None, "SCHEMA", "TABLE")),
        ("TABLE", (None, None, "TABLE")),
    ],
)
def test_parse_relation_name(identifier, expected):
    assert meta._parse_relation_name(identifier) == expected


def test_list_configs_coerces_schedule(monkeypatch):
    rows = [
        FakeRow(
            {
                "CONFIG_ID": "cfg1",
                "NAME": "Config One",
                "DESCRIPTION": None,
                "TARGET_TABLE_FQN": "db.schema.table1",
                "RUN_AS_ROLE": None,
                "DMF_ROLE": None,
                "STATUS": "ACTIVE",
                "OWNER": "data_eng",
                "SCHEDULE_CRON": None,
                "SCHEDULE_TIMEZONE": None,
                "SCHEDULE_ENABLED": None,
            }
        ),
        FakeRow(
            {
                "CONFIG_ID": "cfg2",
                "NAME": "Config Two",
                "DESCRIPTION": None,
                "TARGET_TABLE_FQN": "db.schema.table2",
                "RUN_AS_ROLE": None,
                "DMF_ROLE": None,
                "STATUS": "DRAFT",
                "OWNER": "data_ops",
                "SCHEDULE_CRON": None,
                "SCHEDULE_TIMEZONE": None,
                "SCHEDULE_ENABLED": "false",
            }
        ),
    ]

    session = FakeSession(rows)
    monkeypatch.setattr(meta, "ensure_meta_tables", lambda session: None)

    configs = meta.list_configs(session)

    assert [cfg.config_id for cfg in configs] == ["cfg1", "cfg2"]
    assert configs[0].schedule_enabled is True
    assert configs[1].schedule_enabled is False


def test_get_config_schedule_enabled_default(monkeypatch):
    row = FakeRow(
        {
            "CONFIG_ID": "cfg3",
            "NAME": "Config Three",
            "DESCRIPTION": None,
            "TARGET_TABLE_FQN": "db.schema.table3",
            "RUN_AS_ROLE": None,
            "DMF_ROLE": None,
            "STATUS": "ACTIVE",
            "OWNER": "analyst",
            "SCHEDULE_CRON": None,
            "SCHEDULE_TIMEZONE": None,
            "SCHEDULE_ENABLED": None,
        }
    )

    session = FakeSession([row])

    cfg = meta.get_config(session, "cfg3")

    assert cfg is not None
    assert cfg.config_id == "cfg3"
    assert cfg.schedule_enabled is True
