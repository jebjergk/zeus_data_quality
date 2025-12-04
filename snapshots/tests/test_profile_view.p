import time
from concurrent.futures import TimeoutError

import pytest

pd = pytest.importorskip("pandas")

from views import profile_view


def test_prepare_overview_frame_preserves_rule_columns():
    overview = pd.DataFrame(
        [
            {
                "column_name": "orders_total",
                "data_type": "NUMBER",
                "rule_id": "RULE_123",
                "check_type": "NULL_COUNT",
                "severity": "WARN",
                "rationale": "Null ratio too high",
                "confidence": 0.85,
                "has_suggestion": True,
                "include_in_dq_config": False,
            }
        ]
    )

    prepared = profile_view._prepare_overview_frame(overview)

    assert list(prepared.columns) == profile_view._OVERVIEW_INTERNAL_COLUMNS
    assert prepared.loc["orders_total", "rule_id"] == "RULE_123"
    assert prepared.loc["orders_total", "confidence"] == 0.85
    assert prepared.loc["orders_total", "include_in_dq_config"] is False
    assert prepared.loc["orders_total", "has_suggestion"] is True


def test_overview_grid_widget_key_changes_with_nonce():
    key_first = profile_view._overview_grid_widget_key(
        "DB.SCHEMA.TABLE",
        nonce=0,
    )
    key_second = profile_view._overview_grid_widget_key(
        "DB.SCHEMA.TABLE",
        nonce=1,
    )

    assert key_first != key_second
    assert "DB_SCHEMA_TABLE" in key_first


def test_call_with_timeout_completes():
    result, error = profile_view._call_with_timeout(lambda x: x + 1, 1, 2)

    assert result == 3
    assert error is None


def test_call_with_timeout_handles_timeout():
    def slow_call():
        time.sleep(0.05)

    result, error = profile_view._call_with_timeout(slow_call, 0.01)

    assert result is None
    assert isinstance(error, TimeoutError)


