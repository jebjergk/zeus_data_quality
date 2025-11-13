import pytest

pd = pytest.importorskip("pandas")

from services import profiling_v2


class DummyStatement:
    def __init__(self, frame: pd.DataFrame | None = None):
        self.frame = frame if frame is not None else pd.DataFrame()
        self.collected = False

    def collect(self):
        self.collected = True
        return []

    def to_pandas(self):
        return self.frame.copy()


class RecordingSession:
    def __init__(self, responses: list[pd.DataFrame | None]):
        self.responses = list(responses)
        self.calls = []

    def sql(self, sql: str, params=None):  # pragma: no cover - trivial passthrough
        self.calls.append((sql, params))
        frame = self.responses.pop(0) if self.responses else None
        return DummyStatement(frame)


def test_run_profiling_v2_invokes_procedure():
    session = RecordingSession([None])
    profiling_v2.run_profiling_v2(session, 'db.schema.table')

    assert session.calls, "Stored procedure call was not recorded"
    sql, params = session.calls[0]
    assert "DQ_PROFILE_FULL" in sql
    assert params == ["DB.SCHEMA.TABLE"]
    assert session.responses == []  # responses consumed


def test_run_profiling_v2_requires_table():
    with pytest.raises(profiling_v2.ProfilingError):
        profiling_v2.run_profiling_v2(RecordingSession([]), '')


def test_get_table_profile_summary_returns_frame():
    frame = pd.DataFrame(
        [
            {"TABLE_FQN": "DB.SCHEMA.TABLE", "PROFILED_AT": "2024-05-01", "ROW_COUNT": 100},
            {"TABLE_FQN": "DB.SCHEMA.TABLE", "PROFILED_AT": "2024-05-03", "ROW_COUNT": 250},
        ]
    )
    session = RecordingSession([frame])

    summary_frame = profiling_v2.get_table_profile_summary(session, 'db.schema.table')

    assert list(summary_frame["ROW_COUNT"]) == [100, 250]
    assert summary_frame.shape == (2, 3)


def test_get_column_features_returns_empty_when_no_table():
    df = profiling_v2.get_column_features(RecordingSession([]), '')
    assert df.empty
