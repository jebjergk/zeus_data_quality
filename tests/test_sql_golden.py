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


def test_run_full_profile_invokes_procedure():
    session = RecordingSession([None])
    profiling_v2.run_full_profile(session, 'db.schema.table')

    assert session.calls, "Stored procedure call was not recorded"
    sql, params = session.calls[0]
    assert "DQ_PROFILE_FULL" in sql
    assert params == ["DB.SCHEMA.TABLE"]
    assert session.responses == []  # responses consumed


def test_run_full_profile_requires_table():
    with pytest.raises(profiling_v2.ProfilingError):
        profiling_v2.run_full_profile(RecordingSession([]), '')


def test_fetch_table_summary_returns_latest_row():
    frame = pd.DataFrame(
        [
            {"TABLE_FQN": "DB.SCHEMA.TABLE", "PROFILED_AT": "2024-05-01", "ROW_COUNT": 100},
            {"TABLE_FQN": "DB.SCHEMA.TABLE", "PROFILED_AT": "2024-05-03", "ROW_COUNT": 250},
        ]
    )
    session = RecordingSession([frame])

    summary = profiling_v2.fetch_table_summary(session, 'db.schema.table')

    assert summary["ROW_COUNT"] == 250
    assert summary["PROFILED_AT"] == "2024-05-03"


def test_fetch_column_features_formats_when_no_table():
    df = profiling_v2.fetch_column_features(RecordingSession([]), '')
    assert df.empty
