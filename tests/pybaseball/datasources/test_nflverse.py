import os
import tempfile
from pathlib import Path

import pytest

from pybaseball.datasources import (
    NFLVERSE_DATABASE_PATH,
    NFLverseConnectionError,
    connect_nflverse,
    nflverse_connection,
)


def test_connect_requires_duckdb(monkeypatch):
    monkeypatch.setattr("pybaseball.datasources.nflverse.duckdb", None, raising=False)
    with pytest.raises(NFLverseConnectionError) as excinfo:
        connect_nflverse(NFLVERSE_DATABASE_PATH)
    assert "duckdb" in str(excinfo.value)


@pytest.mark.parametrize("read_only", [True, False])
def test_connect_to_temp_database(read_only: bool):
    duckdb = pytest.importorskip("duckdb")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS example (id INTEGER)")
            conn.execute("INSERT INTO example VALUES (1)")

        conn = connect_nflverse(str(db_path), read_only=read_only)
        try:
            result = conn.execute("SELECT SUM(id) FROM example").fetchone()[0]
            assert result == 1
        finally:
            conn.close()


@pytest.mark.parametrize("context_manager", [False, True])
def test_missing_database_raises(context_manager: bool):
    with tempfile.TemporaryDirectory() as tmpdir:
        missing = Path(tmpdir) / "missing.duckdb"

        if context_manager:
            with pytest.raises(NFLverseConnectionError) as excinfo:
                with nflverse_connection(str(missing)):
                    pass
        else:
            with pytest.raises(NFLverseConnectionError) as excinfo:
                connect_nflverse(str(missing))

        message = str(excinfo.value)
        assert "nflverse" in message
        if "duckdb" in message and "required" in message:
            pytest.skip("duckdb dependency unavailable in test environment")
        assert os.fspath(missing) in message
