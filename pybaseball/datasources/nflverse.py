"""Utilities for accessing the shared nflverse DuckDB database.

The evaluation environment exposes a read-only DuckDB database mounted at
``/nflverse``.  This module provides a small wrapper that validates the
connection parameters and offers a convenient context manager for accessing
that database.  When the database is unavailable, :class:`NFLverseConnectionError`
provides a clear, actionable error message for the caller.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator, Optional

try:  # pragma: no cover - exercised indirectly in tests
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - handled explicitly
    duckdb = None  # type: ignore[assignment]

DEFAULT_DATABASE_PATH = "/nflverse"


class NFLverseConnectionError(RuntimeError):
    """Raised when a connection to the nflverse database cannot be established."""


def _normalise_path(database_path: Optional[str]) -> str:
    """Return a usable DuckDB database path.

    ``None`` resolves to :data:`DEFAULT_DATABASE_PATH`.  Absolute paths are
    preserved as-is while relative paths are expanded against the current
    working directory.  ``""`` is rejected to surface misconfigurations.
    """

    if database_path is None:
        path = DEFAULT_DATABASE_PATH
    else:
        path = os.fspath(database_path)

    if not path:
        raise ValueError("database_path must not be an empty string")

    if path != ":memory:":
        path = os.path.abspath(os.path.expanduser(path))

    return path


def connect(database_path: Optional[str] = None, *, read_only: bool = True):
    """Return a DuckDB connection to the nflverse dataset.

    Parameters
    ----------
    database_path:
        A filesystem path to a DuckDB database.  ``None`` uses the standard
        ``/nflverse`` mount provided in the execution environment.
    read_only:
        When ``True`` (the default) the connection is opened in read-only mode
        to guard the shared dataset from accidental modification.

    Raises
    ------
    NFLverseConnectionError
        If the ``duckdb`` dependency is missing or the database cannot be
        opened.
    """

    if duckdb is None:  # pragma: no cover - validated via tests
        raise NFLverseConnectionError(
            "The 'duckdb' package is required to connect to the nflverse dataset."
            " Install it with `pip install duckdb`."
        )

    path = _normalise_path(database_path)

    if read_only and path != ":memory:" and not os.path.exists(path):
        raise NFLverseConnectionError(
            f"The nflverse database was not found at '{path}'. "
            "Make sure the dataset is available and the path is correct."
        )

    try:
        return duckdb.connect(path, read_only=read_only)
    except Exception as exc:  # pragma: no cover - defensive, exercised in tests
        raise NFLverseConnectionError(
            f"Unable to connect to the nflverse database at '{path}'."
        ) from exc


@contextmanager
def nflverse_connection(
    database_path: Optional[str] = None,
    *,
    read_only: bool = True,
) -> Iterator["duckdb.DuckDBPyConnection"]:
    """Context manager wrapper around :func:`connect`.

    The connection is automatically closed once the context exits, even if an
    exception is raised by the caller.
    """

    connection = connect(database_path=database_path, read_only=read_only)
    try:
        yield connection
    finally:
        connection.close()


__all__ = [
    "DEFAULT_DATABASE_PATH",
    "NFLverseConnectionError",
    "connect",
    "nflverse_connection",
]
