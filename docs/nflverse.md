# nflverse DuckDB integration

Some environments that run `pybaseball` expose a shared DuckDB database at
`/nflverse` containing supplemental play-by-play datasets.  The new
`pybaseball.datasources.nflverse` module provides convenience helpers for
connecting to that database without manually handling connection lifecycle
management.

## Opening a connection

```python
from pybaseball.datasources import connect_nflverse

connection = connect_nflverse()
try:
    # connection is a duckdb.DuckDBPyConnection pointing at /nflverse
    print(connection.execute("SHOW TABLES").fetchall())
finally:
    connection.close()
```

The helper validates that the `duckdb` dependency is available and that the
`/nflverse` mount exists.  If either precondition is not met a clear
`NFLverseConnectionError` is raised.

For scripts that prefer the context-manager pattern you can use
`nflverse_connection`, which automatically closes the connection:

```python
from pybaseball.datasources import nflverse_connection

with nflverse_connection() as connection:
    df = connection.execute("SELECT * FROM some_table LIMIT 5").df()
```

