"""Reads plain Flight descriptors with DuckDB's community airport extension
(airport_take_flight), as a DuckDB user would. The extension must be installed already
(INSTALL airport FROM community). Prints the DuckDB version, then the column names airport got
from the server, then either rows and the sum of the first column or the read error.

Usage: airport_client.py <host> <port> <command|path> <sql text or dotted table name>
"""
import sys

import duckdb


def literal(text):
    """Returns text as a DuckDB string literal."""
    return "'" + text.replace("'", "''") + "'"


def main():
    host, port, kind, target = sys.argv[1:5]
    con = duckdb.connect()
    con.execute("LOAD airport")
    print(f"duckdb={duckdb.__version__}")
    if kind == "command":
        # A VARCHAR descriptor is a command descriptor in airport_take_flight.
        descriptor = literal(target)
    else:
        descriptor = "[" + ", ".join(literal(p) for p in target.split("/")) + "]"
    source = f"airport_take_flight({literal(f'grpc://{host}:{port}')}, {descriptor})"
    columns = [d[0] for d in con.execute(f"SELECT * FROM {source} LIMIT 0").description]
    print("columns=" + ",".join(columns))
    try:
        rows, total = con.execute(
            f'SELECT count(*), coalesce(sum("{columns[0]}"), 0) FROM {source}').fetchone()
        print(f"rows={rows} sum={total}")
    except duckdb.Error as e:
        print(f"error={e}")


if __name__ == "__main__":
    main()
