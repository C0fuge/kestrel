"""Reads plain Flight descriptors with DuckDB's community airport extension
(airport_take_flight), as a DuckDB user would. The extension must be installed already
(INSTALL airport FROM community). Prints one line: rows and the sum of the column `id` (or of
the first column), or the error.

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
    location = f"grpc://{host}:{port}"
    if kind == "command":
        # A VARCHAR descriptor is a command descriptor in airport_take_flight.
        descriptor = literal(target)
    else:
        descriptor = "[" + ", ".join(literal(p) for p in target.split("/")) + "]"
    source = f"airport_take_flight({literal(location)}, {descriptor})"
    column = con.execute(f"SELECT * FROM {source} LIMIT 0").description[0][0]
    rows, total = con.execute(
        f'SELECT count(*), coalesce(sum("{column}"), 0) FROM {source}').fetchone()
    print(f"rows={rows} sum={total}")


if __name__ == "__main__":
    main()
