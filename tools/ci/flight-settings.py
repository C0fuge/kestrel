"""Writes the DuckDB settings a Flight SQL server's queries run with, as tpch-duckdb.py does for plain DuckDB.

Usage: flight-settings.py <host:port> <out>

Item 40(a): runs ``SELECT name, value FROM duckdb_settings() ORDER BY name`` through the ADBC Flight SQL
driver and writes one ``name=value`` per line to <out>. When the server refuses the query, <out> holds
one line ``refused: <first line of the error>`` and the script still exits 0 (the measurement goes on).
"""
import sys

import adbc_driver_flightsql.dbapi as flightsql


def main():
    server, out = sys.argv[1], sys.argv[2]
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    try:
        with flightsql.connect(f"grpc://{server}", db_kwargs=kwargs) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT name, value FROM duckdb_settings() ORDER BY name")
            rows = cursor.fetchall()
        lines = [f"{name}={value}" for name, value in rows]
    except Exception as e:  # noqa: BLE001 - any refusal is reported, not raised
        lines = ["refused: " + (str(e).splitlines() or [type(e).__name__])[0]]
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
