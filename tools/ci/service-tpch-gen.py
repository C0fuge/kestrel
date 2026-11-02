"""Generates TPC-H data, the 22 queries and DuckDB's own answers for the service workflows.

Usage: service-tpch-gen.py <out dir> [scale factor, default 1]

Writes <out>/data/tpch/<table>/data.parquet (the server's data directory is <out>/data, so the
tables are tpch.<table>), <out>/queries/qNN.sql (DuckDB's TPC-H queries with tpch-qualified
tables; q11's fraction scaled as the spec says) and <out>/queries/qNN.json (the answer DuckDB
computes in process, cells as service-check.py renders them). Used by service-security.yml.
"""
import datetime
import decimal
import json
import os
import re
import sys

import duckdb


def cell(value):
    """Renders a value as JSON the way service-check.py renders a fetched cell."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (float, decimal.Decimal)):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def main():
    out = sys.argv[1]
    sf = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    data, queries = os.path.join(out, "data", "tpch"), os.path.join(out, "queries")
    os.makedirs(queries, exist_ok=True)
    db = duckdb.connect()
    db.execute("INSTALL tpch; LOAD tpch")
    db.execute(f"CALL dbgen(sf={sf})")
    tables = [r[0] for r in db.execute("SHOW TABLES").fetchall()]
    for t in tables:
        os.makedirs(os.path.join(data, t), exist_ok=True)
        db.execute(f"COPY {t} TO '{os.path.join(data, t, 'data.parquet')}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    with open(os.path.join(queries, "tables.json"), "w", encoding="utf-8") as f:
        json.dump(sorted(tables), f)
    pattern = re.compile(r"(\b(?:from|join)\s+|,\s*)(" + "|".join(tables) + r")\b", re.IGNORECASE)
    for nr, q in db.execute("SELECT query_nr, query FROM tpch_queries()").fetchall():
        if nr == 11:
            q = q.replace("0.0001000000", "0.0001").replace("0.0001", f"{0.0001 / sf:.10f}")
        rows = [[cell(v) for v in row] for row in db.execute(q).fetchall()]
        with open(os.path.join(queries, f"q{nr:02d}.sql"), "w", encoding="utf-8") as f:
            f.write(pattern.sub(lambda m: m.group(1) + "tpch." + m.group(2), q))
        with open(os.path.join(queries, f"q{nr:02d}.json"), "w", encoding="utf-8") as f:
            json.dump(rows, f)
        print(f"q{nr:02d} duckdb rows {len(rows)}")
    count = db.execute("SELECT count(*) FROM orders WHERE o_orderkey < 10000 AND o_orderstatus = 'F'").fetchone()[0]
    with open(os.path.join(queries, "prepared.json"), "w", encoding="utf-8") as f:
        json.dump({"sql": "SELECT count(*) FROM tpch.orders WHERE o_orderkey < ? AND o_orderstatus = ?",
                   "parameters": [10000, "F"], "answer": count}, f)


if __name__ == "__main__":
    main()
