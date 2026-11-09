"""Generates the TPC-H input and the expected answers of service-cluster.yml (lane 3, SERVICE MATRIX).

Usage: service-cluster-data.py <out dir> <scale factor> <files> [mod|range]

Writes <out>/data/tpch/<table>/part-NNN.parquet (lineitem and orders split into <files> files so
that every node of the cluster plans endpoints, the other tables one file each; ``mod``, the
default, puts key % files into each file, ``range`` gives each file one range of keys, so that
footer min/max tell the files apart - hdfs-point-perf.yml, item 39 (c)),
<out>/queries/qNN.sql (DuckDB's 22 TPC-H queries, tables qualified with the schema ``tpch``, q11's
fraction 0.0001 / SF as the spec has it) and <out>/expected/qNN.parquet: the answer of each query
computed by in-process DuckDB over the same Parquet files - the reference service-cluster-check.py
compares the server's answers with. Also <out>/expected/prepared.txt: the answer of the prepared
query the checker binds (``SELECT count(*) FROM tpch.lineitem WHERE l_quantity < ?`` with 24).
"""
import os
import re
import sys

import duckdb
import pyarrow.parquet as pq

PREPARED = "SELECT count(*) FROM tpch.lineitem WHERE l_quantity < ?"
PREPARED_VALUE = 24


def main():
    out, sf, files = sys.argv[1], float(sys.argv[2]), int(sys.argv[3])
    split = sys.argv[4] if len(sys.argv) > 4 else "mod"
    if split not in ("mod", "range"):
        sys.exit(f"split must be mod or range: {split}")
    data, queries, expected = (os.path.join(out, d) for d in ("data/tpch", "queries", "expected"))
    for d in (queries, expected):
        os.makedirs(d, exist_ok=True)
    gen = duckdb.connect()
    gen.execute("INSTALL tpch; LOAD tpch")
    gen.execute(f"CALL dbgen(sf={sf})")
    tables = [r[0] for r in gen.execute("SHOW TABLES").fetchall()]
    key = {"lineitem": "l_orderkey", "orders": "o_orderkey"}
    for t in tables:
        os.makedirs(os.path.join(data, t), exist_ok=True)
        parts = files if t in key else 1
        top = gen.execute(f"SELECT max({key[t]}) FROM {t}").fetchone()[0] if parts > 1 else 0
        for f in range(parts):
            if parts == 1:
                where = ""
            elif split == "range":
                where = f" WHERE ({key[t]} - 1) * {parts} // {top} = {f} ORDER BY {key[t]}"
            else:
                where = f" WHERE {key[t]} % {parts} = {f}"
            path = os.path.join(data, t, f"part-{f:03d}.parquet")
            gen.execute(f"COPY (SELECT * FROM {t}{where}) TO '{path}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    texts = gen.execute("SELECT query_nr, query FROM tpch_queries()").fetchall()
    gen.close()

    # The reference: DuckDB over the written files, not over dbgen's tables.
    ref = duckdb.connect()
    ref.execute("CREATE SCHEMA tpch")
    for t in tables:
        ref.execute(f"CREATE VIEW tpch.{t} AS SELECT * FROM read_parquet('{os.path.join(data, t)}/*.parquet')")
    pattern = re.compile(r"(\b(?:from|join)\s+|,\s*)(" + "|".join(tables) + r")\b", re.IGNORECASE)
    for nr, q in texts:
        if nr == 11:
            assert "0.0001" in q, q
            q = q.replace("0.0001000000", "0.0001").replace("0.0001", f"{0.0001 / sf:.10f}")
        q = pattern.sub(lambda m: m.group(1) + "tpch." + m.group(2), q)
        with open(os.path.join(queries, f"q{nr:02d}.sql"), "w", encoding="utf-8") as fh:
            fh.write(q)
        table = ref.execute(q).fetch_arrow_table()
        pq.write_table(table, os.path.join(expected, f"q{nr:02d}.parquet"))
        print(f"q{nr:02d} reference rows {table.num_rows}")
    count = ref.execute(PREPARED, [PREPARED_VALUE]).fetchone()[0]
    with open(os.path.join(expected, "prepared.txt"), "w", encoding="utf-8") as fh:
        fh.write(str(count))
    print(f"prepared reference {count}")


if __name__ == "__main__":
    main()
