"""Prepares the Hive cell of service-security.yml: TPC-H files and Hive DDL in four formats plus a view.

Usage: service-hive-data.py <tpch dir> <hive dir>

<tpch dir> is service-tpch-gen.py's output (data/tpch/<table>/data.parquet, queries/). Writes,
under <hive dir> (mounted at the same path in the Hive containers, so table locations are
``file://`` paths both Hive and the server read):

* ``parquet/<table>/data.parquet`` - DuckDB's Parquet file (copied);
* ``text/<table>/data.txt`` - '|' delimited text without header or quoting (Hive's
  LazySimpleSerDe), written by DuckDB;
* ``orc/<table>/``, ``avro/<table>/`` - empty; Hive writes them (``INSERT OVERWRITE ... SELECT``
  from the text tables), so the ORC and Avro files are a real Hive 3's own;
* ``ddl.sql`` - one statement per line: databases ``tpch_parquet``, ``tpch_text``, ``tpch_orc``,
  ``tpch_avro`` (external tables with the columns of the Parquet files in Hive types), the
  inserts, and database ``tpch_views`` with view ``segment_lines`` joining tables of all four
  formats;

and ``<tpch dir>/queries/view.json`` (the view's SQL through the server and DuckDB's answer).
"""
import datetime
import decimal
import json
import os
import shutil
import sys

import duckdb

FORMATS = ("parquet", "text", "orc", "avro")
VIEW = ("CREATE VIEW tpch_views.segment_lines AS SELECT c.c_mktsegment, n.n_name, o.o_orderstatus,"
        " count(*) AS line_count, sum(l.l_quantity) AS quantity, max(l.l_shipdate) AS last_ship"
        " FROM tpch_parquet.customer c JOIN tpch_avro.nation n ON c.c_nationkey = n.n_nationkey"
        " JOIN tpch_text.orders o ON c.c_custkey = o.o_custkey"
        " JOIN tpch_orc.lineitem l ON o.o_orderkey = l.l_orderkey"
        " WHERE l.l_returnflag = 'R' AND n.n_regionkey = 1"
        " GROUP BY c.c_mktsegment, n.n_name, o.o_orderstatus")
VIEW_ANSWER = ("SELECT c.c_mktsegment, n.n_name, o.o_orderstatus, count(*), sum(l.l_quantity),"
               " max(l.l_shipdate) FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey"
               " JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON o.o_orderkey = l.l_orderkey"
               " WHERE l.l_returnflag = 'R' AND n.n_regionkey = 1"
               " GROUP BY c.c_mktsegment, n.n_name, o.o_orderstatus")


def hive_type(duck):
    """Maps a DuckDB column type of the TPC-H files to the Hive type."""
    duck = duck.upper()
    simple = {"INTEGER": "int", "BIGINT": "bigint", "DATE": "date", "VARCHAR": "string",
              "DOUBLE": "double", "SMALLINT": "smallint", "TINYINT": "tinyint", "BOOLEAN": "boolean"}
    if duck in simple:
        return simple[duck]
    if duck.startswith("DECIMAL"):
        return duck.lower()
    raise ValueError(f"no Hive type for {duck}")


def cell(value):
    """Renders a value as service-check.py renders a fetched cell."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (float, decimal.Decimal)):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def main():
    tpch, hive = sys.argv[1], sys.argv[2]
    source = os.path.join(tpch, "data", "tpch")
    db = duckdb.connect()
    ddl = [f"CREATE DATABASE tpch_{f}" for f in FORMATS] + ["CREATE DATABASE tpch_views"]
    inserts = []
    for table in sorted(os.listdir(source)):
        parquet = os.path.join(source, table, "data.parquet")
        db.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{parquet}')")
        columns = ", ".join(f"{name} {hive_type(kind)}" for name, kind, *_ in
                            db.execute(f"DESCRIBE {table}").fetchall())
        for fmt in FORMATS:
            os.makedirs(os.path.join(hive, fmt, table), exist_ok=True)
        shutil.copy(parquet, os.path.join(hive, "parquet", table, "data.parquet"))
        text = os.path.join(hive, "text", table, "data.txt")
        db.execute(f"COPY {table} TO '{text}' (FORMAT csv, DELIMITER '|', HEADER false, QUOTE '',"
                   " ESCAPE '', DATEFORMAT '%Y-%m-%d')")
        with open(text, encoding="utf-8") as f:
            if any("\\" in line for line in f):
                raise ValueError(f"{table}: a backslash in the text, which Hive's serde would read differently")
        location = f"LOCATION 'file://{os.path.join(hive, '{fmt}', table)}'"
        stored = {"parquet": "STORED AS PARQUET",
                  "text": "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE",
                  "orc": "STORED AS ORC", "avro": "STORED AS AVRO"}
        for fmt in FORMATS:
            ddl.append(f"CREATE EXTERNAL TABLE tpch_{fmt}.{table} ({columns}) {stored[fmt]} "
                       + location.replace("{fmt}", fmt))
        for fmt in ("orc", "avro"):
            inserts.append(f"INSERT OVERWRITE TABLE tpch_{fmt}.{table} SELECT * FROM tpch_text.{table}")
    with open(os.path.join(hive, "ddl.sql"), "w", encoding="utf-8") as f:
        f.write("".join(s + ";\n" for s in ddl + inserts + [VIEW]))
    rows = [[cell(v) for v in row] for row in db.execute(VIEW_ANSWER).fetchall()]
    with open(os.path.join(tpch, "queries", "view.json"), "w", encoding="utf-8") as f:
        json.dump({"sql": "SELECT * FROM tpch_views.segment_lines", "answer": rows}, f)
    print(f"{len(ddl)} DDL statements, {len(inserts)} inserts, view rows {len(rows)}")


if __name__ == "__main__":
    main()
