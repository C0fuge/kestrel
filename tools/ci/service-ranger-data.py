"""Writes the answers the Ranger cell of service-security.yml expects: TPC-H under a mask and a row filter.

Usage: service-ranger-data.py <tpch dir> <out dir>

<tpch dir> is service-tpch-gen.py's output. The Ranger cell's policies (service-ranger.sh) give
group analysts select on the TPC-H tables, mask customer.c_phone with MASK_SHOW_LAST_4 and filter
orders with o_orderstatus = 'F'. This script computes, with DuckDB in process and independently
of the server, what those policies leave visible:

* ``<out>/qNN.sql``, ``tables.json`` - copied; ``<out>/qNN.json`` - the 22 answers over the
  masked and filtered tables; ``<out>/prepared.json`` - the prepared query's answer over them;
* ``<out>/extra.json`` - checks for service-check.py ``--extra``: the masked phone numbers of three
  customers, the order count per status (only F remains), and a query of a database without a
  policy that must be refused.

Hive 3.1.3's ``mask_show_last_n(c_phone, 4, 'x', 'x', 'x', -1, '1')`` (Ranger's transformer of
MASK_SHOW_LAST_4) keeps the last 4 characters and replaces every letter and digit before them by
'x'; other characters stay.
"""
import datetime
import decimal
import json
import os
import shutil
import sys

import duckdb

MASKED_PHONE = ("regexp_replace(left(c_phone, greatest(length(c_phone) - 4, 0)), '[A-Za-z0-9]', 'x', 'g')"
                " || right(c_phone, 4)")
ROW_FILTER = "o_orderstatus = 'F'"


def cell(value):
    """Renders a value as service-check.py renders a fetched cell."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (float, decimal.Decimal)):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def answer(db, sql, parameters=None):
    """Runs a query and renders its rows."""
    return [[cell(v) for v in row] for row in db.execute(sql, parameters).fetchall()]


def main():
    tpch, out = sys.argv[1], sys.argv[2]
    source, queries = os.path.join(tpch, "data", "tpch"), os.path.join(tpch, "queries")
    os.makedirs(out, exist_ok=True)
    db = duckdb.connect()
    db.execute("CREATE SCHEMA tpch")
    for table in sorted(os.listdir(source)):
        scan = f"read_parquet('{os.path.join(source, table, 'data.parquet')}')"
        if table == "customer":
            db.execute(f"CREATE VIEW tpch.customer AS SELECT * REPLACE ({MASKED_PHONE} AS c_phone) FROM {scan}")
        elif table == "orders":
            db.execute(f"CREATE VIEW tpch.orders AS SELECT * FROM {scan} WHERE {ROW_FILTER}")
        else:
            db.execute(f"CREATE VIEW tpch.{table} AS SELECT * FROM {scan}")
    shutil.copy(os.path.join(queries, "tables.json"), out)
    for name in sorted(f[:-4] for f in os.listdir(queries) if f.startswith("q") and f.endswith(".sql")):
        with open(os.path.join(queries, name + ".sql"), encoding="utf-8") as f:
            sql = f.read()
        shutil.copy(os.path.join(queries, name + ".sql"), out)
        rows = answer(db, sql.strip().rstrip(";"))
        with open(os.path.join(out, name + ".json"), "w", encoding="utf-8") as f:
            json.dump(rows, f)
        print(f"{name} restricted rows {len(rows)}")
    with open(os.path.join(queries, "prepared.json"), encoding="utf-8") as f:
        prepared = json.load(f)
    prepared["answer"] = answer(db, prepared["sql"], prepared["parameters"])[0][0]
    with open(os.path.join(out, "prepared.json"), "w", encoding="utf-8") as f:
        json.dump(prepared, f)
    phones = "SELECT c_custkey, c_phone FROM tpch.customer WHERE c_custkey <= 3"
    statuses = "SELECT o_orderstatus, count(*) FROM tpch.orders GROUP BY o_orderstatus"
    extra = [
        {"name": "mask", "sql": phones, "answer": answer(db, phones)},
        {"name": "row-filter", "sql": statuses, "answer": answer(db, statuses)},
        {"name": "denied", "sql": "SELECT count(*) FROM tpch_text.nation", "denied": True},
    ]
    if any(not r[1].startswith("xx-") for r in extra[0]["answer"]) or [r[0] for r in extra[1]["answer"]] != ["F"]:
        raise ValueError(f"unexpected restricted answers: {extra[:2]}")
    with open(os.path.join(out, "extra.json"), "w", encoding="utf-8") as f:
        json.dump(extra, f)
    print(f"extra checks: {json.dumps(extra[:2])}")


if __name__ == "__main__":
    main()
