"""Checks a 3-node cluster on HDFS with SASL DataNodes, read short-circuit only (STATUS items 29, 30).

Usage:
  service-hdfs-sasl-check.py <host:port,...> <dir> <split file>

The cell (tools/ci/service-hdfs-cell.sh, topology sasl-cluster) uploads <dir>/data/tpch three times:
``tpch`` with replication 3 (every node holds every file), ``spread`` with replication 2 (each file
whole on two of the three DataNodes) and, as schema ``split``, table ``t``, one lineitem file
<split file> with 1 MiB blocks and replication 1, spread so that no DataNode holds all its blocks.

On every node: ``spread.lineitem`` in parts (header x-flight-result-mode: partial, the client reads
every endpoint from its location) and complete (``distributedComplete=true`` merges it from the
nodes holding the files) must give the row count and sum(l_quantity) of the local files. ``split.t``
(no node holds it whole) is read from a node-local copy (item 30): its count and sum(l_quantity)
must match the local file in both modes, and in parts a join of it with ``spread.orders`` (every
lineitem row has its order) must count its rows; never "Connection reset". Prints ``ok``/``FAIL`` lines and a last line
``result pass`` or ``result fail <first error>``; exits non-zero on a failure.
"""
import decimal
import glob
import os
import sys

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq

SPREAD = "SELECT count(*) AS n, sum(l_quantity) AS q FROM spread.lineitem"
SPLIT = "SELECT count(*) AS n, sum(l_quantity) AS q FROM split.t"
SPLIT_JOIN = ("SELECT count(*) AS n, sum(s.l_quantity) AS q FROM split.t s"
              " JOIN spread.orders o ON s.l_orderkey = o.o_orderkey")


def connect(server, mode):
    """Opens an ADBC Flight SQL connection asking for answers in the given result mode."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": mode}
    return flightsql.connect(f"grpc://{server}", db_kwargs=kwargs)


def run(conn, sql):
    """Runs one query and returns its answer as an Arrow table."""
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetch_arrow_table()


def reference(base, name="*.parquet"):
    """Row count and sum(l_quantity) of the local lineitem files."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", name)),
                          columns=["l_quantity"])
    total = pc.sum(table.column("l_quantity")).as_py()
    return table.num_rows, decimal.Decimal(str(total))


def totals(answer):
    """Adds up the rows of a (possibly per-endpoint) count/sum answer."""
    rows = answer.to_pylist()
    n = sum(int(r["n"] or 0) for r in rows)
    q = sum(decimal.Decimal(str(r["q"])) for r in rows if r["q"] is not None)
    return n, q, len(rows)


def first_line(error):
    """The first line of an error, shortened."""
    return str(error).strip().splitlines()[0][:300] if str(error).strip() else type(error).__name__


def main():
    servers, base, split_file = sys.argv[1].split(","), sys.argv[2], sys.argv[3]
    want_n, want_q = reference(base)
    print(f"reference spread.lineitem: rows {want_n} sum(l_quantity) {want_q}", flush=True)
    split_n, split_q = reference(base, split_file)
    print(f"reference split.t: rows {split_n} sum(l_quantity) {split_q}", flush=True)
    failures = []

    def fail(what):
        failures.append(what)
        print(f"FAIL {what}", flush=True)

    for server in servers:
        for mode in ("partial", "complete"):
            try:
                with connect(server, mode) as conn:
                    n, q, rows = totals(run(conn, SPREAD))
                if n != want_n or abs(q - want_q) > decimal.Decimal("0.0001"):
                    fail(f"{server} {mode} spread.lineitem: rows {n} sum {q} in {rows} row(s),"
                         f" want {want_n} / {want_q}")
                else:
                    print(f"ok {server} {mode} spread.lineitem ({rows} answer row(s))", flush=True)
            except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
                print(f"error text: {e}", flush=True)
                fail(f"{server} {mode} spread.lineitem: {first_line(e)}")
            checks = [("split.t", SPLIT)] + ([("split.t join", SPLIT_JOIN)] if mode == "partial" else [])
            for name, sql in checks:
                try:
                    with connect(server, mode) as conn:
                        n, q, rows = totals(run(conn, sql))
                    if n != split_n or abs(q - split_q) > decimal.Decimal("0.0001"):
                        fail(f"{server} {mode} {name}: rows {n} sum {q} in {rows} row(s),"
                             f" want {split_n} / {split_q}")
                    else:
                        print(f"ok {server} {mode} {name} from a local copy ({rows} answer row(s))",
                              flush=True)
                except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
                    print(f"error text: {e}", flush=True)
                    fail(f"{server} {mode} {name}: {first_line(e)}")
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
