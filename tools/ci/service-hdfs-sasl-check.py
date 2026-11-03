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
lineitem row has its order) must count its rows, in both modes; never "Connection reset". Then the
22 TPC-H queries over ``spread`` (``tpch.`` replaced by ``spread.``) in complete mode on every node
must equal <dir>/expected/qNN.parquet: a non-mergeable answer is computed on the receiving node,
which lacks some of spread's files and reads them from local copies (item 30 part 2). Schema
``chunks`` (every lineitem file, 1 MiB blocks, replication 1: no node holds a file whole): a full
scan and a GROUP BY must match the local files in both modes on every node, and so must a GROUP BY
with ORDER BY and LIMIT, a DISTINCT and a top-5 scan (item 30: answered per batch of copies and
merged when the cache is smaller than a ticket's copies; in parts the client merges the endpoints). Prints ``ok``/``FAIL`` lines and a last line
``result pass`` or ``result fail <first error>``; exits non-zero on a failure.
"""
import decimal
import glob
import importlib.util
import os
import sys

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "service_cluster_check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          "service-cluster-check.py"))
_CLUSTER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_CLUSTER)
difference, load = _CLUSTER.difference, _CLUSTER.load

SPREAD = "SELECT count(*) AS n, sum(l_quantity) AS q FROM spread.lineitem"
SPLIT = "SELECT count(*) AS n, sum(l_quantity) AS q FROM split.t"
CHUNKS_SCAN = "SELECT l_orderkey, l_quantity FROM chunks.lineitem"
CHUNKS_COUNT = "SELECT count(*) AS n FROM chunks.lineitem"
CHUNKS_GROUP = ("SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS q"
                " FROM chunks.lineitem GROUP BY l_returnflag, l_linestatus")
CHUNKS_FIRST_GROUPS = ("SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS q"
                       " FROM chunks.lineitem GROUP BY l_returnflag, l_linestatus"
                       " ORDER BY l_returnflag, l_linestatus LIMIT 3")
CHUNKS_DISTINCT = "SELECT DISTINCT l_returnflag, l_linestatus FROM chunks.lineitem"
CHUNKS_TOP = ("SELECT l_orderkey, l_linenumber FROM chunks.lineitem"
              " ORDER BY l_orderkey DESC, l_linenumber DESC LIMIT 5")
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


def groups(table):
    """Adds up count/sum rows per (l_returnflag, l_linestatus), merging per-endpoint rows."""
    out = {}
    for r in table.to_pylist():
        key = (r["l_returnflag"], r["l_linestatus"])
        n, q = out.get(key, (0, decimal.Decimal(0)))
        out[key] = (n + int(r["n"] or 0),
                    q + (decimal.Decimal(str(r["q"])) if r["q"] is not None else 0))
    return out


def reference_groups(base):
    """count(*) and sum(l_quantity) per (l_returnflag, l_linestatus) of the local lineitem files."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                          columns=["l_returnflag", "l_linestatus", "l_quantity"])
    agg = table.group_by(["l_returnflag", "l_linestatus"]).aggregate(
        [("l_quantity", "count"), ("l_quantity", "sum")])
    return {(r["l_returnflag"], r["l_linestatus"]):
            (r["l_quantity_count"], decimal.Decimal(str(r["l_quantity_sum"])))
            for r in agg.to_pylist()}


def reference_top(base):
    """The first five (l_orderkey, l_linenumber) of the local lineitem files, both descending."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                          columns=["l_orderkey", "l_linenumber"])
    top = table.sort_by([("l_orderkey", "descending"), ("l_linenumber", "descending")]).slice(0, 5)
    return [(r["l_orderkey"], r["l_linenumber"]) for r in top.to_pylist()]


def same_groups(got, want):
    """Tells whether two per-group count/sum maps agree (sums to 1e-4)."""
    return got.keys() == want.keys() and all(
        got[k][0] == want[k][0] and abs(got[k][1] - want[k][1]) <= decimal.Decimal("0.0001")
        for k in want)


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
            checks = [("split.t", SPLIT), ("split.t join", SPLIT_JOIN)]
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
    # chunks: every lineitem file with 1 MiB blocks and replication 1 (no node holds a file whole,
    # every file is read from a local copy); under speed the copy cache is below one file (item 30
    # part 2b: a scan is copied in batches, each deleted before the next).
    want_groups = reference_groups(base)
    want_top = reference_top(base)
    for server in servers:
        for mode in ("partial", "complete"):
            try:
                with connect(server, mode) as conn:
                    scan = run(conn, CHUNKS_SCAN)
                    n = scan.num_rows
                    q = decimal.Decimal(str(pc.sum(scan.column("l_quantity")).as_py() or 0))
                    got_groups = groups(run(conn, CHUNKS_GROUP))
                    # Answered from the footers (Java client): no local copy (item 30 part 2b).
                    counted = sum(int(r["n"] or 0) for r in run(conn, CHUNKS_COUNT).to_pylist())
                    # In parts every endpoint answers for its own files; the client merges them.
                    got_first = groups(run(conn, CHUNKS_FIRST_GROUPS))
                    got_keys = {(r["l_returnflag"], r["l_linestatus"])
                                for r in run(conn, CHUNKS_DISTINCT).to_pylist()}
                    got_top = sorted(((r["l_orderkey"], r["l_linenumber"])
                                      for r in run(conn, CHUNKS_TOP).to_pylist()), reverse=True)[:5]
                first_keys = sorted(want_groups)[:3]
                if mode == "partial":
                    # An endpoint whose files lack a first group answers a later one as well.
                    got_first = {k: v for k, v in got_first.items() if k in first_keys}
                if not same_groups(got_first, {k: want_groups[k] for k in first_keys}):
                    fail(f"{server} {mode} chunks.lineitem GROUP BY ORDER BY LIMIT 3: {got_first}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem GROUP BY ORDER BY LIMIT 3", flush=True)
                if got_keys != set(want_groups):
                    fail(f"{server} {mode} chunks.lineitem DISTINCT: {sorted(got_keys)}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem DISTINCT ({len(got_keys)} rows)", flush=True)
                if got_top != want_top:
                    fail(f"{server} {mode} chunks.lineitem top 5: {got_top}, want {want_top}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem ORDER BY LIMIT 5", flush=True)
                if counted != want_n:
                    fail(f"{server} {mode} chunks.lineitem count(*): {counted}, want {want_n}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem count(*) ({counted})", flush=True)
                if n != want_n or abs(q - want_q) > decimal.Decimal("0.0001"):
                    fail(f"{server} {mode} chunks.lineitem full scan: rows {n} sum {q},"
                         f" want {want_n} / {want_q}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem full scan ({n} rows)", flush=True)
                if not same_groups(got_groups, want_groups):
                    fail(f"{server} {mode} chunks.lineitem GROUP BY: {got_groups}, want {want_groups}")
                else:
                    print(f"ok {server} {mode} chunks.lineitem GROUP BY ({len(got_groups)} groups)",
                          flush=True)
            except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
                print(f"error text: {e}", flush=True)
                fail(f"{server} {mode} chunks.lineitem: {first_line(e)}")
    queries = load(base)
    with_copies = 0
    for i, server in enumerate(servers):
        with connect(server, "complete") as conn:
            for name, sql, want in queries:
                try:
                    got = run(conn, sql.replace("tpch.", "spread."))
                except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
                    print(f"error text: {e}", flush=True)
                    fail(f"{server} complete spread {name}: {first_line(e)}")
                    continue
                diff = difference(got, want)
                if diff:
                    fail(f"{server} complete spread {name}: wrong answer: {diff}")
                else:
                    with_copies += 1
    print(f"ok complete spread TPC-H: {with_copies} of {len(queries) * len(servers)} answers exact",
          flush=True)
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
