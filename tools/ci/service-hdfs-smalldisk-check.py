"""Checks one server whose node-local copy disk is smaller than a query's input (STATUS item 30).

Usage:
  service-hdfs-smalldisk-check.py <host:port> <dir>

The sasl-cluster cell (tools/ci/service-hdfs-cell.sh, profile balanced) stops its three servers and
starts one server in dn1's network namespace whose ``hdfsLocalCopyDir`` is a size-limited tmpfs
holding the largest lineitem file plus ``hdfsLocalCopyMinFreeBytes`` and a little more, so less than
``chunks.lineitem`` (every lineitem file, 1 MiB blocks, replication 1: no DataNode holds a file
whole, every file is read from a copy). The cache limit is left at its default (far above the
disk), so only the free-space guard bounds the copies:

* in parts, a full scan and a GROUP BY over ``chunks.lineitem`` must match the local files: the
  ticket's copies go in batches sized by the disk's room, unused copies evicted for the next batch;
* in complete mode a join of ``chunks.lineitem`` with ``spread.orders`` needs every copy at once:
  it must either be answered exactly or be refused with the guard's message ("No room on the disk
  of the node-local copy cache", naming a file) - never "No space left on device", a short copy or
  "Connection reset";
* afterwards the GROUP BY in parts must still be answered (a refusal leaves nothing reserved);
* in parts, a GROUP BY with HAVING and a count over DISTINCT values (shapes only one server
  answers) must match the local files: merged from the disk-sized batches (``merge=rewrite``);
* item 31 (5), (6), (8): in complete mode a full scan and a GROUP BY over ``chunks.lineitem``, and
  with PLAIN names a count/sum, a filtered count and a GROUP BY (also with ORDER BY and LIMIT) over
  ``lineitem_p`` (schema ``plain``, also never held whole by a node) must match the local files:
  the one table's copies go in disk-sized batches (``hdfs=completeInCopyBatches``) instead of
  being refused; the plain GROUP BY in parts must match too.

Prints ``ok``/``FAIL`` lines, ``join refused`` or ``join answered`` and a last line ``result pass``
or ``result fail <first error>``; exits non-zero on a failure.
"""
import decimal
import glob
import importlib.util
import os
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "service_hdfs_sasl_check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                            "service-hdfs-sasl-check.py"))
_SASL = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_SASL)

CHUNKS_JOIN = ("SELECT count(*) AS n, sum(c.l_quantity) AS q FROM chunks.lineitem c"
               " JOIN spread.orders o ON c.l_orderkey = o.o_orderkey")
REFUSAL = "No room on the disk of the node-local copy cache"
HAVING = ("SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS q,"
          " count(DISTINCT l_shipmode) AS m FROM chunks.lineitem"
          " GROUP BY l_returnflag, l_linestatus HAVING sum(l_quantity) > {bound}")


def reference_having(base):
    """Per (l_returnflag, l_linestatus): count, sum(l_quantity), distinct l_shipmode, and a bound."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                          columns=["l_returnflag", "l_linestatus", "l_quantity", "l_shipmode"])
    agg = table.group_by(["l_returnflag", "l_linestatus"]).aggregate(
        [("l_quantity", "count"), ("l_quantity", "sum"), ("l_shipmode", "count_distinct")])
    rows = {(r["l_returnflag"], r["l_linestatus"]):
            (r["l_quantity_count"], decimal.Decimal(str(r["l_quantity_sum"])),
             r["l_shipmode_count_distinct"]) for r in agg.to_pylist()}
    sums = sorted(v[1] for v in rows.values())
    # Between the smallest and the largest group sum: some groups pass, some do not.
    bound = (sums[0] + sums[-1]) / 2
    return bound, {k: v for k, v in rows.items() if v[1] > bound}


PLAIN_GROUP = ("SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS q"
               " FROM lineitem_p GROUP BY l_returnflag, l_linestatus")
PLAIN_FIRST_GROUPS = PLAIN_GROUP + " ORDER BY l_returnflag, l_linestatus LIMIT 3"
PLAIN_FILTERED = "SELECT count(*) AS n, sum(l_quantity) AS q FROM lineitem_p WHERE l_quantity > {bound}"


def reference_filtered(base, bound):
    """Row count and sum(l_quantity) of the local lineitem rows with l_quantity above a bound."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                          columns=["l_quantity"])
    kept = [decimal.Decimal(str(v)) for v in table.column("l_quantity").to_pylist()
            if v is not None and decimal.Decimal(str(v)) > bound]
    return len(kept), sum(kept, decimal.Decimal(0))


def main():
    server, base = sys.argv[1], sys.argv[2]
    want_n, want_q = _SASL.reference(base)
    want_groups = _SASL.reference_groups(base)
    print(f"reference chunks.lineitem: rows {want_n} sum(l_quantity) {want_q}", flush=True)
    failures = []

    def fail(what):
        failures.append(what)
        print(f"FAIL {what}", flush=True)

    def scan_and_group(when):
        try:
            with _SASL.connect(server, "partial") as conn:
                scan = _SASL.run(conn, _SASL.CHUNKS_SCAN)
                q = decimal.Decimal(str(pc.sum(scan.column("l_quantity")).as_py() or 0))
                got = _SASL.groups(_SASL.run(conn, _SASL.CHUNKS_GROUP))
            if scan.num_rows != want_n or abs(q - want_q) > decimal.Decimal("0.0001"):
                fail(f"{when} partial chunks.lineitem full scan: rows {scan.num_rows} sum {q},"
                     f" want {want_n} / {want_q}")
            else:
                print(f"ok {when} partial chunks.lineitem full scan ({scan.num_rows} rows)", flush=True)
            if not _SASL.same_groups(got, want_groups):
                fail(f"{when} partial chunks.lineitem GROUP BY: {got}, want {want_groups}")
            else:
                print(f"ok {when} partial chunks.lineitem GROUP BY ({len(got)} groups)", flush=True)
        except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
            print(f"error text: {e}", flush=True)
            fail(f"{when} partial chunks.lineitem: {_SASL.first_line(e)}")

    scan_and_group("before")
    try:
        with _SASL.connect(server, "complete") as conn:
            n, q, _ = _SASL.totals(_SASL.run(conn, CHUNKS_JOIN))
        if n != want_n or abs(q - want_q) > decimal.Decimal("0.0001"):
            fail(f"complete chunks join: rows {n} sum {q}, want {want_n} / {want_q}")
        else:
            print("join answered: complete chunks join exact", flush=True)
    except Exception as e:  # noqa: BLE001 - only the guard's refusal is accepted
        text = str(e)
        print(f"error text: {text}", flush=True)
        # ADBC has no resource-exhausted status (it prints INTERNAL); the gRPC status follows in
        # parentheses and must be ResourceExhausted, not Internal.
        if REFUSAL in text and ".parquet" in text and "(ResourceExhausted;" in text:
            print("join refused: complete chunks join refused by the copy disk guard, naming a file",
                  flush=True)
        else:
            fail(f"complete chunks join: not the copy disk guard's refusal: {_SASL.first_line(e)}")
    scan_and_group("after")
    bound, want_having = reference_having(base)
    try:
        with _SASL.connect(server, "partial") as conn:
            answer = _SASL.run(conn, HAVING.format(bound=bound))
        got = {(r["l_returnflag"], r["l_linestatus"]):
               (int(r["n"]), decimal.Decimal(str(r["q"])), int(r["m"])) for r in answer.to_pylist()}
        if got.keys() != want_having.keys() or any(
                got[k][0] != w[0] or got[k][2] != w[2] or abs(got[k][1] - w[1]) > decimal.Decimal("0.0001")
                for k, w in want_having.items()):
            fail(f"partial chunks.lineitem HAVING / count(DISTINCT): {got}, want {want_having}")
        else:
            print(f"ok partial chunks.lineitem HAVING / count(DISTINCT) ({len(got)} of the groups)",
                  flush=True)
    except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
        print(f"error text: {e}", flush=True)
        fail(f"partial chunks.lineitem HAVING / count(DISTINCT): {_SASL.first_line(e)}")
    # Item 31 (5), (6), (8): one-table queries in complete mode read their copies in batches.
    first_groups = dict(sorted(want_groups.items())[:3])
    bound = decimal.Decimal(25)
    want_fn, want_fq = reference_filtered(base, bound)
    checks = [
        ("complete", "chunks.lineitem full scan", _SASL.CHUNKS_SCAN, "scan", None),
        ("complete", "chunks.lineitem GROUP BY", _SASL.CHUNKS_GROUP, "groups", want_groups),
        ("complete", "plain lineitem_p count/sum", _SASL.PLAIN_LINEITEM, "totals", (want_n, want_q)),
        ("complete", "plain lineitem_p filtered count/sum", PLAIN_FILTERED.format(bound=bound),
         "totals", (want_fn, want_fq)),
        ("complete", "plain lineitem_p GROUP BY", PLAIN_GROUP, "groups", want_groups),
        ("complete", "plain lineitem_p GROUP BY ORDER BY LIMIT", PLAIN_FIRST_GROUPS, "groups",
         first_groups),
        ("partial", "plain lineitem_p GROUP BY", PLAIN_GROUP, "groups", want_groups),
    ]
    for mode, what, sql, kind, want in checks:
        try:
            with _SASL.connect(server, mode) as conn:
                answer = _SASL.run(conn, sql)
            if kind == "scan":
                q = decimal.Decimal(str(pc.sum(answer.column("l_quantity")).as_py() or 0))
                good = answer.num_rows == want_n and abs(q - want_q) <= decimal.Decimal("0.0001")
                got = (answer.num_rows, q)
                want = (want_n, want_q)
            elif kind == "totals":
                n, q, _ = _SASL.totals(answer)
                got = (n, q)
                good = n == want[0] and abs(q - want[1]) <= decimal.Decimal("0.0001")
            else:
                got = _SASL.groups(answer)
                good = _SASL.same_groups(got, want)
            if good:
                print(f"ok {mode} {what} ({answer.num_rows} rows)", flush=True)
            else:
                fail(f"{mode} {what}: {got}, want {want}")
        except Exception as e:  # noqa: BLE001 - any error fails the check, with its text
            print(f"error text: {e}", flush=True)
            fail(f"{mode} {what}: {_SASL.first_line(e)}")
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
