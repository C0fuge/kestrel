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
  answers) must match the local files: merged from the disk-sized batches (``merge=rewrite``).

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
        if REFUSAL in text and ".parquet" in text:
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
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
