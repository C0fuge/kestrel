#!/usr/bin/env python3
"""Concurrent distributed complete answers on one node (STATUS item 35).

The maintainer's benchmark rehearsal (one node, local Parquet SF1, ``-Xmx2g
-XX:MaxDirectMemorySize=2g``, ``speed`` + ``distributedComplete=true``) failed 2 of 8 concurrent
``SELECT * FROM tpch.lineitem`` with ``INVALID_ARGUMENT: Could not load buffers ... Unable to allocate
buffer ... due to memory limit``: every merge opened its four endpoint streams at once. This client
runs 8 full scans and 32 small queries at the same time, all in complete mode on one server, streaming
the scans batch by batch (never holding an answer), and checks every answer against the local files:
the scans' row count and sum(l_quantity), the small queries' exact values.

Usage: distributed-concurrency-check.py <host:port> <lineitem parquet dir> [scans] [small queries]
Prints ``result pass`` or ``result fail <first error>``; exits non-zero on a failure.
"""
import concurrent.futures
import decimal
import glob
import os
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCAN = "SELECT * FROM tpch.lineitem"
KWARGS = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}


def reference(base):
    """Exact answers of the scan and of the small queries, from the local files."""
    table = pq.read_table(glob.glob(os.path.join(base, "*.parquet")),
                          columns=["l_quantity", "l_returnflag", "l_linestatus", "l_shipmode"])
    total = decimal.Decimal(str(pc.sum(table.column("l_quantity")).as_py()))
    groups = table.group_by(["l_returnflag", "l_linestatus"]).aggregate([([], "count_all")])
    by_flag = sorted((r["l_returnflag"], r["l_linestatus"], r["count_all"]) for r in groups.to_pylist())
    modes = sorted(set(table.column("l_shipmode").to_pylist()))
    small = {
        "SELECT count(*) AS n FROM tpch.lineitem": [(table.num_rows,)],
        "SELECT sum(l_quantity) AS q FROM tpch.lineitem": [(total,)],
        "SELECT l_returnflag, l_linestatus, count(*) AS n FROM tpch.lineitem"
        " GROUP BY l_returnflag, l_linestatus ORDER BY 1, 2": by_flag,
        "SELECT DISTINCT l_shipmode FROM tpch.lineitem ORDER BY 1": [(m,) for m in modes],
    }
    return (table.num_rows, total), small


def scan(server):
    """Runs the full scan and adds up its rows and l_quantity batch by batch."""
    rows, total, batches = 0, decimal.Decimal(0), 0
    with flightsql.connect(f"grpc://{server}", db_kwargs=KWARGS) as conn, conn.cursor() as cursor:
        cursor.execute(SCAN)
        for batch in cursor.fetch_record_batch():
            batches += 1
            rows += batch.num_rows
            q = pc.sum(batch.column(batch.schema.get_field_index("l_quantity"))).as_py()
            if q is not None:
                total += decimal.Decimal(str(q))
    return (rows, total), batches


def small_query(server, sql):
    """Runs a small query and returns its rows as tuples (decimals as Decimal)."""
    with flightsql.connect(f"grpc://{server}", db_kwargs=KWARGS) as conn, conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    return [tuple(decimal.Decimal(str(v)) if isinstance(v, (float, decimal.Decimal)) else v for v in r)
            for r in rows]


def first_line(e):
    text = str(e).strip()
    return (text.splitlines()[0] if text else type(e).__name__)[:300]


def main():
    server, base = sys.argv[1], sys.argv[2]
    scans = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    smalls = int(sys.argv[4]) if len(sys.argv) > 4 else 32
    want_scan, want_small = reference(base)
    queries = list(want_small)
    print(f"reference rows={want_scan[0]} sum={want_scan[1]}; {scans} scans + {smalls} small queries at once")
    start = time.monotonic()
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=scans + smalls) as pool:
        jobs = {}
        for i in range(scans):
            jobs[pool.submit(scan, server)] = ("scan", i + 1, None)
        for i in range(smalls):
            sql = queries[i % len(queries)]
            jobs[pool.submit(small_query, server, sql)] = ("small", i + 1, sql)
        for job in concurrent.futures.as_completed(jobs):
            kind, nr, sql = jobs[job]
            seconds = time.monotonic() - start
            try:
                got = job.result()
            except Exception as e:  # noqa: BLE001 - every failure is reported
                print(f"{kind} {nr}: error after {seconds:.1f} s: {first_line(e)}")
                failures.append(f"{kind} {nr}: {first_line(e)[:200]}")
                continue
            if kind == "scan":
                answer, batches = got
                ok = answer == want_scan
                print(f"scan {nr}: rows={answer[0]} sum={answer[1]} batches={batches} done at {seconds:.1f} s"
                      f" {'ok' if ok else 'WRONG'}")
                if not ok:
                    failures.append(f"scan {nr}: rows={answer[0]} sum={answer[1]}, want {want_scan}")
            else:
                ok = got == want_small[sql]
                if not ok:
                    print(f"small {nr}: WRONG {got[:5]} want {want_small[sql][:5]} ({sql})")
                    failures.append(f"small {nr}: wrong answer to {sql}")
    print(f"done in {time.monotonic() - start:.1f} s: {scans + smalls - len(failures)} of {scans + smalls} exact")
    print("result pass" if not failures else f"result fail {failures[0]} ({len(failures)} failed)")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
