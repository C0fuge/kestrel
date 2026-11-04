#!/usr/bin/env python3
"""Low direct memory stage of the sasl-cluster speed cell (STATUS item 31 (7)).

The maintainer's stand (3 servers, ``speed``, ``distributedComplete=true``, 512 MiB direct memory)
failed 3 of 4 full scans right after another 6M-row scan with ``OutOfMemoryError: Cannot reserve
16777216 bytes of direct buffer memory``: the node merging the complete answer opened every
endpoint stream at once and each stream not yet read held five batches. The servers here run the
same way; this client runs ``SELECT * FROM tpch.lineitem`` in complete mode four times in a row on
the first server, streaming the batches (never holding the answer), and checks the row count and
sum(l_quantity) against the local files each time.

Usage: service-hdfs-lowdirect-check.py <host:port> <input dir>
Prints ``result pass`` or ``result fail <first error>``; exits non-zero on a failure.
"""
import decimal
import glob
import os
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCAN = "SELECT * FROM tpch.lineitem"
ROUNDS = 4


def reference(base):
    """Row count and sum(l_quantity) of the local lineitem files."""
    table = pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                          columns=["l_quantity"])
    return table.num_rows, decimal.Decimal(str(pc.sum(table.column("l_quantity")).as_py()))


def scan(server):
    """Runs the full scan in complete mode and adds up its rows and l_quantity batch by batch."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    rows, total, batches = 0, decimal.Decimal(0), 0
    with flightsql.connect(f"grpc://{server}", db_kwargs=kwargs) as conn, conn.cursor() as cursor:
        cursor.execute(SCAN)
        reader = cursor.fetch_record_batch()
        for batch in reader:
            batches += 1
            rows += batch.num_rows
            q = pc.sum(batch.column(batch.schema.get_field_index("l_quantity"))).as_py()
            if q is not None:
                total += decimal.Decimal(str(q))
    return rows, total, batches


def main():
    server, base = sys.argv[1], sys.argv[2]
    want = reference(base)
    print(f"reference rows={want[0]} sum={want[1]}")
    failure = None
    for i in range(ROUNDS):
        start = time.monotonic()
        try:
            rows, total, batches = scan(server)
        except Exception as e:  # noqa: BLE001 - every failure is reported
            line = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            print(f"scan {i + 1}: error {line[:300]}")
            failure = failure or f"scan {i + 1}: {line[:200]}"
            continue
        ok = (rows, total) == want
        print(f"scan {i + 1}: rows={rows} sum={total} batches={batches}"
              f" seconds={time.monotonic() - start:.1f} {'ok' if ok else 'WRONG'}")
        if not ok:
            failure = failure or f"scan {i + 1}: rows={rows} sum={total}, want {want[0]} / {want[1]}"
    print("result pass" if failure is None else f"result fail {failure}")
    return 0 if failure is None else 1


if __name__ == "__main__":
    sys.exit(main())
