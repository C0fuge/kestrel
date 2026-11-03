"""Times full-table scans and an export through the ADBC Flight SQL driver (Go) for scan-perf.yml (item 27).

Usage: scan-adbc.py <host:port> <table> <rounds> <clients> <export file> [result mode]

Measures, after one unmeasured pass, <rounds> passes of
- ``x1``: one client reads ``SELECT * FROM <table>`` batch by batch,
- ``x<clients>``: <clients> clients (one connection and thread each) read it at the same time,
- ``export``: one client streams it into a Parquet file (zstd), the way an export job does.
Prints one line per shape and pass: ``x1 r1 12.34 s rows 59986052 mb 9876.5 mbs 800.3 batches 123 sum <hex>``
(mb = Arrow buffer bytes of all batches in MiB, mbs = mb per second over all clients, sum = order-independent
checksum of the rows' first column and row count, equal across configurations when the answers are equal).
Exits non-zero when a pass fails or two passes disagree on rows or sum.
"""
import os
import sys
import threading
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq


def connect(server, mode):
    """Opens a connection that asks for the given result mode."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": mode}
    return flightsql.connect(f"grpc://{server}", db_kwargs=kwargs)


def scan(conn, sql, sink=None):
    """Reads one query batch by batch; returns rows, Arrow bytes, batches and a checksum of the first column."""
    rows = nbytes = batches = total = 0
    with conn.cursor() as cursor:
        cursor.execute(sql)
        reader = cursor.fetch_record_batch()
        writer = None
        try:
            for batch in reader:
                rows += batch.num_rows
                nbytes += batch.nbytes
                batches += 1
                first = pc.sum(batch.column(0)).as_py()
                total = (total + (int(first) if first is not None else 0)) % (1 << 64)
                if sink is not None:
                    if writer is None:
                        writer = pq.ParquetWriter(sink, batch.schema, compression="zstd")
                    writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()
    return rows, nbytes, batches, total


def parallel(conns, sql):
    """Reads the query on every connection at once; returns the per-client results."""
    results, errors = [None] * len(conns), []

    def run(i):
        try:
            results[i] = scan(conns[i], sql)
        except Exception as e:  # noqa: BLE001 - reported below, the pass fails
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(len(conns))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]
    return results


def main():
    server, table, rounds, clients, export = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    mode = sys.argv[6] if len(sys.argv) > 6 else "complete"
    sql = f"SELECT * FROM {table}"
    conns = [connect(server, mode) for _ in range(max(1, clients))]
    seen, failed = {}, False
    try:
        for r in range(rounds + 1):
            shapes = [("x1", lambda: [scan(conns[0], sql)])]
            if clients > 1:
                shapes.append((f"x{clients}", lambda: parallel(conns, sql)))
            shapes.append(("export", lambda: [scan(conns[0], sql, export)]))
            for name, run in shapes:
                t0 = time.perf_counter()
                try:
                    results = run()
                except Exception as e:  # noqa: BLE001 - one failed pass fails the run
                    print(f"{name} r{r} error {str(e).splitlines()[0][:300]}", flush=True)
                    failed = True
                    continue
                seconds = time.perf_counter() - t0
                for rows, _, _, total in results:
                    answer = (rows, total)
                    if seen.setdefault("answer", answer) != answer:
                        print(f"{name} r{r} wrong rows {rows} sum {total:016x}, expected {seen['answer']}", flush=True)
                        failed = True
                rows, nbytes, batches, total = results[0]
                mb = sum(res[1] for res in results) / 1048576
                warm = " (warm-up)" if r == 0 else ""
                print(f"{name} r{r} {seconds:.2f} s rows {rows} mb {mb:.1f} mbs {mb / seconds:.1f} batches {batches}"
                      f" sum {total:016x}{warm}", flush=True)
            if os.path.exists(export):
                os.remove(export)
    finally:
        for c in conns:
            c.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
