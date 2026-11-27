"""Times the TPC-H queries through the ADBC Flight SQL driver (Go), as the CI screening's client does.

Usage: tpch-adbc.py <host:port> <queries dir> <rounds> [result mode]

The ADBC twin of TpchPerfTest for tpch-perf.yml (input client=adbc, STATUS "Bug first" of
2026-10-29: the S3 slowdown was measured by the screening with ADBC). Runs one unmeasured pass,
then <rounds> measured passes, and prints the same lines as TpchPerfTest
(``q01 r1 1234.5 ms rows 4 sum <checksum>`` per query and pass, ``total r<n> <seconds> s`` per
pass). The versioned checksum is shared with native generation and Java, using exact numeric
values rounded HALF_EVEN to 2 decimals. Exits non-zero when an answer's row count differs from ``qNN.rows`` (or is
empty when that file is missing).
"""
import os
from pathlib import Path
import runpy
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql

RawRun = runpy.run_path(str(Path(__file__).with_name("tpch-raw.py")))["RawRun"]
CHECKSUM = runpy.run_path(str(Path(__file__).with_name("tpch-checksum.py")))
normalized = CHECKSUM["canonical_value"]


def read(cursor, sql):
    """Return rows, the existing checksum, referenced Arrow bytes and table chunk count."""
    cursor.execute(sql)
    table = cursor.fetch_arrow_table()
    total = CHECKSUM["checksum_rows"](row.values() for row in table.to_pylist())
    return table.num_rows, total, table.nbytes, len(table.to_batches())


def main():
    server, queries, rounds = sys.argv[1], sys.argv[2], int(sys.argv[3])
    mode = sys.argv[4] if len(sys.argv) > 4 else "complete"
    names = sorted(f[:-4] for f in os.listdir(queries) if f.endswith(".sql"))
    if not names:
        sys.exit(f"no .sql files in {queries}")
    sqls, expected = [], []
    for name in names:
        with open(os.path.join(queries, name + ".sql"), encoding="utf-8") as f:
            sqls.append(f.read().strip().rstrip(";"))
        rows = os.path.join(queries, name + ".rows")
        expected.append(int(Path(rows).read_text().strip()) if os.path.exists(rows) else -1)
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": mode}
    failed = False
    raw = RawRun()
    completed = False
    try:
        with flightsql.connect(f"grpc://{server}", db_kwargs=kwargs) as conn:
            for r in range(rounds + 1):
                total = 0
                for name, sql, want in zip(names, sqls, expected):
                    timestamp = time.time_ns()
                    t0 = time.perf_counter_ns()
                    try:
                        with conn.cursor() as cursor:
                            timestamp = time.time_ns()
                            t0 = time.perf_counter_ns()
                            answer = read(cursor, sql)
                            nanos = time.perf_counter_ns() - t0
                    except Exception as error:
                        raw.record(name, r, timestamp, time.perf_counter_ns() - t0, None, want,
                                   type(error).__name__)
                        raise
                    raw.record(name, r, timestamp, nanos, answer, want)
                    rows, checksum = answer[:2]
                    total += nanos
                    if (want >= 0 and rows != want) or (want < 0 and rows == 0):
                        print(f"{name} rows {rows}, expected {want if want >= 0 else 'at least 1'}", file=sys.stderr)
                        failed = True
                    print(f"{name} r{r} {nanos / 1e6:9.1f} ms rows {rows} sum {checksum:016x}", flush=True)
                print(f"total r{r} {total / 1e9:.2f} s{' (warm-up)' if r == 0 else ''}", flush=True)
        completed = not failed
    finally:
        raw.close(completed)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
