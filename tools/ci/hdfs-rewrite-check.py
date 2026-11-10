"""Checks that a Parquet file rewritten in place on HDFS is read fresh (item 39, Q11).

Usage: hdfs-rewrite-check.py <host:port> <local orders part file> <work dir> <wait seconds>

Run by tools/ci/hdfs-point-cell.sh at the end of a variant, while the server, the NameNode container ``nn1``
and the table ``tpch.orders`` (one directory of files on HDFS under /data/tpch/orders) are still up. The
point and count queries run a few times first, so DuckDB's caches (external file cache, Parquet footers,
hadoopfs file statuses) hold the file. Then the file is replaced on HDFS with its first half of rows
(``hdfs dfs -put -f``, same name, new size and modification time), and after <wait seconds> (the hadoopfs
metadata cache TTL plus a margin) the answers must be the new ones: the order from the dropped half is gone
and the count is lower by the dropped rows. The answers right after the rewrite are printed, not checked
(the hadoopfs metadata cache may serve the old status until its TTL ends). Prints one line
``rewrite fresh=<true|false> ...``; exit code 0 only when the answers after the wait are the new ones.
"""
import os
import subprocess
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.parquet as pq


def answers(conn, key):
    """Returns (rows with o_orderkey = key, rows of tpch.orders), or the error text."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM tpch.orders WHERE o_orderkey = {key}")
            point = cur.fetchall()[0][0]
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM tpch.orders")
            total = cur.fetchall()[0][0]
        return point, total
    except Exception as e:  # noqa: BLE001 - reported in the line
        return "error", (str(e).splitlines() or [type(e).__name__])[0][:200]


def main():
    server, part, work, wait = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
    table = pq.read_table(part)
    keys = table.column("o_orderkey").to_pylist()
    keep = len(keys) // 2
    dropped_key = keys[-1]
    halved = os.path.join(work, "rewrite-" + os.path.basename(part))
    pq.write_table(table.slice(0, keep), halved)
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    with flightsql.connect(f"grpc://{server}", db_kwargs=kwargs) as conn:
        for _ in range(3):
            before = answers(conn, dropped_key)
        remote = "/data/tpch/orders/" + os.path.basename(part)
        subprocess.run(["docker", "cp", halved, "nn1:/tmp/rewrite.parquet"], check=True, capture_output=True)
        subprocess.run(["docker", "exec", "nn1", "hdfs", "dfs", "-put", "-f", "/tmp/rewrite.parquet", remote],
                       check=True, capture_output=True)
        right_after = answers(conn, dropped_key)
        time.sleep(wait)
        after = answers(conn, dropped_key)
    expected = (0, before[1] - (len(keys) - keep)) if before[0] == 1 else None
    fresh = expected is not None and after == expected
    print(f"rewrite fresh={str(fresh).lower()} key={dropped_key} before={before} rightAfter={right_after}"
          f" after{wait:.0f}s={after} expected={expected}")
    return 0 if fresh else 1


if __name__ == "__main__":
    sys.exit(main())
