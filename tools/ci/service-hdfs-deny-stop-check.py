#!/usr/bin/env python3
"""Storage denials and a node stop under load (STATUS item 37 (a), (b)), last stage of the ha-krb cells.

The maintainer's stand (HDFS + Ranger) answered a query whose files the storage refused with
``INTERNAL: Query execution failed: RemoteException: Permission denied`` (SASL DataNodes) or, when
the table's view had failed at start (privileged DataNodes), ``Catalog Error: Table with name ...
does not exist!`` until long after the permission was back. A node stopped under load logged
``Filesystem closed``, Netty ``RejectedExecutionException`` stacks and an allocator leak, and its
clients got ``UNAVAILABLE: io exception`` instead of the stopping message.

Phases (the cell changes the HDFS permissions and stops the server between them):

``denied``   the cell started the server with the files of tpch.nation readable by their owner
             only (the server's principal is not the owner): both queries must fail as access
             denied (gRPC PERMISSION_DENIED, ADBC "Unauthorized"), never as an internal error or a
             missing table; the server keeps answering other tables.
``restored`` the cell gave the files back: the next queries of tpch.nation (no restart) answer
             25 rows.
``load``     runs queries from several threads until the server stops; every failure must be
             UNAVAILABLE (the stopping message while the server drains, a transport error once it
             is gone), never INTERNAL.

Usage: service-hdfs-deny-stop-check.py <phase> <host:port> [<seconds>]
Prints ``ok``/``FAIL`` lines and a last line ``result pass`` or ``result fail <first error>``;
exits non-zero on a failure.
"""
import importlib.util
import os
import sys
import threading
import time

_SPEC = importlib.util.spec_from_file_location(
    "service_hdfs_sasl_check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                            "service-hdfs-sasl-check.py"))
_SASL = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_SASL)

NATION_QUERIES = ["SELECT count(*) AS n FROM tpch.nation",
                  "SELECT n_nationkey, n_name FROM tpch.nation ORDER BY n_nationkey"]
OTHER_QUERY = "SELECT count(*) AS n FROM tpch.region"
LOAD_QUERIES = ["SELECT count(*) AS n FROM tpch.region",
                "SELECT l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM tpch.lineitem"
                " GROUP BY l_returnflag",
                "SELECT o_orderpriority, count(*) AS n FROM tpch.orders GROUP BY o_orderpriority"]
STOPPING = "The server is stopping; retry the query"
THREADS = 8

failures = []


def check(ok, text):
    """Prints one check line and remembers a failure."""
    print(("ok   " if ok else "FAIL ") + text, flush=True)
    if not ok:
        failures.append(text)


def rows(server, sql):
    """Runs one complete-mode query and returns its rows as dictionaries."""
    with _SASL.connect(server, "complete") as conn:
        return _SASL.run(conn, sql).to_pylist()


def error_of(server, sql):
    """Runs one query and returns its error text, or None when it answered."""
    try:
        rows(server, sql)
        return None
    except Exception as e:  # noqa: BLE001 - every driver error is a result here
        return f"{type(e).__name__}: {e}"


def denied():
    """Both nation queries fail as access denied; another table still answers."""
    server = sys.argv[2]
    for sql in NATION_QUERIES:
        text = error_of(server, sql)
        if text is None:
            check(False, f"denied: {sql} answered although its files are not readable")
            continue
        upper = text.upper()
        line = " ".join(text.split())[:300]
        check(("UNAUTHORIZED" in upper or "PERMISSION" in upper) and "INTERNAL" not in upper
              and "DOES NOT EXIST" not in upper, f"denied: {sql} -> {line}")
    n = rows(server, OTHER_QUERY)[0]["n"]
    check(n == 5, f"denied: {OTHER_QUERY} = {n} (want 5)")


def restored():
    """The nation queries answer again without a restart."""
    server = sys.argv[2]
    deadline = time.time() + 30
    text = "never ran"
    while time.time() < deadline:
        text = error_of(server, NATION_QUERIES[0])
        if text is None:
            break
        time.sleep(1)
    check(text is None, f"restored: {NATION_QUERIES[0]} answers again ({' '.join(str(text).split())[:300]})")
    if text is None:
        got = rows(server, NATION_QUERIES[1])
        check(len(got) == 25, f"restored: {NATION_QUERIES[1]} has {len(got)} rows (want 25)")


def load():
    """Queries from several threads until the server is gone; no failure may be INTERNAL."""
    server, seconds = sys.argv[2], float(sys.argv[3])
    stats = {"answered": 0, "stopping": 0, "unavailable": 0, "other": 0}
    others = []
    lock = threading.Lock()
    stop = time.time() + seconds

    def worker(k):
        i = k
        while time.time() < stop:
            sql = LOAD_QUERIES[i % len(LOAD_QUERIES)]
            i += 1
            text = error_of(server, sql)
            with lock:
                if text is None:
                    stats["answered"] += 1
                elif STOPPING in text:
                    stats["stopping"] += 1
                elif "UNAVAILABLE" in text.upper() or "CONNECT" in text.upper() or "IO EXCEPTION" in text.upper():
                    stats["unavailable"] += 1
                    time.sleep(0.2)
                else:
                    stats["other"] += 1
                    others.append(" ".join(text.split())[:300])
                    time.sleep(0.2)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("load: " + " ".join(f"{k}={v}" for k, v in stats.items()), flush=True)
    check(stats["answered"] > 0, f"load: {stats['answered']} answers before the stop")
    check(not others, f"load: failures other than UNAVAILABLE: {len(others)}"
          + (f" (first: {others[0]})" if others else ""))


def main():
    phase = sys.argv[1]
    {"denied": denied, "restored": restored, "load": load}[phase]()
    if failures:
        print(f"result fail {failures[0]}")
        sys.exit(1)
    print("result pass")


if __name__ == "__main__":
    main()
