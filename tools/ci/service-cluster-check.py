"""Checks a running server cluster against service-cluster-data.py's reference (lane 3, SERVICE MATRIX).

Usage:
  service-cluster-check.py check <host:port,...> <dir>
  service-cluster-check.py loss <host:port,...> <dir> <seconds> <clients>

``check`` runs the 22 TPC-H queries through the ADBC Flight SQL driver (Go) in complete mode, query
i on node i mod n, and compares every answer with <dir>/expected/qNN.parquet (column names, row
count, values; rows as a multiset, floating-point values within a relative 1e-9 - the summation
order of a merged answer may change the last bits), then Flight SQL metadata (GetObjects lists the
8 tables of schema ``tpch``, GetTableSchema of ``tpch.lineitem`` has the data files' columns) and
one prepared query with a bound parameter. Prints ``ok``/``FAIL`` lines and a last line
``result pass`` or ``result fail <first error>``; exits non-zero on a failure.

``loss`` keeps <clients> clients running the 22 queries on every node but the last (the one the
workflow stops) for <seconds>, and checks that each answer is either exactly the reference or an
error a client may retry (gRPC UNAVAILABLE); a retryable error is retried once on the next node and
that answer must be exact. A wrong (e.g. partial) answer or a non-retryable error fails. Prints
``load start`` when the load runs, one ``event`` line per answer and the same ``result`` line.
"""
import decimal
import math
import os
import sys
import threading
import time

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.parquet as pq

PREPARED = "SELECT count(*) FROM tpch.lineitem WHERE l_quantity < ?"
PREPARED_VALUE = 24
TABLES = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}


def connect(server):
    """Opens an ADBC Flight SQL connection asking for complete answers."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    return flightsql.connect(f"grpc://{server}", db_kwargs=kwargs)


def cell(value):
    """A comparable cell: decimals as floats, everything else as is."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def sort_key(row):
    """Order of a row in the multiset comparison: floats rounded, None first."""
    out = []
    for v in row:
        if v is None:
            out.append((0, ""))
        elif isinstance(v, float):
            out.append((1, f"{v:.4f}"))
        else:
            out.append((1, str(v)))
    return out


def same(a, b):
    """Whether two cells are equal (floats within a relative 1e-9)."""
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def difference(got, want):
    """None when the Arrow tables hold the same answer, else a one-line description."""
    if got.column_names != want.column_names:
        return f"columns {got.column_names} != {want.column_names}"
    if got.num_rows != want.num_rows:
        return f"rows {got.num_rows} != {want.num_rows}"
    rows_got = sorted(([cell(v) for v in r.values()] for r in got.to_pylist()), key=sort_key)
    rows_want = sorted(([cell(v) for v in r.values()] for r in want.to_pylist()), key=sort_key)
    for i, (g, w) in enumerate(zip(rows_got, rows_want)):
        if len(g) != len(w) or not all(same(x, y) for x, y in zip(g, w)):
            return f"row {i}: {g} != {w}"
    return None


def load(base):
    """The queries and reference answers of <base>, sorted by name."""
    names = sorted(f[:-4] for f in os.listdir(os.path.join(base, "queries")) if f.endswith(".sql"))
    out = []
    for name in names:
        with open(os.path.join(base, "queries", name + ".sql"), encoding="utf-8") as fh:
            sql = fh.read().strip().rstrip(";")
        out.append((name, sql, pq.read_table(os.path.join(base, "expected", name + ".parquet"))))
    return out


def run(conn, sql, parameters=None):
    """Runs one query and returns its answer as an Arrow table."""
    with conn.cursor() as cursor:
        cursor.execute(sql, parameters)
        return cursor.fetch_arrow_table()


def first_line(error):
    """The first line of an error, shortened."""
    return str(error).strip().splitlines()[0][:300] if str(error).strip() else type(error).__name__


def check(servers, base):
    """The ``check`` mode; returns the process exit code."""
    queries = load(base)
    conns = [connect(s) for s in servers]
    failures = []

    def fail(what):
        failures.append(what)
        print(f"FAIL {what}", flush=True)

    for i, (name, sql, want) in enumerate(queries):
        node = i % len(conns)
        t0 = time.perf_counter()
        try:
            got = run(conns[node], sql)
        except Exception as e:  # noqa: BLE001 - every failure is reported
            fail(f"{name} on {servers[node]}: {first_line(e)}")
            continue
        diff = difference(got, want)
        ms = (time.perf_counter() - t0) * 1000
        if diff:
            fail(f"{name} on {servers[node]}: wrong answer: {diff}")
        else:
            print(f"ok {name} on {servers[node]} rows {got.num_rows} {ms:.0f} ms", flush=True)
    if len(queries) != 22:
        fail(f"{len(queries)} queries, expected 22")

    columns = pq.read_schema(os.path.join(base, "data", "tpch", "lineitem", "part-000.parquet")).names
    for node, conn in enumerate(conns):
        try:
            objects = conn.adbc_get_objects(depth="tables", db_schema_filter="tpch").read_all().to_pylist()
            tables = {t["table_name"] for c in objects for s in (c["catalog_db_schemas"] or [])
                      if s["db_schema_name"] == "tpch" for t in (s["db_schema_tables"] or [])}
            if not TABLES <= tables:
                fail(f"metadata on {servers[node]}: GetObjects tables of tpch {sorted(tables)}")
            else:
                print(f"ok metadata GetObjects on {servers[node]}: {len(tables)} tables", flush=True)
            schema = conn.adbc_get_table_schema("lineitem", db_schema_filter="tpch")
            if schema.names != columns:
                fail(f"metadata on {servers[node]}: GetTableSchema lineitem {schema.names} != {columns}")
            else:
                print(f"ok metadata GetTableSchema lineitem on {servers[node]}", flush=True)
        except Exception as e:  # noqa: BLE001
            fail(f"metadata on {servers[node]}: {first_line(e)}")

    with open(os.path.join(base, "expected", "prepared.txt"), encoding="utf-8") as fh:
        want = int(fh.read().strip())
    for node, conn in enumerate(conns):
        try:
            got = run(conn, PREPARED, (PREPARED_VALUE,)).column(0)[0].as_py()
            if got != want:
                fail(f"prepared on {servers[node]}: {got} != {want}")
            else:
                print(f"ok prepared on {servers[node]}: {got}", flush=True)
        except Exception as e:  # noqa: BLE001
            fail(f"prepared on {servers[node]}: {first_line(e)}")
    for c in conns:
        c.close()
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


def retryable(error):
    """Whether a client may retry the error (gRPC UNAVAILABLE)."""
    return "unavailable" in str(error).lower()


def loss(servers, base, seconds, clients):
    """The ``loss`` mode; returns the process exit code."""
    queries = load(base)
    alive = servers[:-1]
    lock = threading.Lock()
    counts = {"ok": 0, "retryable": 0, "retried_ok": 0, "wrong": 0, "error": 0}
    failures = []
    deadline = time.time() + seconds

    def record(kind, line, failure=None):
        with lock:
            counts[kind] += 1
            print(f"event {time.time():.3f} {line}", flush=True)
            if failure:
                failures.append(failure)

    def answer(server, name, sql, want):
        """Runs one query on a fresh connection: (table, None) or (None, error)."""
        try:
            with connect(server) as conn:
                return run(conn, sql), None
        except Exception as e:  # noqa: BLE001
            return None, e

    def client(c):
        i = c * 7
        while time.time() < deadline:
            name, sql, want = queries[i % len(queries)]
            server = alive[i % len(alive)]
            i += 1
            got, error = answer(server, name, sql, want)
            if error is None:
                diff = difference(got, want)
                if diff:
                    record("wrong", f"c{c} {name} {server} wrong {diff}", f"{name} on {server}: wrong answer: {diff}")
                else:
                    record("ok", f"c{c} {name} {server} ok")
                continue
            if not retryable(error):
                record("error", f"c{c} {name} {server} error {first_line(error)}",
                       f"{name} on {server}: non-retryable: {first_line(error)}")
                continue
            record("retryable", f"c{c} {name} {server} unavailable {first_line(error)}")
            other = alive[i % len(alive)]
            got, error = answer(other, name, sql, want)
            diff = difference(got, want) if error is None else first_line(error)
            if diff:
                record("wrong" if error is None else "error", f"c{c} {name} {other} retry failed {diff}",
                       f"{name} retried on {other}: {diff}")
            else:
                record("retried_ok", f"c{c} {name} {other} retry ok")

    print("load start", flush=True)
    threads = [threading.Thread(target=client, args=(c,)) for c in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("summary " + " ".join(f"{k}={v}" for k, v in counts.items()), flush=True)
    if counts["ok"] == 0:
        failures.append("no exact answer at all")
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 1 if failures else 0


def main():
    mode, servers, base = sys.argv[1], sys.argv[2].split(","), sys.argv[3]
    if mode == "check":
        sys.exit(check(servers, base))
    if mode == "loss":
        sys.exit(loss(servers, base, float(sys.argv[4]), int(sys.argv[5])))
    sys.exit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
