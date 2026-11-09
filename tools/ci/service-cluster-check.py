"""Checks a running server cluster against service-cluster-data.py's reference (lane 3, SERVICE MATRIX).

Usage:
  service-cluster-check.py check <host:port,...> <dir>
  service-cluster-check.py locality <host:port,...> <dir>
  service-cluster-check.py loss <host:port,...> <dir> <seconds> <clients>

``check`` runs the 22 TPC-H queries through the ADBC Flight SQL driver (Go) in complete mode, query
i on node i mod n, and compares every answer with <dir>/expected/qNN.parquet (column names, row
count, values; rows as a multiset, floating-point values within a relative 1e-9 - the summation
order of a merged answer may change the last bits), then Flight SQL metadata (GetObjects lists the
8 tables of schema ``tpch``, GetTableSchema of ``tpch.lineitem`` has the data files' columns) and
one prepared query with a bound parameter. With ``SC_PLAIN=1`` it also runs the 22 queries with plain
table names (``tpch.`` removed) against the same answers (D15). Prints ``ok``/``FAIL`` lines and a last line
``result pass`` or ``result fail <first error>``; exits non-zero on a failure.

With the environment variable ``SC_DATANODES`` (comma-separated ``host:port`` of the HDFS DataNodes'
HTTP ports), ``check`` also prints a ``reads total`` line: the change of every DataNode's JMX
block-read counters (``ReadsFromLocalClient`` / ``ReadsFromRemoteClient``) over the 22 queries
(D13 a2). The DataNode's JMX values are a cache refreshed once per metrics period (10 s by default),
so every snapshot first waits ``SC_JMX_SETTLE`` seconds (default 11); per-query deltas were all zero
but one (run 41495736199).

``locality`` (diagnostic, exit code 0 unless the probe cannot run) plans ``LOCALITY_QUERIES`` in
partial mode on the first node with pyarrow.flight, reads every endpoint from its first location and
prints one ``endpoint`` line each: the location and the DataNode counter changes over its read
(settled snapshots between the reads, as above); then
a ``locality`` summary line (endpoints reading only local blocks, mixed, only remote, none). It
needs ``SC_DATANODES`` in the same order as the nodes (DataNode i shares node i's host).

``loss`` keeps <clients> clients running the 22 queries on every node but the last (the one the
workflow stops) for <seconds>, and checks that each answer is either exactly the reference or an
error a client may retry (gRPC UNAVAILABLE); a retryable error is retried once on the next node and
that answer must be exact. A wrong (e.g. partial) answer or a non-retryable error fails. Prints
``load start`` when the load runs, one ``event`` line per answer and the same ``result`` line.
When <dir>/expected/scan.txt exists, one more client streams ``SCAN`` (a merged answer of all
lineitem rows under ``distributedComplete``) on the first node, read at ``SC_SCAN_ROWS_PER_S`` rows
per second (default 150000, ~40 s at SF1; at the kill, 15 s in, ~2/3 of the rows are unread), so that the kill lands inside a merged answer whose
first batch the client already holds (D13 c). Each scan must end with exactly the reference's rows
and sums, or with UNAVAILABLE whose retry on the other node is exact; ``scan`` events say after how
many rows an error came and whether it was the server's "peer lost" status. The summary counts
``scan_ok``, ``scan_peer_lost`` (UNAVAILABLE "peer lost" after the first batch), ``scan_retryable``
(any UNAVAILABLE) and ``scan_retried_ok``; a wrong scan or a non-retryable error fails as above.
"""
import decimal
import math
import os
import re
import sys
import threading
import time
import urllib.request

import adbc_driver_flightsql.dbapi as flightsql
import pyarrow.compute as pc
import pyarrow.parquet as pq

PREPARED = "SELECT count(*) FROM tpch.lineitem WHERE l_quantity < ?"
PREPARED_VALUE = 24
LOCALITY_QUERIES = {
    "lineitem sum": "SELECT sum(l_extendedprice) FROM tpch.lineitem",
    "orders count": "SELECT count(*) FROM tpch.orders WHERE o_totalprice > 1000",
}
STATEMENT_QUERY = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
SCAN = "SELECT l_orderkey, l_linenumber FROM tpch.lineitem"
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


def datanodes():
    """The DataNode HTTP addresses of ``SC_DATANODES`` (empty without it)."""
    return [d for d in os.environ.get("SC_DATANODES", "").split(",") if d]


def block_reads(nodes):
    """Per DataNode (local, remote) block-read counters from JMX, read after the JMX cache settled;
    (None, None) when unreadable."""
    time.sleep(float(os.environ.get("SC_JMX_SETTLE", "11")))
    out = []
    for dn in nodes:
        try:
            with urllib.request.urlopen(f"http://{dn}/jmx", timeout=5) as r:
                text = r.read().decode("utf-8", "replace")
            local = sum(int(v) for v in re.findall(r'"ReadsFromLocalClient" : (\d+)', text))
            remote = sum(int(v) for v in re.findall(r'"ReadsFromRemoteClient" : (\d+)', text))
            out.append((local, remote))
        except Exception:  # noqa: BLE001 - a diagnostic, never a failure
            out.append((None, None))
    return out


def reads_delta(before, after):
    """(text, local, remote) of the counter changes between two block_reads snapshots."""
    parts, local, remote = [], 0, 0
    for i, ((lb, rb), (la, ra)) in enumerate(zip(before, after)):
        if None in (lb, rb, la, ra):
            parts.append(f"dn{i + 1} ?")
            continue
        local += la - lb
        remote += ra - rb
        parts.append(f"dn{i + 1} L+{la - lb} R+{ra - rb}")
    return " ".join(parts), local, remote


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

    dns = datanodes()
    before = block_reads(dns) if dns else None
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
    if os.environ.get("SC_PLAIN") == "1":
        # D15: the same 22 queries with plain table names (no ``tpch.``), against the same answers;
        # under distributedComplete their mergeable shapes must merge, not fall back.
        for i, (name, sql, want) in enumerate(queries):
            node = (i + 1) % len(conns)
            plain = re.sub(r"\btpch\.", "", sql)
            try:
                got = run(conns[node], plain)
            except Exception as e:  # noqa: BLE001 - every failure is reported
                fail(f"plain {name} on {servers[node]}: {first_line(e)}")
                continue
            diff = difference(got, want)
            if diff:
                fail(f"plain {name} on {servers[node]}: wrong answer: {diff}")
            else:
                print(f"ok plain {name} on {servers[node]} rows {got.num_rows}", flush=True)
    if dns:
        text, local, remote = reads_delta(before, block_reads(dns))
        print(f"reads total of the 22 queries: local {local}, remote {remote} ({text})", flush=True)

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


def varint(n):
    """Protobuf base-128 varint."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def field(number, data):
    """One length-delimited protobuf field."""
    return varint(number << 3 | 2) + varint(len(data)) + data


def statement_command(sql):
    """Any(CommandStatementQuery(query=sql)), the Flight SQL descriptor command of a statement."""
    return field(1, STATEMENT_QUERY.encode()) + field(2, field(1, sql.encode()))


def locality(servers, base):
    """The ``locality`` mode; returns the process exit code."""
    import pyarrow.flight as flight  # noqa: PLC0415 - only this mode needs it

    del base
    dns = datanodes()
    if len(dns) != len(servers):
        print(f"locality skipped: SC_DATANODES has {len(dns)} entries for {len(servers)} nodes", flush=True)
        return 1
    hosts = [s.split(":")[0] for s in servers]
    options = flight.FlightCallOptions(headers=[(b"x-flight-result-mode", b"partial")])
    endpoints = local_only = mixed = no_reads = 0
    total_local = total_remote = 0
    before = block_reads(dns)
    for label, sql in LOCALITY_QUERIES.items():
        client = flight.connect(f"grpc://{servers[0]}")
        info = client.get_flight_info(flight.FlightDescriptor.for_command(statement_command(sql)), options)
        after = block_reads(dns)
        text, local, remote = reads_delta(before, after)
        before = after
        print(f"plan {label} on node 1: {len(info.endpoints)} endpoints, planning {text}", flush=True)
        for k, ep in enumerate(info.endpoints):
            uri = ep.locations[0].uri.decode() if ep.locations else f"grpc://{servers[0]}"
            host = re.sub(r"^[a-z+]+://", "", uri).split(":")[0]
            node = hosts.index(host) + 1 if host in hosts else "?"
            reader = flight.connect(uri).do_get(ep.ticket, options)
            rows = reader.read_all().num_rows
            after = block_reads(dns)
            text, local, remote = reads_delta(before, after)
            before = after
            endpoints += 1
            total_local += local
            total_remote += remote
            # A read of node i counts as local on DataNode i (same network namespace) and remote elsewhere.
            if local == 0 and remote == 0:
                no_reads += 1  # served from a cache, or the DataNode counters could not be read
            elif remote == 0:
                local_only += 1
            elif local:
                mixed += 1
            print(f"endpoint {label} #{k} at node {node} ({uri}): rows {rows}, {text}", flush=True)
    print(f"locality partial: {endpoints} endpoints, {local_only} read only local blocks, {mixed} mixed, "
          f"{endpoints - local_only - mixed - no_reads} only remote, {no_reads} no block reads; "
          f"block reads local {total_local}, remote {total_remote}",
          flush=True)
    return 0


def retryable(error):
    """Whether a client may retry the error (gRPC UNAVAILABLE)."""
    return "unavailable" in str(error).lower()


def scan(server, rate):
    """Streams ``SCAN`` at <rate> rows/s: ((rows, sum key, sum line), error, rows read before it)."""
    rows = keys = lines = 0
    try:
        with connect(server) as conn, conn.cursor() as cursor:
            cursor.execute(SCAN)
            start = time.time()
            for batch in cursor.fetch_record_batch():
                rows += batch.num_rows
                keys += pc.sum(batch.column(0)).as_py() or 0
                lines += pc.sum(batch.column(1)).as_py() or 0
                if rate:
                    time.sleep(max(0.0, rows / rate - (time.time() - start)))
        return (rows, keys, lines), None, rows
    except Exception as e:  # noqa: BLE001
        return None, e, rows


def loss(servers, base, seconds, clients):
    """The ``loss`` mode; returns the process exit code."""
    queries = load(base)
    alive = servers[:-1]
    lock = threading.Lock()
    counts = {"ok": 0, "retryable": 0, "retried_ok": 0, "wrong": 0, "error": 0,
              "scan_ok": 0, "scan_peer_lost": 0, "scan_retryable": 0, "scan_retried_ok": 0}
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

    def scanner(want, rate):
        n = 0
        while time.time() < deadline:
            server = alive[n % len(alive)]
            n += 1
            got, error, read = scan(server, rate)
            if error is None:
                if tuple(got) == want:
                    record("scan_ok", f"scan {server} ok rows {got[0]}")
                else:
                    record("wrong", f"scan {server} wrong {got} != {want}", f"scan on {server}: wrong {got} != {want}")
                continue
            if not retryable(error):
                record("error", f"scan {server} error after {read} rows {first_line(error)}",
                       f"scan on {server}: non-retryable after {read} rows: {first_line(error)}")
                continue
            lost = read > 0 and "peer lost" in str(error).lower()
            if lost:
                with lock:
                    counts["scan_peer_lost"] += 1
            record("scan_retryable", f"scan {server} unavailable after {read} rows peerLost={lost} {first_line(error)}")
            other = alive[n % len(alive)]
            got, error, read = scan(other, 0)
            if error is None and tuple(got) == want:
                record("scan_retried_ok", f"scan {other} retry ok rows {got[0]}")
            else:
                diff = f"{got} != {want}" if error is None else first_line(error)
                record("wrong" if error is None else "error", f"scan {other} retry failed {diff}",
                       f"scan retried on {other}: {diff}")

    print("load start", flush=True)
    threads = [threading.Thread(target=client, args=(c,)) for c in range(clients)]
    scan_ref = os.path.join(base, "expected", "scan.txt")
    if os.path.exists(scan_ref):
        with open(scan_ref, encoding="utf-8") as fh:
            want = tuple(int(v) for v in fh.read().split())
        threads.append(threading.Thread(target=scanner,
                                        args=(want, float(os.environ.get("SC_SCAN_ROWS_PER_S", "150000")))))
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
    if mode == "locality":
        sys.exit(locality(servers, base))
    if mode == "loss":
        sys.exit(loss(servers, base, float(sys.argv[4]), int(sys.argv[5])))
    sys.exit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
