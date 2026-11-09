"""Closed-loop point-query load against one server (item 39 (c), lane 4).

Usage: point-query-load.py <host:port> <clients> <seconds> <keys>

Every client is its own process with one ADBC Flight SQL connection (complete mode) and runs the
real-cluster benchmark's small-query classes in turn, each with a fresh random key in the SQL
text (as the benchmark did, so no prepared statement or answer is reused):
``point_orders`` (one order by key), ``point_lineitem`` (the lines of one order) and
``count_filter`` (a filtered count over orders). Prints one line
``c<clients> qps=<q/s> p50=<ms> p95=<ms> p99=<ms> max=<ms> queries=<n> errors=<n> first_error=<text>``.
Exit code 0 when no query failed.
"""
import multiprocessing
import random
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql

QUERIES = {
    "point_orders": "SELECT * FROM tpch.orders WHERE o_orderkey = {k}",
    "point_lineitem": "SELECT * FROM tpch.lineitem WHERE l_orderkey = {k}",
    "count_filter": "SELECT count(*) FROM tpch.orders WHERE o_custkey = {c} AND o_totalprice > 1000",
}


def client(server, seconds, keys, seed, out):
    """Runs queries until the deadline and puts (latencies in ms, errors, first error) on out."""
    rnd = random.Random(seed)
    lat, errors, first = [], 0, ""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    try:
        conn = flightsql.connect(f"grpc://{server}", db_kwargs=kwargs)
    except Exception as e:  # noqa: BLE001 - reported, not raised
        out.put(([], 1, f"connect: {e}"))
        return
    names = list(QUERIES)
    deadline = time.monotonic() + seconds
    i = seed
    with conn:
        while time.monotonic() < deadline:
            sql = QUERIES[names[i % len(names)]].format(k=rnd.randint(1, keys) * 4 - 3,
                                                        c=rnd.randint(1, max(1, keys // 10)))
            i += 1
            t0 = time.monotonic()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.fetch_arrow_table()
                lat.append((time.monotonic() - t0) * 1000.0)
            except Exception as e:  # noqa: BLE001 - counted, the run goes on
                errors += 1
                if not first:
                    first = str(e).splitlines()[0][:200] if str(e) else type(e).__name__
    out.put((lat, errors, first))


def pct(values, p):
    """Returns the p-th percentile (nearest rank) of sorted values, 0 when empty."""
    if not values:
        return 0.0
    return values[min(len(values) - 1, int(len(values) * p / 100.0))]


def main():
    server, clients, seconds, keys = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])
    ctx = multiprocessing.get_context("spawn")
    out = ctx.Queue()
    procs = [ctx.Process(target=client, args=(server, seconds, keys, n + 1, out)) for n in range(clients)]
    for p in procs:
        p.start()
    results = [out.get() for _ in procs]
    for p in procs:
        p.join()
    wall = seconds  # every client runs for this long after its own start
    lat = sorted(x for r in results for x in r[0])
    errors = sum(r[1] for r in results)
    first = next((r[2] for r in results if r[2]), "")
    print(f"c{clients} qps={len(lat) / wall:.1f} p50={pct(lat, 50):.0f} p95={pct(lat, 95):.0f}"
          f" p99={pct(lat, 99):.0f} max={(lat[-1] if lat else 0):.0f} queries={len(lat)} errors={errors}"
          f" first_error={first or '-'}", flush=True)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
