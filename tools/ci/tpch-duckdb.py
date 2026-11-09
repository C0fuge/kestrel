"""Times the TPC-H queries in plain in-process DuckDB over the same S3 endpoint, as a GizmoSQL-like reference.

Usage: tpch-duckdb.py <httpfs extension> <s3 endpoint host:port> <s3 root> <queries dir> <rounds> <settings out>

Item 40(a): GizmoSQL runs plain DuckDB with its httpfs extension and DuckDB's default settings. This
script does the same in one process: LOAD httpfs, one S3 secret (key and secret from S3_KEY / S3_SECRET,
path style, plain HTTP; both skipped when <httpfs extension> is ``-``, for a local <s3 root>), schema ``tpch`` with one view per table directory below <s3 root>
(``read_parquet('<root>/<table>/*.parquet')``), then one unmeasured pass and <rounds> measured passes.
It prints the same lines as tpch-adbc.py (``q01 r1 1234.5 ms rows 4 sum <checksum>``, ``total r<n> <s> s``;
the checksum is computed the same way, so it is comparable with tpch-adbc.py's) and writes
``SELECT name, value FROM duckdb_settings()`` to <settings out> (one ``name=value`` per line). Exits non-zero
when a row count differs from ``qNN.rows``.
"""
import decimal
import hashlib
import os
import sys
import time

import duckdb

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def normalized(value):
    """Renders a cell with floating-point and decimal values rounded to 2 decimals."""
    if isinstance(value, (float, decimal.Decimal)):
        return f"{value:.2f}"
    return "None" if value is None else str(value)


def read(db, sql):
    """Runs one query and returns its row count and order-independent checksum."""
    table = db.execute(sql).fetch_arrow_table()
    total = 0
    for row in table.to_pylist():
        text = "|".join(normalized(v) for v in row.values()) + "|"
        digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
        total = (total + int.from_bytes(digest, "little")) % (1 << 64)
    return table.num_rows, total


def quote(text):
    """Quotes a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def main():
    httpfs, endpoint, root, queries, rounds, settings_out = sys.argv[1:7]
    rounds = int(rounds)
    db = duckdb.connect()
    if httpfs != "-":
        db.execute(f"LOAD {quote(httpfs)}")
        db.execute(
            "CREATE SECRET plain_s3 (TYPE s3, KEY_ID " + quote(os.environ["S3_KEY"]) + ", SECRET "
            + quote(os.environ["S3_SECRET"]) + ", ENDPOINT " + quote(endpoint)
            + ", URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
    db.execute("CREATE SCHEMA tpch")
    for t in TABLES:
        db.execute(f"CREATE VIEW tpch.{t} AS SELECT * FROM read_parquet({quote(root.rstrip('/') + '/' + t + '/*.parquet')})")
    with open(settings_out, "w", encoding="utf-8") as f:
        for name, value in db.execute("SELECT name, value FROM duckdb_settings() ORDER BY name").fetchall():
            f.write(f"{name}={value}\n")
    names = sorted(f[:-4] for f in os.listdir(queries) if f.endswith(".sql"))
    if not names:
        sys.exit(f"no .sql files in {queries}")
    failed = False
    for r in range(rounds + 1):
        total = 0
        for name in names:
            with open(os.path.join(queries, name + ".sql"), encoding="utf-8") as f:
                sql = f.read().strip().rstrip(";")
            rows_file = os.path.join(queries, name + ".rows")
            want = int(open(rows_file).read().strip()) if os.path.exists(rows_file) else -1
            t0 = time.perf_counter_ns()
            rows, checksum = read(db, sql)
            nanos = time.perf_counter_ns() - t0
            total += nanos
            if (want >= 0 and rows != want) or (want < 0 and rows == 0):
                print(f"{name} rows {rows}, expected {want if want >= 0 else 'at least 1'}", file=sys.stderr)
                failed = True
            print(f"{name} r{r} {nanos / 1e6:9.1f} ms rows {rows} sum {checksum:016x}", flush=True)
        print(f"total r{r} {total / 1e9:.2f} s{' (warm-up)' if r == 0 else ''}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
