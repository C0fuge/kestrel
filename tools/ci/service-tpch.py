"""TPC-H data, queries and reference answers for the service matrix (service-*.yml).

Usage:
  service-tpch.py gen <out dir> <scale factor>
      Writes <out>/data/tpch/<table>/data.parquet (DuckDB's dbgen), <out>/csv/<table>.csv (for
      databases loaded with COPY / LOAD DATA), <out>/queries/qNN.sql (DuckDB's 22 TPC-H queries with
      the tables qualified as ``tpch.<table>``), and <out>/reference.json: every query's answer
      computed by in-process DuckDB over the Parquet files, the tables' columns, and the answers of
      the prepared query for its parameters.
  service-tpch.py check <host:port> <out dir> <schema> <result json> [result mode]
      Runs the 22 queries through the ADBC Flight SQL driver with ``tpch.`` replaced by
      ``<schema>.``, compares every answer with the reference (same columns, same rows as a
      multiset; numbers equal within 1e-6 relative or 0.005 absolute), checks the Flight SQL
      metadata (GetSqlInfo, GetObjects: the schema and its 8 tables with their columns,
      GetTableSchema of lineitem) and one prepared query with a parameter, bound twice. Writes
      {"ok": bool, "error": first error line or "", "lines": [...]} to <result json>; exits 1 when
      anything failed.

TPC-H q11's fraction is 0.0001 / SF as in the specification (DuckDB's text has the SF1 value).
"""
import datetime
import decimal
import json
import os
import re
import sys
import time

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
PREPARED = "SELECT count(*) AS n, sum(l_extendedprice) AS s FROM tpch.lineitem WHERE l_quantity < ?"
PARAMETERS = [24, 7]


def norm(value):
    """Normalises a cell: numbers to float, dates and times to ISO text, None kept."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, decimal.Decimal)):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def sort_key(row):
    """Sort key of a normalised row: numbers rounded so that tiny differences keep the order."""
    return tuple((0, "") if v is None else (1, f"{v:.2f}") if isinstance(v, float) else (2, str(v))
                 for v in row)


def gen(out, sf):
    """Generates the data, queries and reference answers."""
    import duckdb
    data, queries, csv = (os.path.join(out, d) for d in ("data/tpch", "queries", "csv"))
    for d in (queries, csv):
        os.makedirs(d, exist_ok=True)
    db = duckdb.connect()
    db.execute("SET memory_limit='8GB'")
    db.execute("INSTALL tpch; LOAD tpch")
    db.execute(f"CALL dbgen(sf={sf})")
    columns = {}
    for t in TABLES:
        os.makedirs(os.path.join(data, t), exist_ok=True)
        db.execute(f"COPY {t} TO '{os.path.join(data, t, 'data.parquet')}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
        db.execute(f"COPY {t} TO '{os.path.join(csv, t + '.csv')}' (FORMAT csv, HEADER false)")
        columns[t] = [[name, typ] for name, typ, *_ in db.execute(f"DESCRIBE {t}").fetchall()]
    # The reference reads the Parquet files, as the server does.
    ref = duckdb.connect()
    ref.execute("CREATE SCHEMA tpch")
    for t in TABLES:
        ref.execute(f"CREATE VIEW tpch.{t} AS SELECT * FROM read_parquet('{os.path.join(data, t, 'data.parquet')}')")
    pattern = re.compile(r"(\b(?:from|join)\s+|,\s*)(" + "|".join(TABLES) + r")\b", re.IGNORECASE)
    answers = {}
    for nr, q in db.execute("SELECT query_nr, query FROM tpch_queries()").fetchall():
        if nr == 11:
            q = q.replace("0.0001000000", "0.0001").replace("0.0001", f"{0.0001 / sf:.10f}")
        q = pattern.sub(lambda m: m.group(1) + "tpch." + m.group(2), q).strip().rstrip(";")
        name = f"q{nr:02d}"
        with open(os.path.join(queries, name + ".sql"), "w", encoding="utf-8") as f:
            f.write(q + "\n")
        cur = ref.execute(q)
        names = [d[0].lower() for d in cur.description]
        rows = [[norm(v) for v in r] for r in cur.fetchall()]
        answers[name] = {"columns": names, "rows": sorted(rows, key=sort_key)}
        print(f"{name} reference rows {len(rows)}")
    prepared = {str(p): [norm(v) for v in ref.execute(PREPARED, [p]).fetchone()] for p in PARAMETERS}
    with open(os.path.join(out, "reference.json"), "w", encoding="utf-8") as f:
        json.dump({"sf": sf, "columns": columns, "answers": answers, "prepared": prepared}, f)


def same(a, b):
    """Whether two normalised cells are equal (numbers within tolerance)."""
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) <= max(0.005, 1e-6 * max(abs(a), abs(b)))
    return a == b


def compare(name, want, columns, rows):
    """Returns the first difference of an answer, or None."""
    if columns != want["columns"]:
        return f"{name}: columns {columns}, expected {want['columns']}"
    rows = sorted(rows, key=sort_key)
    if len(rows) != len(want["rows"]):
        return f"{name}: {len(rows)} rows, expected {len(want['rows'])}"
    for i, (got, exp) in enumerate(zip(rows, want["rows"])):
        if len(got) != len(exp) or not all(same(x, y) for x, y in zip(got, exp)):
            return f"{name}: row {i} {got}, expected {exp}"
    return None


def check(server, out, schema, result, mode):
    """Runs the queries, metadata and prepared checks against a server."""
    import adbc_driver_flightsql.dbapi as flightsql
    with open(os.path.join(out, "reference.json"), encoding="utf-8") as f:
        ref = json.load(f)
    lines, errors = [], []

    def note(text, error=False):
        print(text, flush=True)
        lines.append(text)
        if error:
            errors.append(text)

    def qualify(sql):
        return re.sub(r"\btpch\.", schema + ".", sql)

    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": mode}
    try:
        with flightsql.connect(f"grpc://{server}", db_kwargs=kwargs) as conn:
            total = 0.0
            for name in sorted(ref["answers"]):
                with open(os.path.join(out, "queries", name + ".sql"), encoding="utf-8") as f:
                    sql = qualify(f.read().strip())
                t0 = time.perf_counter()
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        table = cur.fetch_arrow_table()
                except Exception as e:  # noqa: BLE001 - every failure is a result line
                    note(f"{name}: error {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}", True)
                    continue
                secs = time.perf_counter() - t0
                total += secs
                rows = [[norm(v) for v in r.values()] for r in table.to_pylist()]
                diff = compare(name, ref["answers"][name], [c.lower() for c in table.column_names], rows)
                note(diff if diff else f"{name}: ok {len(rows)} rows {secs:.2f} s", diff is not None)
            note(f"total {total:.1f} s")
            # Metadata: SqlInfo, the schema's tables and columns, one table schema.
            try:
                info = conn.adbc_get_info()
                note(f"metadata: GetSqlInfo vendor={info.get('vendor_name')} version={info.get('vendor_version')}")
                objects = conn.adbc_get_objects(depth="all", db_schema_filter=schema.split(".")[-1]).read_all()
                found = {}
                for cat in objects.to_pylist():
                    for sch in cat.get("catalog_db_schemas") or []:
                        if sch["db_schema_name"] != schema.split(".")[-1]:
                            continue
                        for tab in sch.get("db_schema_tables") or []:
                            found[tab["table_name"].lower()] = [c["column_name"].lower()
                                                                for c in tab.get("table_columns") or []]
                for t in TABLES:
                    want = [c[0] for c in ref["columns"][t]]
                    if t not in found:
                        note(f"metadata: GetObjects lacks table {schema}.{t} (has {sorted(found)})", True)
                    elif found[t] != want:
                        note(f"metadata: GetObjects columns of {t} {found[t]}, expected {want}", True)
                if all(t in found for t in TABLES):
                    note(f"metadata: GetObjects {schema}: 8 tables with their columns")
                parts = schema.split(".")
                arrow_schema = conn.adbc_get_table_schema("lineitem", db_schema_filter=parts[-1],
                                                          catalog_filter=parts[0] if len(parts) > 1 else None)
                names = [n.lower() for n in arrow_schema.names]
                want = [c[0] for c in ref["columns"]["lineitem"]]
                if names != want:
                    note(f"metadata: GetTableSchema lineitem {names}, expected {want}", True)
                else:
                    note("metadata: GetTableSchema lineitem ok")
            except Exception as e:  # noqa: BLE001
                note(f"metadata: error {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}", True)
            # A prepared query with one parameter, bound twice on the same statement.
            try:
                with conn.cursor() as cur:
                    sql = qualify(PREPARED)
                    cur.adbc_prepare(sql)
                    for p in PARAMETERS:
                        cur.execute(sql, parameters=(p,))
                        got = [norm(v) for v in cur.fetchone()]
                        want = ref["prepared"][str(p)]
                        if len(got) != len(want) or not all(same(x, y) for x, y in zip(got, want)):
                            note(f"prepared: l_quantity < {p} answered {got}, expected {want}", True)
                        else:
                            note(f"prepared: l_quantity < {p} ok {got}")
            except Exception as e:  # noqa: BLE001
                note(f"prepared: error {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}", True)
    except Exception as e:  # noqa: BLE001
        note(f"connect: error {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}", True)
    ok = not errors
    with open(result, "w", encoding="utf-8") as f:
        json.dump({"ok": ok, "error": errors[0] if errors else "", "lines": lines}, f)
    print("PASS" if ok else f"FAIL: {len(errors)} problem(s), first: {errors[0]}")
    return 0 if ok else 1


def main():
    if len(sys.argv) >= 4 and sys.argv[1] == "gen":
        gen(sys.argv[2], float(sys.argv[3]))
        return 0
    if len(sys.argv) >= 6 and sys.argv[1] == "check":
        return check(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5],
                     sys.argv[6] if len(sys.argv) > 6 else "complete")
    sys.exit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
