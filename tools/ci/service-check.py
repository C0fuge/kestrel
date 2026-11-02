"""Checks a running server end to end through the ADBC Flight SQL driver (Go), for the service workflows.

Usage: service-check.py <uri> <queries dir> [--ca <pem>] [--user <name> --password-env <VAR>]
                        [--token-env <VAR>] [--schema <name> ...] [--view <json>] [--extra <json>]

<queries dir> is service-tpch-gen.py's output. Checks, in this order, each printed as one
``PASS <check>`` or ``FAIL <check>: <reason>`` line:

* ``refused-anonymous`` / ``refused-bad-credential`` (only with --user or --token-env): a call
  without credentials and one with a wrong password (or a token with a broken signature) fail;
* ``sql-info``: GetSqlInfo answers;
* per schema (``--schema``, repeatable, default tpch; the queries' ``tpch.`` qualifiers are
  replaced by the schema, and checks of a schema other than tpch are named ``<schema>:<check>``):
  * ``tables``: GetTables / GetDbSchemas list exactly the TPC-H tables in the schema;
  * ``table-schema``: lineitem's schema has its 16 columns;
  * ``qNN``: the 22 TPC-H queries in complete mode give DuckDB's in-process answer (rows compared
    as sorted multisets; numbers with a relative tolerance of 1e-9, dates as ISO text);
  * ``prepared``: a prepared query with two bound parameters gives DuckDB's answer;
  * with ``--extra`` (a JSON list of ``{"name", "sql", "answer"}`` or ``{"name", "sql",
    "denied": true}``): each query gives its answer, or is refused as unauthorized / permission
    denied (the Ranger cell: masked values, filtered rows, a database without a policy);
* ``view`` (with ``--view``, a JSON file with ``sql`` and ``answer`` rows): the query's answer.

Exits 1 when any check failed. Credentials come from environment variables, never arguments.
"""
import argparse
import datetime
import decimal
import json
import math
import os
import re
import sys

import adbc_driver_flightsql.dbapi as flightsql

RESULTS = []


def report(name, error=None):
    """Prints and records one check's outcome."""
    if error is None:
        print(f"PASS {name}", flush=True)
    else:
        text = " ".join(str(error).split())[:300]
        print(f"FAIL {name}: {text}", flush=True)
    RESULTS.append(error is None)


def cell(value):
    """Renders a fetched cell as service-tpch-gen.py renders DuckDB's."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, (float, decimal.Decimal)):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def key(row):
    """Sort key of a row: numbers rounded to 4 places, so float noise does not reorder rows."""
    return [(0, "") if v is None else (1, f"{v:.4f}") if isinstance(v, float) else (1, str(v)) for v in row]


def same(a, b):
    """Tells whether two rendered cells agree (numbers within a relative 1e-9)."""
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
    return a == b


def compare(got, want):
    """Returns None when two answers agree, else the first difference."""
    if len(got) != len(want):
        return f"{len(got)} rows, expected {len(want)}"
    for i, (g, w) in enumerate(zip(sorted(got, key=key), sorted(want, key=key))):
        if len(g) != len(w) or not all(same(x, y) for x, y in zip(g, w)):
            return f"row {i}: {g} != expected {w}"
    return None


def connect(args, user=None, password=None, token=None):
    """Opens a connection with the given credentials (none: anonymous)."""
    kwargs = {"adbc.flight.sql.rpc.call_header.x-flight-result-mode": "complete"}
    if args.ca:
        with open(args.ca, encoding="utf-8") as f:
            kwargs["adbc.flight.sql.client_option.tls_root_certs"] = f.read()
    if user is not None:
        kwargs["username"] = user
        kwargs["password"] = password
    if token is not None:
        kwargs["adbc.flight.sql.authorization_header"] = f"Bearer {token}"
    return flightsql.connect(args.uri, db_kwargs=kwargs)


def refused(name, args, **credentials):
    """Checks that a query with the given credentials is refused."""
    try:
        with connect(args, **credentials) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchall()
        report(name, "the call was answered")
    except Exception as e:  # pylint: disable=broad-except
        text = str(e)
        if "UNAUTHENTICATED" in text.upper() or "UNAUTHORIZED" in text.upper():
            report(name)
        else:
            report(name, f"refused, but not as unauthenticated: {text}")


def qualify(sql, schema):
    """Replaces the queries' tpch. qualifiers by another schema."""
    return sql if schema == "tpch" else re.sub(r"\btpch\.", schema + ".", sql)


def check_extra(conn, name, check, schema):
    """Runs one --extra check: the query's answer, or its refusal for lack of rights."""
    try:
        with conn.cursor() as cur:
            cur.execute(qualify(check["sql"], schema))
            got = [[cell(v) for v in row] for row in cur.fetchall()]
    except Exception as e:  # pylint: disable=broad-except
        text = str(e).upper()
        if check.get("denied") and ("UNAUTHORIZED" in text or "PERMISSION" in text):
            report(name)
        else:
            report(name, e)
        return
    report(name, f"answered {len(got)} rows, expected a refusal" if check.get("denied")
           else compare(got, check["answer"]))


def check_schema(conn, queries, schema, tables, extra=()):
    """Runs the metadata, TPC-H, prepared and extra checks against one schema."""
    p = "" if schema == "tpch" else schema + ":"
    try:
        objects = conn.adbc_get_objects(depth="tables", db_schema_filter=schema).read_all().to_pylist()
        names = sorted(t["table_name"] for c in objects for s in (c["catalog_db_schemas"] or [])
                       if s["db_schema_name"] == schema for t in (s["db_schema_tables"] or []))
        report(p + "tables", None if names == tables else f"{names}, expected {tables}")
    except Exception as e:  # pylint: disable=broad-except
        report(p + "tables", e)
    try:
        columns = conn.adbc_get_table_schema("lineitem", db_schema_filter=schema)
        report(p + "table-schema", None if len(columns) == 16 else f"{len(columns)} columns: {columns.names}")
    except Exception as e:  # pylint: disable=broad-except
        report(p + "table-schema", e)
    for name in sorted(f[:-4] for f in os.listdir(queries) if f.startswith("q") and f.endswith(".sql")):
        with open(os.path.join(queries, name + ".sql"), encoding="utf-8") as f:
            sql = qualify(f.read().strip().rstrip(";"), schema)
        with open(os.path.join(queries, name + ".json"), encoding="utf-8") as f:
            want = json.load(f)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                got = [[cell(v) for v in row] for row in cur.fetchall()]
            report(p + name, compare(got, want))
        except Exception as e:  # pylint: disable=broad-except
            report(p + name, e)
    with open(os.path.join(queries, "prepared.json"), encoding="utf-8") as f:
        prepared = json.load(f)
    try:
        with conn.cursor() as cur:
            cur.execute(qualify(prepared["sql"], schema), parameters=tuple(prepared["parameters"]))
            got = cur.fetchall()
        report(p + "prepared", compare([[cell(v) for v in r] for r in got], [[prepared["answer"]]]))
    except Exception as e:  # pylint: disable=broad-except
        report(p + "prepared", e)
    for check in extra:
        check_extra(conn, p + check["name"], check, schema)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri")
    parser.add_argument("queries")
    parser.add_argument("--ca")
    parser.add_argument("--user")
    parser.add_argument("--password-env")
    parser.add_argument("--token-env")
    parser.add_argument("--schema", action="append")
    parser.add_argument("--view")
    parser.add_argument("--extra")
    args = parser.parse_args()
    password = os.environ[args.password_env] if args.password_env else None
    token = os.environ[args.token_env] if args.token_env else None

    if args.user:
        refused("refused-anonymous", args)
        refused("refused-bad-credential", args, user=args.user, password=password + "x")
    elif token:
        refused("refused-anonymous", args)
        head, _, rest = token.rpartition(".")
        refused("refused-bad-credential", args, token=head + "." + ("A" if rest[:1] != "A" else "B") + rest[1:])

    with open(os.path.join(args.queries, "tables.json"), encoding="utf-8") as f:
        tables = json.load(f)
    try:
        conn = connect(args, user=args.user, password=password, token=token)
    except Exception as e:  # pylint: disable=broad-except
        report("connect", e)
        sys.exit(1)
    with conn:
        try:
            info = conn.adbc_get_info()
            report("sql-info", None if info else "empty answer")
        except Exception as e:  # pylint: disable=broad-except
            report("sql-info", e)
        extra = ()
        if args.extra:
            with open(args.extra, encoding="utf-8") as f:
                extra = json.load(f)
        for schema in args.schema or ["tpch"]:
            check_schema(conn, args.queries, schema, tables, extra)
        if args.view:
            with open(args.view, encoding="utf-8") as f:
                view = json.load(f)
            try:
                with conn.cursor() as cur:
                    cur.execute(view["sql"])
                    got = [[cell(v) for v in row] for row in cur.fetchall()]
                report("view", compare(got, view["answer"]))
            except Exception as e:  # pylint: disable=broad-except
                report("view", e)
    passed = sum(RESULTS)
    print(f"checks {passed}/{len(RESULTS)} passed", flush=True)
    sys.exit(0 if passed == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
