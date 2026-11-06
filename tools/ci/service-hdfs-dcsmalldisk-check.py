#!/usr/bin/env python3
"""Distributed complete on small copy disks (STATUS item 36 (1)), last stage of the sasl-cluster speed cell.

The maintainer's stand (3 servers, ``-DresourceProfile=speed -DdistributedComplete=true``, a 64 MiB
tmpfs as ``hdfsLocalCopyDir``, default reserve) refused ``SELECT * FROM spread.lineitem_sp`` (no
node holds a file whole) with ``No room ... needs all its copies at once``, and every later query
over that table too: the entry node held its deleted copy files open, so their room stayed used.

The cell restarts its three servers that way: each with its own tmpfs as ``/lcopy``, sized for the
largest lineitem file plus 4 MiB above the default reserve. This client runs, twice, a list of
complete-mode queries over ``chunks.lineitem`` (1 MiB blocks, replication 1: every file is read
from copies) on the first server: a full scan, a projection scan, a filtered scan, count/sum,
DISTINCT, a top-5 and a GROUP BY. Each answer must equal the local files. After each query, no
server may keep a file below ``/lcopy`` open once it is deleted (``/proc/1/fd`` of each server
container, waited for up to 15 s); the second round would also be refused if room leaked.

Usage: service-hdfs-dcsmalldisk-check.py <host:port> <input dir> <container>[,<container>...]
Prints ``ok``/``FAIL`` lines and a last line ``result pass`` or ``result fail <first error>``;
exits non-zero on a failure.
"""
import decimal
import glob
import importlib.util
import os
import subprocess
import sys
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "service_hdfs_sasl_check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                            "service-hdfs-sasl-check.py"))
_SASL = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_SASL)

COPY_DIR = "/lcopy/"
ROUNDS = 2
WAIT_SECONDS = 15
FILTER_BOUND = 25
COUNT = "SELECT count(*) AS n, sum(l_quantity) AS q FROM chunks.lineitem"


def lineitem(base, columns):
    """The local lineitem files as one Arrow table with the given columns."""
    return pq.read_table(glob.glob(os.path.join(base, "data/tpch/lineitem", "*.parquet")),
                         columns=columns)


def quantity_sum(column):
    """sum(l_quantity) of an Arrow column as a Decimal (0 when empty)."""
    total = pc.sum(column).as_py()
    return decimal.Decimal(str(total)) if total is not None else decimal.Decimal(0)


def stream(conn, sql):
    """Runs a scan and adds up its rows and l_quantity batch by batch, never holding the answer."""
    rows, total = 0, decimal.Decimal(0)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        for batch in cursor.fetch_record_batch():
            rows += batch.num_rows
            total += quantity_sum(batch.column(batch.schema.get_field_index("l_quantity")))
    return rows, total


def references(base):
    """The expected answer of every query, by name."""
    table = lineitem(base, ["l_quantity"])
    whole = (table.num_rows, quantity_sum(table.column("l_quantity")))
    kept = table.filter(pc.greater(table.column("l_quantity"), FILTER_BOUND))
    return {
        "scan": whole,
        "projection": whole,
        "filtered": (kept.num_rows, quantity_sum(kept.column("l_quantity"))),
        "count": whole,
        "distinct": set(_SASL.reference_groups(base).keys()),
        "top": _SASL.reference_top(base),
        "groups": _SASL.reference_groups(base),
    }


def answer(conn, name):
    """Runs the query called name and returns its answer in the shape of references()."""
    if name == "scan":
        return stream(conn, "SELECT * FROM chunks.lineitem")
    if name == "projection":
        return stream(conn, _SASL.CHUNKS_SCAN)
    if name == "filtered":
        return stream(conn, f"SELECT l_quantity FROM chunks.lineitem WHERE l_quantity > {FILTER_BOUND}")
    if name == "count":
        return _SASL.totals(_SASL.run(conn, COUNT))[:2]
    if name == "distinct":
        return {(r["l_returnflag"], r["l_linestatus"])
                for r in _SASL.run(conn, _SASL.CHUNKS_DISTINCT).to_pylist()}
    if name == "top":
        return [(r["l_orderkey"], r["l_linenumber"]) for r in _SASL.run(conn, _SASL.CHUNKS_TOP).to_pylist()]
    if name == "groups":
        return _SASL.groups(_SASL.run(conn, _SASL.CHUNKS_GROUP))
    raise ValueError(name)


def same(name, got, want):
    """Tells whether an answer equals the expected one."""
    if name == "groups":
        return _SASL.same_groups(got, want)
    return got == want


def deleted_open(containers):
    """Files below COPY_DIR that a server process still holds open after they were deleted."""
    found = []
    for c in containers:
        out = subprocess.run(["docker", "exec", c, "sh", "-c", "ls -l /proc/1/fd 2>/dev/null"],
                             capture_output=True, text=True, check=False).stdout
        found += [f"{c}:{line.split('-> ', 1)[1]}" for line in out.splitlines()
                  if "-> " + COPY_DIR in line and line.endswith("(deleted)")]
    return found


def settled(containers):
    """Waits up to WAIT_SECONDS for no deleted-open copy; returns the ones left and the seconds waited."""
    start = time.monotonic()
    while True:
        left = deleted_open(containers)
        waited = time.monotonic() - start
        if not left or waited >= WAIT_SECONDS:
            return left, waited
        time.sleep(0.5)


def main():
    server, base, containers = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
    want = references(base)
    names = ["scan", "count", "distinct", "top", "groups", "projection", "filtered", "scan"]
    failure = None
    for r in range(1, ROUNDS + 1):
        for name in names:
            label = f"round {r} {name}"
            start = time.monotonic()
            try:
                with _SASL.connect(server, "complete") as conn:
                    got = answer(conn, name)
                if same(name, got, want[name]):
                    print(f"ok {label} ({time.monotonic() - start:.1f} s)", flush=True)
                else:
                    print(f"FAIL {label}: wrong answer {str(got)[:200]}, want {str(want[name])[:200]}", flush=True)
                    failure = failure or f"{label}: wrong answer"
            except Exception as e:  # noqa: BLE001 - every failure is reported
                print(f"FAIL {label}: {_SASL.first_line(e)}", flush=True)
                failure = failure or f"{label}: {_SASL.first_line(e)[:200]}"
            left, waited = settled(containers)
            if left:
                print(f"FAIL {label}: {len(left)} deleted copy file(s) still open after {waited:.0f} s:"
                      f" {' '.join(left)[:400]}", flush=True)
                failure = failure or f"{label}: {len(left)} deleted copy file(s) still open ({left[0][:120]})"
            elif waited >= 1:
                print(f"ok {label}: deleted copies closed after {waited:.1f} s", flush=True)
    print("result pass" if failure is None else f"result fail {failure}")
    return 0 if failure is None else 1


if __name__ == "__main__":
    sys.exit(main())
