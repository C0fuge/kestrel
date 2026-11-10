#!/usr/bin/env python3
"""File pruning of point queries on SASL DataNodes (item 42 (2)), a stage of the sasl cells.

The maintainer's stand logged no ``views=prunedFiles`` line for a point query on SASL DataNodes
(short-circuit only, node-local copies), while privileged DataNodes pruned the same query to one
of eight files. The cell uploads ``point.orders_r``: tpch.orders sorted by o_orderkey and written
as eight files of one key range each, so that footer min/max tell the files apart.

Modes:

``gen <tpch orders dir> <out dir>``  writes the eight range files.
``check <host:port> <out dir>``      runs complete-mode point queries (a key in the first file, one
                                     in the last, one in no file) and compares each answer with
                                     pyarrow's over the same files; the cell then counts the
                                     server's ``views=prunedFiles`` lines.

Prints ``ok``/``FAIL`` lines and a last line ``result pass`` or ``result fail <first error>``;
exits non-zero on a failure.
"""
import glob
import importlib.util
import math
import os
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

FILES = 8
COLUMNS = ["o_orderkey", "o_custkey", "o_totalprice", "o_orderdate"]


def gen(source, out):
    """Writes tpch.orders as FILES files of one o_orderkey range each."""
    table = pq.read_table(sorted(glob.glob(os.path.join(source, "*.parquet")))).sort_by("o_orderkey")
    os.makedirs(out, exist_ok=True)
    per = math.ceil(table.num_rows / FILES)
    for i in range(FILES):
        pq.write_table(table.slice(i * per, per), os.path.join(out, f"part-{i:03d}.parquet"))
    print(f"wrote {FILES} files of {per} rows to {out}", flush=True)


def main():
    if sys.argv[1] == "gen":
        gen(sys.argv[2], sys.argv[3])
        return 0
    spec = importlib.util.spec_from_file_location(
        "service_hdfs_sasl_check", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                "service-hdfs-sasl-check.py"))
    sasl = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sasl)
    server, out = sys.argv[2], sys.argv[3]
    files = sorted(glob.glob(os.path.join(out, "*.parquet")))
    first = pq.read_table(files[0], columns=COLUMNS)
    last = pq.read_table(files[-1], columns=COLUMNS)
    everything = pq.read_table(files, columns=COLUMNS)
    keys = [first["o_orderkey"][first.num_rows // 2].as_py(), last["o_orderkey"][last.num_rows // 2].as_py(), 0]
    failures = []
    for key in keys:
        sql = f"SELECT {', '.join(COLUMNS)} FROM point.orders_r WHERE o_orderkey = {key}"
        want = sorted(tuple(r.values()) for r in everything.filter(pc.equal(everything["o_orderkey"], key))
                      .to_pylist())
        try:
            with sasl.connect(server, "complete") as conn:
                got = sorted(tuple(r.values()) for r in sasl.run(conn, sql).select(COLUMNS).to_pylist())
            ok = got == want
            text = f"point {key}: {len(got)} row(s), want {len(want)}" + ("" if ok else f" got {got} want {want}")
        except Exception as e:  # noqa: BLE001 - every driver error is a failure here
            ok = False
            text = f"point {key}: {type(e).__name__}: {str(e).splitlines()[0][:200]}"
        print(("ok   " if ok else "FAIL ") + text, flush=True)
        if not ok:
            failures.append(text)
    print("result pass" if not failures else f"result fail {failures[0]}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
