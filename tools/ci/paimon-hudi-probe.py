"""Lane 4 (catalogs and sources), item C4 (b) probe: can a light reader - the table format's own metadata read by
the plugin, the data files read by DuckDB's read_parquet as for every other table - answer Apache Paimon and Apache
Hudi tables correctly, and what must it refuse? No Paimon or Hudi library is used.

    python -I paimon-hudi-probe.py <fixtureDir> [<extensionDir>]

<fixtureDir> is the output of tools/ci/paimon-hudi-fixtures.py (expected.tsv holds Spark's answers). Needs the pip
packages duckdb (same version as the server's duckdb_jdbc), fastavro, backports.zstd (Paimon writes its manifests
as zstd-compressed Avro, a codec DuckDB's avro extension does not read) and pyroaring (Paimon's deletion vectors).
<extensionDir>, when given, may hold
paimon.duckdb_extension / hudi.duckdb_extension found for this DuckDB version; each one found is loaded and its
functions listed.

Every table gets one of: "ok" (equal to Spark), "refused: <reason>" (the light reader declines; acceptable) or
"WRONG" (a different answer - exit status 1: a reader must never answer wrongly). Results are printed and appended
to $GITHUB_STEP_SUMMARY when set.
"""

import json
import os
import re
import struct
import sys

import duckdb
import fastavro
import pyroaring

SUMMARY = ["| table | where | result | detail |", "|---|---|---|---|"]
FAILED = []


class Refused(Exception):
    """The light reader cannot answer this table correctly."""


def lit(value):
    return "'" + value.replace("'", "''") + "'"


def report(table, where, result, detail):
    detail = str(detail).replace("\n", " ")
    print(f"RESULT {table} | {where} | {result} | {detail}")
    SUMMARY.append(f"| {table} | {where} | {result} | `{detail[:400].replace('|', '/').replace('`', chr(39))}` |")


def answer_sql(source, where):
    return (f"SELECT count(*), sum(id), coalesce(string_agg(name, '' ORDER BY id), '') FROM ({source}) t"
            + (f" WHERE {where}" if where else ""))


def files_list(paths):
    return "[" + ", ".join(lit(p) for p in paths) + "]"


# ---------------------------------------------------------------------------------------------------------- Paimon

def avro_rows(path):
    with open(path, "rb") as f:
        return list(fastavro.reader(f))


def paimon_source(table):
    """SQL over read_parquet for the latest snapshot of a Paimon table, or Refused."""
    snapshots = [int(n[len("snapshot-"):]) for n in os.listdir(os.path.join(table, "snapshot"))
                 if n.startswith("snapshot-")]
    if not snapshots:
        raise Refused("no snapshot")
    with open(os.path.join(table, "snapshot", f"snapshot-{max(snapshots)}"), encoding="utf-8") as f:
        snap = json.load(f)
    with open(os.path.join(table, "schema", f"schema-{snap['schemaId']}"), encoding="utf-8") as f:
        schema = json.load(f)
    options = schema.get("options", {})
    pks = schema.get("primaryKeys", [])
    if options.get("file.format", "parquet") != "parquet":
        raise Refused("file.format " + options["file.format"])
    if pks and options.get("merge-engine", "deduplicate") != "deduplicate":
        raise Refused("merge-engine " + options["merge-engine"])
    if options.get("bucket") == "-2":
        raise Refused("postponed bucket")
    live = {}
    for key in ("baseManifestList", "deltaManifestList"):
        if not snap.get(key):
            continue
        for manifest in avro_rows(os.path.join(table, "manifest", snap[key])):
            for entry in avro_rows(os.path.join(table, "manifest", manifest["_FILE_NAME"])):
                meta = entry["_FILE"]
                ident = (bytes(entry["_PARTITION"]), entry["_BUCKET"], meta["_FILE_NAME"])
                if entry["_KIND"] == 0:
                    live[ident] = meta
                else:
                    live.pop(ident, None)
    dv_mode = options.get("deletion-vectors.enabled", "false") == "true"
    deleted = paimon_deletion_vectors(table, snap)
    if deleted and not dv_mode:
        raise Refused("deletion vectors on a table without deletion-vectors.enabled")
    names = {}
    for root, _, files in os.walk(table):
        if os.path.basename(root).startswith("bucket-"):
            for name in files:
                names[(os.path.basename(root), name)] = os.path.join(root, name)
    paths = []
    schema_ids = set()
    level0 = 0
    for (_, bucket, name), meta in live.items():
        if meta.get("_EXTERNAL_PATH"):
            raise Refused("external data file path")
        if pks and dv_mode and meta["_LEVEL"] == 0:
            # With deletion vectors, Paimon's readers skip level-0 files of primary-key tables until compaction.
            level0 += 1
            continue
        path = names.get((f"bucket-{bucket}", name))
        if path is None:
            raise Refused("data file not found: " + name)
        paths.append(path)
        schema_ids.add(meta["_SCHEMA_ID"])
    current = {fld["id"]: fld["name"] for fld in schema["fields"]}
    for sid in schema_ids - {schema["id"]}:
        with open(os.path.join(table, "schema", f"schema-{sid}"), encoding="utf-8") as f:
            old = {fld["id"]: fld["name"] for fld in json.load(f)["fields"]}
        if any(current.get(fid) not in (None, nm) for fid, nm in old.items()):
            raise Refused(f"schema {sid} renames columns")
    columns = ", ".join('"' + fld["name"] + '"' for fld in schema["fields"])
    if not paths:
        return None, f"level-0 files skipped: {level0}"
    scan = f"read_parquet({files_list(sorted(paths))}, union_by_name = true, filename = true, file_row_number = true)"
    by_path = {os.path.basename(p): p for p in paths}
    dropped = [f"(filename = {lit(by_path[n])} AND file_row_number IN ({', '.join(str(r) for r in sorted(rows))}))"
               for n, rows in deleted.items() if n in by_path]
    if dropped:
        scan = f"(SELECT * FROM {scan} WHERE NOT ({' OR '.join(dropped)}))"
    note = f"level-0 files skipped: {level0}, files with deletion vectors: {len(dropped)}"
    if not pks:
        return f"SELECT {columns} FROM {scan}", note
    parts = schema.get("partitionKeys", [])
    keys = ", ".join([f'"{p}"' for p in parts] + [f'"_KEY_{k}"' for k in pks if k not in parts])
    return (f"SELECT {columns} FROM (SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY _SEQUENCE_NUMBER"
            f" DESC) AS rn__ FROM {scan}) WHERE rn__ = 1 AND _VALUE_KIND IN (0, 2)"), note


def paimon_deletion_vectors(table, snap):
    """Data file name -> deleted row positions, from the snapshot's index manifest (Paimon's bitmap format:
    a version byte, then per range a big-endian length, the magic number, a portable 32-bit RoaringBitmap, a CRC)."""
    if not snap.get("indexManifest"):
        return {}
    deleted = {}
    for entry in avro_rows(os.path.join(table, "manifest", snap["indexManifest"])):
        if entry["_INDEX_TYPE"] != "DELETION_VECTORS":
            continue
        if entry["_KIND"] != 0:
            raise Refused("index manifest with a DELETE entry")
        with open(os.path.join(table, "index", entry["_FILE_NAME"]), "rb") as f:
            blob = f.read()
        if blob[0] != 1:
            raise Refused(f"deletion vector file version {blob[0]}")
        for rng in entry["_DELETIONS_VECTORS_RANGES"]:
            offset, length = rng["f1"], rng["f2"]
            size, magic = struct.unpack(">ii", blob[offset:offset + 8])
            if magic != 1581511376 or size != length:
                raise Refused("unknown deletion vector encoding")
            bitmap = pyroaring.BitMap.deserialize(blob[offset + 8:offset + 4 + size])
            if len(bitmap) != rng["_CARDINALITY"]:
                raise Refused("deletion vector cardinality mismatch")
            deleted[rng["f0"]] = list(bitmap)
    return deleted


# ------------------------------------------------------------------------------------------------------------ Hudi

BASE_FILE = re.compile(r"^(?P<fid>[0-9a-f-]+-\d+)_(?P<token>[0-9-]+)_(?P<ts>\d+)\.parquet$")
LOG_FILE = re.compile(r"^\.(?P<fid>[0-9a-f-]+-\d+)_(?P<ts>\d+)\.log\.\d+(_[0-9-]+)?$")
INSTANT = re.compile(r"^(?P<ts>\d+)(_(?P<done>\d+))?\.(?P<action>[a-z]+)(\.(?P<state>requested|inflight))?$")


def hudi_source(table):
    """SQL over read_parquet for the latest snapshot of a Hudi table, or Refused."""
    hoodie = os.path.join(table, ".hoodie")
    props = {}
    with open(os.path.join(hoodie, "hoodie.properties"), encoding="utf-8") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                props[key] = value
    if props.get("hoodie.table.base.file.format", "PARQUET") != "PARQUET":
        raise Refused("base file format " + props["hoodie.table.base.file.format"])
    layout2 = props.get("hoodie.timeline.layout.version") == "2"
    folder = os.path.join(hoodie, props.get("hoodie.timeline.path", "timeline")) if layout2 else hoodie
    completed = {}   # requested instant -> (completion order key, action)
    pending = []
    for name in os.listdir(folder):
        m = INSTANT.match(name)
        if not m:
            continue
        if m.group("state") or (m.group("action") == "inflight"):
            pending.append((m.group("ts"), m.group("action")))
        else:
            completed[m.group("ts")] = (m.group("done") or m.group("ts"), m.group("action"))
    open_actions = {a for ts, a in pending if ts not in completed}
    if open_actions & {"compaction", "replacecommit", "clustering", "logcompaction"}:
        raise Refused("pending table service: " + ", ".join(sorted(open_actions)))
    replaced = set()
    for ts, (done, action) in completed.items():
        if action != "replacecommit":
            continue
        path = os.path.join(folder, f"{ts}_{done}.replacecommit" if layout2 else f"{ts}.replacecommit")
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(0)
            meta = next(iter(fastavro.reader(f))) if head == b"Obj\x01" else json.load(f)
        for part, ids in (meta.get("partitionToReplaceFileIds") or {}).items():
            replaced.update((part, fid) for fid in ids)
    groups = {}
    logs = []
    for root, dirs, files in os.walk(table):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        part = os.path.relpath(root, table)
        part = "" if part == "." else part
        for name in files:
            base = BASE_FILE.match(name)
            log = LOG_FILE.match(name)
            if base and base.group("ts") in completed and (part, base.group("fid")) not in replaced:
                order = completed[base.group("ts")][0]
                key = (part, base.group("fid"))
                if key not in groups or groups[key][0] < order:
                    groups[key] = (order, base.group("ts"), os.path.join(root, name))
            elif log:
                logs.append((part, log.group("fid"), log.group("ts"), name))
    for part, fid, ts, name in logs:
        kept = groups.get((part, fid))
        if kept is None:
            if layout2 and ts not in completed:
                continue
            raise Refused("log file without a readable base file: " + name)
        if layout2:
            # 1.x names a log file after the delta commit that wrote it.
            if ts in completed and completed[ts][0] > kept[0]:
                raise Refused("merge-on-read log file newer than its base: " + name)
        elif ts >= kept[1]:
            # 0.x names a log file after the base instant of its file slice.
            raise Refused("merge-on-read log file in the latest file slice: " + name)
    paths = sorted(g[2] for g in groups.values())
    if not paths:
        return None, None
    return f"SELECT * EXCLUDE (_hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, " \
           f"_hoodie_partition_path, _hoodie_file_name) FROM read_parquet({files_list(paths)}, " \
           f"union_by_name = true, hive_partitioning = false)", None


# ------------------------------------------------------------------------------------------------------------ main

def table_dir(fixtures, label):
    fmt, name = label.split("/", 1)
    if fmt == "paimon":
        return os.path.join(fixtures, "paimon", "db.db", name)
    return os.path.join(fixtures, fmt, name)


def main(fixtures, ext_dir):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    print("duckdb", duckdb.__version__)
    if ext_dir:
        for name in ("paimon", "hudi"):
            path = os.path.join(ext_dir, f"{name}.duckdb_extension")
            if not os.path.exists(path):
                report("extension " + name, "", "not available", "no build for this DuckDB version")
                continue
            before = {r[0] for r in con.sql("SELECT DISTINCT function_name FROM duckdb_functions()").fetchall()}
            try:
                con.sql("LOAD " + lit(path))
                added = sorted({r[0] for r in con.sql("SELECT DISTINCT function_name FROM duckdb_functions()")
                               .fetchall()} - before)
                report("extension " + name, "", "loaded", ", ".join(added))
            except duckdb.Error as e:
                report("extension " + name, "", "load failed", e)
    with open(os.path.join(fixtures, "expected.tsv"), encoding="utf-8") as f:
        expected = [line.rstrip("\n").split("\t") for line in f if line.strip()]
    for label, where, want in expected:
        reader = paimon_source if label.startswith("paimon/") else hudi_source
        try:
            source, note = reader(table_dir(fixtures, label))
            got = "0|None|" if source is None else "|".join(
                str(v) for v in con.sql(answer_sql(source, where)).fetchone())
        except Refused as e:
            report(label, where, "refused", e)
            continue
        except (duckdb.Error, OSError, KeyError, ValueError) as e:
            report(label, where, "WRONG (error)", f"{type(e).__name__}: {e}")
            FAILED.append(label)
            continue
        if got == want:
            report(label, where, "ok", got + (f" ({note})" if note else ""))
        else:
            report(label, where, f"WRONG (expected {want})", got)
            FAILED.append(label)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("## Paimon / Hudi light-reader probe\n\n" + "\n".join(SUMMARY) + "\n")
    if FAILED:
        print("WRONG answers: " + ", ".join(FAILED))
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")
