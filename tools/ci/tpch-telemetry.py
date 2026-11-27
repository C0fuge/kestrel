#!/usr/bin/env python3
"""Retain TPC-H container evidence and derive optional summaries without deleting raw files."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import subprocess
import time

percentiles = runpy.run_path(str(Path(__file__).with_name("tpch-raw.py")))["percentiles"]
COUNTERS = ("cpu.stat", "cpu.max", "cpuset.cpus.effective", "memory.current", "memory.peak", "memory.stat",
            "memory.events", "memory.max", "memory.swap.max", "io.stat", "pids.current")
EVENTS = ("jdk.CPULoad,jdk.ThreadCPULoad,jdk.GCHeapSummary,jdk.GarbageCollection,"
          "jdk.DirectBufferStatistics,jdk.PhysicalMemory,jdk.JVMInformation,jdk.OSInformation,"
          "jdk.CPUInformation,jdk.InitialSystemProperty")


def redacted(text):
    """Keep generated test credentials out of public text artifacts."""
    for name in ("S3_KEY", "S3_SECRET"):
        if os.environ.get(name):
            text = text.replace(os.environ[name], "<redacted>")
    return text


def save_json(path, value):
    """Write structured evidence, redacting generated S3 test keys."""
    path.write_text(redacted(json.dumps(value, indent=2)) + "\n")


def run(command, timeout=60):
    """Keep partial diagnostic output when a collector itself times out."""
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        def decoded(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        return subprocess.CompletedProcess(command, 124, decoded(error.stdout), decoded(error.stderr) + "\ncollector timed out\n")


def capture(command, path):
    """Retain the command, complete output and return code even when collection fails."""
    result = run(command)
    path.write_text(redacted(result.stdout + result.stderr))
    save_json(path.with_name(path.name + ".command.json"), {"argv": command, "status": result.returncode})
    return result


def monitor(container, directory):
    """Flush timestamped raw cgroup and process CPU/RSS/I/O counters once per second."""
    command = ["docker", "exec", container, "sh", "-c",
               "for f in " + " ".join("/sys/fs/cgroup/" + name for name in COUNTERS)
               + " /proc/1/stat /proc/1/status /proc/1/io /proc/1/smaps_rollup; "
                 "do echo ===$f; cat $f; done"]
    save_json(directory / "monitor-command.json", {"argv": command, "period_seconds": 1,
              "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "page_bytes": os.sysconf("SC_PAGE_SIZE")})
    with (directory / "resources.jsonl").open("x") as output:
        index = 0
        while not (directory / "monitor.stop").exists():
            started = time.monotonic()
            result = run(command, timeout=15)
            sample = {"timestamp_ns": time.time_ns(), "status": result.returncode,
                      "stdout": result.stdout, "stderr": result.stderr}
            if directory.name != "plain" and index % 5 == 0:
                heap = run(["docker", "exec", container, "jcmd", "1", "GC.heap_info"], timeout=15)
                sample["heap"] = {"status": heap.returncode, "stdout": heap.stdout, "stderr": heap.stderr}
            output.write(json.dumps(sample) + "\n")
            output.flush()
            index += 1
            time.sleep(max(0, 1 - (time.monotonic() - started)))


def finish(container, directory):
    """Stop gracefully, keep logs/chunks on failure, and extract JFR data with the host JDK."""
    capture(["docker", "stop", "--time", "30", container], directory / "stop.log")
    (directory / "monitor.stop").touch()
    capture(["docker", "logs", "--timestamps", container], directory / "server.log")
    capture(["docker", "inspect", "--format",
             '{"state":{{json .State}},"created":{{json .Created}},"image":{{json .Image}},'
             '"path":{{json .Path}},"args":{{json .Args}},"memory":{{json .HostConfig.Memory}},'
             '"memory_swap":{{json .HostConfig.MemorySwap}},"nano_cpus":{{json .HostConfig.NanoCpus}},'
             '"cpuset":{{json .HostConfig.CpusetCpus}}}', container], directory / "container.json")
    # JVM repository directories are owner-only, and the server uses the image's root user.
    capture(["sudo", "chmod", "-R", "a+rX", str(directory)], directory / "artifact-permissions.log")
    log = (directory / "server.log").read_text()
    (directory / "warnings-errors.log").write_text("\n".join(
        line for line in log.splitlines() if re.search(r"\bWARN(?:ING)?\b|\bERROR\b", line)) + "\n")
    profile = directory / "server.jfr"
    profiles = [profile] if profile.is_file() else []
    if not profiles:
        repositories = sorted({path.parent for path in (directory / "jfr-repository").rglob("*.jfr")})
        for index, repository in enumerate(repositories):
            recovered = directory / f"recovered-{index}.jfr"
            capture(["jfr", "assemble", str(repository), str(recovered)], directory / f"recovery-{index}.log")
            if recovered.is_file():
                profiles.append(recovered)
    for recording in profiles:
        capture(["jfr", "summary", str(recording)], directory / (recording.stem + "-jfr-summary.txt"))
        # Environment events are disabled in the recording configuration before server startup.
        capture(["jfr", "print", "--json", "--events", EVENTS, str(recording)],
                directory / (recording.stem + "-jfr-events.json"))
    save_json(directory / "finished.json", {"finished_ns": time.time_ns(), "jfr_present": profile.is_file()})
    capture(["docker", "rm", container], directory / "remove.log")


def validate(directory):
    """Require complete client samples and resource/JVM evidence before comparing speeds."""
    errors = []
    labels = [path for path in directory.iterdir() if path.is_dir() and (path / "container-argv.json").is_file()]
    if not labels:
        errors.append("no configurations started")
    expected = {path.stem for path in (directory / "queries").glob("*.sql")}
    for label in labels:
        for name in ("container.json", "server.log", "mvn.log"):
            if not (label / name).is_file():
                errors.append(f"{label.name}: missing {name}")
        status = label / "client-exit-status.txt"
        if not status.is_file() or status.read_text().strip() != "0":
            errors.append(f"{label.name}: client did not complete successfully")
        resources = label / "resources.jsonl"
        samples = [json.loads(line) for line in resources.read_text().splitlines()] if resources.is_file() else []
        if not any(row["status"] == 0 for row in samples):
            errors.append(f"{label.name}: no successful process/cgroup sample")
        csvs = list((label / "client").glob("*.csv"))
        rows = []
        for path in csvs:
            with path.open() as source:
                rows.extend(csv.DictReader(source))
        if not rows or any(row["ok"] != "1" for row in rows) or {row["query"] for row in rows} != expected:
            errors.append(f"{label.name}: missing, failed or incomplete query samples")
        if label.name != "plain":
            if not (label / "server.jfr").is_file() or not (label / "gc.log").is_file():
                errors.append(f"{label.name}: missing complete server JFR or GC log")
            startup_heap = label / "startup-heap-status.txt"
            if not any(row.get("heap", {}).get("status") == 0 for row in samples) and not (
                    startup_heap.is_file() and startup_heap.read_text().strip() == "0"):
                errors.append(f"{label.name}: no successful heap sample")
    save_json(directory / "telemetry-validation.json", {"labels": [label.name for label in labels], "errors": errors})
    if errors:
        raise ValueError("Incomplete telemetry: " + "; ".join(errors))


def summarize(directory):
    """Derive per-query measured percentiles from either client schema, retaining every CSV."""
    for path in directory.rglob("*"):
        if path.is_file() and path.suffix in (".txt", ".log", ".json", ".jsonl", ".csv", ".yml"):
            original = path.read_text()
            cleaned = redacted(original)
            if cleaned != original:
                path.write_text(cleaned)
    for path in directory.rglob("*.csv"):
        if path.name not in ("samples.csv", "queries.csv"):
            continue
        with path.open() as source:
            rows = list(csv.DictReader(source))
        queries = {}
        for query in sorted({row["query"] for row in rows}):
            measured = [row for row in rows if row["query"] == query and row["warmup"] == "0" and row["ok"] == "1"]
            if measured:
                queries[query] = {"count": len(measured),
                                  **percentiles([int(row["latency_ns"]) for row in measured])}
        save_json(path.with_name("query-percentiles.json"), {
            "source": path.name, "samples": len(rows), "failed_samples": sum(row["ok"] != "1" for row in rows),
            "queries": queries, "scope": "successful measured samples; warmups/failures remain in source CSV"})
    files = sorted(path for path in directory.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
    with (directory / "SHA256SUMS").open("w") as output:
        for path in files:
            with path.open("rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            output.write(digest + "  " + str(path.relative_to(directory)) + "\n")


def main():
    """Expose small lifecycle operations for the existing shell workflow."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("monitor", "finish", "command", "summarize", "validate"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.operation == "command":
        save_json(args.directory, args.arguments)
    elif args.operation in ("summarize", "validate"):
        {"summarize": summarize, "validate": validate}[args.operation](args.directory)
    else:
        {"monitor": monitor, "finish": finish}[args.operation](args.arguments[0], args.directory)


if __name__ == "__main__":
    main()
