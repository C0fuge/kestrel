#!/usr/bin/env python3
"""Retain TPC-H container evidence and derive optional summaries without deleting raw files."""

import argparse
from collections import Counter
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
CHECKSUM_VERSION = runpy.run_path(str(Path(__file__).with_name("tpch-checksum.py")))["VERSION"]
COUNTERS = ("cpu.stat", "cpu.max", "cpuset.cpus.effective", "memory.current", "memory.peak", "memory.stat",
            "memory.events", "memory.max", "memory.swap.max", "io.stat", "pids.current")
EVENTS = ("jdk.CPULoad,jdk.ThreadCPULoad,jdk.GCHeapSummary,jdk.GarbageCollection,"
          "jdk.DirectBufferStatistics,jdk.PhysicalMemory,jdk.JVMInformation,jdk.OSInformation,"
          "jdk.CPUInformation,jdk.InitialSystemProperty")
PROC_FILES = ("/proc/1/stat", "/proc/1/status", "/proc/1/io", "/proc/1/smaps_rollup")
RESOURCE_FILES = tuple("/sys/fs/cgroup/" + name for name in COUNTERS) + PROC_FILES


def write_plan(directory):
    """Record all requested slots before measuring, using dispatch inputs rather than observed samples."""
    inputs = json.loads((directory / "inputs.json").read_text())
    rounds, clients = int(inputs["rounds"]), int(inputs["clients"])
    if rounds < 1 or clients < 1:
        raise ValueError("TPC-H requires positive rounds and clients")
    labels = ["default"]
    if clients == 1 and str(inputs.get("quick", False)).lower() != "true":
        labels.append("threads4")
    # GitHub omits empty optional dispatch strings from toJSON(inputs).
    labels += [label for label in ("baseline", "extra") if inputs.get(label, "")]
    more = inputs.get("more", "")
    if more:
        # Bash read -a preserves interior empty fields, but drops the final delimiter's empty field.
        configs = more.split("|")
        if not configs[-1]:
            configs.pop()
        labels += [f"more{index + 1}" for index in range(len(configs))]
    if str(inputs.get("plain", False)).lower() == "true":
        labels.append("plain")
    queries = sorted(path.stem for path in (directory / "queries").glob("*.sql"))
    if not queries:
        raise ValueError("No requested SQL queries")
    save_json(directory / "run-plan.json", {
        "rounds": rounds, "warmup_round": 0, "queries": queries,
        "labels": {label: {"client": "plain" if label == "plain" else inputs["client"],
                           "clients": clients if label != "plain" and inputs["client"] == "java" else 1}
                   for label in labels},
    })


def parsed_files(text):
    """Keep each counter's own read status so a later successful cat cannot hide failure."""
    pattern = r"^@@FILE ([^\n]+)\n(.*?)\n@@STATUS \1 ([0-9]+)$"
    return {name: {"text": body, "status": int(status)}
            for name, body, status in re.findall(pattern, text, re.M | re.S)}


def resource_errors(files):
    """Require actual CPU/RSS/I/O and limit fields, permitting a valid empty cgroup I/O file."""
    errors = []
    def checked(path, pattern):
        entry = files.get(path, {})
        if entry.get("status") != 0 or not re.search(pattern, entry.get("text", ""), re.M):
            errors.append(path)
        return entry.get("text", "")
    base = "/sys/fs/cgroup/"
    def fields(path, names):
        text = checked(path, r"\S")
        for name in names:
            if not re.search(r"^" + re.escape(name) + r"\s+[0-9]+$", text, re.M):
                errors.append(path + ":" + name)
    fields(base + "cpu.stat", ("usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"))
    checked(base + "cpu.max", r"^(?:max|[0-9]+) [1-9][0-9]*$")
    checked(base + "cpuset.cpus.effective", r"^[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*$")
    for name in ("memory.current", "memory.peak", "pids.current"):
        checked(base + name, r"^[0-9]+$")
    for name in ("memory.max", "memory.swap.max"):
        checked(base + name, r"^(?:max|[0-9]+)$")
    fields(base + "memory.stat", ("anon", "file"))
    fields(base + "memory.events", ("low", "high", "max", "oom", "oom_kill"))
    io = files.get(base + "io.stat", {})
    if io.get("status") != 0:
        errors.append(base + "io.stat")
    for line in io.get("text", "").splitlines():
        if not re.fullmatch(r"[0-9]+:[0-9]+(?: [a-z_]+=[0-9]+)+", line) or any(
                not re.search(r"\b" + name + r"=[0-9]+\b", line) for name in ("rbytes", "wbytes", "rios", "wios")):
            errors.append(base + "io.stat:fields")
    stat = checked("/proc/1/stat", r"^1 \(.+\) ")
    try:
        fields_after_comm = stat.rsplit(")", 1)[1].split()
        if len(fields_after_comm) < 22 or any(int(fields_after_comm[index]) < 0 for index in (11, 12, 20, 21)):
            errors.append("/proc/1/stat:cpu/rss")
    except (IndexError, ValueError):
        errors.append("/proc/1/stat:cpu/rss")
    for name in ("VmRSS", "Threads"):
        checked("/proc/1/status", r"^" + name + r":\s+[0-9]+" + (r" kB$" if name == "VmRSS" else r"$"))
    fields("/proc/1/io", ("rchar:", "wchar:", "syscr:", "syscw:", "read_bytes:", "write_bytes:"))
    for name in ("Rss", "Pss"):
        checked("/proc/1/smaps_rollup", r"^" + name + r":\s+[0-9]+ kB$")
    return errors


def heap_evidence(text):
    """Recognize an actual heap usage response without requiring a garbage collection."""
    return bool(re.search(r"heap|generation|PSYoungGen|ParOldGen", text, re.I)
                and re.search(r"\bused\s+[0-9]+", text))


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
               "for f in " + " ".join(RESOURCE_FILES) + "; "
               "do printf '\\n@@FILE %s\\n' \"$f\"; cat \"$f\"; code=$?; "
               "printf '\\n@@STATUS %s %s\\n' \"$f\" \"$code\"; done"]
    save_json(directory / "monitor-command.json", {"argv": command, "period_seconds": 1,
              "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "page_bytes": os.sysconf("SC_PAGE_SIZE")})
    with (directory / "resources.jsonl").open("x") as output:
        index = 0
        while not (directory / "monitor.stop").exists():
            started = time.monotonic()
            result = run(command, timeout=15)
            sample = {"timestamp_ns": time.time_ns(), "status": result.returncode,
                      "stdout": result.stdout, "stderr": result.stderr, "files": parsed_files(result.stdout)}
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
    # Only this stopped task's mounted output is reassigned; plain/JFR files were written as root.
    capture(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(directory)],
            directory / "artifact-permissions.log")
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
        summary = capture(["jfr", "summary", str(recording)], directory / (recording.stem + "-jfr-summary.txt"))
        # Environment events are disabled in the recording configuration before server startup.
        events = capture(["jfr", "print", "--json", "--events", EVENTS, str(recording)],
                         directory / (recording.stem + "-jfr-events.json"))
        with recording.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        save_json(directory / (recording.stem + "-jfr-extraction.json"), {
            "sha256": digest, "bytes": recording.stat().st_size,
            "summary_status": summary.returncode, "events_status": events.returncode})
    save_json(directory / "finished.json", {"finished_ns": time.time_ns(), "jfr_present": profile.is_file()})
    capture(["docker", "rm", container], directory / "remove.log")


def recording_errors(label):
    """Bind readable, successfully extracted event evidence to the retained complete recording."""
    try:
        profile = label / "server.jfr"
        with profile.open("rb") as source:
            if source.read(4) != b"FLR\0" or profile.stat().st_size < 68:
                return ["empty or malformed server JFR"]
            source.seek(0)
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        extracted = json.loads((label / "server-jfr-extraction.json").read_text())
        if extracted != {"sha256": digest, "bytes": profile.stat().st_size, "summary_status": 0, "events_status": 0}:
            return ["JFR extraction failed or recording changed after extraction"]
        summary = (label / "server-jfr-summary.txt").read_text()
        if not re.search(r"\bVersion:\s+[0-9]+\.[0-9]+", summary) or not re.search(r"\bChunks:\s+[1-9][0-9]*", summary):
            return ["unreadable JFR summary"]
        events = json.loads((label / "server-jfr-events.json").read_text())["recording"]["events"]
        if not isinstance(events, list) or not events or any(
                not isinstance(event.get("values"), dict) for event in events):
            return ["unreadable JFR events"]
        kinds = {event["type"] for event in events}
        if not {"jdk.JVMInformation", "jdk.DirectBufferStatistics"} <= kinds:
            return ["missing JVM/direct-buffer JFR events"]
        jvm = [event["values"] for event in events if event["type"] == "jdk.JVMInformation"]
        direct = [event["values"] for event in events if event["type"] == "jdk.DirectBufferStatistics"]
        if not any(isinstance(values.get("jvmVersion"), str) and values["jvmVersion"] for values in jvm):
            return ["missing actual JVM version event fields"]
        if not any(all(type(values.get(name)) is int for name in ("count", "totalCapacity", "memoryUsed")) for values in direct):
            return ["missing actual direct-buffer event fields"]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return ["missing or unreadable complete server JFR extraction"]
    return []


def query_errors(label, plan, names, rounds):
    """Require each requested query/client/round exactly once, including the warmup round."""
    filename = "queries.csv" if plan["client"] == "java" else "samples.csv"
    try:
        queries = label.parent / "queries"
        if (queries / "checksum-version.txt").read_text().strip() != CHECKSUM_VERSION:
            return ["unsupported or mismatched oracle checksum version"]
        expected_checksums = {name: (queries / (name + ".checksum")).read_text().strip() for name in names}
        expected_rows = {name: int((queries / (name + ".rows")).read_text().strip()) for name in names}
        if any(not re.fullmatch(r"[0-9a-f]{16}", value) for value in expected_checksums.values()):
            return ["invalid oracle checksum reference"]
        with (label / "client" / filename).open() as source:
            rows = list(csv.DictReader(source))
        slots = []
        for row in rows:
            client = int(row["client"]) if plan["client"] == "java" else int(row.get("client", "0"))
            round_number = int(row["round"])
            byte_field = "arrow_bytes" if plan["client"] == "java" else "arrow_buffer_bytes"
            batch_field = "batches" if plan["client"] == "java" else "table_batches"
            if row["ok"] != "1" or row["warmup"] != str(int(round_number == 0)) or row.get("error_type", ""):
                return ["failed query or inconsistent warmup/status"]
            if int(row["latency_ns"]) <= 0 or any(int(row[name]) < 0 for name in ("rows", byte_field, batch_field)):
                return ["missing query latency/answer/buffer evidence"]
            if not re.fullmatch(r"[0-9a-f]{16}", row["checksum"]):
                return ["invalid query checksum"]
            if row["checksum_version"] != CHECKSUM_VERSION:
                return ["unsupported or mismatched raw checksum version"]
            if row["checksum"] != expected_checksums[row["query"]] or int(row["rows"]) != expected_rows[row["query"]]:
                return [f"oracle answer mismatch for {row['query']} client {client} round {round_number}"]
            if plan["client"] == "java":
                if int(row["start_epoch_ms"]) <= 0:
                    return ["missing query timestamp"]
                int(row["start_ns"])
            elif int(row["timestamp_ns"]) <= 0:
                return ["missing query timestamp"]
            slots.append((row["query"], client, round_number))
        expected = {(query, client, round_number) for query in names for client in range(plan["clients"])
                    for round_number in range(rounds + 1)}
        counts = Counter(slots)
        missing, extra = expected - counts.keys(), counts.keys() - expected
        duplicates = [slot for slot, count in counts.items() if count != 1]
        if missing or extra or duplicates:
            return [f"incomplete query slots: missing={len(missing)}, unexpected={len(extra)}, duplicates={len(duplicates)}"]
    except (OSError, ValueError, KeyError, TypeError):
        return ["missing or malformed query samples"]
    return []


def validate(directory):
    """Require the explicit requested plan and complete raw measurements before comparing speeds."""
    errors, labels = [], {}
    try:
        plan = json.loads((directory / "run-plan.json").read_text())
        labels = plan["labels"]
        names, rounds = plan["queries"], plan["rounds"]
        if (not isinstance(labels, dict) or not labels or not isinstance(names, list) or not names
                or not all(isinstance(name, str) for name in names)
                or type(rounds) is not int or rounds < 1 or len(set(names)) != len(names)):
            raise ValueError("invalid plan")
        if set(names) != {path.stem for path in (directory / "queries").glob("*.sql")}:
            raise ValueError("requested SQL inputs changed")
        for label_name, requested in labels.items():
            if requested["client"] not in ("java", "adbc", "plain") or type(requested["clients"]) is not int or requested["clients"] < 1:
                raise ValueError("invalid client plan")
            label = directory / label_name
            failures = []
            for name in ("container-argv.json", "container.json", "server.log", "mvn.log"):
                if not (label / name).is_file() or not (label / name).stat().st_size:
                    failures.append(f"missing {name}")
            status = label / "client-exit-status.txt"
            if not status.is_file() or status.read_text().strip() != "0":
                failures.append("client did not complete successfully")
            samples = []
            resources = label / "resources.jsonl"
            if resources.is_file():
                for line in resources.read_text().splitlines():
                    try:
                        samples.append(json.loads(line))
                    except ValueError:
                        failures.append("malformed resource sample")
            if not any(row.get("status") == 0 and not resource_errors(row.get("files", {})) for row in samples):
                failures.append("no complete required process/cgroup fields with individual read statuses")
            failures += query_errors(label, requested, names, rounds)
            if requested["client"] != "plain":
                failures += recording_errors(label)
                gc = label / "gc.log"
                if not gc.is_file() or not re.search(r"\[(?:info|debug|trace)\]\[gc(?:[\], ])", gc.read_text()):
                    failures.append("missing actual GC log evidence")
                startup = label / "startup-heap-status.txt"
                startup_text = label / "startup-heap.txt"
                heap = any(row.get("heap", {}).get("status") == 0 and
                           heap_evidence(row["heap"].get("stdout", "")) for row in samples)
                heap |= (startup.is_file() and startup.read_text().strip() == "0" and
                         startup_text.is_file() and heap_evidence(startup_text.read_text()))
                if not heap:
                    failures.append("no successful actual heap usage snapshot")
            errors += [f"{label_name}: {failure}" for failure in failures]
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        errors.append(f"missing or invalid requested run plan/evidence: {type(error).__name__}")
    save_json(directory / "telemetry-validation.json", {
        "labels": list(labels) if isinstance(labels, dict) else [], "errors": errors})
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
    parser.add_argument("operation", choices=("monitor", "finish", "command", "summarize", "validate", "plan"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.operation == "command":
        save_json(args.directory, args.arguments)
    elif args.operation in ("summarize", "validate", "plan"):
        {"summarize": summarize, "validate": validate, "plan": write_plan}[args.operation](args.directory)
    else:
        {"monitor": monitor, "finish": finish}[args.operation](args.arguments[0], args.directory)


if __name__ == "__main__":
    main()
