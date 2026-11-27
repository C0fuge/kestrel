#!/usr/bin/env python3
"""Run paired embedded Flight SQL screenings and retain every measurement."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import statistics
import subprocess
import time


HARNESS = Path("flight-server/src/test/java/pro/surpin/data/arrowflight/server")
IMAGE = "maven:3.9-eclipse-temurin-21"
JVM = "-Xms512m -Xmx2g -XX:MaxDirectMemorySize=1g -XX:ActiveProcessorCount=4 -Duser.home=/tmp"


def checked_options(text):
    """Accept only non-secret server property arguments, never shell commands."""
    options = shlex.split(text)
    pattern = (r"-D(?:arrowflight\.experimental\.[A-Za-z0-9_.-]+|duckDb[A-Za-z0-9]+|resourceProfile"
               r"|perf\.(?:producingStreams|threads|threadsUnderLoad|shared))=[A-Za-z0-9_.,:+/-]+")
    if any(not re.fullmatch(pattern, option) for option in options):
        raise ValueError("Candidate options must be allowed -Dserver.property=value arguments")
    if any(re.search(r"password|secret|token|credential|keytab", option, re.I) for option in options):
        raise ValueError("Do not put credentials in a public screening run")
    overridden = ("-DduckDbThreads", "-DduckDbShared", "-DduckDbMemoryLimit=",
                  "-DduckDbWarmConnections=", "-DduckDbGroups=")
    if any(option.startswith(overridden) for option in options):
        raise ValueError("The embedded harness overrides these settings; use perf.threads, "
                         "perf.threadsUnderLoad or perf.shared")
    return options


def planned_measurement_seconds(rounds, seconds, clients):
    """Bound trial work so the job retains time for builds and artifact upload."""
    planned = 2 * rounds * (seconds * len(clients) + max(3, seconds // 3) + 20) + 30
    if len(clients) > 8 or planned > 900:
        raise ValueError("At most eight concurrency steps and fifteen planned measurement minutes are allowed")
    return planned


def save_json(path, value):
    """Write structured metadata without shell interpolation."""
    path.write_text(json.dumps(value, indent=2) + "\n")


def command_log(command, path):
    """Preserve the exact command and its complete output."""
    save_json(path.with_suffix(".command.json"), command)
    with path.open("w") as output:
        subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=True)


def summary_of(folder, clients):
    """Summarize individual samples and retain typed answer evidence by workload key."""
    result, answers = {}, {}
    for concurrency in clients:
        with (folder / f"c{concurrency}.csv").open() as source:
            rows = list(csv.DictReader(source))
        if not rows:
            raise ValueError(f"No samples for concurrency {concurrency}")
        latency = sorted(int(row["latency_ns"]) / 1e6 for row in rows)
        wall = dict(line.split("=", 1) for line in
                    (folder / f"c{concurrency}-wall.txt").read_text().splitlines())
        seconds = float(wall["wall_seconds"])
        result[str(concurrency)] = {
            "queries": len(rows), "qps": len(rows) / seconds,
            "p50_ms": latency[len(latency) // 2],
            "p95_ms": latency[int(len(latency) * .95)],
            "p99_ms": latency[int(len(latency) * .99)], "max_ms": latency[-1],
            "cpu_ms_per_query": int(wall["process_cpu_ns"]) / 1e6 / len(rows),
            "arrow_bytes": sum(int(row["arrow_bytes"]) for row in rows),
            "batches": sum(int(row["batches"]) for row in rows),
            "arrow_buffer_mb_s": sum(int(row["arrow_bytes"]) for row in rows) / 1e6 / seconds,
        }
        for row in rows:
            key = (concurrency, row["thread"], row["sequence"], row["query_kind"], row["key"])
            answers[key] = tuple(row[name] for name in ("rows", "checksum", "arrow_bytes", "batches"))
    with (folder / "resources.csv").open() as source:
        resources = list(csv.DictReader(source))
    if not resources or not (folder / "profile.jfr").is_file():
        raise ValueError("Missing resource samples or JFR profile")
    result["resource_peaks"] = {
        name: max(int(row[name]) for row in resources)
        for name in ("rss_kib", "heap_used", "direct_bytes", "gc_count", "gc_ms", "threads")
    }
    save_json(folder / "summary.json", result)
    return result, answers


def matching_answers(first, second):
    """Require equal rows, values, buffers and batches for every overlapping query."""
    common = first.keys() & second.keys()
    mismatches = [key for key in common if first[key] != second[key]]
    if not common or mismatches:
        raise ValueError(f"Answer gate failed: overlap={len(common)}, mismatches={mismatches[:5]}")
    return len(common)


def run_container(name, source, output, data, cache, arguments, cpu_mode, cpu_set, options):
    """Run one dedicated container while retaining cgroup counters and full logs."""
    output.mkdir(parents=True)
    cpu = ["--cpuset-cpus=" + cpu_set] if cpu_mode == "pinned" else ["--cpus=4"]
    command = ["docker", "run", "--name", name, "--hostname", "screen-node",
               "--add-host", "screen-node:127.0.0.1", "--network", "host",
               *cpu, "--memory=8g", "--memory-swap=8g", "--user", f"{os.getuid()}:{os.getgid()}",
               "-e", "HOME=/tmp", "-e", "MAVEN_CONFIG=/tmp/.m2", "-e", "LC_ALL=C.UTF-8",
               "-e", "JAVA_TOOL_OPTIONS=" + JVM, "-v", str(source) + ":/work",
               "-v", str(output) + ":/output", "-v", str(data) + ":/data",
               "-v", str(cache) + ":/maven-cache", "-w", "/work", IMAGE,
               "mvn", "-B", "-ntp", "-Dmaven.repo.local=/maven-cache", "-pl", "flight-server", "-am",
               *arguments, *options]
    save_json(output / "command.json", command)
    process = None
    try:
        with (output / "maven.log").open("w") as log, (output / "cgroup.jsonl").open("w") as metrics:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            while process.poll() is None:
                # Only this task-owned container is inspected; output contains no environment secrets.
                sample = subprocess.run(["docker", "exec", name, "sh", "-c",
                                         "for f in cpu.stat memory.current memory.events io.stat pids.current; "
                                         "do echo /sys/fs/cgroup/$f; cat /sys/fs/cgroup/$f; done"],
                                        capture_output=True, text=True)
                metrics.write(json.dumps({"time_ns": time.time_ns(), "cgroup": sample.stdout,
                                          "error": sample.stderr, "status": sample.returncode}) + "\n")
                metrics.flush()
                time.sleep(1)
            if process.wait() != 0:
                raise RuntimeError(f"{name} failed; complete output is in {output.name}/maven.log")
    finally:
        inspected = subprocess.run(["docker", "inspect", "--format",
                                    "state={{json .State}} memory={{.HostConfig.Memory}} "
                                    "nano_cpus={{.HostConfig.NanoCpus}} cpuset={{.HostConfig.CpusetCpus}}", name],
                                   capture_output=True, text=True)
        (output / "container-state.txt").write_text(inspected.stdout + inspected.stderr)
        reports = source / "flight-server/target/surefire-reports"
        for report in reports.glob("*SmallQueryThroughputPerfTest*"):
            shutil.copyfile(report, output / report.name)
        text = (output / "maven.log").read_text(errors="replace")
        (output / "warnings-errors.log").write_text("\n".join(
            line for line in text.splitlines() if re.search(r"\bWARN(?:ING)?\b|\bERROR\b", line)) + "\n")
        # These names are unique to this run; never prune or remove unrelated containers.
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False)


def main():
    """Build both revisions, measure alternating pairs and write an auditable summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rounds = int(os.environ.get("SCREEN_ROUNDS", "3"))
    seconds = int(os.environ.get("SCREEN_SECONDS", "15"))
    clients = [int(value) for value in os.environ.get("SCREEN_CLIENTS", "1,8,32").split(",")]
    if not 1 <= rounds <= 5 or not 3 <= seconds <= 60 or not clients or any(not 1 <= c <= 64 for c in clients):
        raise ValueError("Expected 1..5 rounds, 3..60 seconds and 1..64 clients")
    if clients != sorted(set(clients)):
        raise ValueError("Client counts must be unique and increasing")
    planned = planned_measurement_seconds(rounds, seconds, clients)
    mode = os.environ.get("SCREEN_CPU_MODE", "pinned")
    if mode not in ("pinned", "quota"):
        raise ValueError("CPU mode must be pinned or quota")
    options = checked_options(os.environ.get("SCREEN_VARIANT_OPTIONS", ""))
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = {"A": args.baseline.resolve(), "B": args.candidate.resolve()}
    cache = Path.home() / ".m2/repository"
    cache.mkdir(parents=True, exist_ok=True)
    data = output / "data"
    data.mkdir()
    affinity = sorted(os.sched_getaffinity(0))
    if len(affinity) < 4:
        raise ValueError("This screening requires four available CPUs")
    cpu_set = ",".join(map(str, affinity[:4]))
    commits = {side: subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
               for side, source in sources.items()}
    save_json(output / "screen.json", {"commits": commits, "rounds": rounds, "seconds": seconds,
              "clients": clients, "variant_options": options, "cpu_mode": mode, "cpu_set": cpu_set,
              "planned_measurement_seconds": planned,
              "scope": "embedded server and clients share one JVM; screening ratios, not production QPS"})
    command_log(["lscpu"], output / "hardware.txt")
    command_log(["docker", "version"], output / "docker-version.txt")
    command_log(["docker", "pull", IMAGE], output / "image-pull.txt")
    command_log(["docker", "image", "inspect", "--format", "{{.Id}} {{json .RepoDigests}}", IMAGE],
                output / "image-identity.txt")
    # The same collector/client implementation measures both source revisions.
    for filename in ("SmallQueryThroughputPerfTest.java", "PerfRunArtifacts.java"):
        source = sources["B"] / HARNESS / filename
        if not source.is_file():
            raise ValueError(f"Candidate lacks the raw measurement harness: {filename}")
        shutil.copyfile(source, sources["A"] / HARNESS / filename)
    identity = re.sub(r"[^A-Za-z0-9_.-]", "-", os.environ.get("GITHUB_RUN_ID", "local"))
    prefix = "opt-screen-" + identity + "-" + str(os.getpid())
    for side in ("A", "B"):
        run_container(prefix + "-build-" + side.lower(), sources[side], output / ("build-" + side),
                      data, cache, ["package", "-DskipTests", "-Dcheckstyle.skip", "-Dspotbugs.skip"],
                      mode, cpu_set, [])
    common = ["test", "-Pperf", "-Dgroups=perf", "-DexcludedGroups=", "-Djacoco.skip=true",
              "-Dtest=SmallQueryThroughputPerfTest", "-Dsurefire.failIfNoSpecifiedTests=false",
              "-Dcheckstyle.skip", "-Dspotbugs.skip", "-Dperf.threads=2", "-Dperf.shared=2:512MB",
              "-Dperf.dataDir=/data", "-Dperf.output=/output"]
    # Generate and warm the exact data set before either ranked side; preserve this attempt too.
    run_container(prefix + "-preflight", sources["A"], output / "preflight", data, cache,
                  common + ["-Dperf.clients=1", "-Dperf.seconds=3"], mode, cpu_set, [])
    measurements, evidence = [], []
    for index in range(1, rounds + 1):
        order = ("A", "B") if index % 2 else ("B", "A")
        pair = {}
        for side in order:
            folder = output / f"r{index}-{side}"
            run_container(prefix + f"-r{index}-" + side.lower(), sources[side], folder, data, cache,
                          common + ["-Dperf.clients=" + ",".join(map(str, clients)), "-Dperf.seconds=" + str(seconds)],
                          mode, cpu_set, options if side == "B" else [])
            pair[side] = summary_of(folder, clients)
        overlap = matching_answers(pair["A"][1], pair["B"][1])
        evidence.append({"round": index, "order": list(order), "overlapping_answers": overlap, "mismatches": 0})
        measurements.append({side: pair[side][0] for side in ("A", "B")})
    save_json(output / "correctness.json", evidence)
    save_json(output / "measurements.json", measurements)
    lines = ["## Optimization screening", "", "Candidate / baseline ratios on one shared GitHub runner.",
             "Embedded server and clients share four CPUs and one JVM. Absolute QPS is not a production estimate.",
             "", f"A: `{commits['A']}`; B: `{commits['B']}`; CPU mode: `{mode}`.", "",
             "| Clients | QPS ratio | p95 ratio | p99 ratio | CPU/query ratio |", "|---:|---:|---:|---:|---:|"]
    for concurrency in clients:
        key = str(concurrency)
        ratios = [statistics.median(row["B"][key][metric] / row["A"][key][metric] for row in measurements)
                  for metric in ("qps", "p95_ms", "p99_ms", "cpu_ms_per_query")]
        lines.append("| " + str(concurrency) + " | " + " | ".join(f"{value:.3f}" for value in ratios) + " |")
    lines += ["", f"All {rounds} paired answer gates passed. All samples, hashes, JFR profiles, resource/cgroup",
              "measurements, runtime flags, startup stages, hardware and full logs are retained as artifacts.",
              "No TPC-H or full-scan workload was run by this small-query screening."]
    (output / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    try:
        main()
    finally:
        # Keep raw failed attempts as well; checksum every retained artifact at job exit.
        parsed = argparse.ArgumentParser(add_help=False)
        parsed.add_argument("--output", type=Path)
        known, _ = parsed.parse_known_args()
        if known.output and known.output.is_dir():
            files = sorted(path for path in known.output.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
            (known.output / "SHA256SUMS").write_text("".join(
                hashlib.sha256(path.read_bytes()).hexdigest() + "  " + str(path.relative_to(known.output)) + "\n"
                for path in files))
