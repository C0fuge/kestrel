#!/usr/bin/env python3
"""Collect explicitly verified on-CPU profiles of the existing embedded screening harness."""

import argparse
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import runpy
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request


VERSION = "4.6"
SOURCE_COMMIT = "557276c2950c14a5ad7073e2bef5a16616309247"
ASSET = f"async-profiler-{VERSION}-linux-x64.tar.gz"
ASSET_SHA256 = "1e79ed54d8952889a2e74f993d22ee925e3322460d4f37a2ed95bfb88585da36"
URL = f"https://github.com/async-profiler/async-profiler/releases/download/v{VERSION}/{ASSET}"
HERE = Path(__file__).resolve().parent
SCREEN = runpy.run_path(str(HERE / "optimization-screen.py"))
SYSTEM = runpy.run_path(str(HERE / "benchmark-system.py"))
SCOPE = ("embedded server, Flight clients, JVM/compiler/GC and collectors share one JVM; "
         "whole-process on-CPU attribution, not pure-server CPU or an A/B ranking")


def save(path, value):
    """Create structured evidence without replacing earlier raw files."""
    with path.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def digest(path):
    """Hash a file without buffering large libraries or recordings."""
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(command, folder, label, cwd=None, timeout=120, monitor=None, env=None):
    """Retain a complete command/log/status and stop only its process group on failure."""
    save(folder / (label + ".command.json"), command)
    process = None
    status = {"start_epoch_ns": time.time_ns(), "start_monotonic_ns": time.monotonic_ns()}
    try:
        with (folder / (label + ".log")).open("x") as log:
            process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + timeout
            while process.poll() is None:
                if monitor:
                    monitor()
                if time.monotonic() > deadline:
                    raise TimeoutError(f"{label} exceeded {timeout} seconds")
                time.sleep(.25)
            status["exit_code"] = process.returncode
            if process.returncode:
                raise RuntimeError(f"{label} failed ({process.returncode}); inspect {label}.log")
    except BaseException as error:
        status["failure"] = type(error).__name__ + ": " + str(error)
        raise
    finally:
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        status["exit_code"] = process.returncode if process else None
        status["end_epoch_ns"] = time.time_ns()
        status["end_monotonic_ns"] = time.monotonic_ns()
        save(folder / (label + ".status.json"), status)


def install_profiler(folder):
    """Download one fixed official asset and extract regular files after its SHA-256 gate."""
    archive = folder / ASSET
    with urllib.request.urlopen(URL, timeout=60) as source, archive.open("xb") as target:
        shutil.copyfileobj(source, target)
    if digest(archive) != ASSET_SHA256:
        raise ValueError("async-profiler release asset SHA-256 mismatch")
    destination = folder / "async-profiler"
    destination.mkdir()
    prefix = f"async-profiler-{VERSION}-linux-x64/"
    with tarfile.open(archive) as source:
        for member in source.getmembers():
            if not member.isfile():
                continue
            if not member.name.startswith(prefix) or ".." in Path(member.name).parts:
                raise ValueError("Unexpected profiler archive path")
            path = destination / member.name[len(prefix):]
            path.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as data, path.open("xb") as target:
                shutil.copyfileobj(data, target)
            path.chmod(member.mode & 0o755)
    save(folder / "profiler-release.json", {"version": VERSION, "source_commit": SOURCE_COMMIT,
         "url": URL, "sha256": ASSET_SHA256})
    return destination


def profiler_command(event, folder):
    """Select a CPU timer explicitly; cpu-clock forces perf without automatic wall fallback."""
    if event not in ("ctimer", "cpu-clock"):
        raise ValueError("CPU event must be ctimer or cpu-clock")
    return (f"start,event={event},interval=10ms,cstack=dwarf,clock=monotonic,jfr,"
            f"file={folder}/cpu-%p.jfr,log={folder}/async-profiler-%p.log"
            + (",alluser" if event == "cpu-clock" else ""))


def launch_environment(profiler, event, folder):
    """Enable native pthread hooks before JVM startup, without modifying the caller environment."""
    inherited = dict(os.environ)
    for key in ("LD_PRELOAD", "ASPROF_COMMAND", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
        if inherited.get(key):
            raise ValueError(f"Run in a clean environment: {key} is already set")
    # The harness's standard JDK recording may include initial environment events.
    # Give this local synthetic benchmark no runner/service credential environment.
    env = {key: inherited[key] for key in ("PATH", "JAVA_HOME", "LANG", "LC_ALL", "TZ") if key in inherited}
    env.update(HOME="/tmp", USER="profile-user", LOGNAME="profile-user", TMPDIR="/tmp")
    env["LD_PRELOAD"] = str(profiler / "lib/libasyncProfiler.so")
    env["ASPROF_COMMAND"] = profiler_command(event, folder)
    return env


def event_values(path):
    """Read complete JDK JSON output, failing on malformed or missing recordings."""
    return json.loads(path.read_text())["recording"]["events"]


def frame_names(sample):
    """Keep unresolved native frames visible rather than assuming every method has a class."""
    for frame in (sample.get("stackTrace") or {}).get("frames", []):
        method = frame.get("method") or {}
        yield ((method.get("type") or {}).get("name") or "", method.get("name") or "[unknown]")


def inspect_cpu_events(events, expected):
    """Require actual CPU engine/settings and nonempty samples, never a wall profile."""
    settings = {}
    samples = []
    for event in events:
        value = event["values"]
        if event["type"] == "jdk.ActiveSetting":
            settings.setdefault(value["name"], set()).add(value.get("value"))
        elif event["type"] == "jdk.ExecutionSample":
            samples.append(value)
        elif "WallClock" in event["type"]:
            raise ValueError("Wall clock samples are not an on-CPU profile")
    engine = "ctimer" if expected == "ctimer" else "perf_events"
    for key, required in (("engine", engine), ("event", expected), ("version", VERSION),
                          ("interval", "10000000")):
        if settings.get(key) != {required}:
            raise ValueError(f"Wrong or missing actual CPU setting {key}: {settings.get(key)}")
    if "wall" in settings or settings.get("cstack") != {"dwarf"}:
        raise ValueError("Missing native DWARF stacks or unexpected wall timer")
    if not samples or not any((sample.get("stackTrace") or {}).get("frames") for sample in samples):
        raise ValueError("CPU recording has no readable execution stacks")
    threads = Counter(str(sample["sampledThread"]["osThreadId"]) for sample in samples)
    return {"actual_engine": engine, "requested_event": expected, "samples": len(samples),
            "samples_by_os_tid": dict(threads), "settings": {k: sorted(v, key=str) for k, v in settings.items()}}, samples


def extract_profile(folder, recording, event):
    """Retain readable JFR metadata and every CPU event without altering the recording."""
    stem = recording.stem
    run(["jfr", "summary", str(recording)], folder, stem + "-summary")
    run(["jfr", "print", "--json", "--stack-depth", "2048", "--events",
         "jdk.ActiveSetting,jdk.ExecutionSample,profiler.WallClockSample", str(recording)],
        folder, stem + "-events")
    return inspect_cpu_events(event_values(folder / (stem + "-events.log")), event)


def check_probe(samples, counters):
    """Reject JVM-only coverage and a waiting native thread masquerading as a CPU hotspot."""
    busy_tid, wait_tid = counters["native_busy_tid"], counters["native_wait_tid"]
    native = [s for s in samples if s["sampledThread"]["osThreadId"] == busy_tid]
    waiting = [s for s in samples if s["sampledThread"]["osThreadId"] == wait_tid]
    java = [s for s in samples if any(cls == "CpuProfileProbe" for cls, name in frame_names(s))]
    symbol = any("ap_native_busy" in name for s in native for cls, name in frame_names(s))
    if len(native) < 15 or len(java) < 15 or not symbol:
        raise ValueError("CPU preflight missed busy Java or unattached native worker/symbol")
    if len(waiting) > max(2, len(native) // 10):
        raise ValueError("CPU preflight sampled the waiting worker too frequently")
    if (counters["native_busy_cpu_ns"] < 790_000_000 or counters["java_busy_cpu_ns"] < 790_000_000
            or counters["native_wait_cpu_ns"] > 50_000_000):
        raise ValueError("CPU preflight worker CPU counters contradict busy/waiting phases")
    return {"native_busy_samples": len(native), "java_busy_stack_samples": len(java),
            "native_wait_samples": len(waiting), "unattached_native_symbol_verified": symbol,
            "native_worker_counters": counters}


def preflight(output, profiler, event):
    """Compile and run a tiny controlled JNI CPU/wait probe before any expensive build."""
    folder = output / "preflight"
    folder.mkdir()
    sources = HERE / "cpu-profile-probe"
    for source in sources.iterdir():
        shutil.copyfile(source, folder / source.name)
    java = Path(shutil.which("java")).resolve()
    jdk = java.parent.parent
    run(["gcc", "-shared", "-fPIC", "-O2", "-g", "-fno-omit-frame-pointer", "-pthread",
         "-I", str(jdk / "include"), "-I", str(jdk / "include/linux"),
         str(folder / "cpu-profile-probe.c"), "-o", str(folder / "libprobe.so")], folder, "gcc")
    run(["javac", str(folder / "CpuProfileProbe.java")], folder, "javac")
    run([str(java), "-agentpath:" + str(profiler / "lib/libasyncProfiler.so"),
         "-cp", str(folder), "CpuProfileProbe", str(folder / "libprobe.so"), str(folder)],
        folder, "probe", env=launch_environment(profiler, event, folder), timeout=30)
    recordings = list(folder.glob("cpu-*.jfr"))
    if len(recordings) != 1:
        raise ValueError("CPU preflight did not retain exactly one recording")
    metadata, samples = extract_profile(folder, recordings[0], event)
    counters = dict((key, int(value)) for key, value in
                    (line.split("=", 1) for line in (folder / "probe.log").read_text().splitlines()
                     if line.startswith("native_")))
    counters.update(dict((key, int(value)) for key, value in
                         (line.split("=", 1) for line in (folder / "java-clock.txt").read_text().splitlines())))
    save(folder / "verified.json", {**metadata, **check_probe(samples, counters)})


def capture_file(path):
    """Keep independent read failures so one successful counter never masks another failure."""
    try:
        return {"text": path.read_text(), "status": "ok"}
    except OSError as error:
        return {"status": "error", "error": str(error)}


class ProcessCollector:
    """Sample only this run's target JVM and retain mapped native binaries once by content hash."""

    def __init__(self, folder, libraries):
        self.folder, self.libraries = folder, libraries
        self.seen = set()
        self.stream = (folder / "process.jsonl").open("x")

    def sample(self):
        """Keep OS thread CPU counters, maps, memory, IO and cgroup counters with clock brackets."""
        for launch in self.folder.glob("java-launch-*.json"):
            pid = json.loads(launch.read_text())["pid"]
            proc = Path("/proc") / str(pid)
            if not proc.exists():
                continue
            sample = {"pid": pid, "monotonic_ns": time.monotonic_ns(), "epoch_ns": time.time_ns(),
                      "files": {name: capture_file(proc / name)
                                for name in ("stat", "status", "io", "smaps_rollup", "maps", "cgroup")},
                      "tasks": {task.name: {name: capture_file(task / name) for name in ("stat", "comm")}
                                for task in (proc / "task").glob("*")},
                      "host_kernel": SYSTEM["host_sample"]()}
            group = next((line[3:] for line in sample["files"]["cgroup"].get("text", "").splitlines()
                          if line.startswith("0::")), None)
            root = Path("/sys/fs/cgroup")
            base = (root / group.lstrip("/")).resolve() if group is not None else None
            sample["cgroup_path"] = str(base) if base is not None else None
            sample["cgroup"] = ({name: capture_file(base / name)
                                 for name in ("cpu.stat", "cpu.max", "cpuset.cpus.effective", "memory.current",
                                              "memory.max", "memory.events", "io.stat", "cpu.pressure",
                                              "memory.pressure", "io.pressure")}
                                if base is not None and base.is_relative_to(root) else
                                {"status": "unsupported", "error": "No accessible cgroup v2 target path"})
            self.stream.write(json.dumps(sample) + "\n")
            self.stream.flush()
            for line in sample["files"]["maps"].get("text", "").splitlines():
                fields = line.split(None, 5)
                if len(fields) != 6 or "x" not in fields[1] or ".so" not in fields[5]:
                    continue
                name = fields[5]
                if name in self.seen:
                    continue
                path = Path(name.removesuffix(" (deleted)"))
                if path.is_file():
                    sha = digest(path)
                    target = self.libraries / (sha + "-" + path.name)
                    if not target.exists():
                        shutil.copyfile(path, target)
                    metadata = self.folder / ("native-" + sha + ".json")
                    if not metadata.exists():
                        save(metadata, {"mapped_path": name, "sha256": sha, "retained_file": target.name})
                    self.seen.add(name)

    def close(self):
        """Close the append-only raw resource stream."""
        self.stream.close()


def write_wrapper(folder, java, profiler, event):
    """Profile the Surefire fork only, leaving Maven and version discovery uninstrumented."""
    # Surefire 3.0.0 requires bin/java and reads its parent JDK release file.
    jdk = folder / "profile-jdk"
    (jdk / "bin").mkdir(parents=True)
    shutil.copyfile(Path(java).parent.parent / "release", jdk / "release")
    path = jdk / "bin/java"
    payload = {"java": java, "library": str(profiler / "lib/libasyncProfiler.so"),
               "command": profiler_command(event, folder), "folder": str(folder)}
    script = '''#!/usr/bin/env python3
import json, os, pathlib, sys, time
config = CONFIG
args = sys.argv[1:]
env = dict(os.environ)
if any("surefirebooter" in arg or "ForkedBooter" in arg for arg in args):
    env = {key: env[key] for key in ("PATH", "JAVA_HOME", "LANG", "LC_ALL", "TZ") if key in env}
    env.update(HOME="/tmp", USER="profile-user", LOGNAME="profile-user", TMPDIR="/tmp")
    env["JAVA_HOME"] = str(pathlib.Path(config["java"]).parent.parent)
    pid = os.getpid()
    before = time.monotonic_ns()
    epoch = time.time_ns()
    after = time.monotonic_ns()
    argv = [config["java"], "-agentpath:" + config["library"], *args]
    record = pathlib.Path(config["folder"]) / ("java-launch-%d.json" % pid)
    temporary = record.with_suffix(".tmp")
    with temporary.open("x") as out:
        json.dump({"pid": pid, "argv": argv, "epoch_ns": epoch,
                   "monotonic_before_ns": before, "monotonic_after_ns": after,
                   "runtime_environment": env,
                   "LD_PRELOAD": config["library"], "ASPROF_COMMAND": config["command"]}, out)
    temporary.rename(record)
    env["LD_PRELOAD"] = config["library"]
    env["ASPROF_COMMAND"] = config["command"]
else:
    argv = [config["java"], *args]
os.execve(config["java"], argv, env)
'''.replace("CONFIG", repr(payload), 1)
    path.write_text(script)
    path.chmod(0o755)
    return path


def thread_cpu_coverage(path, samples):
    """Expose unsampled CPU-active OS threads using conservative first-to-last observed tick deltas."""
    ticks = {}
    for line in path.read_text().splitlines():
        for tid, task in json.loads(line).get("tasks", {}).items():
            stat = task.get("stat", {})
            if stat.get("status") != "ok":
                continue
            fields = stat["text"].rsplit(")", 1)[1].split()
            user, system = int(fields[11]), int(fields[12])
            previous = ticks.setdefault(tid, [user, system, user, system])
            previous[2], previous[3] = user, system
    if not ticks:
        raise ValueError("No actual target OS thread CPU counters were retained")
    counts = Counter(str(s["sampledThread"]["osThreadId"]) for s in samples)
    frequency = os.sysconf("SC_CLK_TCK")
    coverage = {tid: {"observed_user_seconds": (last_user - first_user) / frequency,
                      "observed_system_seconds": (last_system - first_system) / frequency,
                      "cpu_samples": counts[tid]}
                for tid, (first_user, first_system, last_user, last_system) in ticks.items()}
    return {"scope": "first-to-last sampled OS thread counters; conservative lifetime lower bounds",
            "clock_ticks_per_second": frequency, "threads": coverage,
            "unsampled_active_tids": [tid for tid, row in coverage.items()
                                      if row["observed_user_seconds"] >= .5 and not row["cpu_samples"]]}


def validate_trial(folder, clients, event):
    """Require completed answer/resource evidence and native worker CPU coverage, retaining failures."""
    summary, _ = SCREEN["summary_of"](folder, [clients])
    run(["jfr", "summary", str(folder / "profile.jfr")], folder, "harness-jfr-summary")
    for label in ("warmup-c8", f"c{clients}"):
        with (folder / (label + ".csv")).open() as source:
            rows = list(csv.DictReader(source))
        if not rows or any(row.get("ok") != "1" for row in rows):
            raise ValueError("Missing or unsuccessful raw answer rows")
        if {row["query_kind"] for row in rows} != {"0", "1", "2"}:
            raise ValueError("Mixed workload lacks one of its query kinds")
    recordings = list(folder.glob("cpu-*.jfr"))
    launches = list(folder.glob("java-launch-*.json"))
    if len(recordings) != 1 or len(launches) != 1:
        raise ValueError("Expected one explicitly profiled Surefire JVM")
    metadata, samples = extract_profile(folder, recordings[0], event)
    coverage = thread_cpu_coverage(folder / "process.jsonl", samples)
    save(folder / "thread-cpu-coverage.json", coverage)
    if coverage["unsampled_active_tids"]:
        raise ValueError("CPU profile missed OS threads with at least 0.5 observed user CPU seconds")
    worker = [s for s in samples if not s["sampledThread"].get("javaThreadId")
              and any("duckdb" in cls.lower() for cls, name in frame_names(s))]
    resolved_worker = [s for s in worker if any("duckdb" in cls.lower()
                       and ("duckdb::" in name or name.startswith("duckdb_")
                            or (name.startswith("_Z") and "6duckdb" in name))
                       for cls, name in frame_names(s))]
    depth = Counter(sum(not name.startswith(("[", "0x")) and "unknown" not in name.lower()
                        for cls, name in frame_names(s)) for s in resolved_worker)
    if len(resolved_worker) < 10 or sum(count for length, count in depth.items() if length >= 2) < 10:
        raise ValueError("Insufficient resolved, unwound unattached DuckDB native worker CPU stacks")
    unresolved = Counter(name for s in samples for cls, name in frame_names(s)
                         if name.startswith(("[", "0x")) or "unknown" in name.lower())
    native = [json.loads(path.read_text()) for path in folder.glob("native-*.json")]
    if not any("duckdb" in item["mapped_path"].lower() for item in native):
        raise ValueError("Actual mapped DuckDB native binary was not retained")
    save(folder / "cpu-verified.json", {**metadata, "scope": SCOPE,
         "unattached_duckdb_worker_samples": len(worker), "profile_window": "entire Surefire JVM lifetime",
         "resolved_unattached_duckdb_worker_samples": len(resolved_worker),
         "unattached_duckdb_worker_tids": sorted({s["sampledThread"]["osThreadId"] for s in resolved_worker}),
         "resolved_worker_stack_depth_counts": dict(depth),
         "unresolved_frame_occurrences": dict(unresolved),
         "truncated_stack_samples": sum(bool((s.get("stackTrace") or {}).get("truncated")) for s in samples),
         "phase_clock": "c*-wall.txt uses System.nanoTime; java-launch clock bracket uses Linux monotonic",
         "raw_metric_summary": summary})


def manifest(folder):
    """Bind every retained raw and derived file, including failed attempts."""
    with (folder / "SHA256SUMS").open("x") as target:
        for path in sorted(folder.rglob("*")):
            if path.is_file() and path.name != "SHA256SUMS":
                target.write(digest(path) + "  " + path.relative_to(folder).as_posix() + "\n")


def symbol_metadata(libraries):
    """Retain native symbol/build-ID listings even when the workload or coverage gate failed."""
    errors = []
    for library in list(libraries.iterdir()):
        if not library.name[:64].isalnum() or library.name[64:65] != "-" or library.suffix in (".json", ".log"):
            continue
        for label, arguments in (("symbols", ["nm", "-C", "--dynamic", "--defined-only"]),
                                 ("elf", ["readelf", "-n", "-S"])):
            name = library.name + "-" + label
            if (libraries / (name + ".status.json")).exists():
                continue
            try:
                run([*arguments, str(library)], libraries, name)
            except Exception as error:
                errors.append({"library": library.name, "operation": label, "error": str(error)})
    if errors:
        save(libraries / "extraction-errors.json", errors)


def main():
    """Build once and collect representative baseline profiles without comparison or promotion."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--event", choices=("ctimer", "cpu-clock"), default="ctimer")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    if platform.machine() != "x86_64" or sys.platform != "linux" or not 5 <= args.seconds <= 60:
        raise ValueError("Requires Linux x86_64 and 5..60 measured seconds")
    output, source = args.output.resolve(), args.source.resolve()
    if any(char in str(output) for char in ", \n"):
        raise ValueError("Profiler output path cannot contain commas, spaces or newlines")
    output.mkdir(parents=True, exist_ok=False)
    try:
        # No inherited JVM/native agent may change the measurement or expose caller configuration.
        launch_environment(output, args.event, output)
        affinity = sorted(os.sched_getaffinity(0))
        if len(affinity) < 4 and not args.preflight_only:
            raise ValueError("Four available logical CPUs are required")
        save(output / "plan.json", {"scope": SCOPE, "clients": [8, 32], "seconds": args.seconds,
             "heavy_every": 8, "requested_event": args.event, "interval_ns": 10_000_000,
             "cpus": affinity[:4], "kernel_release": platform.release(), "kernel_version": platform.version(),
             "workflow_sha": os.environ.get("GITHUB_SHA"), "run_id": os.environ.get("GITHUB_RUN_ID"),
             "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
             "rlimits": {name: resource.getrlimit(getattr(resource, name))
                         for name in ("RLIMIT_NOFILE", "RLIMIT_SIGPENDING", "RLIMIT_AS", "RLIMIT_MEMLOCK", "RLIMIT_CPU")},
             "experiments": "integerLiteralParameters=false, cachePreparedSchemaMessages=false"})
        SYSTEM["save_host_snapshot"](output / "system-host-before.json", output)
        for label, command in (("hardware", ["lscpu"]), ("java-version", ["java", "-version"]),
                               ("compiler-version", ["gcc", "--version"]),
                               ("commit", ["git", "-C", str(source), "rev-parse", "HEAD"]),
                               ("source-diff", ["git", "-C", str(source), "diff", "HEAD"])):
            run(command, output, label)
        if not re.search(r'version "21(?:\.|\")', (output / "java-version.log").read_text()):
            raise ValueError("The CPU diagnostic requires Java 21")
        profiler = install_profiler(output)
        control = output / "control"
        control.mkdir()
        for name in ("cpu-profile.py", "optimization-screen.py", "benchmark-system.py"):
            shutil.copyfile(HERE / name, control / name)
        for name in ("SmallQueryThroughputPerfTest.java", "PerfRunArtifacts.java"):
            shutil.copyfile(source / SCREEN["HARNESS"] / name, control / name)
        print("Starting CPU/native-worker preflight", flush=True)
        preflight(output, profiler, args.event)
        if args.preflight_only:
            return
        data, libraries = output / "data", output / "native-libraries"
        data.mkdir()
        libraries.mkdir()
        java = str(Path(shutil.which("java")).resolve())
        print("Building one baseline; full Maven output retained", flush=True)
        run(["mvn", "-B", "-ntp", "-pl", "flight-server", "-am", "package", "-DskipTests",
             "-Dcheckstyle.skip", "-Dspotbugs.skip"], output, "build", cwd=source, timeout=900)
        for clients in (8, 32):
            folder = output / f"c{clients}"
            folder.mkdir()
            wrapper = write_wrapper(folder, java, profiler, args.event)
            collector = ProcessCollector(folder, libraries)
            print(f"Profiling baseline at {clients} clients (mixed every8); full log in c{clients}/maven.log", flush=True)
            try:
                run(["taskset", "-c", ",".join(map(str, affinity[:4])), "mvn", "-B", "-ntp", "-pl",
                     "flight-server", "-am", "test", "-Pperf", "-Dgroups=perf", "-DexcludedGroups=",
                     "-Djacoco.skip=true", "-Dtest=SmallQueryThroughputPerfTest",
                     "-Dsurefire.failIfNoSpecifiedTests=false", "-Dcheckstyle.skip", "-Dspotbugs.skip",
                     "-Djvm=" + str(wrapper), "-DargLine=" + SCREEN["JVM"],
                     "-Dperf.clients=" + str(clients), "-Dperf.seconds=" + str(args.seconds),
                     "-Dperf.heavyEvery=8", "-Dperf.threads=2", "-Dperf.shared=2:512MB",
                     "-Dperf.dataDir=" + str(data), "-Dperf.output=" + str(folder),
                     "-Darrowflight.experimental.integerLiteralParameters=false",
                     "-Darrowflight.experimental.cachePreparedSchemaMessages=false"],
                    folder, "maven", cwd=source, timeout=240, monitor=collector.sample)
            finally:
                collector.close()
                reports = source / "flight-server/target/surefire-reports"
                for report in reports.glob("*SmallQueryThroughputPerfTest*"):
                    shutil.copyfile(report, folder / report.name)
            validate_trial(folder, clients, args.event)
            recording = next(folder.glob("cpu-*.jfr"))
            for name, options in (("flame.html", []), ("collapsed.txt", ["--output", "collapsed"])):
                run([str(profiler / "bin/jfrconv"), *options, "--threads", str(recording), str(folder / name)],
                    folder, "convert-" + name)
        (output / "summary.md").write_text("## CPU profile collection\n\nBoth mixed baseline profiles passed "
            "the answer, actual CPU-engine and unattached DuckDB worker gates.\n\n" + SCOPE
            + ". The CPU recordings include JVM startup, warmup, measurement and shutdown. "
            "Use the retained phase clocks before attributing measured-query costs.\n")
    except BaseException as error:
        save(output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        if (output / "native-libraries").is_dir():
            symbol_metadata(output / "native-libraries")
        SYSTEM["save_host_snapshot"](output / "system-host-after.json", output)
        manifest(output)


if __name__ == "__main__":
    def interrupted(signum, frame):
        """Preserve files and terminate only the active task process group on interruption."""
        raise InterruptedError(f"Received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    main()
