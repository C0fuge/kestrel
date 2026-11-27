#!/usr/bin/env python3
"""Build isolated, portable JDBC compiler candidates; never rank or deploy them."""

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import signal
import shlex
import subprocess
import tarfile
import tempfile
import time
import urllib.request
import uuid
import zipfile


JDBC = "a561d2ddadde231402411ce76112779bc34b57f0"
CORE = "069cc9f9b5be802405797faecc284961b07c70ef"
BASE = "quay.io/pypa/manylinux_2_28_x86_64@sha256:55a36963bc0504cdf004011001c1072306402f0b8b83be5a5eba7068f93eec53"
FLAGS = "-march=x86-64 -mtune=generic -D_GLIBCXX_USE_CXX11_ABI=1"
SNAPPY = {"snappy.cc": "fdf65fc9a34465224869751981ea517df5b63e378f4d874191da7e298260b588",
          "snappy_version.hpp": "b51e605cecc9619e0c37c92369371a261193184948127a31ad330dc8f8d3c0e1"}
HERE = Path(__file__).resolve().parent
SYSTEM = runpy.run_path(str(HERE / "benchmark-system.py"))
EFFECTIVE = runpy.run_path(str(HERE / "native-compiler/effective.py"))
RESOURCES = runpy.run_path(str(HERE / "native-compiler/resources.py"))
STOCK_NATIVE = "b36f739cca01c475867e332e496da8c95bcac37478d55f40ac9b3149e30f285c"
STOCK_JAR = "04ae3375d7c40dc88dc9bf0014b82ba5e66715564584af4e3abea39c7ba144d3"
SMOKES = {"loaded_native", "source_version", "jdbc_types", "arrow_types", "snappy_jdbc",
          "snappy_arrow", "cancel_and_reuse", "native_extensions"}
ANSWERS = {"jdbc_types": (1, "3d935bc5ed35e2e0"), "arrow_types": (1, "3d935bc5ed35e2e0"),
           "snappy_jdbc": (128, "d814e1d9a8012e52"), "snappy_arrow": (128, "d814e1d9a8012e52"),
           "native_extensions": (128, "d814e1d9a8012e52"), "source_version": (1, "9abf82c842a008b6"),
           "cancel_and_reuse": (1, "616fa64e259babae")}
UPSTREAM = ("test_connection", "test_execute_exception", "test_multiple_statements_execution")


def save(path, value):
    """Write a new evidence file without replacing a previous attempt."""
    with path.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def digest(path):
    """Hash a retained file with bounded memory."""
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def download(url, path):
    """Retain the exact public response and its hash before using it."""
    with urllib.request.urlopen(url, timeout=90) as response, path.open("xb") as output:
        shutil.copyfileobj(response, output)
    save(path.with_suffix(path.suffix + ".identity.json"), {"url": url, "sha256": digest(path),
                                                         "bytes": path.stat().st_size})


def unpack(archive, destination):
    """Extract a source archive after rejecting links and escaping member paths."""
    destination.mkdir()
    with tarfile.open(archive) as source:
        members = source.getmembers()
        roots = {Path(member.name).parts[0] for member in members}
        if len(roots) != 1:
            raise ValueError("Expected one archive root")
        for member in members:
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts or not (member.isfile() or member.isdir()):
                raise ValueError("Unsafe source archive member")
        source.extractall(destination, filter="data")
    return destination / roots.pop()


def verify_source(source):
    """Check the vendored source graph and decoder bytes before either build."""
    version = (source / "src/duckdb/src/function/table/version/pragma_version.cpp").read_text()
    if '#define DUCKDB_VERSION "v1.5.6"' not in version or f'#define DUCKDB_SOURCE_ID "{CORE[:10]}"' not in version:
        raise ValueError("Vendored DuckDB identity differs")
    for name, expected in SNAPPY.items():
        if digest(source / "src/duckdb/third_party/snappy" / name) != expected:
            raise ValueError("Vendored decoder differs: " + name)
    return {"jdbc_commit": JDBC, "duckdb_commit": CORE, "duckdb_version": "v1.5.6",
            "source_graph": "JDBC vendors src/duckdb; no submodule", "snappy_sha256": SNAPPY}


def prepare_source(source, evidence):
    """Apply the same official jemalloc recipe and Linux linker policy to both compilers."""
    before = digest(source / "CMakeLists.txt")
    text = (source / "CMakeLists.txt").read_text()
    old = 'if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")'
    if text.count(old) != 1:
        raise ValueError("Upstream GNU linker policy changed")
    text = text.replace(old, 'if(CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")')
    (source / "CMakeLists.txt").write_text(text)
    patch = source / "data/vendoring/jemalloc_enforce.patch"
    run(["patch", "-p1", "-i", str(patch)], evidence / "jemalloc-patch", cwd=source / "src/duckdb")
    save(evidence / "common-source-adaptations.json", {
        "cmake_before": before, "cmake_after": digest(source / "CMakeLists.txt"),
        "change": "Apply upstream GNU Linux symbol-binding/version-script policy to Clang too",
        "official_jemalloc_patch_sha256": digest(patch)})


def stock_reference(output):
    """Retain the unchanged published JAR and the exact currently measured native reference."""
    root = output / "stock"
    root.mkdir()
    url = "https://repo.maven.apache.org/maven2/org/duckdb/duckdb_jdbc/1.5.6.0/duckdb_jdbc-1.5.6.0"
    for suffix in ("jar", "jar.sha256", "pom"):
        download(url + "." + suffix, root / ("duckdb_jdbc." + suffix))
    jar = root / "duckdb_jdbc.jar"
    if digest(jar) != STOCK_JAR or STOCK_JAR != (root / "duckdb_jdbc.jar.sha256").read_text().strip():
        raise ValueError("Published stock JAR checksum differs from current stock reference")
    native = root / "libduckdb_java.so_linux_amd64"
    with zipfile.ZipFile(jar) as archive, archive.open(native.name) as source, native.open("xb") as target:
        shutil.copyfileobj(source, target)
    if digest(native) != STOCK_NATIVE:
        raise ValueError("Stock reference differs from the actual measured native library")
    save(root / "reference.json", {"jar_sha256": digest(jar), "native_sha256": digest(native),
         "scope": "unchanged published fallback/third reference; no adoption claim from compiler pair alone"})


def shared_extensions(common):
    """Fetch one stock signed extension byte set for explicit LOAD by all three variants."""
    root = common / "extensions"
    root.mkdir()
    result = {}
    for name in ("httpfs", "iceberg"):
        archive = root / (name + ".duckdb_extension.gz")
        download("https://extensions.duckdb.org/v1.5.6/linux_amd64/" + archive.name, archive)
        native = root / (name + ".duckdb_extension")
        with gzip.open(archive, "rb") as source, native.open("xb") as target:
            shutil.copyfileobj(source, target)
        result[name] = {"sha256": digest(native), "bytes": native.stat().st_size,
                        "signature_check": "Required by each stock/GCC/Clang LOAD; never bypassed"}
    save(common / "shared-extensions.json", result)


def verify_upstream(root):
    """Require each intended upstream test to execute successfully under native connection limits."""
    result = {}
    for name in UPSTREAM:
        text = (root / ("upstream-" + name + ".log")).read_text()
        if (root / ("upstream-" + name + "-status.txt")).read_text().strip() != "0":
            raise ValueError("Upstream smoke process failed")
        successes = re.findall(r"^(\w+)#(test_\w+) success in [0-9]+ seconds$", text, re.M)
        if ("TestDuckDBJDBC", name) not in successes or not re.search(r"^OK$", text, re.M) or re.search(r"^FAILED$|No tests found|failed with", text, re.M):
            raise ValueError("Intended upstream test did not execute successfully")
        limits = root / ("upstream-" + name) / "native-limits.tsv"
        result[name] = {"successful_tests": successes, "limited_connections": native_limit_rows(limits)}
    save(root / "upstream-verified.json", result)


def native_limit_rows(path):
    """Require real native thread, memory and temporary-file limits for each smoke connection."""
    lines = path.read_text().splitlines()
    if not lines:
        raise ValueError("Missing effective native connection limits")
    for line in lines:
        values = dict(item.split("=", 1) for item in line.split("\t"))
        if values.get("threads") != "1" or any(values.get(key) not in ("256.0 MiB", "256 MiB")
                for key in ("memory_limit", "max_temp_directory_size")) or not values.get("temp_directory"):
            raise ValueError("Native smoke limits differ")
    return len(lines)


def run(argv, prefix, cwd=None, timeout=600):
    """Retain argv, elapsed time, exit status and complete output, including failures."""
    started = time.time_ns()
    status = -1
    try:
        with prefix.with_suffix(".log").open("xb") as output:
            process = subprocess.Popen(argv, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                stop_process(process)
                status = 124
            except BaseException:
                stop_process(process)
                status = 130
                raise
    finally:
        save(prefix.with_suffix(".status.json"), {"argv": argv, "status": status,
             "start_epoch_ns": started, "elapsed_ns": time.time_ns() - started})
    if status:
        raise RuntimeError(f"{prefix.name} failed: {status}; retained complete log")


def stop_process(process):
    """Stop only this command's process group, including child smoke JVMs on timeout."""
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def compiler_command(compiler, image, source, evidence, name, phase="configure"):
    """Give both compilers identical limits, mounts, ISA, standard-library ABI and release flags."""
    if compiler not in ("gcc", "clang"):
        raise ValueError("Unknown compiler")
    if phase not in ("configure", "build"):
        raise ValueError("Unknown phase")
    return ["docker", "run", "--name", name, "--hostname", "native-screen", "--cpus=2", "--memory=10g",
            "--memory-swap=10g", "--network=none", "--pids-limit=1024", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--user", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp/native-home", "-e", "TMPDIR=/tmp",
            "-v", f"{source.parent / 'container-passwd'}:/etc/passwd:ro",
            "-v", f"{source.parent / 'container-group'}:/etc/group:ro",
            "-v", f"{source}:/source", "-v", f"{evidence}:/evidence",
            "-v", f"{HERE / 'native-compiler'}:/control:ro", "-w", "/source", image,
            "bash", "/control/build.sh", compiler, FLAGS, phase]


def prepare_identity(image, work, output):
    """Provide an image-only Unix identity for the runner UID without restoring capabilities."""
    for filename in ("passwd", "group"):
        run(["docker", "run", "--rm", "--name", "native-identity-" + uuid.uuid4().hex,
             "--network=none", image, "cat", "/etc/" + filename], output / ("image-" + filename))
        text = (output / ("image-" + filename + ".log")).read_text().rstrip("\n") + "\n"
        identifier = os.getuid() if filename == "passwd" else os.getgid()
        if not any(line.split(":")[2] == str(identifier) for line in text.splitlines() if len(line.split(":")) >= 3):
            text += (f"native-build:x:{identifier}:{os.getgid()}:Native build:/tmp/native-home:/bin/sh\n"
                     if filename == "passwd" else f"native-build:x:{identifier}:\n")
        (work / ("container-" + filename)).write_text(text)
    save(output / "runner-identity.json", {"uid": os.getuid(), "gid": os.getgid(), "home": "/tmp/native-home",
                                         "capabilities": "ALL dropped; numeric runner identity owns mounted files"})


def candidate_phase(compiler, image, pristine, work, output, phase):
    """Configure or build one task-owned candidate while keeping independent phase evidence."""
    evidence = output / compiler
    source = work / compiler
    if phase == "configure":
        evidence.mkdir()
        shutil.copytree(pristine, source)
        prepare_source(source, evidence)
    name = f"native-compiler-{uuid.uuid4().hex}-{compiler}-{phase}"
    save(evidence / ("owned-container-" + phase + ".json"), {"name": name})
    command = compiler_command(compiler, image, source, evidence, name, phase)
    SYSTEM["save_host_snapshot"](evidence / ("host-before-" + phase + ".json"), output)
    stop = False

    def sample():
        """Retain optional raw build-container cgroup counters without sampling query timings."""
        with (evidence / (phase + "-resources.jsonl")).open("x") as stream:
            while not stop:
                result = SYSTEM["optional_command"](["docker", "exec", name, "sh", "-c",
                    SYSTEM["framed_read_command"](("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.current",
                        "/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory.events", "/sys/fs/cgroup/io.stat"))],
                    "build_container")
                stream.write(json.dumps({"epoch_ns": time.time_ns(), "sample": result}) + "\n")
                stream.flush()
                for _ in range(50):
                    if stop:
                        return
                    time.sleep(.1)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            sampler = executor.submit(sample)
            try:
                run(command, evidence / phase, timeout=600 if phase == "configure" else 3900)
            finally:
                stop = True
                sampler.result(timeout=20)
    finally:
        SYSTEM["save_host_snapshot"](evidence / ("host-after-" + phase + ".json"), output)
        state = SYSTEM["optional_command"](["docker", "inspect", "--format", "{{json .State}}", name], "build_container")
        save(evidence / (phase + "-container-state.json"), state)
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False)
    RESOURCES["verify"](evidence, phase)
    return evidence


def check_probes(output):
    """Reject a wrong decoder, raised ISA, different libstdc++ or incompatible declared ABI."""
    probes, common, toolchains, installations = {}, {}, {}, {}
    for compiler in ("gcc", "clang"):
        folder = output / compiler
        effective = json.loads((folder / "effective.json").read_text())
        macros = dict(re.findall(r"^#define (\w+) (.*)$", (folder / "macros.txt").read_text(), re.M))
        if macros.get("SNAPPY_NEW_VERSION") != ("false" if compiler == "gcc" else "true"):
            raise ValueError("Wrong decoder selection: " + compiler)
        if macros.get("_GLIBCXX_USE_CXX11_ABI") != "1" or macros.get("__cplusplus") != "201103L":
            raise ValueError("Wrong C++ standard/ABI: " + compiler)
        if any(name in macros for name in ("__AVX__", "__AVX2__", "__BMI__", "__BMI2__", "__SSSE3__", "__SSE3__", "__SSE4_1__", "__SSE4_2__")):
            raise ValueError("ISA exceeds portable x86-64 baseline")
        commands = json.loads((folder / "compile_commands.json").read_text())
        decoder = [item for item in commands if item["file"].endswith("/third_party/snappy/snappy.cc")]
        if len(decoder) != 1:
            raise ValueError("Actual decoder compilation is unproven")
        flags = EFFECTIVE["command"](decoder[0], compiler)
        if effective.get("entry") != decoder[0] or effective.get("probe_argv") != flags or effective.get("compile_database_sha256") != digest(folder / "compile_commands.json"):
            raise ValueError("Actual compile argv does not match effective probe evidence")
        if effective.get("decoder") != macros["SNAPPY_NEW_VERSION"] or effective.get("abi") != "1" or effective.get("cplusplus") != "201103L" or effective.get("compiler") != compiler:
            raise ValueError("Effective compiler contract differs")
        if effective.get("glibcxx_header_version") != macros.get("__GLIBCXX__") or not re.fullmatch(r"[0-9a-f]{64}", effective.get("libstdcxx_sha256", "")):
            raise ValueError("Effective standard-library identity differs")
        probes[compiler] = effective
        toolchains[compiler] = (folder / "gcc-toolchain-root.txt").read_text().strip()
        installations[compiler] = (folder / "gcc-install-dir.txt").read_text().strip()
        libgcc = Path((folder / "gcc-libgcc-file.txt").read_text().strip())
        if not libgcc.is_absolute() or str(libgcc.parent) != installations[compiler]:
            raise ValueError("Recorded GCC installation differs from driver libgcc")
        configuration = json.loads((folder / "default-config.json").read_text())
        if configuration.get("compiler") != compiler or configuration.get("disabled_in_actual_argv") != (compiler == "clang"):
            raise ValueError("Missing actual automatic-configuration provenance")
        for item in configuration["automatic_configs"]:
            retained = item["retained"]
            if not re.fullmatch(r"default-config-[0-9]+\.cfg", retained) or not Path(item["path"]).is_absolute() or digest(folder / retained) != item["sha256"]:
                raise ValueError("Automatic compiler configuration identity differs")
        common[compiler] = common_decoder_arguments(effective, toolchains[compiler], installations[compiler])
    if any(probes["gcc"][key] != probes["clang"][key] for key in ("libstdcxx", "libstdcxx_sha256", "glibcxx_header_version", "glibcxx_config_header", "glibcxx_config_sha256", "abi_features")):
        raise ValueError("Compiler builds use different standard libraries")
    if toolchains["gcc"] != toolchains["clang"] or installations["gcc"] != installations["clang"] or common["gcc"] != common["clang"] or probes["gcc"]["entry"]["directory"] != probes["clang"]["entry"]["directory"]:
        raise ValueError("Actual common decoder arguments differ beyond compiler/toolchain selection")
    probes["verified_common_decoder_argv"] = common["gcc"]
    save(output / "verified-compiler-probes.json", probes)


def common_decoder_arguments(effective, toolchain, installation):
    """Normalize only exact recorded Clang configuration and GCC installation selectors."""
    return EFFECTIVE["common_arguments"](effective["probe_argv"], effective["compiler"], toolchain, installation)


def verify_smoke(evidence):
    """Require all exact smoke slots and actual mapped/native-JAR hash agreement before success."""
    root = evidence / "smoke"
    with (root / "queries.csv").open() as source:
        rows = list(csv.DictReader(source))
    if len(rows) != len(SMOKES) or {row["query"] for row in rows} != SMOKES:
        raise ValueError("Missing or duplicate native smoke attempts")
    for row in rows:
        if row["ok"] != "1" or row["error_type"] or row["checksum_version"] != "tpch-v2-sha256-64-numeric2":
            raise ValueError("Failed or incompatible smoke answer")
        if (row["query"] != "loaded_native" and int(row["rows"]) < 1) or int(row["latency_ns"]) < 0:
            raise ValueError("Missing smoke metrics")
        if row["query"] in ANSWERS and (int(row["rows"]), row["checksum"]) != ANSWERS[row["query"]]:
            raise ValueError("Smoke answer differs from fixed canonical oracle")
        if row["query"] in ("arrow_types", "snappy_arrow", "native_extensions"):
            if int(row["arrow_bytes"]) <= 0 or int(row["batches"]) <= 0:
                raise ValueError("Missing actual Arrow buffer metrics")
    native_hash = digest(evidence / "libduckdb_java.so_linux_amd64")
    identity = json.loads((root / "loaded-native.json").read_text())
    if identity.get("sha256") != native_hash or identity.get("expected_sha256") != native_hash:
        raise ValueError("Actual loaded native library differs")
    runtime = json.loads((root / "libstdcxx-runtime.json").read_text())
    if not re.fullmatch(r"[0-9a-f]{64}", runtime.get("sha256", "")):
        raise ValueError("Actual mapped standard-library identity is missing")
    cancelled = json.loads((root / "cancellation-attempt.json").read_text())
    if cancelled.get("interrupted") is not True or cancelled.get("rows") != -1 or cancelled.get("elapsed_ns", -1) < 0:
        raise ValueError("Actual cancellation attempt is unproven")
    readiness = json.loads((root / "cancel-readiness.json").read_text())
    if readiness.get("method") == "query_progress":
        if readiness.get("rows_processed", 0) <= 0 or readiness.get("total_rows", 0) <= 0 or not 0 <= readiness.get("percentage", -1) < 100:
            raise ValueError("Native progress readiness was not observed")
    elif readiness.get("method") == "native_execute_thread_cpu":
        before, after = readiness.get("thread_cpu_start_ns", -1), readiness.get("thread_cpu_end_ns", -1)
        if before < 0 or after - before < 1_000_000 or readiness.get("task_incomplete") is not True or readiness.get("native_method") != "duckdb_jdbc_execute" or readiness.get("cpu_start_epoch_ms", -1) < 0 or readiness.get("epoch_ms", -1) < readiness.get("cpu_start_epoch_ms", 0):
            raise ValueError("Native execute CPU readiness was not observed")
        for stage in ("before", "after"):
            if "org.duckdb.DuckDBNative.duckdb_jdbc_execute(Native Method)" not in (root / ("cancel-stack-" + stage + ".txt")).read_text():
                raise ValueError("Missing actual native execution stack")
    else:
        raise ValueError("Unrecognized cancellation readiness proof")
    requested = json.loads((root / "cancel-request.json").read_text())
    terminated = json.loads((root / "cancel-termination.json").read_text())
    if requested.get("epoch_ms", 0) < readiness.get("epoch_ms", 1) or terminated.get("terminated") is not True or terminated.get("epoch_ms", 0) < requested.get("epoch_ms", 1):
        raise ValueError("Cancellation/termination ordering is unproven")
    native_limit_rows(root / "native-limits.tsv")
    extensions = json.loads((evidence.parent / "common/shared-extensions.json").read_text())
    for name in ("httpfs", "iceberg"):
        loaded = json.loads((root / (name + "-loaded.json")).read_text())
        if loaded.get("sha256") != extensions[name]["sha256"]:
            raise ValueError("Mapped extension differs from shared signed byte set")
    verify_upstream(root)
    with zipfile.ZipFile(evidence / "duckdb_jdbc.jar") as archive:
        with archive.open("libduckdb_java.so_linux_amd64") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != native_hash:
                raise ValueError("JAR/native artifact mismatch")
    save(root / "verified.json", {"ok": True, "smoke_slots": sorted(SMOKES), "mapped_native_sha256": native_hash})


def seal(output, name="SHA256SUMS"):
    """Checksum partial artifacts too, without editing earlier evidence."""
    if output.is_dir():
        with tempfile.NamedTemporaryFile("w", dir=output.parent, prefix="native-checksums-") as stream:
            for path in sorted(output.rglob("*")):
                if path.is_file() and path.name != name:
                    stream.write(digest(path) + "  " + str(path.relative_to(output)) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.link(stream.name, output / name)


def main():
    """Prepare exact public sources and run two isolated cloud-only build/smoke candidates."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--mode", choices=("preflight", "build-smoke"), default="preflight")
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("Heavy native builds run only in explicitly dispatched GitHub Actions")
    if os.getuid() == 0:
        raise RuntimeError("Use the non-root GitHub runner identity")
    signal.signal(signal.SIGTERM, interrupted)
    output, work = args.output.resolve(), args.work.resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        work.mkdir(parents=True, exist_ok=False)
        save(output / "plan.json", {"jdbc_commit": JDBC, "core_commit": CORE, "base_image": BASE, "flags": FLAGS,
             "mode": args.mode,
             "stock_reference": {"jar_sha256": STOCK_JAR, "native_sha256": STOCK_NATIVE, "download_mode": "build-smoke"},
             "scope": "build and correctness smoke only; no benchmark, ranking, deployment or dependency change",
             "compilers": ["gcc", "clang"], "build_parallelism": 2, "build_memory_bytes": 10 * 1024**3})
        for repo, ref, expected in (("duckdb/duckdb-java", "v1.5.6.0", JDBC), ("duckdb/duckdb", "v1.5.6", CORE)):
            target = output / (repo.split("/")[-1] + "-api.json")
            download(f"https://api.github.com/repos/{repo}/commits/{ref}", target)
            if json.loads(target.read_text())["sha"] != expected:
                raise ValueError("Upstream tag identity differs")
        archive = output / "jdbc-source.tar.gz"
        download(f"https://codeload.github.com/duckdb/duckdb-java/tar.gz/{JDBC}", archive)
        pristine = unpack(archive, work / "unpacked")
        save(output / "source-version-manifest.json", verify_source(pristine))
        SYSTEM["save_host_snapshot"](output / "host-prepare.json", output)
        run(["lscpu"], output / "hardware")
        image = "native-compiler-toolbox:" + str(os.getpid())
        dockerfile = work / "Dockerfile"
        dockerfile.write_text(f"FROM {BASE}\nRUN dnf install -y clang llvm java-1.8.0-openjdk-devel ninja-build ccache time patch binutils && dnf clean all\n")
        (work / ".dockerignore").write_text("*\n!Dockerfile\n")
        shutil.copyfile(dockerfile, output / "Dockerfile")
        run(["docker", "build", "--progress=plain", "-t", image, "-f", str(dockerfile), str(work)], output / "toolbox-build", timeout=900)
        run(["docker", "image", "inspect", "--format", "{{.Id}}", image], output / "toolbox-image")
        image = (output / "toolbox-image.log").read_text().strip()
        prepare_identity(image, work, output)
        failed = []
        for compiler in ("gcc", "clang"):
            try:
                candidate_phase(compiler, image, pristine, work, output, "configure")
            except Exception as error:
                failed.append(compiler)
                save(output / (compiler + "-configure-failure.json"), {"error_type": type(error).__name__, "message": str(error)})
        if failed:
            raise RuntimeError("Configure/preprocessor preflight failed: " + ",".join(failed))
        check_probes(output)
        save(output / "preflight-result.json", {"ok": True, "stage": "preflight", "epoch_ns": time.time_ns(), "native_engine_compiled": False,
             "scope": "Both source adaptations/configurations and actual decoder compiler contracts verified before build"})
        if args.mode == "preflight":
            save(output / "result.json", {"ok": True, "mode": args.mode, "native_engine_compiled": False, "smokes_run": False})
            return
        stock_reference(output)
        for compiler in ("gcc", "clang"):
            try:
                candidate_phase(compiler, image, pristine, work, output, "build")
            except Exception as error:
                failed.append(compiler)
                save(output / (compiler + "-build-failure.json"), {"error_type": type(error).__name__, "message": str(error)})
        if failed:
            raise RuntimeError("Native builds failed: " + ",".join(failed))
        common = output / "common"
        common.mkdir()
        shared_extensions(common)
        shutil.copyfile(output / "gcc/duckdb_jdbc_tests.jar", common / "upstream-tests.jar")
        corpus_sha = None
        for compiler in ("gcc", "stock", "clang"):
            try:
                run(["bash", str(HERE / "native-compiler/smoke.sh"), str(output / compiler), str(common / "fixture.parquet"),
                     str(common / "extensions"), str(common / "upstream-tests.jar")], output / (compiler + "-smoke"), timeout=600)
                verify_smoke(output / compiler)
                current = digest(common / "fixture.parquet")
                if compiler != "gcc" and current != corpus_sha:
                    raise ValueError("Candidate changed the common baseline-written corpus")
                shared = json.loads((common / "shared-extensions.json").read_text())
                if any(digest(common / "extensions" / (name + ".duckdb_extension")) != shared[name]["sha256"]
                       for name in ("httpfs", "iceberg")):
                    raise ValueError("Shared signed extension bytes changed during smoke")
            except Exception as error:
                failed.append(compiler)
                save(output / (compiler + "-smoke-failure.json"), {"error_type": type(error).__name__, "message": str(error)})
            finally:
                if compiler == "gcc" and (common / "fixture.parquet").is_file():
                    corpus_sha = digest(common / "fixture.parquet")
                    save(common / "input-identity.json", {"writer": "gcc", "sha256": corpus_sha,
                         "bytes": (common / "fixture.parquet").stat().st_size, "readers": ["gcc", "stock", "clang"]})
        if failed:
            save(output / "result.json", {"ok": False, "failed": failed, "scope": "correctness smoke; no performance conclusion"})
            raise RuntimeError("Native smoke failed; all earlier outputs retained")
        runtimes = {compiler: json.loads((output / compiler / "smoke/libstdcxx-runtime.json").read_text())
                    for compiler in ("gcc", "stock", "clang")}
        if any(runtimes["gcc"]["sha256"] != runtimes[name]["sha256"] for name in ("stock", "clang")):
            raise ValueError("Rebuilt candidates loaded different libstdc++ runtimes")
        save(output / "verified-runtime.json", runtimes)
        save(output / "result.json", {"ok": True, "mode": args.mode, "variants": ["gcc", "stock", "clang"],
             "native_engine_compiled": True, "smokes_run": True, "scope": "correctness smoke; no performance conclusion"})
    except BaseException as error:
        save(output / "failure.json", {"error_type": type(error).__name__, "message": str(error)})
        raise
    finally:
        seal(output, "RAW-SHA256SUMS")


def interrupted(signum, frame):
    """Unwind task-owned processes and preserve partial artifacts on workflow termination."""
    raise KeyboardInterrupt("Native build interrupted")


if __name__ == "__main__":
    main()
