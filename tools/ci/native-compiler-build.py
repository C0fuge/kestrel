#!/usr/bin/env python3
"""Build isolated, portable JDBC compiler candidates; never rank or deploy them."""

import argparse
import concurrent.futures
import csv
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
STOCK_NATIVE = "b36f739cca01c475867e332e496da8c95bcac37478d55f40ac9b3149e30f285c"
STOCK_JAR = "04ae3375d7c40dc88dc9bf0014b82ba5e66715564584af4e3abea39c7ba144d3"
SMOKES = {"loaded_native", "source_version", "jdbc_types", "arrow_types", "snappy_jdbc",
          "snappy_arrow", "cancel_and_reuse", "native_extensions"}
ANSWERS = {"jdbc_types": (1, "3d935bc5ed35e2e0"), "arrow_types": (1, "3d935bc5ed35e2e0"),
           "snappy_jdbc": (128, "d814e1d9a8012e52"), "snappy_arrow": (128, "d814e1d9a8012e52"),
           "native_extensions": (128, "d814e1d9a8012e52"), "source_version": (1, "9abf82c842a008b6"),
           "cancel_and_reuse": (1, "616fa64e259babae")}


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


def compiler_command(compiler, image, source, evidence, name):
    """Give both compilers identical limits, mounts, ISA, standard-library ABI and release flags."""
    if compiler not in ("gcc", "clang"):
        raise ValueError("Unknown compiler")
    return ["docker", "run", "--name", name, "--hostname", "native-screen", "--cpus=2", "--memory=10g",
            "--memory-swap=10g", "--pids-limit=1024", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "-v", f"{source}:/source", "-v", f"{evidence}:/evidence",
            "-v", f"{HERE / 'native-compiler'}:/control:ro", "-w", "/source", image,
            "bash", "/control/build.sh", compiler, FLAGS]


def build_one(compiler, image, pristine, work, output):
    """Build one task-owned native candidate while keeping resource and failure evidence."""
    evidence = output / compiler
    evidence.mkdir()
    source = work / compiler
    shutil.copytree(pristine, source)
    prepare_source(source, evidence)
    name = f"native-compiler-{uuid.uuid4().hex}-{compiler}"
    save(evidence / "owned-container.json", {"name": name})
    command = compiler_command(compiler, image, source, evidence, name)
    SYSTEM["save_host_snapshot"](evidence / "host-before.json", output)
    stop = False

    def sample():
        """Retain optional raw build-container cgroup counters without sampling query timings."""
        with (evidence / "resources.jsonl").open("x") as stream:
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
                run(command, evidence / "build", timeout=3900)
            finally:
                stop = True
                sampler.result(timeout=20)
    finally:
        SYSTEM["save_host_snapshot"](evidence / "host-after.json", output)
        state = SYSTEM["optional_command"](["docker", "inspect", "--format", "{{json .State}}", name], "build_container")
        save(evidence / "container-state.json", state)
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False)
    return evidence


def check_probes(output):
    """Reject a wrong decoder, raised ISA, different libstdc++ or incompatible declared ABI."""
    probes = {}
    for compiler in ("gcc", "clang"):
        folder = output / compiler
        macros = dict(re.findall(r"^#define (\w+) (.*)$", (folder / "macros.txt").read_text(), re.M))
        if macros.get("SNAPPY_NEW_VERSION") != ("false" if compiler == "gcc" else "true"):
            raise ValueError("Wrong decoder selection: " + compiler)
        if macros.get("_GLIBCXX_USE_CXX11_ABI") != "1" or macros.get("__cplusplus") != "201103L":
            raise ValueError("Wrong C++ standard/ABI: " + compiler)
        if any(name in macros for name in ("__AVX__", "__AVX2__", "__BMI2__", "__SSSE3__")):
            raise ValueError("ISA exceeds portable x86-64 baseline")
        commands = json.loads((folder / "compile_commands.json").read_text())
        decoder = [item for item in commands if item["file"].endswith("/third_party/snappy/snappy.cc")]
        if len(decoder) != 1:
            raise ValueError("Actual decoder compilation is unproven")
        flags = decoder[0].get("arguments") or shlex.split(decoder[0]["command"])
        if Path(flags[0]).name != ("g++" if compiler == "gcc" else "clang++"):
            raise ValueError("Actual decoder compiler differs from the probe")
        if any(flag not in flags for flag in shlex.split(FLAGS) + ["-O3", "-DNDEBUG", "-std=gnu++11"]):
            raise ValueError("Actual decoder compile flags differ")
        if any("SNAPPY_NEW_VERSION" in flag or "-march=native" in flag for flag in flags):
            raise ValueError("Forced decoder or native ISA is forbidden")
        probes[compiler] = {"decoder": macros["SNAPPY_NEW_VERSION"], "libstdcxx": (folder / "libstdcxx.txt").read_text().strip(),
                            "glibcxx_header_version": macros.get("__GLIBCXX__"), "abi": macros["_GLIBCXX_USE_CXX11_ABI"],
                            "actual_decoder_compile": decoder[0]}
    if probes["gcc"]["libstdcxx"] != probes["clang"]["libstdcxx"] or probes["gcc"]["glibcxx_header_version"] != probes["clang"]["glibcxx_header_version"]:
        raise ValueError("Compiler builds use different standard libraries")
    save(output / "verified-compiler-probes.json", probes)


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
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("Heavy native builds run only in explicitly dispatched GitHub Actions")
    signal.signal(signal.SIGTERM, interrupted)
    output, work = args.output.resolve(), args.work.resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        work.mkdir(parents=True, exist_ok=False)
        save(output / "plan.json", {"jdbc_commit": JDBC, "core_commit": CORE, "base_image": BASE, "flags": FLAGS,
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
        stock_reference(output)
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
        failed = []
        for compiler in ("gcc", "clang"):
            try:
                build_one(compiler, image, pristine, work, output)
            except Exception as error:
                failed.append(compiler)
                save(output / (compiler + "-failure.json"), {"error_type": type(error).__name__, "message": str(error)})
        if failed:
            raise RuntimeError("Native builds failed: " + ",".join(failed))
        check_probes(output)
        common = output / "common"
        common.mkdir()
        corpus_sha = None
        for compiler in ("gcc", "clang"):
            try:
                run(["bash", str(HERE / "native-compiler/smoke.sh"), str(output / compiler), str(common / "fixture.parquet")], output / (compiler + "-smoke"), timeout=600)
                verify_smoke(output / compiler)
                current = digest(common / "fixture.parquet")
                if compiler == "clang" and current != corpus_sha:
                    raise ValueError("Candidate changed the common baseline-written corpus")
            except Exception as error:
                failed.append(compiler)
                save(output / (compiler + "-smoke-failure.json"), {"error_type": type(error).__name__, "message": str(error)})
            finally:
                if compiler == "gcc" and (common / "fixture.parquet").is_file():
                    corpus_sha = digest(common / "fixture.parquet")
                    save(common / "input-identity.json", {"writer": "gcc", "sha256": corpus_sha,
                         "bytes": (common / "fixture.parquet").stat().st_size, "readers": ["gcc", "clang"]})
        if failed:
            save(output / "result.json", {"ok": False, "failed": failed, "scope": "correctness smoke; no performance conclusion"})
            raise RuntimeError("Native smoke failed; all earlier outputs retained")
        runtimes = {compiler: json.loads((output / compiler / "smoke/libstdcxx-runtime.json").read_text())
                    for compiler in ("gcc", "clang")}
        if runtimes["gcc"]["sha256"] != runtimes["clang"]["sha256"]:
            raise ValueError("Rebuilt candidates loaded different libstdc++ runtimes")
        save(output / "verified-runtime.json", runtimes)
        save(output / "result.json", {"ok": True, "scope": "correctness smoke; no performance conclusion"})
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
