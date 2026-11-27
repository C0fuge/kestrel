"""Check build identity and failure preservation without Docker, downloads or compilation."""

import importlib.util
import io
import csv
import json
import subprocess
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
import zipfile
from unittest import mock


SPEC = importlib.util.spec_from_file_location("native_compiler_build", Path(__file__).with_name("native-compiler-build.py"))
BUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILD)


class NativeCompilerBuildTest(unittest.TestCase):
    """Require exact provenance, safe ISA and immutable raw failures before a cloud build."""

    def test_both_container_commands_differ_only_by_compiler_selection(self):
        """Both builds use one image and the same ISA, source, evidence and resource contract."""
        gcc = BUILD.compiler_command("gcc", "sha256:test", Path("source"), Path("raw"), "owned")
        clang = BUILD.compiler_command("clang", "sha256:test", Path("source"), Path("raw"), "owned")
        self.assertEqual([x for x in gcc if x != "gcc"], [x for x in clang if x != "clang"])
        self.assertIn("--memory=10g", gcc)
        self.assertIn("--cpus=2", gcc)
        self.assertIn("-march=x86-64", gcc[-2])
        self.assertEqual(f"{BUILD.os.getuid()}:{BUILD.os.getgid()}", gcc[gcc.index("--user") + 1])
        self.assertIn("HOME=/tmp/native-home", gcc)
        self.assertIn("--network=none", gcc)
        self.assertTrue(any(value.endswith(":/etc/passwd:ro") for value in gcc))
        self.assertNotIn("-march=native", " ".join(gcc))
        self.assertNotIn("SNAPPY_NEW_VERSION=", " ".join(gcc))
        with self.assertRaises(ValueError):
            BUILD.compiler_command("unknown", "image", Path("s"), Path("o"), "owned")

    def test_unpack_rejects_traversal_and_links_before_extracting_anything(self):
        """An archive cannot modify files outside the fresh source tree."""
        for name, kind in (("root/../escaped", tarfile.REGTYPE), ("/escaped", tarfile.REGTYPE),
                           ("root/link", tarfile.SYMTYPE)):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                with tarfile.open(root / "source.tar", "w") as archive:
                    info = tarfile.TarInfo(name)
                    info.type = kind
                    info.linkname = "../../escaped"
                    archive.addfile(info, io.BytesIO())
                with self.assertRaises(ValueError):
                    BUILD.unpack(root / "source.tar", root / "source")
                self.assertFalse((root / "escaped").exists())

    def test_source_identity_and_decoder_hash_are_required(self):
        """A version label alone cannot admit changed decoder sources."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            version = root / "src/duckdb/src/function/table/version/pragma_version.cpp"
            version.parent.mkdir(parents=True)
            version.write_text('#define DUCKDB_VERSION "v1.5.6"\n#define DUCKDB_SOURCE_ID "069cc9f9b5"\n')
            decoder = root / "src/duckdb/third_party/snappy"
            decoder.mkdir(parents=True)
            for name in BUILD.SNAPPY:
                (decoder / name).write_text("changed")
            with self.assertRaisesRegex(ValueError, "decoder differs"):
                BUILD.verify_source(root)
            version.write_text("wrong source version")
            with self.assertRaisesRegex(ValueError, "identity differs"):
                BUILD.verify_source(root)

    def probes(self, root):
        """Write minimal complete compiler evidence for contract mutation tests."""
        for compiler in ("gcc", "clang"):
            folder = root / compiler
            folder.mkdir()
            (folder / "macros.txt").write_text("#define SNAPPY_NEW_VERSION " +
                ("false" if compiler == "gcc" else "true") + "\n#define __cplusplus 201103L\n"
                "#define _GLIBCXX_USE_CXX11_ABI 1\n#define __GLIBCXX__ 20260101\n")
            (folder / "libstdcxx.txt").write_text("/portable/libstdc++.so\n")
            (folder / "gcc-toolchain-root.txt").write_text("/portable/toolchain\n")
            (folder / "gcc-install-dir.txt").write_text("/portable/toolchain/lib/gcc/14\n")
            (folder / "gcc-libgcc-file.txt").write_text("/portable/toolchain/lib/gcc/14/libgcc.a\n")
            BUILD.save(folder / "default-config.json", {"compiler": compiler, "automatic_configs": [],
                       "disabled_in_actual_argv": compiler == "clang"})
            entry = {
                "file": "/source/src/duckdb/third_party/snappy/snappy.cc",
                "directory": "/source/build/release",
                "command": ("g++" if compiler == "gcc" else "clang++") + " " + BUILD.FLAGS
                           + (" --no-default-config --gcc-install-dir=/portable/toolchain/lib/gcc/14 --gcc-toolchain=/portable/toolchain"
                              if compiler == "clang" else "")
                           + " -O3 -DNDEBUG -std=gnu++11 -c /source/src/duckdb/third_party/snappy/snappy.cc -o snappy.o"}
            (folder / "compile_commands.json").write_text(json.dumps([entry]))
            BUILD.save(folder / "effective.json", {"compiler": compiler, "entry": entry,
                "probe_argv": BUILD.EFFECTIVE["command"](entry, compiler),
                "compile_database_sha256": BUILD.digest(folder / "compile_commands.json"),
                "decoder": "false" if compiler == "gcc" else "true", "abi": "1", "cplusplus": "201103L",
                "libstdcxx": "/portable/libstdc++.so", "libstdcxx_sha256": "a" * 64,
                "glibcxx_header_version": "20260101", "glibcxx_config_header": "/portable/bits/c++config.h",
                "glibcxx_config_sha256": "b" * 64, "abi_features": {}})

    def test_probe_contract_rejects_wrong_decoder_isa_library_and_compile_flags(self):
        """Actual decoder flags and shared standard-library provenance must match their probes."""
        for mutation in ("decoder", "isa", "library", "compile"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.probes(root)
                if mutation == "decoder":
                    path = root / "gcc/macros.txt"
                    path.write_text(path.read_text().replace("false", "true"))
                elif mutation == "isa":
                    with (root / "clang/macros.txt").open("a") as stream:
                        stream.write("#define __AVX2__ 1\n")
                elif mutation == "library":
                    path = root / "clang/effective.json"
                    path.write_text(path.read_text().replace("/portable/libstdc++.so", "/other/library"))
                else:
                    path = root / "clang/compile_commands.json"
                    path.write_text(path.read_text().replace("-march=x86-64", "-march=native"))
                with self.assertRaises(ValueError):
                    BUILD.check_probes(root)
                self.assertFalse((root / "verified-compiler-probes.json").exists())

    def test_valid_probes_preserve_actual_compilation_commands(self):
        """A successful probe retains the evidence used by the gate."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.probes(root)
            BUILD.check_probes(root)
            result = json.loads((root / "verified-compiler-probes.json").read_text())
            self.assertEqual("true", result["clang"]["decoder"])
            self.assertIn("entry", result["gcc"])

    def update_actual_flags(self, root, compiler, extra):
        """Keep mutated compile argv and its derived evidence mutually consistent."""
        folder = root / compiler
        path = folder / "compile_commands.json"
        entries = json.loads(path.read_text())
        entries[0]["command"] += " " + extra
        path.write_text(json.dumps(entries))
        proof_path = folder / "effective.json"
        proof = json.loads(proof_path.read_text())
        proof["entry"] = entries[0]
        proof["probe_argv"] = BUILD.EFFECTIVE["command"](entries[0], compiler)
        proof["compile_database_sha256"] = BUILD.digest(path)
        proof_path.write_text(json.dumps(proof))

    def test_exact_common_argv_rejects_one_sided_nonmacro_optimization_options(self):
        """Matching macros cannot hide otherwise consistent one-sided compiler options."""
        for extra in ("-fno-inline", "-fno-tree-vectorize", "--param=max-inline-insns-single=0"):
            for compiler in ("gcc", "clang"):
                with self.subTest(extra=extra, compiler=compiler), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    self.probes(root)
                    self.update_actual_flags(root, compiler, extra)
                    with self.assertRaisesRegex(ValueError, "common decoder arguments differ"):
                        BUILD.check_probes(root)
                    self.assertFalse((root / "verified-compiler-probes.json").exists())

    def test_exact_clang_configuration_selectors_are_required(self):
        """Missing, duplicate or redirected configuration selectors cannot normalize away."""
        changes = (("--no-default-config", ""),
                   ("--no-default-config", "--no-default-config --no-default-config"),
                   ("--gcc-install-dir=/portable/toolchain/lib/gcc/14", "--gcc-install-dir=/other/15"),
                   ("--gcc-install-dir=/portable/toolchain/lib/gcc/14", ""),
                   ("--gcc-toolchain=/portable/toolchain", "--gcc-toolchain=/other"),
                   ("--gcc-toolchain=/portable/toolchain", ""),
                   ("-O3", "-O3 --config=/other.cfg"),
                   ("-O3", "-O3 --config-system-dir=/other"),
                   ("-O3", "-O3 --gcc-install-dir=/other/15"))
        for old, new in changes:
            with self.subTest(new=new), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.probes(root)
                folder = root / "clang"
                path = folder / "compile_commands.json"
                path.write_text(path.read_text().replace(old, new))
                entry = json.loads(path.read_text())[0]
                proof_path = folder / "effective.json"
                proof = json.loads(proof_path.read_text())
                proof.update(entry=entry, probe_argv=BUILD.EFFECTIVE["command"](entry, "clang"),
                             compile_database_sha256=BUILD.digest(path))
                proof_path.write_text(json.dumps(proof))
                with self.assertRaisesRegex(ValueError, "configuration/toolchain selector"):
                    BUILD.check_probes(root)
                self.assertFalse((root / "verified-compiler-probes.json").exists())

    def test_default_configuration_contents_are_hashed_and_required(self):
        """Automatic configuration provenance is retained even though actual compilation disables it."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "automatic.cfg"
            config.write_text("--gcc-install-dir=/other/15\n")
            retain = BUILD.EFFECTIVE["retain_default_config"]
            with mock.patch.dict(retain.__globals__, {"capture": lambda *args:
                    ("Configuration file: " + str(config) + "\n", "")}):
                retain(["clang++", "--no-default-config"], "clang", str(root), root)
            metadata = json.loads((root / "default-config.json").read_text())
            self.assertTrue(metadata["disabled_in_actual_argv"])
            item = metadata["automatic_configs"][0]
            self.assertEqual(config.read_bytes(), (root / item["retained"]).read_bytes())
            self.assertEqual(BUILD.digest(config), item["sha256"])
        for field in ("disabled_in_actual_argv", "config_hash", "installation"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.probes(root)
                folder = root / "clang"
                path = folder / "default-config.json"
                value = json.loads(path.read_text())
                if field == "disabled_in_actual_argv":
                    value[field] = False
                elif field == "config_hash":
                    (folder / "default-config-0.cfg").write_text("original")
                    value["automatic_configs"] = [{"retained": "default-config-0.cfg", "path": "/etc/clang/driver.cfg", "sha256": "f" * 64}]
                else:
                    (folder / "gcc-libgcc-file.txt").write_text("/other/15/libgcc.a")
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    BUILD.check_probes(root)

    def test_actual_compile_argv_rejects_all_late_conflicting_overrides(self):
        """Fixed standalone macros cannot hide overrides in the actual decoder compilation."""
        injected = ("-mavx2", "-march=x86-64-v3", "-march=haswell", "-msse4.2", "-mbmi2",
                    "-D_GLIBCXX_USE_CXX11_ABI=0", "-U_GLIBCXX_USE_CXX11_ABI", "-std=gnu++17", "-O0",
                    "-Ofast", "-DSNAPPY_NEW_VERSION=true", "-stdlib=libc++", "@hidden-flags",
                    "-D _GLIBCXX_USE_CXX11_ABI=0", "-fabi-version=0", "-Xclang -target-cpu haswell")
        for compiler in ("gcc", "clang"):
            for extra in injected:
                with self.subTest(compiler=compiler, extra=extra):
                    source = "/source/third_party/snappy/snappy.cc"
                    argv = [("g++" if compiler == "gcc" else "clang++"), *BUILD.shlex.split(BUILD.FLAGS),
                            "-O3", "-DNDEBUG", "-std=gnu++11", "-c", source, *BUILD.shlex.split(extra), "-o", "out.o"]
                    with self.assertRaises(ValueError):
                        BUILD.EFFECTIVE["command"]({"arguments": argv, "file": source}, compiler)

    def test_preprocessing_uses_the_actual_source_and_cmake_flags(self):
        """The effective probe cannot silently substitute a standalone fixed-flags translation unit."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            library = root / "libstdc++.so"
            library.write_bytes(b"shared standard library")
            header = root / "bits/c++config.h"
            header.parent.mkdir()
            header.write_text("shared standard library configuration")
            source = "/source/third_party/snappy/snappy.cc"
            entry = {"file": source, "directory": "/source/build/release", "arguments":
                ["g++", *BUILD.shlex.split(BUILD.FLAGS), "-O3", "-DNDEBUG", "-std=gnu++11",
                 "-DACTUAL_CMAKE_SENTINEL=1", "-c", source, "-o", "snappy.o"]}
            (root / "gcc-toolchain-root.txt").write_text("/portable/toolchain\n")
            (root / "gcc-install-dir.txt").write_text("/portable/toolchain/lib/gcc/14\n")
            database = root / "compile_commands.json"
            database.write_text(json.dumps([entry]))
            calls = []
            def capture(argv, cwd, path):
                calls.append(argv)
                if path.name == "macros.txt":
                    return ("#define SNAPPY_NEW_VERSION false\n#define __cplusplus 201103L\n"
                            "#define _GLIBCXX_USE_CXX11_ABI 1\n#define __GLIBCXX__ 20260101\n"
                            "#define __x86_64__ 1\n#define NDEBUG 1\n", "")
                if path.name == "include-trace.txt":
                    return "", ". " + str(header) + "\n"
                return (str(library) if path.name == "libstdcxx.txt" else "", "")
            probe = BUILD.EFFECTIVE["probe"]
            with mock.patch.dict(probe.__globals__, {"capture": capture}):
                probe(database, "gcc", root)
            actual = next(argv for argv in calls if "-dM" in argv)
            self.assertIn(source, actual)
            self.assertIn("-DACTUAL_CMAKE_SENTINEL=1", actual)
            self.assertNotIn("-c", actual)
            self.assertNotIn("snappy.o", actual)
            self.assertEqual(BUILD.digest(database), json.loads((root / "effective.json").read_text())["compile_database_sha256"])

    def cgroup_fixture(self, root):
        """Create complete cgroup-v2 counters with the declared container limits."""
        cgroup = root / "cgroup"
        cgroup.mkdir()
        values = {"cpu.stat": "usage_usec 3\nuser_usec 2\nsystem_usec 1\n",
                  "cpu.max": "200000 100000\n", "memory.current": "1024\n", "memory.peak": "2048\n",
                  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
                  "memory.max": str(10 * 1024 ** 3), "memory.swap.max": "0\n",
                  "io.stat": "8:0 rbytes=1 wbytes=2 rios=1 wios=1\n", "pids.max": "1024\n"}
        for name, value in values.items():
            (cgroup / name).write_text(value)
        return cgroup

    def phase_snapshots(self, root, cgroup):
        """Capture real file reads for both edges without a sampler or container startup race."""
        for edge in ("before", "exit"):
            self.assertEqual(0, BUILD.RESOURCES["snapshot"](root, "configure", edge,
                             None if edge == "before" else 0, cgroup))
        for suffix in ("recipe", "retention"):
            (root / ("configure-" + suffix + "-exit-status.txt")).write_text("0\n")

    def test_short_phase_requires_both_actual_cgroup_edges_and_limits(self):
        """An immediate phase has verifiable before/EXIT snapshots independent of periodic sampling."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.phase_snapshots(root, self.cgroup_fixture(root))
            result = BUILD.RESOURCES["verify"](root, "configure")
            self.assertEqual(0, result["status"])
            self.assertEqual(["before", "exit"], result["edges"])
            with self.assertRaises(FileExistsError):
                BUILD.RESOURCES["snapshot"](root, "configure", "before")

    def test_cgroup_snapshot_read_failures_retain_every_status_and_original_failure(self):
        """Unavailable and unreadable counters leave complete framed evidence and a failing status."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cgroup = self.cgroup_fixture(root)
            (cgroup / "memory.peak").unlink()
            (cgroup / "cpu.stat").unlink()
            (cgroup / "cpu.stat").mkdir()
            self.assertEqual(1, BUILD.RESOURCES["snapshot"](root, "build", "exit", 7, cgroup))
            result = json.loads((root / "build-cgroup-exit.json").read_text())
            self.assertEqual(7, result["recipe_status"])
            self.assertEqual(set(BUILD.RESOURCES["FILES"]), set(result["files"]))
            self.assertEqual("FileNotFoundError", result["files"]["memory.peak"]["error_type"])
            self.assertEqual("IsADirectoryError", result["files"]["cpu.stat"]["error_type"])
            self.assertEqual(0, result["files"]["memory.current"]["status"])

    def test_cgroup_gate_rejects_status_missing_counters_wrong_limits_and_frame(self):
        """A successful shell exit cannot replace required individual reads, values or frame order."""
        for mutation in ("read", "cpu", "memory", "swap", "pids", "quota", "frame", "exit", "retention", "missing"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.phase_snapshots(root, self.cgroup_fixture(root))
                path = root / "configure-cgroup-exit.json"
                value = json.loads(path.read_text())
                if mutation == "read":
                    value["files"]["io.stat"]["status"] = 1
                elif mutation == "cpu":
                    value["files"]["cpu.stat"]["text"] = "usage_usec 0"
                elif mutation in ("memory", "swap", "pids", "quota"):
                    name = {"memory": "memory.max", "swap": "memory.swap.max", "pids": "pids.max", "quota": "cpu.max"}[mutation]
                    value["files"][name]["text"] = {"memory": "max", "swap": "1", "pids": "max", "quota": "400000 100000"}[mutation]
                elif mutation == "frame":
                    value["begin_epoch_ns"] = 0
                elif mutation == "exit":
                    value["recipe_status"] = 7
                elif mutation == "retention":
                    (root / "configure-retention-exit-status.txt").write_text("1")
                else:
                    del value["files"]["memory.peak"]
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    BUILD.RESOURCES["verify"](root, "configure")

    def test_exit_trap_preserves_original_failure_and_rejects_snapshot_failure(self):
        """Run the actual trap body and prove cleanup cannot replace an earlier nonzero status."""
        recipe = (BUILD.HERE / "native-compiler/build.sh").read_text()
        trap = recipe[recipe.index("retain() {"):recipe.index("trap retain EXIT")]
        command = '/opt/python/cp312-cp312/bin/python3 /control/resources.py /evidence "$phase" exit --recipe-status "$rc"'
        for original, snapshot, expected in ((7, 1, 7), (0, 1, 1), (0, 0, 0)):
            with self.subTest(original=original, snapshot=snapshot), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                body = trap.replace(command, "false" if snapshot else "true").replace("/evidence/", str(root) + "/")
                script = "set -euo pipefail\nphase=configure\nbuild=/nonexistent-native-test\n" + body + "trap retain EXIT\nexit " + str(original)
                result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
                self.assertEqual(expected, result.returncode, result.stderr)
                self.assertEqual(str(original), (root / "configure-recipe-exit-status.txt").read_text().strip())
                self.assertEqual(str(snapshot), (root / "configure-retention-exit-status.txt").read_text().strip())

    def test_failed_and_timed_out_commands_keep_output_and_original_status(self):
        """Partial command output remains auditable even after a real process timeout."""
        for code, timeout in ((7, 5), (124, .05)):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temp:
                prefix = Path(temp) / "attempt"
                program = ("print('partial',flush=True); " +
                           ("raise SystemExit(7)" if code == 7 else "import time;time.sleep(30)"))
                with self.assertRaises(RuntimeError):
                    BUILD.run([sys.executable, "-c", program], prefix, timeout=timeout)
                self.assertIn("partial", prefix.with_suffix(".log").read_text())
                self.assertEqual(code, json.loads(prefix.with_suffix(".status.json").read_text())["status"])
                with self.assertRaises(FileExistsError):
                    BUILD.run([sys.executable, "-c", "pass"], prefix)

    def test_sealing_preserves_partial_artifacts_and_rejects_reuse(self):
        """A failed attempt is hashed without replacing prior evidence."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "failure.log").write_text("original failure")
            BUILD.seal(root)
            self.assertIn(BUILD.digest(root / "failure.log"), (root / "SHA256SUMS").read_text())
            with self.assertRaises(FileExistsError):
                BUILD.seal(root)
            self.assertEqual("original failure", (root / "failure.log").read_text())

    def test_interrupted_checksum_collection_cannot_publish_a_partial_manifest(self):
        """A failed hash pass leaves original artifacts and no falsely complete SHA256SUMS."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "raw"
            root.mkdir()
            (root / "failure.log").write_text("preserved")
            with mock.patch.object(BUILD, "digest", side_effect=OSError("interrupted read")):
                with self.assertRaises(OSError):
                    BUILD.seal(root)
            self.assertFalse((root / "SHA256SUMS").exists())
            self.assertEqual("preserved", (root / "failure.log").read_text())

    def test_main_rejects_local_heavy_build_before_creating_output(self):
        """Focused checks cannot accidentally download or start a local native build."""
        with tempfile.TemporaryDirectory() as temp, mock.patch.dict(BUILD.os.environ, {}, clear=True), \
                mock.patch.object(sys, "argv", ["native", "--output", temp + "/raw", "--work", temp + "/work"]):
            with self.assertRaisesRegex(RuntimeError, "only.*GitHub"):
                BUILD.main()
            self.assertFalse((Path(temp) / "raw").exists())

    def test_cloud_root_identity_is_rejected_before_output_or_downloads(self):
        """A cloud flag cannot revive the root-without-DAC mount failure."""
        with tempfile.TemporaryDirectory() as temp, mock.patch.dict(BUILD.os.environ, {"GITHUB_ACTIONS": "true"}), \
                mock.patch.object(BUILD.os, "getuid", return_value=0), \
                mock.patch.object(sys, "argv", ["native", "--output", temp + "/raw", "--work", temp + "/work"]), \
                mock.patch.object(BUILD, "download") as download:
            with self.assertRaisesRegex(RuntimeError, "non-root"):
                BUILD.main()
            download.assert_not_called()
            self.assertFalse((Path(temp) / "raw").exists())

    def test_default_preflight_and_failed_contract_never_start_native_builds(self):
        """Both candidates must configure and pass the gate before any explicit build mode can compile."""
        for mode, gate_error in ((None, None), ("build-smoke", ValueError("unverified actual flags"))):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                output, work = root / "raw", root / "work"
                argv = ["native", "--output", str(output), "--work", str(work)]
                if mode:
                    argv += ["--mode", mode]
                def download(url, path):
                    path.write_text(json.dumps({"sha": BUILD.JDBC if "duckdb-java/commits" in url else BUILD.CORE})
                                    if "/commits/" in url else "source archive fixture")
                def run(command, prefix, **kwargs):
                    prefix.with_suffix(".log").write_text("sha256:fixture-image\n")
                with mock.patch.dict(BUILD.os.environ, {"GITHUB_ACTIONS": "true"}), \
                        mock.patch.object(BUILD.os, "getuid", return_value=1000), \
                        mock.patch.object(BUILD.signal, "signal"), mock.patch.object(sys, "argv", argv), \
                        mock.patch.object(BUILD, "download", side_effect=download), \
                        mock.patch.object(BUILD, "unpack", return_value=root / "pristine"), \
                        mock.patch.object(BUILD, "verify_source", return_value={"verified": True}), \
                        mock.patch.object(BUILD, "run", side_effect=run), \
                        mock.patch.object(BUILD, "prepare_identity"), \
                        mock.patch.object(BUILD, "candidate_phase") as phases, \
                        mock.patch.object(BUILD, "check_probes", side_effect=gate_error), \
                        mock.patch.object(BUILD, "stock_reference") as stock, \
                        mock.patch.object(BUILD, "shared_extensions", side_effect=AssertionError("Unexpected smoke")), \
                        mock.patch.dict(BUILD.SYSTEM, {"save_host_snapshot": lambda *args: None}):
                    if gate_error:
                        with self.assertRaises(ValueError):
                            BUILD.main()
                    else:
                        BUILD.main()
                        self.assertEqual("preflight", json.loads((output / "result.json").read_text())["mode"])
                    self.assertEqual(["configure", "configure"], [call.args[-1] for call in phases.call_args_list])
                    stock.assert_not_called()
                self.assertTrue((output / "RAW-SHA256SUMS").exists())

    def smoke_fixture(self, root):
        """Write complete synthetic smoke slots and a matching native JAR."""
        native = root / "libduckdb_java.so_linux_amd64"
        native.write_bytes(b"isolated native fixture")
        with zipfile.ZipFile(root / "duckdb_jdbc.jar", "w") as archive:
            archive.write(native, native.name)
        smoke = root / "smoke"
        smoke.mkdir()
        BUILD.save(smoke / "loaded-native.json", {"sha256": BUILD.digest(native), "expected_sha256": BUILD.digest(native)})
        BUILD.save(smoke / "libstdcxx-runtime.json", {"sha256": "a" * 64})
        BUILD.save(smoke / "cancellation-attempt.json", {"interrupted": True, "rows": -1, "elapsed_ns": 1})
        BUILD.save(smoke / "cancel-readiness.json", {"method": "query_progress", "rows_processed": 10,
                   "total_rows": 10000, "percentage": .1, "epoch_ms": 1})
        BUILD.save(smoke / "cancel-request.json", {"epoch_ms": 2})
        BUILD.save(smoke / "cancel-termination.json", {"terminated": True, "epoch_ms": 3})
        limits = "threads=1\tmemory_limit=256.0 MiB\tmax_temp_directory_size=256.0 MiB\ttemp_directory=/isolated/tmp\n"
        (smoke / "native-limits.tsv").write_text(limits)
        common = root.parent / "common"
        common.mkdir(exist_ok=True)
        BUILD.save(common / "shared-extensions.json", {name: {"sha256": name * 64} for name in ("httpfs", "iceberg")})
        for name in ("httpfs", "iceberg"):
            BUILD.save(smoke / (name + "-loaded.json"), {"sha256": name * 64})
        for name in BUILD.UPSTREAM:
            (smoke / ("upstream-" + name + ".log")).write_text("TestDuckDBJDBC#" + name + " success in 0 seconds\nOK\n")
            (smoke / ("upstream-" + name + "-status.txt")).write_text("0\n")
            folder = smoke / ("upstream-" + name)
            folder.mkdir()
            (folder / "native-limits.tsv").write_text(limits)
        rows = []
        for name in sorted(BUILD.SMOKES):
            count, checksum = BUILD.ANSWERS.get(name, (1, "0000000000000000"))
            rows.append({"query": name, "rows": count, "checksum": checksum, "latency_ns": 1, "arrow_bytes": 32,
                         "batches": 1, "ok": 1, "error_type": "", "checksum_version": "tpch-v2-sha256-64-numeric2"})
        return rows

    def write_smoke_rows(self, root, rows):
        """Replace only a synthetic fixture's raw rows."""
        with (root / "smoke/queries.csv").open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_smoke_gate_requires_every_slot_actual_native_and_fixed_answers(self):
        """Missing, duplicated, failed or identically wrong answers cannot pass the smoke gate."""
        for mutation in ("missing", "duplicate", "failure", "checksum", "loaded", "jar"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "gcc"
                root.mkdir()
                rows = self.smoke_fixture(root)
                if mutation == "missing":
                    rows.pop()
                elif mutation == "duplicate":
                    rows[-1] = rows[0]
                elif mutation == "failure":
                    rows[0]["ok"] = 0
                elif mutation == "checksum":
                    next(row for row in rows if row["query"] == "arrow_types")["checksum"] = "0000000000000000"
                elif mutation == "loaded":
                    (root / "smoke/loaded-native.json").write_text('{"sha256":"other","expected_sha256":"other"}')
                else:
                    with zipfile.ZipFile(root / "duckdb_jdbc.jar", "w") as archive:
                        archive.writestr("libduckdb_java.so_linux_amd64", "different binary")
                self.write_smoke_rows(root, rows)
                with self.assertRaises(ValueError):
                    BUILD.verify_smoke(root)
                self.assertFalse((root / "smoke/verified.json").exists())

    def test_complete_smoke_keeps_loaded_binary_proof(self):
        """A successful gate records the exact mapped binary rather than merely an intended path."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "gcc"
            root.mkdir()
            self.write_smoke_rows(root, self.smoke_fixture(root))
            BUILD.verify_smoke(root)
            result = json.loads((root / "smoke/verified.json").read_text())
            self.assertEqual(BUILD.digest(root / "libduckdb_java.so_linux_amd64"), result["mapped_native_sha256"])

    def test_smoke_rejects_different_shared_extensions_unbounded_native_and_unproved_readiness(self):
        """Success exits cannot replace byte-equality, actual native limits or execution evidence."""
        for mutation in ("extensions", "limits", "readiness", "termination"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "stock"
                root.mkdir()
                self.write_smoke_rows(root, self.smoke_fixture(root))
                if mutation == "extensions":
                    (root / "smoke/httpfs-loaded.json").write_text('{"sha256":"different"}')
                elif mutation == "limits":
                    (root / "smoke/native-limits.tsv").write_text("threads=16\tmemory_limit=unlimited\n")
                elif mutation == "readiness":
                    (root / "smoke/cancel-readiness.json").write_text('{"method":"query_progress","rows_processed":0}')
                else:
                    (root / "smoke/cancel-termination.json").write_text('{"terminated":false}')
                with self.assertRaises(ValueError):
                    BUILD.verify_smoke(root)

    def test_upstream_exit_zero_requires_intended_nonzero_successful_tests(self):
        """Empty or unrelated upstream runs cannot satisfy selected test coverage."""
        for content in ("OK\n", "No tests found\nOK\n", "Other#test_wrong success in 0 seconds\nOK\n",
                        "TestDuckDBJDBC#test_connection failed with problem\nOK\n"):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "gcc"
                root.mkdir()
                self.smoke_fixture(root)
                (root / "smoke/upstream-test_connection.log").write_text(content)
                with self.assertRaises(ValueError):
                    BUILD.verify_upstream(root / "smoke")

    def test_native_stack_cpu_readiness_needs_both_stacks_cpu_advance_and_incomplete_task(self):
        """A latch or one native-looking stack cannot replace actual execution evidence."""
        for mutation in (None, "cpu", "stack", "incomplete", "timestamp"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / "gcc"
                root.mkdir()
                self.write_smoke_rows(root, self.smoke_fixture(root))
                smoke = root / "smoke"
                readiness = {"method": "native_execute_thread_cpu", "thread_cpu_start_ns": 1,
                             "thread_cpu_end_ns": 2_000_001, "task_incomplete": True,
                             "native_method": "duckdb_jdbc_execute", "cpu_start_epoch_ms": 0, "epoch_ms": 1}
                for stage in ("before", "after"):
                    (smoke / ("cancel-stack-" + stage + ".txt")).write_text(
                        "org.duckdb.DuckDBNative.duckdb_jdbc_execute(Native Method)")
                if mutation == "cpu":
                    readiness["thread_cpu_end_ns"] = readiness["thread_cpu_start_ns"]
                elif mutation == "stack":
                    (smoke / "cancel-stack-after.txt").write_text("before prepare only")
                elif mutation == "incomplete":
                    readiness["task_incomplete"] = False
                elif mutation == "timestamp":
                    readiness["epoch_ms"] = 4
                (smoke / "cancel-readiness.json").write_text(json.dumps(readiness))
                if mutation:
                    with self.assertRaises(ValueError):
                        BUILD.verify_smoke(root)
                else:
                    BUILD.verify_smoke(root)

    def test_stock_reference_requires_published_jar_and_measured_native_hashes(self):
        """The third reference must be the existing stock binary, not just a version label."""
        for tampered in (False, True):
            with self.subTest(tampered=tampered), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                fixture = root / "fixture.jar"
                with zipfile.ZipFile(fixture, "w") as archive:
                    archive.writestr("libduckdb_java.so_linux_amd64", "stock native")
                native_hash = BUILD.hashlib.sha256(b"stock native").hexdigest()
                jar_hash = BUILD.digest(fixture)
                def download(url, path):
                    if url.endswith(".jar"):
                        path.write_bytes(fixture.read_bytes())
                    elif url.endswith(".sha256"):
                        path.write_text("wrong" if tampered else jar_hash)
                    else:
                        path.write_text("published pom")
                output = root / "raw"
                output.mkdir()
                with mock.patch.object(BUILD, "download", side_effect=download), \
                        mock.patch.object(BUILD, "STOCK_JAR", jar_hash), \
                        mock.patch.object(BUILD, "STOCK_NATIVE", native_hash):
                    if tampered:
                        with self.assertRaises(ValueError):
                            BUILD.stock_reference(output)
                    else:
                        BUILD.stock_reference(output)
                        result = json.loads((output / "stock/reference.json").read_text())
                        self.assertEqual(native_hash, result["native_sha256"])
