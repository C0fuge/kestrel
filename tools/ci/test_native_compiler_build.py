"""Check build identity and failure preservation without Docker, downloads or compilation."""

import importlib.util
import io
import csv
import json
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
        self.assertIn("-march=x86-64", gcc[-1])
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
            (folder / "compile_commands.json").write_text(json.dumps([{
                "file": "/source/src/duckdb/third_party/snappy/snappy.cc",
                "command": ("g++" if compiler == "gcc" else "clang++") + " " + BUILD.FLAGS
                           + " -O3 -DNDEBUG -std=gnu++11 -c snappy.cc"}]))

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
                    (root / "clang/libstdcxx.txt").write_text("/other/library")
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
            self.assertIn("actual_decoder_compile", result["gcc"])

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
                root = Path(temp)
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
            root = Path(temp)
            self.write_smoke_rows(root, self.smoke_fixture(root))
            BUILD.verify_smoke(root)
            result = json.loads((root / "smoke/verified.json").read_text())
            self.assertEqual(BUILD.digest(root / "libduckdb_java.so_linux_amd64"), result["mapped_native_sha256"])

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
