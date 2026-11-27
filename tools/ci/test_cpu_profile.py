"""Failure-oriented checks for CPU attribution, native coverage and retained evidence."""

import json
import hashlib
import io
import os
from pathlib import Path
import runpy
import sys
import tempfile
import tarfile
import unittest
from unittest.mock import patch


PROFILE = runpy.run_path(str(Path(__file__).with_name("cpu-profile.py")))


def sample(tid=1, name="CpuProfileProbe", method="main", java_id=1):
    """Build a minimal JDK JSON sample with explicit OS versus Java thread identity."""
    return {"sampledThread": {"osThreadId": tid, "javaThreadId": java_id},
            "stackTrace": {"frames": [{"method": {"type": {"name": name}, "name": method}}]}}


def events(engine="ctimer", event="ctimer"):
    """Build recording settings that identify a real explicit CPU timer."""
    settings = {"version": "4.6", "engine": engine, "event": event,
                "interval": "10000000", "cstack": "dwarf"}
    return [{"type": "jdk.ActiveSetting", "values": {"name": k, "value": v}}
            for k, v in settings.items()] + [{"type": "jdk.ExecutionSample", "values": sample()}]


class CpuProfileTest(unittest.TestCase):
    """Reject plausible-looking incomplete profiles before attribution is published."""

    def test_explicit_backends_and_wall_fallback(self):
        for requested, backend in (("ctimer", "ctimer"), ("cpu-clock", "perf_events")):
            metadata, _ = PROFILE["inspect_cpu_events"](events(backend, requested), requested)
            self.assertEqual(backend, metadata["actual_engine"])
        for invalid in (events("wall"), events("itimer"), events(event="cpu"), events()[:-1],
                        events() + [{"type": "profiler.WallClockSample", "values": {}}],
                        events() + [{"type": "jdk.ActiveSetting", "values": {"name": "wall", "value": "10"}}]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PROFILE["inspect_cpu_events"](invalid, "ctimer")

    def test_empty_unreadable_or_wrong_interval(self):
        empty = events()
        empty[-1]["values"]["stackTrace"] = None
        wrong = events()
        wrong[3]["values"]["value"] = "1000"
        for invalid in (empty, wrong):
            with self.assertRaises(ValueError):
                PROFILE["inspect_cpu_events"](invalid, "ctimer")

    def test_native_busy_waiting_and_symbol_gate(self):
        counters = {"native_busy_tid": 2, "native_wait_tid": 3, "native_busy_cpu_ns": 800_000_000,
                    "native_wait_cpu_ns": 10_000, "java_busy_cpu_ns": 800_000_000}
        java = [sample()] * 40
        native = [sample(2, "libprobe.so", "ap_native_busy", None)] * 40
        verified = PROFILE["check_probe"](java + native, counters)
        self.assertEqual(40, verified["native_busy_samples"])
        invalid_cases = (java, native, java + native + [sample(3)] * 10,
                         java + [sample(2, "libprobe.so", "[unknown]", None)] * 40)
        for invalid in invalid_cases:
            with self.assertRaises(ValueError):
                PROFILE["check_probe"](invalid, counters)
        with self.assertRaises(ValueError):
            PROFILE["check_probe"](java + native, {**counters, "native_wait_cpu_ns": 100_000_000})

    def test_actual_duckdb_workers_require_native_identity_and_symbols(self):
        function_globals = PROFILE["validate_trial"].__globals__
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            for name in ("cpu-1.jfr", "java-launch-1.json"):
                (folder / name).touch()
            (folder / "native-a.json").write_text(json.dumps({"mapped_path": "/tmp/libduckdb_java.so"}))
            for label in ("warmup-c8", "c32"):
                (folder / (label + ".csv")).write_text("query_kind,ok\n0,1\n1,1\n2,1\n")
            native = sample(2, "libduckdb_java.so", "duckdb::Executor::ExecuteTask", None)
            native["stackTrace"]["frames"].append(sample(2, "libc.so", "start_thread", None)["stackTrace"]["frames"][0])
            with patch.dict(function_globals["SCREEN"], {"summary_of": lambda *args: ({}, {})}):
                with patch.dict(function_globals, {"run": lambda *args: None,
                                "thread_cpu_coverage": lambda *args: {"unsampled_active_tids": []}}):
                    for invalid in (sample(2, "libduckdb_java.so", "duckdb::Executor", 2),
                                    sample(2, "libduckdb_java.so", "[unknown]", None)):
                        with patch.dict(function_globals, {"extract_profile": lambda *args: ({}, [invalid])}):
                            with self.assertRaisesRegex(ValueError, "unattached DuckDB"):
                                PROFILE["validate_trial"](folder, 32, "ctimer")
                            (folder / "thread-cpu-coverage.json").unlink()
                    with patch.dict(function_globals, {"extract_profile": lambda *args: ({}, [native] * 10)}):
                        PROFILE["validate_trial"](folder, 32, "ctimer")
                self.assertEqual(10, json.loads((folder / "cpu-verified.json").read_text())[
                    "resolved_unattached_duckdb_worker_samples"])

    def test_wrapper_scopes_preload_to_surefire_and_uses_unique_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            java = folder / "runtime/bin/java"
            java.parent.mkdir(parents=True)
            (java.parent.parent / "release").write_text('JAVA_VERSION="21.0.12"\n')
            wrapper = PROFILE["write_wrapper"](folder, str(java), folder, "ctimer")
            self.assertEqual("bin", wrapper.parent.name)
            self.assertEqual('JAVA_VERSION="21.0.12"\n', (wrapper.parent.parent / "release").read_text())
            script = compile(wrapper.read_text(), str(wrapper), "exec")
            with patch.object(sys, "argv", [str(wrapper), "-version"]), patch("os.execve") as execute:
                exec(script, {})
                self.assertNotIn("ASPROF_COMMAND", execute.call_args.args[2])
                self.assertEqual([str(java), "-version"], execute.call_args.args[1])
            with patch.object(sys, "argv", [str(wrapper), "-jar", "surefirebooter.jar"]), patch("os.execve") as execute:
                with patch.dict(os.environ, {"GITHUB_TOKEN": "synthetic-test-secret"}):
                    exec(script, {})
                env = execute.call_args.args[2]
                self.assertNotIn("GITHUB_TOKEN", env)
                self.assertIn("cpu-%p.jfr", env["ASPROF_COMMAND"])
                self.assertEqual(str(folder / "lib/libasyncProfiler.so"), env["LD_PRELOAD"])
                self.assertIn("-agentpath:", execute.call_args.args[1][1])
            self.assertEqual(1, len(list(folder.glob("java-launch-*.json"))))
            self.assertNotIn("ASPROF_COMMAND", os.environ)

    def test_failure_log_and_exit_status_survive(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            with self.assertRaises(RuntimeError):
                PROFILE["run"]([sys.executable, "-c", "print('retained'); raise SystemExit(7)"], folder, "failed")
            self.assertEqual("retained\n", (folder / "failed.log").read_text())
            self.assertEqual(7, json.loads((folder / "failed.status.json").read_text())["exit_code"])
            PROFILE["manifest"](folder)
            self.assertIn("failed.log", (folder / "SHA256SUMS").read_text())

    def test_existing_evidence_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "raw.json"
            PROFILE["save"](path, {"retained": True})
            with self.assertRaises(FileExistsError):
                PROFILE["save"](path, {})
            self.assertTrue(json.loads(path.read_text())["retained"])

    def test_timeout_preserves_termination_and_partial_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            with self.assertRaises(TimeoutError):
                PROFILE["run"]([sys.executable, "-u", "-c", "import time; print('partial'); time.sleep(30)"],
                               folder, "timeout", timeout=.05)
            self.assertEqual("partial\n", (folder / "timeout.log").read_text())
            self.assertEqual(-15, json.loads((folder / "timeout.status.json").read_text())["exit_code"])

    def test_clean_environment_and_no_implicit_cpu_or_wall(self):
        for invalid in ("cpu", "wall", "itimer"):
            with self.assertRaises(ValueError):
                PROFILE["profiler_command"](invalid, Path("/output"))
        for variable in ("LD_PRELOAD", "ASPROF_COMMAND", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            with patch.dict(os.environ, {variable: "existing-agent"}):
                with self.assertRaises(ValueError):
                    PROFILE["launch_environment"](Path("/profiler"), "ctimer", Path("/output"))

    def test_release_digest_mismatch_preserves_original_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            with patch("urllib.request.urlopen", return_value=io.BytesIO(b"wrong asset")):
                with self.assertRaisesRegex(ValueError, "SHA-256"):
                    PROFILE["install_profiler"](folder)
            self.assertEqual(b"wrong asset", (folder / PROFILE["ASSET"]).read_bytes())
            self.assertFalse((folder / "async-profiler").exists())

    def test_release_archive_cannot_escape_destination(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w:gz") as target:
            member = tarfile.TarInfo("async-profiler-4.6-linux-x64/../../escaped")
            member.size = 1
            target.addfile(member, io.BytesIO(b"x"))
        payload = archive.getvalue()
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            with patch("urllib.request.urlopen", return_value=io.BytesIO(payload)), patch.dict(
                    PROFILE["install_profiler"].__globals__, {"ASSET_SHA256": hashlib.sha256(payload).hexdigest()}):
                with self.assertRaisesRegex(ValueError, "archive path"):
                    PROFILE["install_profiler"](folder)
            self.assertFalse((folder.parent / "escaped").exists())

    def test_optional_counter_failure_is_explicit(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            result = PROFILE["capture_file"](folder / "absent")
            self.assertEqual("error", result["status"])
            self.assertIn("error", result)

    def test_unresolved_native_method_has_explicit_name(self):
        self.assertEqual([("", "[unknown]")], list(PROFILE["frame_names"](
            {"stackTrace": {"frames": [{"method": None}]}})))

    def test_cpu_active_unsampled_os_thread_is_exposed(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "process.jsonl"
            def row(user):
                fields = ["S", *("0" for _ in range(10)), str(user), "0"]
                return {"tasks": {"2": {"stat": {"status": "ok", "text": "2 (native ) worker) " + " ".join(fields)}}}}
            path.write_text(json.dumps(row(10)) + "\n" + json.dumps(row(210)) + "\n")
            missing = PROFILE["thread_cpu_coverage"](path, [sample()])
            self.assertEqual(["2"], missing["unsampled_active_tids"])
            present = PROFILE["thread_cpu_coverage"](path, [sample(2)])
            self.assertEqual([], present["unsampled_active_tids"])


if __name__ == "__main__":
    unittest.main()
