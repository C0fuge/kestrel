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
            "stackTrace": {"frames": [{"type": "JIT compiled" if java_id else "C++",
                                        "method": {"type": {"name": name}, "name": method}}]}}


def native_worker():
    """Represent the actual retained DuckDB native worker root without Java frames."""
    result = sample(2, "libduckdb_java.so", "duckdb::Executor::ExecuteTask", None)
    for cls, method in (("libduckdb_java.so", "duckdb::TaskScheduler::ExecuteForever"),
                        ("libasyncProfiler.so", "thread_start_wrapper")):
        result["stackTrace"]["frames"].extend(sample(2, cls, method, None)["stackTrace"]["frames"])
    return result


def events(engine="ctimer", event="ctimer"):
    """Build recording settings that identify a real explicit CPU timer."""
    settings = {"version": "4.6", "engine": engine, "event": event,
                "interval": "10000000", "cstack": "dwarf", "clock": "monotonic"}
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

    def test_missing_wrong_or_conflicting_clock_is_rejected(self):
        valid = events()
        missing = [event for event in valid if event["values"].get("name") != "clock"]
        wrong = [event if event["values"].get("name") != "clock" else
                 {"type": "jdk.ActiveSetting", "values": {"name": "clock", "value": "tsc"}}
                 for event in valid]
        conflicting = valid + [{"type": "jdk.ActiveSetting", "values": {"name": "clock", "value": "tsc"}}]
        for invalid in (missing, wrong, conflicting):
            with self.assertRaisesRegex(ValueError, "clock"):
                PROFILE["inspect_cpu_events"](invalid, "ctimer")

    def test_actual_jvm_flags_reject_absent_duplicate_and_conflicting_limits(self):
        flags = list(PROFILE["FIXED_JVM"])
        PROFILE["check_jvm_arguments"](flags)
        for invalid in (flags[1:], flags + ["-Xmx2g"], flags + ["-Xmx4g"],
                        flags + ["-XX:MaxHeapSize=4294967296"], flags[:-1] + ["-Duser.home=/elsewhere"]):
            with self.subTest(flags=invalid), self.assertRaises(ValueError):
                PROFILE["check_jvm_arguments"](invalid)

    def test_target_pid_selects_exact_recording_and_preserves_auxiliary_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            launch = folder / "java-launch-12.json"
            launch.write_text(json.dumps({"pid": 12}))
            target = folder / "cpu-12.jfr"
            target.write_bytes(b"target")
            child = folder / "cpu-13.jfr"
            child.write_bytes(b"native child")
            chosen, metadata, auxiliary = PROFILE["target_recording"](folder)
            self.assertEqual(target, chosen)
            self.assertEqual([child], auxiliary)
            target.unlink()
            with self.assertRaisesRegex(ValueError, "exact target"):
                PROFILE["target_recording"](folder)
            self.assertEqual(b"native child", child.read_bytes())
            target.write_bytes(b"target")
            launch.write_text(json.dumps({"pid": 13}))
            with self.assertRaisesRegex(ValueError, "disagree"):
                PROFILE["target_recording"](folder)
            launch.write_text(json.dumps({"pid": 12}))
            (folder / "java-launch-13.json").write_text(json.dumps({"pid": 13}))
            with self.assertRaisesRegex(ValueError, "exactly one"):
                PROFILE["target_recording"](folder)

    def test_unreadable_auxiliary_recording_is_retained_and_never_substituted(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            target, child = folder / "cpu-12.jfr", folder / "cpu-13.jfr"
            target.write_bytes(b"target")
            child.write_bytes(b"partial inherited child")
            with patch.dict(PROFILE["recording_inventory"].__globals__,
                            {"run": lambda *args: (_ for _ in ()).throw(RuntimeError("partial chunk"))}):
                inventory = PROFILE["recording_inventory"](folder, target, [child])
            self.assertEqual("target", inventory[0]["role"])
            self.assertFalse(inventory[1]["readable"])
            self.assertEqual("partial chunk", inventory[1]["extraction_error"])
            self.assertEqual(b"partial inherited child", child.read_bytes())

    def test_identity_and_actual_runtime_flags_reject_wrong_process_and_stale_cpu(self):
        flags = list(PROFILE["FIXED_JVM"])
        launch = {"pid": 12, "argv": ["java", *flags], "epoch_ns": 1577836800000000000}
        cpu = [{"startTime": "2020-01-01T00:00:00.123456789Z"}]
        info = {"pid": 12, "jvmStartTime": "2020-01-01T00:00:00Z", "jvmArguments": " ".join(flags)}
        jvm = [{"type": "jdk.JVMInformation", "values": info}]
        runtime = "arguments=[" + ", ".join(flags) + "]\nprocessors=4\n"
        status = {"end_epoch_ns": 1577836801000000000}
        result = PROFILE["check_target_identity"](launch, cpu, jvm, runtime, status)
        self.assertEqual(12, result["pid"])
        invalids = ((cpu, [], runtime),
                    (cpu, [{"type": "jdk.JVMInformation", "values": {**info, "pid": 13}}], runtime),
                    (cpu, [{"type": "jdk.JVMInformation", "values": {**info, "jvmArguments": "-ea"}}], runtime),
                    ([{"startTime": "2019-12-31T23:59:00Z"}], jvm, runtime),
                    (cpu, jvm, "arguments=[]\nprocessors=4\n"),
                    (cpu, jvm, runtime.replace("processors=4", "processors=32")))
        for bad_cpu, bad_jvm, bad_runtime in invalids:
            with self.assertRaises(ValueError):
                PROFILE["check_target_identity"](launch, bad_cpu, bad_jvm, bad_runtime, status)

    def test_java_zero_thread_id_is_not_a_native_worker(self):
        native = native_worker()
        self.assertEqual("native_duckdb_scheduler_worker", PROFILE["classify_duckdb_sample"](native))
        foreground = native_worker()
        foreground["sampledThread"]["javaThreadId"] = 0
        foreground["stackTrace"]["frames"].append({"type": "JIT compiled", "method": {
            "type": {"name": "java/lang/Thread", "package": {"name": "java/lang"}}, "name": "run"}})
        self.assertEqual("java_frames", PROFILE["classify_duckdb_sample"](foreground))
        native["stackTrace"]["frames"][0]["type"] = "unrecognized"
        self.assertEqual("unknown_frame_types", PROFILE["classify_duckdb_sample"](native))
        self.assertEqual("native_unclassified", PROFILE["classify_duckdb_sample"](
            sample(2, "libduckdb_java.so", "duckdb::Executor::ExecuteTask", None)))

    def test_module_names_and_addresses_are_not_resolved_function_frames(self):
        for name in ("/usr/lib/libc.so.6", "libstdc++.so.6", "[unknown]", "0x1234"):
            self.assertFalse(PROFILE["resolved_symbol"](name))
        self.assertTrue(PROFILE["resolved_symbol"]("duckdb::TaskScheduler::ExecuteForever"))

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
            (folder / "cpu-1.jfr").write_bytes(b"fixture")
            (folder / "java-launch-1.json").write_text(json.dumps({"pid": 1}))
            (folder / "runtime.txt").write_text("")
            (folder / "maven.status.json").write_text("{}")
            (folder / "native-a.json").write_text(json.dumps({"mapped_path": "/tmp/libduckdb_java.so"}))
            for label in ("warmup-c8", "c32"):
                (folder / (label + ".csv")).write_text("query_kind,ok\n0,1\n1,1\n2,1\n")
            native = native_worker()
            with patch.dict(function_globals["SCREEN"], {"summary_of": lambda *args: ({}, {})}):
                with patch.dict(function_globals, {"run": lambda *args: None,
                                "event_values": lambda *args: [], "check_target_identity": lambda *args: {},
                                "thread_cpu_coverage": lambda *args: {"unsampled_active_tids": []}}):
                    for invalid in (sample(2, "libduckdb_java.so", "duckdb::Executor", 2),
                                    sample(2, "libduckdb_java.so", "[unknown]", None)):
                        with patch.dict(function_globals, {"extract_profile": lambda *args: ({}, [invalid])}):
                            with self.assertRaisesRegex(ValueError, "unattached DuckDB"):
                                PROFILE["validate_trial"](folder, 32, "ctimer")
                            (folder / "thread-cpu-coverage.json").unlink()
                            (folder / "cpu-recording-inventory.json").unlink()
                            (folder / "target-identity.json").unlink()
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
            with patch.object(sys, "argv", [str(wrapper), "-jar", "surefirebooter.jar"]), \
                    patch("os.execve") as execute, patch("os.dup2"):
                with patch.dict(os.environ, {"GITHUB_TOKEN": "synthetic-test-secret"}):
                    exec(script, {})
                env = execute.call_args.args[2]
                self.assertNotIn("GITHUB_TOKEN", env)
                self.assertIn("cpu-%p.jfr", env["ASPROF_COMMAND"])
                self.assertEqual(str(folder / "lib/libasyncProfiler.so"), env["LD_PRELOAD"])
                self.assertTrue(any(arg.startswith("-agentpath:") for arg in execute.call_args.args[1]))
                PROFILE["check_jvm_arguments"](execute.call_args.args[1])
                self.assertIn("log=stderr", env["ASPROF_COMMAND"])
                self.assertNotIn("%p.log", env["ASPROF_COMMAND"])
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
