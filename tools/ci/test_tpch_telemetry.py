"""Exercise TPC-H raw evidence and failure retention without containers or client packages."""

import contextlib
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


HERE = Path(__file__).parent
RAW = runpy.run_path(str(HERE / "tpch-raw.py"))
TELEMETRY = runpy.run_path(str(HERE / "tpch-telemetry.py"))


def resource_fixture():
    """Represent real cgroup v2 and proc fields without relying on the test host's limits."""
    values = {
        "cpu.stat": "usage_usec 10\nuser_usec 7\nsystem_usec 3\nnr_periods 1\nnr_throttled 0\nthrottled_usec 0",
        "cpu.max": "400000 100000", "cpuset.cpus.effective": "0-3", "memory.current": "100",
        "memory.peak": "200", "memory.stat": "anon 50\nfile 50", "memory.max": "8589934592",
        "memory.swap.max": "8589934592", "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0",
        "io.stat": "", "pids.current": "20",
    }
    result = {"/sys/fs/cgroup/" + name: {"status": 0, "text": text} for name, text in values.items()}
    result.update({
        "/proc/1/stat": {"status": 0, "text": "1 (java) S " + " ".join("1" for _ in range(21))},
        "/proc/1/status": {"status": 0, "text": "VmRSS:\t100 kB\nThreads:\t20"},
        "/proc/1/io": {"status": 0, "text": "rchar: 0\nwchar: 0\nsyscr: 0\nsyscw: 0\nread_bytes: 0\nwrite_bytes: 0"},
        "/proc/1/smaps_rollup": {"status": 0, "text": "Rss: 100 kB\nPss: 90 kB"},
    })
    return result


def write_rows(label, rows, client="java"):
    """Write a complete or deliberately damaged client fixture in its actual CSV schema."""
    path = label / "client" / ("queries.csv" if client == "java" else "samples.csv")
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def valid_run(directory):
    """Build an internally bound extraction fixture with two clients, two queries and two measured rounds."""
    (directory / "queries").mkdir(parents=True)
    for name in ("q01", "q02"):
        (directory / "queries" / (name + ".sql")).write_text("select 1")
        (directory / "queries" / (name + ".rows")).write_text("1")
        (directory / "queries" / (name + ".checksum")).write_text("0000000000000001")
    (directory / "queries/checksum-version.txt").write_text(TELEMETRY["CHECKSUM_VERSION"])
    (directory / "inputs.json").write_text(json.dumps({
        "rounds": "2", "clients": "2", "client": "java", "quick": True,
        "baseline": "", "extra": "", "more": "", "plain": False}))
    TELEMETRY["write_plan"](directory)
    label = directory / "default"
    (label / "client").mkdir(parents=True)
    for name in ("container-argv.json", "container.json", "server.log", "mvn.log"):
        (label / name).write_text("complete evidence")
    (label / "client-exit-status.txt").write_text("0")
    (label / "resources.jsonl").write_text(json.dumps({
        "status": 0, "files": resource_fixture(),
        "heap": {"status": 0, "stdout": "garbage-first heap total 1024K, used 512K"}}) + "\n")
    recording = b"FLR\0" + bytes(100)
    (label / "server.jfr").write_bytes(recording)
    (label / "server-jfr-extraction.json").write_text(json.dumps({
        "sha256": hashlib.sha256(recording).hexdigest(), "bytes": len(recording),
        "summary_status": 0, "events_status": 0}))
    (label / "server-jfr-summary.txt").write_text("Version: 2.1\nChunks: 1\n")
    (label / "server-jfr-events.json").write_text(json.dumps({"recording": {"events": [
        {"type": "jdk.JVMInformation", "values": {"jvmVersion": "21 test JVM"}},
        {"type": "jdk.DirectBufferStatistics", "values": {"count": 0, "totalCapacity": 0, "memoryUsed": 0}}]}}))
    (label / "gc.log").write_text("[0.001s][info][gc] Using G1\n")
    rows = [{"query": query, "client": client, "round": round_number, "warmup": int(round_number == 0),
             "start_epoch_ms": 1700000000000, "start_ns": 1, "latency_ns": 100,
             "rows": 1, "checksum": "0000000000000001", "arrow_bytes": 8, "batches": 1,
             "ok": 1, "error_type": "", "checksum_version": TELEMETRY["CHECKSUM_VERSION"]}
            for query in ("q01", "q02") for client in range(2) for round_number in range(3)]
    write_rows(label, rows)
    return label, rows


class TpchTelemetryTest(unittest.TestCase):
    """Check preserved samples, explicit failure markers, and independent label artifacts."""

    def test_raw_warmup_failures_and_measured_percentiles(self):
        """Warmups and failed attempts survive, and cannot improve measured successful tails."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "client"
            raw = RAW["RawRun"](directory)
            raw.record("q01", 0, 100, 1000, (2, 42, 40, 2), 2)
            raw.record("q01", 1, 200, 10, (2, 42, 40, 2), 2)
            raw.record("q01", 2, 300, 90, (2, 42, 40, 2), 2)
            raw.record("q02", 1, 400, 5, (1, 0, 8, 1), 2)
            raw.record("q02", 2, 500, 3, None, 2, "RuntimeError")
            raw.close(False)
            with (directory / "samples.csv").open() as source:
                samples = list(csv.DictReader(source))
            self.assertEqual(["1", "0", "0", "0", "0"], [row["warmup"] for row in samples])
            self.assertEqual("answer_mismatch", samples[3]["error_type"])
            self.assertEqual(("0", "-1", "-1"), tuple(samples[4][name] for name in ("ok", "rows", "arrow_buffer_bytes")))
            summary = json.loads((directory / "client-summary.json").read_text())
            self.assertFalse(summary["completed"])
            self.assertEqual(2, summary["failed_samples"])
            self.assertEqual({"count": 2, "p50_ns": 90, "p95_ns": 90, "p99_ns": 90, "max_ns": 90,
                              "arrow_buffer_bytes": 80, "table_batches": 4}, summary["queries"]["q01"])
            self.assertEqual("q01", json.loads((directory / "first-query.json").read_text())["query"])
            self.assertEqual(RAW["CHECKSUM_VERSION"], samples[0]["checksum_version"])
            self.assertEqual(RAW["CHECKSUM_VERSION"], json.loads((directory / "client.json").read_text())["checksum_version"])
            with self.assertRaises(FileExistsError):
                RAW["RawRun"](directory)

    def test_collectors_keep_timeout_output_and_redact_test_keys(self):
        """A timed-out diagnostic retains its partial output, status, and exact non-secret argv."""
        with tempfile.TemporaryDirectory() as temp, mock.patch.dict(os.environ, {"S3_KEY": "test-key", "S3_SECRET": "test-secret"}):
            path = Path(temp) / "log"
            with mock.patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(
                    ["collector"], 60, output=b"partial test-secret", stderr=b"test-key")):
                TELEMETRY["capture"](["collector", "test-secret"], path)
            self.assertIn("partial <redacted>", path.read_text())
            command = json.loads(path.with_name("log.command.json").read_text())
            self.assertEqual(124, command["status"])
            self.assertEqual(["collector", "<redacted>"], command["argv"])

    def test_finish_stops_before_extracting_and_keeps_crash_chunks(self):
        """Graceful stop precedes extraction; a missing final recording recovers without deleting chunks."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            chunks = directory / "jfr-repository/chunks"
            chunks.mkdir(parents=True)
            original = chunks / "chunk.jfr"
            original.write_bytes(b"original chunk")
            commands = []

            def run(command, **kwargs):
                commands.append(command)
                if command[:2] == ["jfr", "assemble"]:
                    Path(command[-1]).write_bytes(b"recovered recording")
                return subprocess.CompletedProcess(command, 0, "complete output\n", "")

            with mock.patch.object(subprocess, "run", side_effect=run):
                TELEMETRY["finish"]("task-container", directory)
            self.assertEqual(["docker", "stop", "--time", "30", "task-container"], commands[0])
            self.assertEqual(b"original chunk", original.read_bytes())
            self.assertTrue((directory / "recovered-0-jfr-summary.txt").is_file())
            self.assertTrue((directory / "monitor.stop").is_file())
            self.assertEqual(["docker", "rm", "task-container"], commands[-1])
            self.assertFalse(any("-f" in command for command in commands))

    def test_finish_restores_writable_ownership_for_plain_client_derivation(self):
        """Host derivation must work after a stopped plain container created a root-owned client directory."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            label = directory / "plain"
            client = label / "client"
            client.mkdir(parents=True)
            raw = client / "samples.csv"
            raw.write_text("query,round,warmup,latency_ns,ok\nq01,0,1,100,1\nq01,1,0,10,1\n")
            original = raw.read_bytes()
            ownership = {"runner": False}
            write_text = Path.write_text

            def owned_write(path, *args, **kwargs):
                if path.parent == client and not ownership["runner"]:
                    raise PermissionError("client directory is owned by the container root user")
                return write_text(path, *args, **kwargs)

            def diagnostic(command, **kwargs):
                if command[:3] == ["sudo", "chown", "-R"]:
                    self.assertEqual(f"{os.getuid()}:{os.getgid()}", command[3])
                    self.assertEqual(str(label), command[4])
                    ownership["runner"] = True
                return subprocess.CompletedProcess(command, 0, "complete evidence\n", "")

            with mock.patch.object(Path, "write_text", owned_write), \
                    mock.patch.object(subprocess, "run", side_effect=diagnostic):
                with self.assertRaises(PermissionError):
                    TELEMETRY["summarize"](directory)
                TELEMETRY["finish"]("task-owned-plain-container", label)
                TELEMETRY["summarize"](directory)
            self.assertTrue((client / "query-percentiles.json").is_file())
            self.assertTrue((directory / "SHA256SUMS").is_file())
            self.assertEqual(original, raw.read_bytes())

    def test_monitor_flushes_raw_process_and_heap_evidence(self):
        """A complete sampling interval retains unparsed counters plus the heap diagnostic."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            files = resource_fixture()
            raw = "\n".join(f"@@FILE {name}\n{entry['text']}\n@@STATUS {name} {entry['status']}"
                            for name, entry in files.items())

            def sample(command, **kwargs):
                (directory / "monitor.stop").touch()
                return subprocess.CompletedProcess(command, 0, raw, "raw diagnostic stderr")

            with mock.patch.object(subprocess, "run", side_effect=sample), mock.patch("time.sleep"):
                TELEMETRY["monitor"]("task-container", directory)
            rows = [json.loads(line) for line in (directory / "resources.jsonl").read_text().splitlines()]
            self.assertEqual(1, len(rows))
            self.assertEqual(raw, rows[0]["stdout"])
            self.assertEqual(files, rows[0]["files"])
            self.assertEqual([], TELEMETRY["resource_errors"](rows[0]["files"]))
            self.assertEqual("raw diagnostic stderr", rows[0]["heap"]["stderr"])
            command = json.loads((directory / "monitor-command.json").read_text())["argv"]
            self.assertIn("/sys/fs/cgroup/cpu.stat", command[-1])
            self.assertIn("/proc/1/io", command[-1])

    def test_protocol_keeps_early_read_failure_before_later_success(self):
        """Per-file framing retains an early missing counter even when the shell's last cat succeeds."""
        files = resource_fixture()
        files["/sys/fs/cgroup/memory.current"] = {"status": 1, "text": ""}
        raw = "\n".join(f"@@FILE {name}\n{entry['text']}\n@@STATUS {name} {entry['status']}"
                        for name, entry in files.items())
        parsed = TELEMETRY["parsed_files"](raw)
        self.assertEqual(0, parsed["/proc/1/smaps_rollup"]["status"])
        self.assertEqual(1, parsed["/sys/fs/cgroup/memory.current"]["status"])
        self.assertIn("/sys/fs/cgroup/memory.current", TELEMETRY["resource_errors"](parsed))

    def test_summary_keeps_both_labels_and_java_schema(self):
        """Derivation supports the Java schema and never replaces earlier labels' raw samples/logs."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            for label, latency in (("default", 10), ("extra", 90)):
                client = directory / label / "client"
                client.mkdir(parents=True)
                (client / "queries.csv").write_text(
                    f"query,round,warmup,latency_ns,ok\nq01,0,1,1000,1\nq01,1,0,{latency},1\n")
                (client.parent / "mvn.log").write_text(label + " full log\n")
            TELEMETRY["summarize"](directory)
            for label, latency in (("default", 10), ("extra", 90)):
                path = directory / label / "client/query-percentiles.json"
                self.assertEqual(latency, json.loads(path.read_text())["queries"]["q01"]["p99_ns"])
                self.assertEqual(label + " full log\n", (directory / label / "mvn.log").read_text())
            self.assertIn("default/client/queries.csv", (directory / "SHA256SUMS").read_text())
            self.assertIn("extra/client/queries.csv", (directory / "SHA256SUMS").read_text())

    def test_complete_requested_run_with_no_gc_is_valid(self):
        """A real heap snapshot and GC initialization are enough when no collection occurs."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            valid_run(directory)
            TELEMETRY["validate"](directory)
            self.assertEqual([], json.loads((directory / "telemetry-validation.json").read_text())["errors"])
        for text in ("garbage-first heap total 1024K, used 512K", "ZHeap used 4M, capacity 64M",
                     "PSYoungGen total 1024K, used 512K", "def new generation total 1024K, used 512K"):
            self.assertTrue(TELEMETRY["heap_evidence"](text))
        self.assertFalse(TELEMETRY["heap_evidence"]("no actual heap information"))

    def test_validation_requires_every_requested_slot_and_label(self):
        """Missing final rounds, clients, warmups and duplicate rows cannot pass a name-only check."""
        cases = {
            "missing_last_round": lambda rows: [row for row in rows if row["round"] != 2],
            "missing_client": lambda rows: [row for row in rows if row["client"] != 1],
            "missing_warmup": lambda rows: [row for row in rows if row["round"] != 0],
            "duplicate_warmup": lambda rows: rows + [rows[0]],
            "warmup_only_duplicates": lambda rows: [rows[0], rows[0]],
        }
        with tempfile.TemporaryDirectory() as temp:
            for name, mutate in cases.items():
                with self.subTest(case=name):
                    directory = Path(temp) / name
                    label, rows = valid_run(directory)
                    write_rows(label, mutate(rows))
                    with self.assertRaisesRegex(ValueError, "incomplete query slots"):
                        TELEMETRY["validate"](directory)
                    self.assertTrue((label / "client/queries.csv").exists())
            directory = Path(temp) / "omitted_label"
            valid_run(directory)
            plan = json.loads((directory / "run-plan.json").read_text())
            plan["labels"]["extra"] = {"client": "java", "clients": 2}
            (directory / "run-plan.json").write_text(json.dumps(plan))
            with self.assertRaisesRegex(ValueError, "extra:"):
                TELEMETRY["validate"](directory)
            (directory / "run-plan.json").unlink()
            with self.assertRaisesRegex(ValueError, "requested run plan"):
                TELEMETRY["validate"](directory)

    def test_oracle_gate_rejects_wrong_values_and_mismatched_versions(self):
        """A successful complete client whose values disagree with generation cannot pass ranking."""
        with tempfile.TemporaryDirectory() as temp:
            for case in ("wrong_checksum", "wrong_rows", "old_raw_version", "old_reference_version", "missing_reference"):
                with self.subTest(case=case):
                    directory = Path(temp) / case
                    label, rows = valid_run(directory)
                    if case == "wrong_checksum":
                        # Every repeated answer agrees with itself, but the independent oracle says 1.
                        for row in rows:
                            row["checksum"] = "0000000000000002"
                    elif case == "wrong_rows":
                        rows[0]["rows"] = 2
                    elif case == "old_raw_version":
                        rows[0]["checksum_version"] = "legacy"
                    elif case == "old_reference_version":
                        (directory / "queries/checksum-version.txt").write_text("legacy")
                    else:
                        (directory / "queries/q01.checksum").unlink()
                    write_rows(label, rows)
                    with self.assertRaises(ValueError):
                        TELEMETRY["validate"](directory)
                    self.assertTrue((label / "client/queries.csv").exists())

    def test_validation_rejects_unreadable_or_stale_jfr_evidence(self):
        """Nonempty files, failed extraction, invalid JSON and post-extraction changes cannot prove readability."""
        cases = {
            "empty_recording": ("server.jfr", b""),
            "malformed_recording": ("server.jfr", b"not a recording"),
            "changed_recording": ("server.jfr", b"FLR\0" + bytes(101)),
            "bad_summary": ("server-jfr-summary.txt", "not a summary"),
            "bad_events": ("server-jfr-events.json", "not json"),
            "empty_events": ("server-jfr-events.json", '{"recording":{"events":[]}}'),
            "bad_gc": ("gc.log", "not gc evidence"),
        }
        with tempfile.TemporaryDirectory() as temp:
            for name, (filename, value) in cases.items():
                with self.subTest(case=name):
                    directory = Path(temp) / name
                    label, _ = valid_run(directory)
                    path = label / filename
                    path.write_bytes(value) if isinstance(value, bytes) else path.write_text(value)
                    with self.assertRaises(ValueError):
                        TELEMETRY["validate"](directory)
                    self.assertTrue(path.exists())
            directory = Path(temp) / "failed_extraction"
            label, _ = valid_run(directory)
            path = label / "server-jfr-extraction.json"
            extracted = json.loads(path.read_text())
            extracted["events_status"] = 1
            path.write_text(json.dumps(extracted))
            with self.assertRaisesRegex(ValueError, "JFR extraction failed"):
                TELEMETRY["validate"](directory)

    def test_validation_rejects_partial_fields_despite_outer_success(self):
        """A final successful cat and zero exit code cannot hide required fields that failed earlier."""
        with tempfile.TemporaryDirectory() as temp:
            for case in ("missing_file", "failed_read", "missing_cpu_field", "fake_heap"):
                with self.subTest(case=case):
                    directory = Path(temp) / case
                    label, _ = valid_run(directory)
                    path = label / "resources.jsonl"
                    sample = json.loads(path.read_text())
                    if case == "missing_file":
                        del sample["files"]["/sys/fs/cgroup/memory.current"]
                    elif case == "failed_read":
                        sample["files"]["/proc/1/io"]["status"] = 1
                    elif case == "missing_cpu_field":
                        sample["files"]["/sys/fs/cgroup/cpu.stat"]["text"] = "nr_periods 1"
                    else:
                        sample["heap"]["stdout"] = "no actual heap information"
                    path.write_text(json.dumps(sample) + "\n")
                    with self.assertRaisesRegex(ValueError, "heap usage" if case == "fake_heap" else "required process/cgroup fields"):
                        TELEMETRY["validate"](directory)

    def test_explicit_plan_preserves_all_configurations_and_effective_clients(self):
        """ADBC/plain use one client while Java uses the requested count, with all optional labels listed."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            valid_run(directory)
            inputs = json.loads((directory / "inputs.json").read_text())
            inputs.update(client="adbc", clients="1", quick=False, baseline="ref", extra="-Dsetting=true",
                          more="one||three|", plain=True)
            (directory / "inputs.json").write_text(json.dumps(inputs))
            TELEMETRY["write_plan"](directory)
            plan = json.loads((directory / "run-plan.json").read_text())
            self.assertEqual(["default", "threads4", "baseline", "extra", "more1", "more2", "more3", "plain"],
                             list(plan["labels"]))
            self.assertTrue(all(value["clients"] == 1 for value in plan["labels"].values()))
            self.assertEqual(2, plan["rounds"])

    def test_plan_accepts_omitted_empty_dispatch_inputs(self):
        """GitHub's sparse toJSON payload must preserve requested labels without requiring empty keys."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            valid_run(directory)
            required = {"rounds": "2", "clients": "1", "client": "adbc", "quick": True, "plain": False}
            cases = (
                ({}, ["default"]),
                ({"extra": "-Darrowflight.experimental.integerLiteralParameters=true",
                  "more": "-DresourceProfile=speed|-DresourceProfile=speed -Darrowflight.experimental.integerLiteralParameters=true"},
                 ["default", "extra", "more1", "more2"]),
                ({"baseline": "requested-ref"}, ["default", "baseline"]),
                ({"baseline": "", "extra": "", "more": "", "s3latency": ""}, ["default"]),
            )
            for optional, labels in cases:
                with self.subTest(optional=optional):
                    requested = {**required, **optional}
                    (directory / "inputs.json").write_text(json.dumps(requested))
                    TELEMETRY["write_plan"](directory)
                    plan = json.loads((directory / "run-plan.json").read_text())
                    self.assertEqual(labels, list(plan["labels"]))
                    self.assertTrue(all(value["clients"] == 1 for value in plan["labels"].values()))
                    self.assertEqual(2, plan["rounds"])
                    self.assertEqual(requested, json.loads((directory / "inputs.json").read_text()))

    def test_python_clients_preserve_checksums_and_actual_table_chunks(self):
        """Both clients report the same checksum, referenced buffer size and observed table chunks."""
        table = mock.Mock(num_rows=2, nbytes=40)
        table.to_batches.return_value = [object(), object()]
        table.to_pylist.return_value = [{"x": 1.25}, {"x": 2.5}]
        adbc = types.ModuleType("adbc_driver_flightsql")
        adbc.dbapi = types.ModuleType("adbc_driver_flightsql.dbapi")
        duckdb = types.ModuleType("duckdb")
        with mock.patch.dict(sys.modules, {"adbc_driver_flightsql": adbc,
                                         "adbc_driver_flightsql.dbapi": adbc.dbapi, "duckdb": duckdb}):
            adbc_client = runpy.run_path(str(HERE / "tpch-adbc.py"))
            plain_client = runpy.run_path(str(HERE / "tpch-duckdb.py"))
        cursor = mock.Mock()
        cursor.fetch_arrow_table.return_value = table
        cursor.execute.return_value.fetch_arrow_table.return_value = table
        self.assertEqual(adbc_client["read"](cursor, "query"), plain_client["read"](cursor, "query"))
        self.assertEqual((2, 40, 2), tuple(adbc_client["read"](cursor, "query")[index] for index in (0, 2, 3)))

    def test_adbc_exception_flushes_failed_attempt_and_keeps_print_format(self):
        """A measured read failure retains its row after the successful warmup's compatible output."""
        table = mock.Mock(num_rows=2, nbytes=40)
        table.to_batches.return_value = [object(), object()]
        table.to_pylist.return_value = [{"x": 1}, {"x": 2}]
        adbc = types.ModuleType("adbc_driver_flightsql")
        adbc.dbapi = types.ModuleType("adbc_driver_flightsql.dbapi")
        adbc.dbapi.connect = mock.MagicMock()
        connection = adbc.dbapi.connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetch_arrow_table.side_effect = [table, RuntimeError("read failed")]
        with mock.patch.dict(sys.modules, {"adbc_driver_flightsql": adbc, "adbc_driver_flightsql.dbapi": adbc.dbapi}):
            client = runpy.run_path(str(HERE / "tpch-adbc.py"))
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            queries = directory / "queries"
            queries.mkdir()
            (queries / "q01.sql").write_text("select 1")
            (queries / "q01.rows").write_text("2")
            output = io.StringIO()
            with mock.patch.dict(os.environ, {"TPCH_RAW_DIR": str(directory / "client")}), \
                    mock.patch.object(sys, "argv", ["tpch-adbc", "127.0.0.1:32010", str(queries), "1"]), \
                    contextlib.redirect_stdout(output), self.assertRaisesRegex(RuntimeError, "read failed"):
                client["main"]()
            self.assertRegex(output.getvalue(), r"q01 r0\s+[0-9.]+ ms rows 2 sum [0-9a-f]{16}")
            self.assertIn("total r0", output.getvalue())
            with (directory / "client/samples.csv").open() as source:
                samples = list(csv.DictReader(source))
            self.assertEqual(2, len(samples))
            self.assertEqual(("1", "0", "RuntimeError"),
                             tuple(samples[1][name] for name in ("round", "ok", "error_type")))
            self.assertFalse(json.loads((directory / "client/client-summary.json").read_text())["completed"])


if __name__ == "__main__":
    unittest.main()
