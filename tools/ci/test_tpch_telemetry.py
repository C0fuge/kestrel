"""Exercise TPC-H raw evidence and failure retention without containers or client packages."""

import contextlib
import csv
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

    def test_monitor_flushes_raw_process_and_heap_evidence(self):
        """A complete sampling interval retains unparsed counters plus the heap diagnostic."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)

            def sample(command, **kwargs):
                (directory / "monitor.stop").touch()
                return subprocess.CompletedProcess(command, 0, "raw counter output", "raw diagnostic stderr")

            with mock.patch.object(subprocess, "run", side_effect=sample), mock.patch("time.sleep"):
                TELEMETRY["monitor"]("task-container", directory)
            rows = [json.loads(line) for line in (directory / "resources.jsonl").read_text().splitlines()]
            self.assertEqual(1, len(rows))
            self.assertEqual("raw counter output", rows[0]["stdout"])
            self.assertEqual("raw diagnostic stderr", rows[0]["heap"]["stderr"])
            command = json.loads((directory / "monitor-command.json").read_text())["argv"]
            self.assertIn("/sys/fs/cgroup/cpu.stat", command[-1])
            self.assertIn("/proc/1/io", command[-1])

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

    def test_validation_refuses_partial_or_missing_metrics(self):
        """Printed timings alone cannot pass the telemetry gate."""
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / "queries").mkdir()
            (directory / "queries/q01.sql").write_text("select 1")
            label = directory / "default"
            (label / "client").mkdir(parents=True)
            (label / "container-argv.json").write_text("[]")
            with self.assertRaisesRegex(ValueError, "Incomplete telemetry"):
                TELEMETRY["validate"](directory)
            (label / "client-exit-status.txt").write_text("0")
            for name in ("container.json", "server.log", "mvn.log"):
                (label / name).write_text("complete evidence")
            (label / "resources.jsonl").write_text('{"status":0,"heap":{"status":0}}\n')
            (label / "server.jfr").write_bytes(b"recording")
            (label / "gc.log").write_text("gc")
            (label / "client/queries.csv").write_text("query,warmup,ok\nq01,0,1\n")
            TELEMETRY["validate"](directory)
            self.assertEqual([], json.loads((directory / "telemetry-validation.json").read_text())["errors"])
            (label / "client/queries.csv").write_text("query,warmup,ok\nq01,0,0\n")
            with self.assertRaisesRegex(ValueError, "failed or incomplete"):
                TELEMETRY["validate"](directory)

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
