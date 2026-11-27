#!/usr/bin/env python3
"""Check public screening argument boundaries and the answer-equivalence gate."""

import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("screen", Path(__file__).with_name("optimization-screen.py"))
SCREEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCREEN)


class OptimizationScreenTest(unittest.TestCase):
    """Checks safety and correctness decisions without starting containers."""

    def test_rejects_shell_and_non_server_properties(self):
        """Workflow input cannot introduce commands or Maven/output settings."""
        for value in ("-DduckDbThreads=2;uname", "-Dmaven.repo.local=/tmp/other", "-Dperf.output=/elsewhere",
                      "-Dperf.heavyEvery=8", "-Dperf.warmupSeconds=30", "-Dperf.warmupMode=per-step",
                      "-DduckDbPassword=secret", "--settings /tmp/settings.xml", "-DduckDbThreads=4",
                      "-DduckDbSharedDatabase=true", "-DduckDbMemoryLimit=1GB"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                SCREEN.checked_options(value)
        self.assertEqual(["-Darrowflight.experimental.integerLiteralParameters=true", "-Dperf.producingStreams=2"],
                         SCREEN.checked_options("-Darrowflight.experimental.integerLiteralParameters=true "
                                                "-Dperf.producingStreams=2"))
        self.assertEqual(["-Dperf.threads=4", "-Dperf.shared=4:1GB"],
                         SCREEN.checked_options("-Dperf.threads=4 -Dperf.shared=4:1GB"))

    def test_heavy_interval_is_bounded(self):
        """A common workload interval accepts both boundaries and rejects malformed input."""
        for value in ("0", "1", "8", "64"):
            self.assertEqual(int(value), SCREEN.checked_heavy_every(value))
        for value in ("-1", "65", "1.5", "eight", "8 -Dperf.seconds=60"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                SCREEN.checked_heavy_every(value)

    def test_measurement_budget_leaves_time_to_upload(self):
        """Bound excessive dispatch inputs before a long cloud run can start."""
        self.assertEqual(1230, SCREEN.planned_measurement_seconds(3, 15, [1, 8, 32]))
        self.assertLess(SCREEN.planned_measurement_seconds(3, 15, [1, 8, 32]), 1500)
        with self.assertRaises(ValueError):
            SCREEN.planned_measurement_seconds(5, 60, list(range(1, 65)))
        with self.assertRaises(ValueError):
            SCREEN.planned_measurement_seconds(5, 60, [1, 2, 4, 8, 16, 32, 64])

    def test_warmup_configuration_is_bounded_and_default_plan_is_unchanged(self):
        """Common controls reject malformed input and retain one automatic eight-client warmup by default."""
        for value in ("0", "1", "120"):
            self.assertEqual(int(value), SCREEN.checked_warmup_seconds(value))
        for value in ("-1", "121", "", "1.5", "later", "8 -Dperf.seconds=60"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                SCREEN.checked_warmup_seconds(value)
        for mode in ("legacy", "per-step"):
            self.assertEqual(mode, SCREEN.checked_warmup_mode(mode))
        for mode in ("", "each", "PER_STEP"):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                SCREEN.checked_warmup_mode(mode)
        self.assertEqual({"mode": "legacy", "configured_seconds": 0,
                          "steps": [{"label": "warmup-c8", "clients": 8, "seconds": 5}]},
                         SCREEN.warmup_plan(15, [1, 8, 32]))
        self.assertEqual(306, SCREEN.nominal_measurement_seconds(3, 15, [1, 8, 32]))

    def test_budget_counts_each_warmup_and_the_longer_unranked_preflight(self):
        """Every warmup, measured and preflight step can consume its own thirty-second drain."""
        self.assertEqual(1223, SCREEN.planned_measurement_seconds(2, 15, [1, 8, 32], 12, "per-step"))
        self.assertEqual(613, SCREEN.planned_measurement_seconds(1, 3, [8], 120, "per-step"))
        self.assertLess(SCREEN.planned_measurement_seconds(3, 15, [1, 8, 32], 30, "legacy"), 1500)
        self.assertEqual(1067, SCREEN.planned_measurement_seconds(2, 30, [8, 32], 20, "per-step"))
        with self.assertRaises(ValueError):
            SCREEN.planned_measurement_seconds(3, 15, [1, 8, 32], 30, "per-step")

    def test_requested_warmup_plan_must_be_emitted_exactly(self):
        """An older or ineffective harness cannot silently ignore common warmup flags."""
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            with self.assertRaisesRegex(ValueError, "updated candidate harness"):
                SCREEN.verify_warmup_plan(folder, 15, [8, 32], 7, "per-step")
            actual = ("warmup_mode,configured_warmup_seconds,step,label,clients,seconds,measured\n"
                      "per-step,7,1,warmup-step-1-c8,8,7,false\n"
                      "per-step,7,2,c8,8,15,true\n"
                      "per-step,7,3,warmup-step-2-c32,32,7,false\n"
                      "per-step,7,4,c32,32,15,true\n")
            path = folder / "warmup-plan.csv"
            path.write_text(actual)
            SCREEN.verify_warmup_plan(folder, 15, [8, 32], 7, "per-step")
            for replacement in (actual.replace("warmup-step-2-c32", "warmup-c8"),
                                actual.replace(",32,7,false", ",8,7,false"),
                                actual.replace(",8,7,false", ",8,5,false"),
                                actual.replace("per-step", "legacy")):
                path.write_text(replacement)
                with self.assertRaisesRegex(ValueError, "differs from the common request"):
                    SCREEN.verify_warmup_plan(folder, 15, [8, 32], 7, "per-step")
            path.write_text(actual.splitlines()[0] + "\nlegacy,7,1,warmup-c8,8,7,false\nlegacy,7,2,c1,1,15,true\n")
            SCREEN.verify_warmup_plan(folder, 15, [1], 7, "legacy")

    def test_custom_warmup_rejects_an_older_candidate_before_starting_containers(self):
        """Historical candidates remain default-compatible and must opt into the new harness for new controls."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = root / "candidate" / SCREEN.HARNESS
            harness.mkdir(parents=True)
            (harness / "SmallQueryThroughputPerfTest.java").write_text("historical harness")
            arguments = ["screen", "--baseline", str(root / "baseline"), "--candidate", str(root / "candidate"),
                         "--output", str(root / "output")]
            with mock.patch.dict(SCREEN.os.environ, {"SCREEN_WARMUP_MODE": "per-step", "SCREEN_ROUNDS": "1"}, clear=True), \
                    mock.patch("sys.argv", arguments), mock.patch.object(SCREEN, "run_container") as runner:
                with self.assertRaisesRegex(ValueError, "updated.*harness"):
                    SCREEN.main()
            runner.assert_not_called()
            self.assertFalse((root / "output").exists())

    def test_shared_single_jvm_budget_and_legacy_arguments(self):
        """Other collectors reuse the same workload/drain/setup contract without changing default flags."""
        self.assertEqual({"nominal_workload_seconds": 50, "drain_seconds": 60, "setup_seconds": 20,
                          "planned_work_seconds": 130}, SCREEN.trial_budget(30, [32], 20, "per-step"))
        self.assertEqual([], SCREEN.warmup_arguments(0, "legacy"))
        self.assertEqual(["-Dperf.warmupSeconds=20", "-Dperf.warmupMode=per-step"],
                         SCREEN.warmup_arguments(20, "per-step"))
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            SCREEN.require_warmup_harness(source, 0, "legacy")
            with self.assertRaisesRegex(ValueError, "updated.*harness"):
                SCREEN.require_warmup_harness(source, 20, "per-step")
            harness = source / SCREEN.HARNESS / "SmallQueryThroughputPerfTest.java"
            harness.parent.mkdir(parents=True)
            harness.write_text("supports perf.warmupMode")
            SCREEN.require_warmup_harness(source, 20, "per-step")

    def test_numeric_container_user_has_native_unix_identity(self):
        """Hadoop's native Unix login must resolve the runner UID even when the image cannot."""
        passwd = "root:x:0:0:root:/root:/bin/sh\n"
        groups = "root:x:0:\n"
        mapped = SCREEN.container_identity(passwd, groups, 1001, 1001)
        self.assertIn("screen-user-1001:x:1001:1001:", mapped[0])
        self.assertIn("screen-group-1001:x:1001:", mapped[1])
        self.assertEqual(mapped, SCREEN.container_identity(*mapped, 1001, 1001))
        self.assertEqual((passwd, groups), SCREEN.container_identity(passwd, groups, 0, 0))

    def test_answer_gate_checks_values_and_requires_overlap(self):
        """Different timing samples may overlap partially, but overlapping answers must match."""
        first = {(1, "0", "1", "0", "42"): ("1", "99", "8", "1")}
        self.assertEqual(1, SCREEN.matching_answers(first, dict(first)))
        for other in ({}, {(1, "0", "1", "0", "42"): ("1", "98", "8", "1")}):
            with self.assertRaises(ValueError):
                SCREEN.matching_answers(first, other)

    def test_summary_uses_raw_samples_and_retains_tail_metrics(self):
        """Summary percentiles, throughput and transfer totals come from all raw samples."""
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            with (folder / "c1.csv").open("w") as output:
                writer = csv.writer(output)
                writer.writerow(("thread", "sequence", "query_kind", "key", "start_ns", "latency_ns",
                                 "rows", "checksum", "arrow_bytes", "batches"))
                for index, latency in enumerate((1_000_000, 2_000_000, 9_000_000)):
                    writer.writerow((0, index, 2 if index == 2 else 0, index, index * 100, latency,
                                     1, index, 8, 1))
            (folder / "c1-wall.txt").write_text("wall_seconds=2\nprocess_cpu_ns=3000000\n")
            (folder / "resources.csv").write_text(
                "rss_kib,heap_used,direct_bytes,gc_count,gc_ms,threads\n10,20,30,1,2,3\n11,22,33,2,3,4\n")
            (folder / "profile.jfr").write_bytes(b"test-profile")
            summary, answers = SCREEN.summary_of(folder, [1])
            self.assertEqual(1.5, summary["1"]["qps"])
            self.assertEqual(2, summary["1"]["p50_ms"])
            self.assertEqual(9, summary["1"]["p95_ms"])
            self.assertEqual(9, summary["1"]["p99_ms"])
            self.assertEqual(24, summary["1"]["arrow_bytes"])
            self.assertEqual(3, len(answers))
            self.assertEqual(11, summary["resource_peaks"]["rss_kib"])
            self.assertEqual("legacy: no per-sample status", summary["1"]["answer_evidence"])
            self.assertEqual({"queries": 2, "qps": 1, "p50_ms": 2, "p95_ms": 2,
                              "p99_ms": 2, "max_ms": 2, "sample_fraction": 2 / 3},
                             summary["1"]["query_kinds"]["0"])
            self.assertEqual({"queries": 1, "qps": .5, "p50_ms": 9, "p95_ms": 9,
                              "p99_ms": 9, "max_ms": 9, "sample_fraction": 1 / 3},
                             summary["1"]["query_kinds"]["2"])
            lines = (folder / "c1.csv").read_text().splitlines()
            (folder / "c1.csv").write_text(lines[0] + ",ok,error_type\n" + "".join(
                line + ",1,\n" for line in lines[1:]))
            modern, _ = SCREEN.summary_of(folder, [1])
            self.assertEqual("all samples ok=1", modern["1"]["answer_evidence"])
            self.assertEqual(summary["1"]["query_kinds"], modern["1"]["query_kinds"])

    def test_summary_rejects_failed_samples_before_ranking(self):
        """Failed execution and incorrect answers stay raw evidence and cannot become fast samples."""
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            for error_type in ("answer_mismatch", "RuntimeException"):
                with self.subTest(error_type=error_type):
                    (folder / "c1.csv").write_text(
                        "thread,sequence,query_kind,key,start_ns,latency_ns,rows,checksum,arrow_bytes,batches,ok,error_type\n"
                        f"0,0,2,1,0,1000000,-1,0,-1,-1,0,{error_type}\n")
                    with self.assertRaisesRegex(ValueError, f"Failed samples.*{error_type}"):
                        SCREEN.summary_of(folder, [1])
                    self.assertFalse((folder / "summary.json").exists())
                    self.assertIn(error_type, (folder / "c1.csv").read_text())

    def test_paired_kinds_keep_short_query_tail_regression_visible(self):
        """A total throughput gain must retain slower short-query tails in each paired ratio."""
        measurements = []
        for baseline_scale in (1, 4):
            baseline = {metric: baseline_scale for metric in SCREEN.KIND_METRICS}
            fast = {**baseline, "qps": baseline_scale * 2}
            slow_tail = {**baseline, "p95_ms": baseline_scale * 3,
                         "p99_ms": baseline_scale * 5, "max_ms": baseline_scale * 7}
            measurements.append({
                "A": {"8": {**baseline, "cpu_ms_per_query": 1, "query_kinds": {"0": baseline, "2": baseline}}},
                "B": {"8": {**fast, "cpu_ms_per_query": 1, "query_kinds": {"0": slow_tail, "2": fast}}},
            })
        ratios = SCREEN.paired_ratios(measurements, [8])["8"]
        self.assertEqual({"pairs": [2, 2], "median": 2}, ratios["aggregate"]["qps"])
        self.assertEqual({"pairs": [5, 5], "median": 5}, ratios["query_kinds"]["0"]["p99_ms"])
        self.assertEqual(7, ratios["query_kinds"]["0"]["max_ms"]["median"])
        del measurements[1]["B"]["8"]["query_kinds"]["2"]
        with self.assertRaisesRegex(ValueError, "Query kinds differ"):
            SCREEN.paired_ratios(measurements, [8])

    def test_trial_failure_keeps_optional_snapshots_and_existing_cleanup(self):
        """Per-trial metadata uses the created directory, retains flags and survives a failed Maven child."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            output = root / "trial"
            snapshots = []

            def snapshot(path, filesystem):
                self.assertTrue(output.is_dir())
                self.assertFalse((output / "resources.csv").exists())
                self.assertEqual(output, filesystem)
                path.write_text("optional snapshot")
                snapshots.append(path.name)
                return True

            process = mock.Mock()
            process.poll.return_value = 7
            process.wait.return_value = 7
            with mock.patch.dict(SCREEN.SYSTEM, {"save_host_snapshot": snapshot}), \
                    mock.patch.object(SCREEN.subprocess, "Popen", return_value=process) as popen, \
                    mock.patch.object(SCREEN.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "state", "")) as run:
                with self.assertRaisesRegex(RuntimeError, "failed"):
                    SCREEN.run_container("owned-container", source, output, root / "data", root / "cache",
                                         ["test", "-Dperf.output=/output"], "pinned", "0-3", ["-Dperf.producingStreams=2"])
            command = popen.call_args.args[0]
            child = command[command.index("benchmark-trial") + 1:]
            self.assertEqual(["mvn", "-B", "-ntp", "-Dmaven.repo.local=/maven-cache", "-pl", "flight-server", "-am",
                              "test", "-Dperf.output=/output", "-Dperf.producingStreams=2"], child)
            self.assertEqual(["system-host-before.json", "system-host-after.json"], snapshots)
            self.assertTrue((output / "maven.log").exists())
            self.assertTrue((output / "container-state.txt").exists())
            self.assertEqual(["docker", "rm", "-f", "owned-container"], run.call_args.args[0])

    def test_main_passes_same_workload_to_both_sides(self):
        """The actual measurement plan shares the workload while limiting variant properties to B."""
        trials, checks, metadata = self.shared_plan({})
        self.assertEqual([], checks)
        self.assertEqual({"mode": "legacy", "configured_seconds": 0,
                          "steps": [{"label": "warmup-c8", "clients": 8, "seconds": 5}]}, metadata["warmup_plan"])
        for args in trials:
            self.assertFalse(any(value.startswith("-Dperf.warmup") for value in args[5]))

    def test_main_passes_and_verifies_custom_warmup_on_both_sides_and_preflight(self):
        """Warmup is common, verified for each actual attempt and cannot become a B-only workload change."""
        trials, checks, metadata = self.shared_plan({"SCREEN_WARMUP_MODE": "per-step", "SCREEN_WARMUP_SECONDS": "7"})
        self.assertEqual(5, len(checks))
        self.assertEqual([("preflight", 3, [1], 7, "per-step"), ("r1-A", 15, [1], 7, "per-step"),
                          ("r1-B", 15, [1], 7, "per-step"), ("r2-B", 15, [1], 7, "per-step"),
                          ("r2-A", 15, [1], 7, "per-step")], checks)
        self.assertEqual({"mode": "per-step", "configured_seconds": 7,
                          "steps": [{"label": "warmup-step-1-c1", "clients": 1, "seconds": 7}]},
                         metadata["warmup_plan"])
        for args in trials:
            self.assertEqual(1, args[5].count("-Dperf.warmupSeconds=7"))
            self.assertEqual(1, args[5].count("-Dperf.warmupMode=per-step"))

    def shared_plan(self, warmup_environment):
        """Exercise the actual runner plan with mocked containers and optional warmup controls."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("baseline", "candidate"):
                harness = root / side / SCREEN.HARNESS
                harness.mkdir(parents=True)
                for filename in ("SmallQueryThroughputPerfTest.java", "PerfRunArtifacts.java"):
                    (harness / filename).write_text("test harness perf.warmupMode")
            summary = {"1": {**dict.fromkeys(SCREEN.KIND_METRICS, 1), "cpu_ms_per_query": 1,
                             "query_kinds": {"0": {**dict.fromkeys(SCREEN.KIND_METRICS, 1),
                                                   "sample_fraction": 1}}}}
            environment = {"SCREEN_ROUNDS": "2", "SCREEN_CLIENTS": "1", "SCREEN_HEAVY_EVERY": "8",
                           "SCREEN_VARIANT_OPTIONS": "-Dperf.producingStreams=2", **warmup_environment}
            arguments = ["screen", "--baseline", str(root / "baseline"), "--candidate", str(root / "candidate"),
                         "--output", str(root / "output")]
            with mock.patch.dict(SCREEN.os.environ, environment, clear=True), \
                    mock.patch("sys.argv", arguments), \
                    mock.patch.object(SCREEN.os, "sched_getaffinity", return_value={0, 1, 2, 3}), \
                    mock.patch.object(SCREEN.Path, "home", return_value=root), \
                    mock.patch.object(SCREEN.subprocess, "check_output", return_value="test-commit"), \
                    mock.patch.object(SCREEN, "command_log", side_effect=lambda command, path: path.write_text("")), \
                    mock.patch.object(SCREEN, "run_container") as runner, \
                    mock.patch.object(SCREEN, "verify_warmup_plan") as verify_plan, \
                    mock.patch.object(SCREEN, "summary_of", return_value=(summary, {(1,): ("answer",)})):
                SCREEN.main()
            trials = [call.args for call in runner.call_args_list if "-Dperf.heavyEvery=8" in call.args[5]]
            self.assertEqual(["preflight", "r1-A", "r1-B", "r2-B", "r2-A"], [args[2].name for args in trials])
            for args in trials:
                self.assertEqual(1, args[5].count("-Dperf.heavyEvery=8"))
                self.assertEqual(["-Dperf.producingStreams=2"] if args[2].name.endswith("-B") else [], args[8])
            self.assertTrue((root / "output/paired-ratios.json").is_file())
            checks = [(call.args[0].name, *call.args[1:]) for call in verify_plan.call_args_list]
            return trials, checks, json.loads((root / "output/screen.json").read_text())


if __name__ == "__main__":
    unittest.main()
