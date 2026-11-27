#!/usr/bin/env python3
"""Check public screening argument boundaries and the answer-equivalence gate."""

import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location("screen", Path(__file__).with_name("optimization-screen.py"))
SCREEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCREEN)


class OptimizationScreenTest(unittest.TestCase):
    """Checks safety and correctness decisions without starting containers."""

    def test_rejects_shell_and_non_server_properties(self):
        """Workflow input cannot introduce commands or Maven/output settings."""
        for value in ("-DduckDbThreads=2;uname", "-Dmaven.repo.local=/tmp/other", "-Dperf.output=/elsewhere",
                      "-DduckDbPassword=secret", "--settings /tmp/settings.xml"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                SCREEN.checked_options(value)
        self.assertEqual(["-Darrowflight.experimental.integerLiteralParameters=true", "-Dperf.producingStreams=2"],
                         SCREEN.checked_options("-Darrowflight.experimental.integerLiteralParameters=true "
                                                "-Dperf.producingStreams=2"))

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
                    writer.writerow((0, index, 0, index, index * 100, latency, 1, index, 8, 1))
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


if __name__ == "__main__":
    unittest.main()
