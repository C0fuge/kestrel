"""Optional append-only TPC-H client evidence, independent of client dependencies."""

import csv
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import sys
import time


FIELDS = ("query", "round", "warmup", "timestamp_ns", "latency_ns", "rows", "checksum",
          "arrow_buffer_bytes", "table_batches", "expected_rows", "ok", "error_type")


def percentiles(values):
    """Keep the observed tails without interpolation or dropping individual samples."""
    values = sorted(values)
    return {name: values[min(len(values) - 1, int(len(values) * fraction))]
            for name, fraction in (("p50_ns", .5), ("p95_ns", .95), ("p99_ns", .99), ("max_ns", 1))}


class RawRun:
    """Flush every query before proceeding so failures retain completed and failed samples."""

    def __init__(self, directory=None):
        requested = directory or os.environ.get("TPCH_RAW_DIR")
        self.directory = Path(requested) if requested else None
        self.samples = []
        self.started_ns = time.time_ns()
        self.output = None
        if self.directory is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.output = (self.directory / "samples.csv").open("x", newline="")
            self.writer = csv.DictWriter(self.output, fieldnames=FIELDS)
            self.writer.writeheader()
            self.output.flush()
            versions = {}
            for package in ("pyarrow", "adbc-driver-flightsql", "adbc-driver-manager", "duckdb"):
                try:
                    versions[package] = importlib.metadata.version(package)
                except importlib.metadata.PackageNotFoundError:
                    pass
            self.save("client.json", {"started_ns": self.started_ns, "argv": sys.argv,
                      "python": sys.version, "versions": versions,
                      "bytes_semantics": "Arrow table.nbytes: referenced buffer ranges, never wire bytes",
                      "batches_semantics": "len(table.to_batches()): table chunk layout, never transport batches"})

    def save(self, name, value):
        """Write supplementary metadata while retaining the append-only samples."""
        if self.directory is not None:
            (self.directory / name).write_text(json.dumps(value, indent=2) + "\n")

    def record(self, query, round_number, timestamp_ns, latency_ns, answer, expected_rows, error_type=""):
        """Persist success, row mismatches, and execution failures with explicit status."""
        if self.output is None:
            return
        rows, checksum, size, batches = answer if answer is not None else (-1, 0, -1, -1)
        correct = answer is not None and (rows == expected_rows if expected_rows >= 0 else rows > 0)
        sample = dict(zip(FIELDS, (query, round_number, int(round_number == 0), timestamp_ns, latency_ns,
                                  rows, f"{checksum:016x}", size, batches, expected_rows, int(correct),
                                  error_type or ("" if correct else "answer_mismatch"))))
        self.writer.writerow(sample)
        self.output.flush()
        if not self.samples:
            self.save("first-query.json", {**sample, "client_to_first_completion_ns":
                      timestamp_ns + latency_ns - self.started_ns})
        self.samples.append(sample)

    def close(self, completed=False):
        """Write measured-query percentiles and a completion marker even on client failure."""
        if self.output is None:
            return
        self.output.close()
        measured = [row for row in self.samples if not row["warmup"] and row["ok"]]
        self.save("client-summary.json", {
            "completed": completed, "finished_ns": time.time_ns(), "samples": len(self.samples),
            "failed_samples": sum(not row["ok"] for row in self.samples),
            "process_cpu_seconds": resource.getrusage(resource.RUSAGE_SELF).ru_utime
                                   + resource.getrusage(resource.RUSAGE_SELF).ru_stime,
            "process_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "queries": {query: {"count": len(rows), **percentiles([row["latency_ns"] for row in rows]),
                                "arrow_buffer_bytes": sum(row["arrow_buffer_bytes"] for row in rows),
                                "table_batches": sum(row["table_batches"] for row in rows)}
                        for query in sorted({row["query"] for row in measured})
                        if (rows := [row for row in measured if row["query"] == query])},
        })
