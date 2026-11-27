"""Check optional scoped system snapshots without Docker or a benchmark workload."""

import errno
import importlib.util
import json
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("benchmark_system", Path(__file__).with_name("benchmark-system.py"))
SYSTEM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SYSTEM)


class BenchmarkSystemTest(unittest.TestCase):
    """Keep unsupported metrics nonfatal and trial wrappers faithful to child exits and arguments."""

    def test_optional_files_keep_unsupported_and_permission_statuses(self):
        """Missing PSI support and access errors retain distinct per-file evidence and scope."""
        for error in (FileNotFoundError(errno.ENOENT, "unsupported kernel"),
                      PermissionError(errno.EACCES, "not permitted")):
            with self.subTest(error=type(error).__name__), mock.patch.object(Path, "read_text", side_effect=error):
                sample = SYSTEM.read_optional("/proc/pressure/cpu", "runner_host")
                self.assertEqual(error.errno, sample["status"])
                self.assertEqual(type(error).__name__, sample["error_type"])
                self.assertEqual("runner_host", sample["scope"])
                self.assertTrue(sample["optional"])
                self.assertEqual("", sample["text"])

    def test_host_snapshot_retains_memory_capacity_and_safe_docker_fields(self):
        """The prepare snapshot keeps MemTotal/Available and capacity without broad daemon or env dumps."""
        def read(path, **kwargs):
            if str(path) == "/proc/meminfo":
                return "MemTotal: 8000000 kB\nMemAvailable: 4000000 kB\n"
            if str(path).startswith("/proc/pressure/"):
                raise FileNotFoundError(errno.ENOENT, "unsupported PSI")
            return "cpu 1 2 3 4 5 6 7 8 9 10\n"

        with tempfile.TemporaryDirectory() as temp, mock.patch.object(Path, "read_text", read), \
                mock.patch.object(SYSTEM.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "capacity", "")) as run:
            path = Path(temp) / "host.json"
            self.assertTrue(SYSTEM.save_host_snapshot(path, Path(temp)))
            with path.open() as source:
                snapshot = json.load(source)
            self.assertIn("MemAvailable", snapshot["files"]["/proc/meminfo"]["text"])
            self.assertEqual(errno.ENOENT, snapshot["files"]["/proc/pressure/io"]["status"])
            self.assertEqual("runner_host_filesystems", snapshot["capacity"]["filesystems"]["scope"])
            self.assertEqual("docker_daemon", snapshot["capacity"]["docker"]["scope"])
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(["df", "-Pk", "--", "/", temp], commands[0])
            self.assertEqual(["docker", "info", "--format", SYSTEM.DOCKER_INFO_FORMAT], commands[1])
            self.assertEqual({"NCPU", "MemTotal", "Driver", "CgroupDriver", "CgroupVersion"},
                             set(re.findall(r"\.([A-Za-z]+)", SYSTEM.DOCKER_INFO_FORMAT)))

    def test_command_failures_and_unsavable_snapshot_are_nonfatal(self):
        """Timeout output and unavailable commands survive while an optional write cannot mask a trial."""
        with mock.patch.object(SYSTEM.subprocess, "run", side_effect=subprocess.TimeoutExpired(
                ["docker"], 10, output=b"partial", stderr=b"timeout")):
            sample = SYSTEM.optional_command(["docker", "info"], "docker_daemon")
            self.assertEqual(124, sample["status"])
            self.assertEqual("partial", sample["stdout"])
        with mock.patch.object(SYSTEM.subprocess, "run", side_effect=FileNotFoundError(errno.ENOENT, "missing")):
            self.assertEqual(errno.ENOENT, SYSTEM.optional_command(["df"], "runner_host")["status"])
        with mock.patch.object(SYSTEM, "host_sample", return_value={}), \
                mock.patch.object(SYSTEM, "optional_command", return_value={}), \
                mock.patch.object(Path, "write_text", side_effect=PermissionError("read-only")):
            self.assertFalse(SYSTEM.save_host_snapshot(Path("ignored.json"), Path(".")))

    def test_framed_reads_keep_an_early_failure_before_later_success(self):
        """Individual optional file statuses cannot be hidden by a successful final read."""
        with tempfile.TemporaryDirectory() as temp:
            existing = Path(temp) / "counter with spaces"
            missing = Path(temp) / "unsupported"
            existing.write_text("some avg10=0.00 total=3\n")
            result = subprocess.run(["sh", "-c", SYSTEM.framed_read_command([missing, existing])],
                                    capture_output=True, text=True, check=True)
            self.assertIn(f"@@STATUS {missing} 1", result.stdout)
            self.assertIn(f"@@STATUS {existing} 0", result.stdout)
            self.assertIn("@@SCOPE container_kernel_hostwide", result.stdout)

    def test_trial_wrapper_preserves_failure_and_literal_arguments(self):
        """Unsupported reads affect neither a successful child nor a failing child's exit status."""
        for code in (0, 7):
            with self.subTest(exit_code=code), tempfile.TemporaryDirectory(prefix="benchmark system ") as temp:
                output = Path(temp)
                missing = output / "unsupported"
                sentinel = output / "previous.txt"
                sentinel.write_text("preserved")
                literal = "-Dproperty=$(printf not-executed)"
                command = [sys.executable, "-c", f"import sys; print(sys.argv[1]); sys.exit({code})", literal]
                result = subprocess.run(SYSTEM.wrapped_trial(command, output, [missing]), capture_output=True, text=True)
                self.assertEqual(code, result.returncode)
                self.assertEqual(literal, result.stdout.strip())
                for phase in ("before", "after"):
                    text = (output / f"system-container-{phase}.txt").read_text()
                    self.assertIn(f"@@STATUS {missing} 1", text)
                    self.assertIn("@@TIMESTAMP_NS", text)
                self.assertEqual("preserved", sentinel.read_text())
                self.assertFalse((output / "resources.csv").exists())

    def test_trial_wrapper_forwards_termination_and_keeps_after_snapshot(self):
        """TERM and INT reach the child without inheriting an asynchronous shell's ignored INT."""
        for sent, expected in ((signal.SIGTERM, 43), (signal.SIGINT, 130)):
            with self.subTest(signal=sent), tempfile.TemporaryDirectory() as temp:
                output = Path(temp)
                ready, stopped = output / "ready", output / "stopped"
                program = ("import pathlib,signal,sys,time; "
                           "signal.signal(signal.SIGTERM, lambda *args: "
                           "(pathlib.Path(sys.argv[2]).write_text('TERM'), sys.exit(43))); "
                           "pathlib.Path(sys.argv[1]).write_text(str(signal.getsignal(signal.SIGINT)==signal.SIG_IGN)); "
                           "time.sleep(30)")
                command = SYSTEM.wrapped_trial([sys.executable, "-c", program, str(ready), str(stopped)], output, [])
                process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                try:
                    deadline = time.monotonic() + 3
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(ready.exists(), "child did not initialize")
                    self.assertEqual("False", ready.read_text())
                    process.send_signal(sent)
                    process.communicate(timeout=3)
                    self.assertEqual(expected, process.returncode)
                    if sent == signal.SIGTERM:
                        self.assertEqual("TERM", stopped.read_text())
                    self.assertTrue((output / "system-container-after.txt").is_file())
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
