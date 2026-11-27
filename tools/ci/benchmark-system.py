#!/usr/bin/env python3
"""Collect optional, scoped runner/kernel pressure evidence without changing benchmark gates."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


HOST_PERIODIC_FILES = ("/proc/stat", "/proc/pressure/cpu", "/proc/pressure/memory", "/proc/pressure/io")
HOST_SNAPSHOT_FILES = ("/proc/meminfo", *HOST_PERIODIC_FILES)
CGROUP_PRESSURE_FILES = tuple("/sys/fs/cgroup/" + name + ".pressure" for name in ("cpu", "memory", "io"))
DOCKER_INFO_FORMAT = ('{"ncpu":{{.NCPU}},"mem_total_bytes":{{.MemTotal}},'
                      '"storage_driver":{{json .Driver}},"cgroup_driver":{{json .CgroupDriver}},'
                      '"cgroup_version":{{json .CgroupVersion}}}')


def read_optional(path, scope):
    """Retain each optional file's own status, including unsupported-kernel and permission errors."""
    sample = {"timestamp_ns": time.time_ns(), "scope": scope, "optional": True}
    try:
        sample.update(status=0, text=Path(path).read_text(errors="replace"), error="", error_type="")
    except OSError as error:
        sample.update(status=error.errno or 1, text="", error=str(error), error_type=type(error).__name__)
    return sample


def host_sample(memory=False):
    """Read runner-host kernel counters, which include clients and unrelated host work."""
    paths = HOST_SNAPSHOT_FILES if memory else HOST_PERIODIC_FILES
    return {"timestamp_ns": time.time_ns(), "scope": "runner_host", "optional": True,
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "cpu_fields": "user nice system idle iowait irq softirq steal guest guest_nice",
            "psi_units": "avg10/60/300 percent; total microseconds",
            "files": {path: read_optional(path, "runner_host") for path in paths}}


def optional_command(command, scope):
    """Retain optional diagnostic failures and timeout output without affecting trial status."""
    sample = {"timestamp_ns": time.time_ns(), "scope": scope, "optional": True, "argv": command}
    try:
        result = subprocess.run(command, capture_output=True, text=True, errors="replace", timeout=10)
        sample.update(status=result.returncode, stdout=result.stdout, stderr=result.stderr)
    except OSError as error:
        sample.update(status=error.errno or 1, stdout="", stderr=str(error))
    except subprocess.TimeoutExpired as error:
        def decoded(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        sample.update(status=124, stdout=decoded(error.stdout), stderr=decoded(error.stderr) + "\ncollector timed out\n")
    return sample


def save_host_snapshot(path, filesystem):
    """Save host memory/pressure and capacity evidence in an existing task-owned output directory."""
    sample = host_sample(memory=True)
    sample["capacity"] = {
        "filesystems": optional_command(["df", "-Pk", "--", "/", str(filesystem)], "runner_host_filesystems"),
        # No daemon hostname, environment, proxy URLs, registry config, labels or credentials.
        "docker": optional_command(["docker", "info", "--format", DOCKER_INFO_FORMAT], "docker_daemon"),
    }
    try:
        path.write_text(json.dumps(sample, indent=2) + "\n")
        return True
    except OSError as error:
        print("Optional system snapshot could not be saved: " + type(error).__name__, file=sys.stderr)
        return False


def framed_read_command(paths):
    """Frame fixed file reads with scope and individual status for an in-container shell."""
    return ("for f in " + " ".join(shlex.quote(str(path)) for path in paths) + "; "
            "do case \"$f\" in /sys/fs/cgroup/*) scope=container_cgroup;; "
            "/proc/1/*) scope=container_process;; *) scope=container_kernel_hostwide;; esac; "
            "printf '\\n@@SCOPE %s\\n@@FILE %s\\n' \"$scope\" \"$f\"; "
            "cat \"$f\" 2>&1; code=$?; printf '\\n@@STATUS %s %s\\n' \"$f\" \"$code\"; done")


def wrapped_trial(command, output="/output", paths=None):
    """Capture kernel/cgroup snapshots around a child while preserving exits and forwarding signals."""
    paths = (*HOST_SNAPSHOT_FILES, *CGROUP_PRESSURE_FILES) if paths is None else paths
    snapshot = framed_read_command(paths)
    script = """snapshot() {
  { printf '@@TIMESTAMP_NS %s\\n' "$(date +%s%N)";
    @@READ_FILES@@
  } > @@OUTPUT@@/system-container-"$1".txt
}
on_exit() {
  status=$?
  trap - EXIT
  snapshot after || :
  exit "$status"
}
trap on_exit EXIT
snapshot before || :
env --default-signal=INT,QUIT -- "$@" &
trial_pid=$!
trap 'kill -TERM "$trial_pid" 2>/dev/null || :' TERM
trap 'kill -INT "$trial_pid" 2>/dev/null || :' INT
while :; do
  wait "$trial_pid"
  status=$?
  kill -0 "$trial_pid" 2>/dev/null || break
done
exit "$status"
""".replace("@@READ_FILES@@", snapshot).replace("@@OUTPUT@@", shlex.quote(str(output)))
    return ["sh", "-c", script, "benchmark-trial", *command]


def main():
    """Write an optional prepare-time snapshot without making unavailable counters mandatory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--filesystem", type=Path, required=True)
    args = parser.parse_args()
    save_host_snapshot(args.output, args.filesystem)


if __name__ == "__main__":
    main()
