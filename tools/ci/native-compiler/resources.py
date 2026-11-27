#!/usr/bin/env python3
"""Retain mandatory cgroup-v2 snapshots inside each short-lived compiler container."""

import argparse
import json
from pathlib import Path
import time


FILES = ("cpu.stat", "cpu.max", "memory.current", "memory.peak", "memory.events", "memory.max",
         "memory.swap.max", "io.stat", "pids.max")
SCOPE = "compiler_container_cgroup_v2"


def snapshot(output, phase, edge, recipe_status=None, root=Path("/sys/fs/cgroup")):
    """Record every read result even when one required counter is unreadable."""
    value = {"phase": phase, "edge": edge, "scope": SCOPE, "begin_epoch_ns": time.time_ns(),
             "recipe_status": recipe_status, "files": {}}
    for name in FILES:
        try:
            text = (root / name).read_text()
            result = {"status": 0, "text": text, "error_type": ""}
        except OSError as error:
            result = {"status": 1, "text": "", "error_type": type(error).__name__, "error": str(error)}
        value["files"][name] = result
    value["end_epoch_ns"] = time.time_ns()
    with (output / (phase + "-cgroup-" + edge + ".json")).open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
    return int(any(item["status"] for item in value["files"].values()))


def verify(folder, phase):
    """Require both framed snapshots, real counters and the declared native build limits."""
    values = []
    for edge in ("before", "exit"):
        value = json.loads((folder / (phase + "-cgroup-" + edge + ".json")).read_text())
        if value.get("scope") != SCOPE or value.get("phase") != phase or value.get("edge") != edge:
            raise ValueError("Wrong cgroup snapshot frame")
        if not 0 < value["begin_epoch_ns"] <= value["end_epoch_ns"]:
            raise ValueError("Missing cgroup snapshot timestamps")
        if value.get("recipe_status") != (None if edge == "before" else 0):
            raise ValueError("Cgroup snapshot records failed recipe")
        files = value["files"]
        if set(files) != set(FILES) or any(item.get("status") != 0 or item.get("error_type") for item in files.values()):
            raise ValueError("Required cgroup snapshot read failed")
        def text(name):
            return files[name]["text"].strip()
        def counters(name):
            return {key: int(number) for key, number in (line.split() for line in text(name).splitlines())}
        cpu = counters("cpu.stat")
        events = counters("memory.events")
        if any(cpu.get(name, -1) < 0 for name in ("usage_usec", "user_usec", "system_usec")) or any(
                events.get(name, -1) < 0 for name in ("oom", "oom_kill")):
            raise ValueError("Missing actual cgroup CPU/memory counters")
        current, peak = int(text("memory.current")), int(text("memory.peak"))
        if not 0 <= current <= peak or int(text("memory.max")) != 10 * 1024 ** 3 or text("memory.swap.max") != "0":
            raise ValueError("Wrong cgroup memory counters/limits")
        quota, period = map(int, text("cpu.max").split())
        if period <= 0 or quota != 2 * period or text("pids.max") != "1024":
            raise ValueError("Wrong cgroup CPU/process limits")
        for line in text("io.stat").splitlines():
            device, *fields = line.split()
            if ":" not in device or not fields or any(int(field.split("=", 1)[1]) < 0 for field in fields):
                raise ValueError("Invalid actual cgroup IO counters")
        values.append(value)
    if values[0]["end_epoch_ns"] > values[1]["begin_epoch_ns"]:
        raise ValueError("Cgroup snapshot order differs")
    if (folder / (phase + "-recipe-exit-status.txt")).read_text().strip() != "0" or (
            folder / (phase + "-retention-exit-status.txt")).read_text().strip() != "0":
        raise ValueError("Failed recipe or EXIT retention")
    return {"scope": SCOPE, "edges": ["before", "exit"], "status": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("phase", choices=("configure", "build"))
    parser.add_argument("edge", choices=("before", "exit"))
    parser.add_argument("--recipe-status", type=int)
    args = parser.parse_args()
    raise SystemExit(snapshot(args.output, args.phase, args.edge, args.recipe_status))
