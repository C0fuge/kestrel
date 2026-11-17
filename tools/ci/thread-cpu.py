"""Per-thread CPU of a running process over an interval (item 54: where one scan stream spends its CPU).

Usage: thread-cpu.py <pid> <seconds> [top]

Reads /proc/<pid>/task/*/stat twice, <seconds> apart, and prints the CPU of each thread name group
(digits in thread names folded to ``#``, so ``duckdb-worker-3`` and ``duckdb-worker-7`` add up) as a
percentage of one core, busiest first, then the busiest single threads: a single thread near 100 %
is the one-stream bottleneck, many groups well below it mean the stream waits elsewhere.
"""
import os
import re
import sys
import time

TICKS = os.sysconf("SC_CLK_TCK")


def sample(pid):
    """Returns {tid: (name, ticks)} of every thread of the process."""
    out = {}
    base = f"/proc/{pid}/task"
    for tid in os.listdir(base):
        try:
            with open(f"{base}/{tid}/stat", encoding="utf-8", errors="replace") as f:
                stat = f.read()
        except OSError:
            continue
        name = stat[stat.index("(") + 1:stat.rindex(")")]
        fields = stat[stat.rindex(")") + 2:].split()
        out[tid] = (name, int(fields[11]) + int(fields[12]))  # utime + stime
    return out


def main():
    pid, seconds = sys.argv[1], float(sys.argv[2])
    top = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    before = sample(pid)
    time.sleep(seconds)
    after = sample(pid)
    threads, groups = [], {}
    for tid, (name, ticks) in after.items():
        used = (ticks - before.get(tid, (name, 0))[1]) / TICKS / seconds * 100
        threads.append((used, name, tid))
        key = re.sub(r"\d+", "#", name)
        groups[key] = groups.get(key, 0) + used
    total = sum(groups.values())
    print(f"total {total:.0f}% over {seconds:.0f} s, {len(after)} threads | groups:"
          + "".join(f" {k}={v:.0f}%" for k, v in sorted(groups.items(), key=lambda kv: -kv[1]) if v >= 1))
    print("busiest:" + "".join(f" {n}={u:.0f}%" for u, n, _ in sorted(threads, reverse=True)[:top]))


if __name__ == "__main__":
    main()
