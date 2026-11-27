# Diagnostic on-CPU profiling

`cpu-profile.yml` is optional: manual dispatch or a deliberate push to
`perf/cpu-profile-*`. The narrow push trigger lets a reviewed workflow run on a
diagnostic branch before it exists on the default branch. It defaults to explicit
ctimer and five measured seconds; it never runs on ordinary branches/main.
It profiles one source revision with the
existing `SmallQueryThroughputPerfTest`, mixed interval 8, separately at 8 and 32
clients. Integer literal parameters and prepared schema-message experiments are
explicitly off. It creates no A/B ratio or promotion recommendation. Maven builds
once; each concurrency gets a fresh JVM and output folder. Four logical CPUs are
pinned, with the existing harness JVM/DuckDB limits recorded in the exact commands.
The job uses host Java 21 without sudo, kernel changes, capabilities or privileged
containers. Host limits/cgroup access are recorded rather than assuming isolation.

The profile covers **combined embedded server, clients, JVM compiler/GC and
collectors**, including startup, warmup and teardown. It is not pure-server CPU.
The original harness `profile.jfr` remains separate: its native method residency
samples are not CPU attribution. Async-profiler CPU recordings are `cpu-<pid>.jfr`.
Raw measured phase windows are in `c*-wall.txt`; the wrapper records epoch time
bracketed by Linux monotonic reads before exec. The preflight checks that Java
`nanoTime` uses the same monotonic clock. These clocks support approximate phase
selection; preserve brackets/calibration and do not silently count startup as
measured-query CPU. Periodic OS-thread counters include unattached native workers.

The default event is explicit `ctimer`, a per-thread CPU clock that works without
perf privileges. Its timer resolution is limited by kernel jiffies and it cannot
collect kernel stacks. Optional `cpu-clock,alluser` forces perf user-space CPU
sampling and fails if unavailable. The helper never requests automatic `cpu`,
`itimer` or wall fallback. It reads actual `engine`, `event`, version, interval and
native stack mode and monotonic clock from every recording before declaring success. CPU samples
represent running time, subject to sampling/timer granularity; sample percentages
are statistical estimates, not exact per-query costs.

The official async-profiler 4.6 Linux x64 asset is pinned to SHA-256
`1e79ed54d8952889a2e74f993d22ee925e3322460d4f37a2ed95bfb88585da36`
and source commit `557276c2950c14a5ad7073e2bef5a16616309247`. Its archive is retained.
The version-specific [CPU engine documentation](https://github.com/async-profiler/async-profiler/blob/v4.6/docs/CpuSamplingEngines.md)
describes perf/ctimer semantics and automatic fallbacks. The
[native hooks](https://github.com/async-profiler/async-profiler/blob/557276c2950c14a5ad7073e2bef5a16616309247/src/hooks.cpp)
register new pthreads; [ctimer implementation](https://github.com/async-profiler/async-profiler/blob/557276c2950c14a5ad7073e2bef5a16616309247/src/ctimer_linux.cpp)
creates per-thread CPU timers. The
[native loading documentation](https://github.com/async-profiler/async-profiler/blob/v4.6/docs/ProfilingNonJavaApplications.md)
describes `LD_PRELOAD` and `ASPROF_COMMAND`.

A mandatory tiny Java/JNI preflight burns approximately 0.8 CPU seconds in each
of one Java thread and one pthread that never attaches to Java, while another
native thread waits for two wall seconds. It requires resolved native busy
samples, Java samples, low waiting samples, and independent native thread CPU
counters. Both workers are created after profiler startup. The same launch
method is used for the benchmark: only the target Surefire fork receives
`LD_PRELOAD`, `ASPROF_COMMAND` and JVM-agent initialization through a dedicated
`-Djvm` wrapper. Maven/version discovery and the caller environment are untouched.
Per-PID filenames prevent profile collisions. Plain startup `-agentpath` alone
missed the busy unattached pthread in the controlled Java 21 preflight, so it
does not satisfy this workflow's coverage gate.

Each actual benchmark must also contain at least ten resolved DuckDB CPU samples
with unwound stacks of two or more frames on threads with no Java thread ID.
Otherwise collection fails and its partial evidence is
uploaded. The helper reports unresolved frame occurrences and truncated stacks;
inspect these before attributing a hotspot. It uses native DWARF unwinding and
rejects any OS thread with at least 0.5 first-to-last observed user CPU seconds
but no CPU samples. The per-thread comparison is a conservative lifetime lower
bound, not exact measurement-window CPU. It retains native worker TIDs and stack
depth counts. It
retains every observed executable shared library by SHA-256, including the actual
mapped DuckDB library, plus symbol/build-ID/section listings. This preserves
available symbols and unwind data; it does not manufacture missing debug symbols.

Artifacts retain the original CPU and harness JFRs, full extracted CPU events,
flame/collapsed views, full Maven/Surefire/agent logs, exact JVM and build argv,
release identity, source commit/diff, harness sources, generated small input data,
raw per-query answers/checksums/Arrow buffer bytes/batches, warmup, wall/process
CPU counters, heap/direct/RSS/GC samples, process maps/task CPU/IO, host/pressure
snapshots and per-file read errors. Native binary collection and host sampling
are diagnostic observer overhead. Every complete or failed attempt gets a
`SHA256SUMS`; artifact upload runs on failure. Original raw files are never
replaced by summaries. The target fork receives an allowlisted runtime environment
and synthetic HOME/USER values; inherited runner/service credentials are excluded.
The standard harness JDK JFR may record that synthetic environment. Unrelated
process environments or command lines are not collected.

For a cheap cloud gate use `preflight_only=true`; for an initial actual-query
unwind check use `seconds=5`. Review those artifacts before a longer diagnostic
profile. Neither mode is a performance comparison. Local heavy builds or
benchmarks are unnecessary.
