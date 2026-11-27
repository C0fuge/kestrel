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
pinned. The target wrapper directly injects `-Xms512m -Xmx2g`, a 1 GiB direct-memory
limit, four active JVM processors and synthetic `user.home=/tmp`. These limits
do not depend on Maven's `-DargLine`: the inherited POM's late `@{argLine}` expansion
can ignore that user property. The gate checks exact unique flags in wrapper argv,
the standard harness JFR's actual JVM arguments and the harness RuntimeMXBean
arguments, plus the actual available-processor count. Conflicting overrides fail.
The job uses host Java 21 without sudo, kernel changes, capabilities or privileged
containers. Host limits/cgroup access are recorded rather than assuming isolation.

Optional workflow inputs `warmup_mode=legacy|per-step` and `warmup_seconds=0..120`
map to CLI `--warmup-mode` and `--warmup-seconds`. Defaults remain a single eight-client
warmup for `max(3, measured seconds/3)` seconds in each JVM. Zero means automatic
duration; a positive value overrides it. `per-step` warms the measured concurrency.
Because each fresh JVM measures only one concurrency, its raw warmup prefix is
`warmup-step-1-c8` or `warmup-step-1-c32`, followed by `c8` or `c32`. Legacy prefixes
remain `warmup-c8` in both JVMs. No setting guarantees steady state, and absolute QPS
from separate runs is not an A/B causal comparison.

The CPU helper reuses screening validation, planning, arguments and the actual-plan
gate. Unsupported custom requests fail before profiler download or build. Custom
requests require the exact `warmup-plan.csv`; a new harness's explicit default plan
is checked too. Historical source without that file remains supported only with
legacy defaults and is labelled as having no explicit plan CSV. Every selected raw
warmup and measurement label must retain successful mixed-query samples. `plan.json`
records intended per-JVM plans, nominal duration, workload bounds and command caps;
`cpu-verified.json` records checked plans, actual label names and evidence scope.
`preflight_only=true` performs no workload warmup or measurement.

Each fork's workload bound includes configured warmup/measurement durations, a
30-second drain for each step and 20 seconds of setup. A plan above its existing
240-second Maven-fork timeout is rejected before download/build. For per-step 20-second
warmup and 30-second measurement the bound is 130 seconds per fork, including 60 seconds
of drains; total nominal workload is 100 seconds across both JVMs. This is a workload
bound, not the entire diagnostic's elapsed-time guarantee.

The existing 900-second build and 240-second fork caps are command maxima. Their sum
does not account for profiler download/preflight, JFR parsing/conversion or symbols,
so they are not presented as an additive global runtime bound. An overall 1,440-second
monotonic work deadline now covers download, preflight, build, validation and conversion.
An alarm interrupts blocking Python/I/O work, and each command is limited to its
remaining work time. One minute remains inside the unchanged 25-minute workflow step
for finalization; the 35-minute job retains ten further minutes for artifact upload.
The deadline may stop a run before any command uses its full individual cap. It is
disarmed during failure sealing; native-symbol commands still obey the expired
deadline and retain explicit failed-command statuses instead of silently running
unbounded. Original partial recordings, logs and failure evidence remain preserved.

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
native stack mode and monotonic clock from the target recording before declaring success. CPU samples
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
method is used for the benchmark: the wrapper injects only the target Surefire fork's
`LD_PRELOAD`, `ASPROF_COMMAND` and JVM-agent initialization through a dedicated
`-Djvm` wrapper. Maven/version discovery and the caller environment are untouched.
Native children spawned by Java naturally inherit that environment and may also
write CPU JFRs. The helper selects only `cpu-<sole wrapper launch PID>.jfr`; it never
chooses the first file or treats the auxiliary recordings as the target. It binds
that selection to the standard harness JFR's JVMInformation PID/start/arguments,
RuntimeMXBean arguments and CPU sample timestamps within the launch lifetime.
Preloaded CPU recordings can lack JVMInformation, so the separate standard JDK
recording supplies that independent process identity. Duplicate launches, missing
target files, mismatched identities and stale timestamps fail.

Every auxiliary recording is retained, hashed and inventoried; its extraction
status and sample count remain separate. Auxiliary files may contain no samples
or incomplete chunks. Their errors do not replace target identity or CPU evidence.
The profiler does not expand `%p` in log filenames, and its file logger uses
[`creat()`](https://github.com/async-profiler/async-profiler/blob/557276c2950c14a5ad7073e2bef5a16616309247/src/log.cpp),
which truncates a shared filename on child startup. Instead, the wrapper owns one
`O_APPEND` stderr descriptor to `async-profiler.log` and requests `log=stderr`.
The target and inherited children share this append stream, preserving JVM/native
stderr and profiler diagnostics without truncation. Unprefixed log lines cannot
be assigned to a particular child. The full Maven log and Surefire reports are
retained separately. Plain startup `-agentpath` alone
missed the busy unattached pthread in the controlled Java 21 preflight, so it
does not satisfy this workflow's coverage gate.

Each actual benchmark must also contain at least ten resolved DuckDB CPU samples
with at least two actual symbol frames, the pinned native
`duckdb::TaskScheduler::ExecuteForever` root and profiler `thread_start_wrapper`,
and no Java/interpreted/JIT or unknown-type frames. A zero/missing Java thread ID
does not prove an unattached native thread: real Java/gRPC stacks can have ID zero.
Module-only `.so` names do not count as resolved function frames. Java, unknown
and other native samples are retained and classified separately.
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

The first diagnostic run `46043760942` retained its original raw evidence. It was
misconfigured: intended JVM limits were absent, native child recordings caused a
selection failure, and zero Java thread IDs were insufficient worker evidence.
It is not a performance pass or an optimization recommendation. The corrected
gate rejects that launch's missing flags; its original raw files remain unchanged.
