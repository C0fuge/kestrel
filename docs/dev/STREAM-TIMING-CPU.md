# Optional CPU counters in stream timing

`streamTimingLog=true` adds four CPU deltas to each existing successful `streamTiming`
line. It defaults to false. When disabled, the adapter creates no CPU sampler and
performs no CPU counter reads. Existing timing fields, SQL redaction/truncation,
stream behavior and retry/error logging remain unchanged.

| Field | Counter interval |
|---|---|
| `executeProcessCpuNs` | Whole-process CPU through the `executeMs` interval |
| `executeCallerThreadCpuNs` | Current caller-thread CPU through the same interval |
| `totalProcessCpuNs` | Whole-process CPU through the `totalMs` interval |
| `totalCallerThreadCpuNs` | Current caller-thread CPU through the same interval |

Values are nanoseconds; `-1` means unsupported, disabled, failed or non-monotonic
counters. Zero is a valid measured delta, including for intervals shorter than the
counter's resolution. The collector uses existing JDK management beans and never
enables JVM thread-CPU monitoring. Counter reads are sequential around the existing
wall-clock marks, so the intervals are approximately aligned and include small
measurement overhead. JDK counters have nanosecond precision without guaranteed
nanosecond accuracy; see the [process CPU counter](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html#getProcessCpuTime())
and [thread CPU counter](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/ThreadMXBean.html#getCurrentThreadCpuTime())
contracts. Virtual-thread CPU time is unsupported by this management interface.

The execute interval begins inside `streamPrepared`, after SQL checking, statement
selection/preparation and parameter binding. It covers setup, producer-gate wait,
`executeQuery` and Arrow export until the reader opens. Total adds batch production,
listener readiness and outbound handoff. Existing successful-stream timing excludes
resource closure and logging itself; failures and retries follow their existing paths.

These are overlapping process/caller intervals, not exclusive CPU attributed to one
query. Process deltas include DuckDB worker threads, GC/JIT, telemetry, concurrent
queries and other process work; they can exceed elapsed wall time. Caller-thread
deltas include only the foreground stream thread, including native work on that
thread, and omit its DuckDB workers. Concurrent stream process deltas overlap and
must not be summed as distinct query CPU. The new counters cannot retrospectively
explain older recordings that did not contain them.
