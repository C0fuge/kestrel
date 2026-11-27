# Common small-query warmup controls

The default remains one eight-client warmup for `max(3, perf.seconds/3)` seconds,
followed by the configured measured client counts. Historical commands and raw
artifacts are unchanged. An opt-in mode can warm each measured concurrency separately.
Neither duration nor concurrency matching guarantees steady state.

| Harness property | Workflow input / runner environment | Default | Accepted values |
|---|---|---|---|
| `perf.warmupMode` | `warmup_mode` / `SCREEN_WARMUP_MODE` | `legacy` | `legacy`, `per-step` |
| `perf.warmupSeconds` | `warmup_seconds` / `SCREEN_WARMUP_SECONDS` | `0` | `0` for automatic duration, explicit `1..120` seconds |

For example, clients `8,32`, measured duration `30`, warmup mode `per-step` and
warmup seconds `20` run: 20 seconds at eight clients, measure eight; 20 seconds at
32 clients, measure 32. Warmup uses the same SQL mix, client protocol and answer
checks as measurement. A warmup failure aborts the attempt and preserves partial
rows through the existing cancellation and cleanup paths. It cannot become a
successful ranked sample.

The screening runner supplies these controls in common arguments to A, B and its
unranked preflight. Candidate-only `variant_options` cannot set either property.
The same two candidate harness files are still overlaid on the baseline; no extra
Java helper dependency is introduced. Opt-in requests require the updated candidate
harness. The runner rejects an older harness before starting containers and checks
the actual `warmup-plan.csv` against the requested sequence before ranking each
attempt. Default runs remain compatible with historical candidate harnesses.

`screen.json` records resolved ranked/preflight warmup plans. The updated Java
harness also writes `warmup-plan.csv` once before executing the plan, with mode,
configured duration, step order, effective clients/duration, measured status and
raw prefix. Existing `runtime.txt` retains explicit properties. Warmup labels are:

- Legacy: `warmup-c8`, preserving existing CSV/wall/gate names.
- Per step: `warmup-step-1-c8`, `warmup-step-2-c32`, and so on.
- Measurement: existing `c8`, `c32`, and so on.

Every prefix applies consistently to query CSV, wall and gate files. Plan/CSV/wall/gate
creation refuses to overwrite existing evidence. The indexed warmup names avoid
collisions with the legacy warmup and measured steps. The default query loop,
deadline, workload oracle and cleanup semantics are unchanged.

The runner distinguishes nominal configured workload seconds from a conservative
planned-work bound: every warmup and measured step on both sides, in all rounds,
plus preflight receives its own 30-second in-flight drain allowance. Existing setup
allowances add 20 seconds per ranked attempt and 24 seconds for preflight. An explicit
long warmup also increases preflight's nominal duration. `screen.json` records both
totals. The default dispatch is 306 nominal workload seconds and a 1,230-second bound.
The 1,500-second planned-work cap leaves ten minutes for builds inside the unchanged
35-minute build/measure step. The 45-minute job still reserves ten further minutes
for partial artifact publication. Longer opt-in plans may need fewer rounds or client
counts. Planning does not promise exact elapsed runtime on a shared runner.

Use the retained CPU samples, compiler activity, completion-time bins, tails,
GC and memory samples to assess remaining ramp effects. A diagnostic found new
server workers when rising from eight warmup clients to 32 measured clients,
while compiler activity continued late in the measured phase. That motivates
controlled comparisons; it does not prove warmup caused a latency change or that
the final bins represent steady state. Compare settings within the same paired
experiment rather than ranking absolute QPS across runners.

## Runtime validation

[Fast CI 46051600568](https://github.com/C0fuge/kestrel/actions/runs/46051600568)
passes 163 selected tests and Checkstyle on source `37703d62`, with all test sources
compiled. [Same-source A/A 46052077357](https://github.com/C0fuge/kestrel/actions/runs/46052077357)
validates per-step three-second warmups at one and 32 clients. Actual preflight/A/B
plans match, all 7,562 warmup/measured answers pass an independent full oracle, and
all three JFRs are readable. The 32-client warmups contain 32 actual client threads.
All five container/wrapper exits are zero without OOM. The artifact's CRC, all 121
manifest entries and exact file set pass; its SHA-256 matches GitHub's digest:
`4d03fbed0cded2f38f8fc3180bcf05bab5d9cfc6fdaef11b678530cee854c142`.
Optional periodic cgroup reads outside container lifetimes retain their unavailable
statuses. This checks the pipeline, with no speedup, A/A ranking or plateau claim.
