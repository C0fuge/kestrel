# Lane 3 - distributed (branch `lane/distributed`)

See `CLAUDE.md`, "Third and fourth lanes". Moved here from `STATUS.md` stage 3 on 2026-10-28 (maintainer).

## Next steps

D1. **Item 18 step 2 - distributed complete mode.** Read item 18 and the maintainer's answers in `STATUS.md` Open questions
    first (identity option (b) signed internal call with the ticket secret, accepted only from live members and only when
    `distributedComplete=true`, default false; reuse the server's TLS settings/trust store; peer failure before the first
    batch -> single-node fallback, after it -> fail). Step 1 (`CompleteAnswerMerge`) is on `main`. Do: node-to-node DoGet of
    the partial endpoints + merge in a DuckDB of the planning node behind the opt-in, fallback on refusal/peer error before
    the first batch.
D2. Item 18 step 3 - cluster integration test (3 nodes in CI, answers = single-node answers, TPC-H shapes that merge;
    peer killed before / during the stream), then a perf test: heavy JDBC queries on 3 nodes on vs off.
D3. Faster detection of a crashed member (`STATUS.md` Open question, item 0d follow-up): plan from a local cluster snapshot
    refreshed by the heartbeat thread instead of blocking map reads; keep `hazelcast.max.no.heartbeat.seconds` as is.

## Log

- 2026-10-28 (maintainer): lane created; D1-D3.
