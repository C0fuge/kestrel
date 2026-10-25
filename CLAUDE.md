# Working in this repository (instructions for Claude cloud sessions)

This is the maintainer's development fork of the Arrow Flight SQL server (upstream:
github.com/VadimSurpin/arrow-flight-server; our changes on top: own engine, cluster/failover, S3/lakeFS/Iceberg/Hive
plugins, DuckDB 1.4.5, hadoopfs timeouts, many fixes). Read first: `AGENTS.md` (code conventions — Javadoc,
Checkstyle, SpotBugs, tests with every change), `README.md`, `CHANGELOG.md`, and `docs/dev/STATUS.md` (state,
backlog, decisions, history pointers). The change record of earlier local work is summarised in docs/dev/STATUS.md.

## Session routine

1. **Lock.** `git fetch origin`. If `docs/dev/LOCK` holds a UTC time less than 150 minutes old, stop. Otherwise write
   `date -u +%FT%TZ` to it, commit `[skip ci] lock`, push. Empty it in the last commit of the session.
2. Work on `main` (if rejected, `claude/server-dev`). Never force-push. The maintainer's local agents may also push
   to `main` (stand fixes) — always `git pull --rebase` before pushing.
3. Build/test in GitHub Actions (workflow `dev-ci.yml`; create it in the first session). `gh api` works in sessions;
   `gh workflow run` may not — dispatch with `gh api -X POST repos/C0fuge/kestrel/actions/workflows/<f>/dispatches -f ref=main`.
   Local Maven in the session is fine too when it can download dependencies.
4. Take the first open item of "Next steps" in `docs/dev/STATUS.md`; finish one item properly (code + tests + CI green
   + CHANGELOG line + STATUS update).
5. Before ending (≤ ~90 min): update `docs/dev/STATUS.md` (dated Log line with CI run ids, Next steps, Open
   questions), release the lock, commit, push. Nothing may stay only in the session.

## Rules

- The repo is **public** (since 2026-10-24, maintainer's decision: free Actions minutes). Everything pushed —
  code, docs, releases, CI logs — is visible to anyone: never commit secrets, credentials or personal data (names,
  e-mail addresses, home paths, host names of the maintainer's machines or of production systems). Local commits use
  `C0fuge <189659128+C0fuge@users.noreply.github.com>`. Keep the repo low-profile: no description, topics or website,
  no links to it from other projects.
- Release `ci-assets` holds test inputs (DuckDB extensions incl. our hadoopfs build,
  runtime libs, test data) — use them in CI. Upstream's heavy workflows (Benchmark Pages,
  Load Test, Release) are disabled — keep them disabled.
- Actions minutes are free (public repo), but runners are shared: cache Maven, run the full build once
  per change set, skip `perf` tests in CI unless asked.
- Correctness first: never return wrong or partial results silently; every bug fix gets a regression test.
- Don't change the wire behaviour clients depend on (Flight SQL commands, result modes `partial`/`complete`,
  headers `x-flight-result-mode`, `x-flight-location-failover`, ticket format compatibility within a release, auth
  providers) without writing it into Open questions first.
- No secrets in the repo; test credentials generated in the job.
- Commit/push only to this repository. Release artifacts (jars) go to this repo's releases only; the maintainer's local
  coordinator distributes them to the client repos.
- Purely additive features that keep existing behaviour unchanged (new SQL syntax, new optional settings, new
  Flight actions) do not need the maintainer's approval: implement them, document them, and mention them in the Log.
  Ask only for changes to existing behaviour, defaults, wire format or security.
- Anything needing the maintainer's decision → "Open questions" in docs/dev/STATUS.md.
- Docs and commit messages in English.
