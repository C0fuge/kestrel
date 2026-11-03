#!/usr/bin/env bash
# SERVICE MATRIX gate of Dev CI's package=true (STATUS item 25): a pre-release is published only when
# the latest run of every .github/workflows/service-*.yml on the same commit is green.
#
# Usage: tools/ci/service-gate.sh <commit sha> <branch> [minutes]
#
# For each service workflow: no run on the commit yet, or the latest one was cancelled -> dispatch it
# on <branch> (only while <branch> still points at <commit>, else the dispatched run would test
# another commit: then the gate fails and says so); then wait (at most <minutes>, default 180) until
# the latest run on the commit has completed. Exit 0 only when every latest run succeeded. Writes a
# table workflow -> run -> result to $GITHUB_STEP_SUMMARY. Needs GH_TOKEN with actions: write.
set -euo pipefail
sha=$1 ref=$2 limit=${3:-180}
repo=$GITHUB_REPOSITORY
summary=${GITHUB_STEP_SUMMARY:-/dev/null}
mapfile -t wfs < <(cd .github/workflows && ls service-*.yml)
[ "${#wfs[@]}" -gt 0 ] || { echo "no service-*.yml workflows"; exit 1; }

latest() { # workflow file -> "<id> <status> <conclusion> <url>" of the newest run on the commit, or ""
  local i
  for i in 1 2 3 4; do
    gh api "repos/$repo/actions/workflows/$1/runs?head_sha=$sha&per_page=1" \
      --jq '.workflow_runs[0] // empty | "\(.id) \(.status) \(.conclusion // "-") \(.html_url)"' && return 0
    sleep $(( i * 10 ))
  done
  echo "::error::cannot list the runs of $1" >&2; exit 1
}
dispatch() {
  local head
  head=$(gh api "repos/$repo/commits/$ref" --jq .sha)
  if [ "$head" != "$sha" ]; then
    echo "::error::$1 has no usable run on $sha and $ref has moved on to $head: dispatch it on a branch at $sha"
    return 1
  fi
  gh api -X POST "repos/$repo/actions/workflows/$1/dispatches" -f ref="$ref" > /dev/null
  echo "$1: dispatched on $ref ($sha)"
}

declare -A tries=() state=() handled=()
for wf in "${wfs[@]}"; do tries[$wf]=0; done
deadline=$(( $(date +%s) + limit * 60 ))
while :; do
  pending=0
  for wf in "${wfs[@]}"; do
    [ -n "${state[$wf]:-}" ] && continue
    run=$(latest "$wf")
    if [ -z "$run" ]; then
      # Not run on this commit yet (or the dispatched run is not listed yet).
      if [ "${tries[$wf]}" -eq 0 ]; then
        tries[$wf]=1
        dispatch "$wf" || { state[$wf]="fail - ($ref moved, not dispatched)"; continue; }
      fi
      pending=1; continue
    fi
    if [[ $run == *" completed cancelled "* ]]; then
      # Cancelled (e.g. by a newer push of the same group): run it again, at most twice per workflow.
      id=${run%% *}
      if [ "${handled[$wf]:-}" != "$id" ] && [ "${tries[$wf]}" -lt 2 ]; then
        handled[$wf]=$id; tries[$wf]=$(( tries[$wf] + 1 ))
        dispatch "$wf" || { state[$wf]="fail - ($ref moved, not dispatched)"; continue; }
      elif [ "${handled[$wf]:-}" != "$id" ]; then
        state[$wf]="cancelled $id ${run##* }"; continue
      fi
      pending=1; continue
    fi
    read -r id status conclusion url <<< "$run"
    if [ "$status" != completed ]; then pending=1; continue; fi
    state[$wf]="$conclusion $id $url"
    echo "$wf: run $id $conclusion"
  done
  [ "$pending" -eq 0 ] && break
  if [ "$(date +%s)" -ge "$deadline" ]; then
    for wf in "${wfs[@]}"; do [ -n "${state[$wf]:-}" ] || state[$wf]="fail - not completed within $limit min"; done
    break
  fi
  sleep 60
done

# The verdict is taken here, not inside the piped table below: a pipe runs its left side in a
# subshell, and a failure recorded there was lost (the 5122cea1 pre-release passed a failed gate).
ok=0
for wf in "${wfs[@]}"; do
  read -r conclusion rest <<< "${state[$wf]}"
  [ "$conclusion" = success ] || ok=1
done
{
  echo "### Service matrix gate ($sha)"
  echo
  echo "| workflow | result | run |"
  echo "|---|---|---|"
  for wf in "${wfs[@]}"; do
    read -r conclusion rest <<< "${state[$wf]}"
    echo "| $wf | $([ "$conclusion" = success ] && echo success || echo "**${conclusion}**") | ${rest} |"
  done
} | tee -a "$summary"
[ "$ok" -eq 0 ] || echo "::error::service matrix gate: not every service-*.yml run on $sha is green - no pre-release"
exit "$ok"
