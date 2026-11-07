#!/usr/bin/env bash
# Publish a package=true pre-release of one commit without racing other pushes (STATUS item 37 process note).
#
# Usage: tools/ci/package-release.sh [commit]   (default: origin/main)
#
# Dev CI's service-gate needs a green run of every .github/workflows/service-*.yml on the same commit
# and dispatches missing ones only while the dispatched branch still points at that commit. A
# dispatch on main fails as soon as anyone pushes (even a [skip ci] lock/unlock commit). This script
# pushes a branch pkg/<short sha> at the commit, which nobody else moves, and dispatches the four
# service workflows and Dev CI (package=true) on it at once, so they run in parallel. The pre-release
# tag is server-<version>-<12-char sha> as for a dispatch on main. Delete the branch afterwards:
#   git push origin --delete pkg/<short sha>
# Needs: git with push access, gh (GH_TOKEN or a gh login) with actions: write.
set -euo pipefail
repo=${GITHUB_REPOSITORY:-C0fuge/kestrel}
git fetch -q origin main
sha=$(git rev-parse --verify "${1:-origin/main}^{commit}")
branch="pkg/${sha:0:8}"
git push -q origin "$sha:refs/heads/$branch"
echo "branch $branch -> $sha"
for wf in $(cd .github/workflows && ls service-*.yml); do
  gh api -X POST "repos/$repo/actions/workflows/$wf/dispatches" -f ref="$branch" > /dev/null
  echo "dispatched $wf"
done
gh api -X POST "repos/$repo/actions/workflows/dev-ci.yml/dispatches" -f ref="$branch" -F 'inputs[package]=true' > /dev/null
echo "dispatched dev-ci.yml (package=true)"
echo "runs: gh api 'repos/$repo/actions/runs?head_sha=$sha' --jq '.workflow_runs[] | \"\\(.id) \\(.name) \\(.status) \\(.conclusion)\"'"
