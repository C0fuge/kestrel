#!/usr/bin/env bash
# Builds the Ubuntu 20.04 server image of the HDFS service workflows (hadoopfs links libssl.so.1.1; the runner's JDK 21
# is mounted at run time).
#
# Usage: tools/ci/server-image.sh <build dir> <image tag> <apt package>...
#
# The runners' Ubuntu mirror sometimes stalls or fails (2026-11-10: `apt-get update ... did not complete` in eight
# Service cluster cells and two Service hdfs cells), so apt runs with bounded per-request retries and timeouts and the
# whole `docker build` is tried up to three times, with a `server-image:` line per try in the job log.
set -euo pipefail
dir=$1 tag=$2; shift 2
[ "$#" -gt 0 ] || { echo "server-image: no packages given"; exit 2; }
apt_opts="-o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30"
mkdir -p "$dir"
printf '%s\n' 'FROM eclipse-temurin:17-jdk-focal' \
  "RUN apt-get $apt_opts update -qq && DEBIAN_FRONTEND=noninteractive apt-get $apt_opts install -y -qq --no-install-recommends $* > /dev/null && rm -rf /var/lib/apt/lists/*" \
  > "$dir/Dockerfile"
for t in 1 2 3; do
  echo "server-image: try $t $(date -u +%T)"
  if timeout 600 docker build -q -t "$tag" "$dir"; then
    echo "server-image: $tag built $(date -u +%T)"
    exit 0
  fi
  sleep $((t * 15))
done
echo "server-image: $tag failed after 3 tries"
exit 1
