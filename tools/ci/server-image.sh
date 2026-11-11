#!/usr/bin/env bash
# Builds the Ubuntu 20.04 server image of the HDFS service workflows (hadoopfs links libssl.so.1.1; the runner's JDK 21
# is mounted at run time).
#
# Usage: tools/ci/server-image.sh <build dir> <image tag> <apt package>...
#
# The runners' Ubuntu mirror sometimes stalls or fails (2026-11-10: `apt-get update ... did not complete` in eight
# Service cluster cells and two Service hdfs cells), so apt runs with bounded per-request retries and timeouts and the
# whole `docker build` is tried up to three times, with a `server-image:` line per try in the job log. Try 1 uses the
# Azure Ubuntu mirror the runners' own apt uses, try 2 the image's default archive.ubuntu.com (2026-11-10 05:45Z
# archive.ubuntu.com reset connections while azure.archive.ubuntu.com answered), try 3 kernel.org's full mirror of the
# archive (2026-11-11: runners could not resolve any *.ubuntu.com host for hours).
set -euo pipefail
dir=$1 tag=$2; shift 2
[ "$#" -gt 0 ] || { echo "server-image: no packages given"; exit 2; }
pkgs="$*"
apt_opts="-o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30"
mirror_line() { # base URL of an Ubuntu archive mirror, e.g. http://azure.archive.ubuntu.com/ubuntu
  printf 'RUN sed -i -e "s#http://archive.ubuntu.com/ubuntu#%s#g" -e "s#http://security.ubuntu.com/ubuntu#%s#g" /etc/apt/sources.list' "$1" "$1"
}
mkdir -p "$dir"
dockerfile() { # mirror line or ""
  printf '%s\n' 'FROM eclipse-temurin:17-jdk-focal' ${1:+"$1"} \
  "RUN apt-get $apt_opts update -qq && DEBIAN_FRONTEND=noninteractive apt-get $apt_opts install -y -qq --no-install-recommends $pkgs > /dev/null && rm -rf /var/lib/apt/lists/*" \
  > "$dir/Dockerfile"
}
for t in 1 2 3; do
  case $t in
    1) mirror=http://azure.archive.ubuntu.com/ubuntu; dockerfile "$(mirror_line "$mirror")" ;;
    2) mirror=http://archive.ubuntu.com/ubuntu; dockerfile "" ;;
    *) mirror=http://mirrors.kernel.org/ubuntu; dockerfile "$(mirror_line "$mirror")" ;;
  esac
  echo "server-image: try $t $(date -u +%T) mirror $mirror"
  if timeout 600 docker build -q -t "$tag" "$dir"; then
    echo "server-image: $tag built $(date -u +%T)"
    exit 0
  fi
  sleep $((t * 15))
done
echo "server-image: $tag failed after 3 tries"
exit 1
