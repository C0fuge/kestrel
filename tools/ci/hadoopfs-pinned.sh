#!/usr/bin/env bash
# Puts the pinned hadoopfs build into an extracted DuckDB extension set (item 39, last step).
#
# Usage: tools/ci/hadoopfs-pinned.sh <dir with the extracted duckdb-extensions-<version>.tar.zst> [<duckdb version>]
#
# The `duckdb-extensions-v1.5.6.tar.zst` set in `ci-assets` holds hadoopfs 51b7220-afc3. The CI / release default is
# 51b7220-afc4 (file-status + located-blocks cache with `hdfs_metadata_cache_ttl_ms`, lazy open from cached blocks,
# real modification times), published in `ci-assets` as its own file. This script downloads it (GH_TOKEN, GITHUB_REPOSITORY),
# checks its SHA-256 and replaces every `<version>/linux_amd64/hadoopfs.duckdb_extension` under the directory (none
# there: it is added as `<dir>/<version>/linux_amd64/hadoopfs.duckdb_extension`). Other DuckDB versions are left alone.
set -euo pipefail
dir=$1 version=${2:-${DUCKDB_VERSION:-v1.5.6}}
[ "$version" = v1.5.6 ] || { echo "hadoopfs-pinned: no pinned build for $version, set unchanged"; exit 0; }
name=hadoopfs-v1.5.6-afc4.duckdb_extension
sha=64a97a6b08e1f725c980d46f25d664a1c3edde8a73b624ac31bdc75f9095942c
tmp=$(mktemp -d)
gh release download ci-assets --repo "$GITHUB_REPOSITORY" --pattern "$name" --dir "$tmp"
echo "$sha  $tmp/$name" | sha256sum -c --quiet - || { echo "hadoopfs-pinned: $name checksum mismatch"; exit 1; }
targets=$(find "$dir" -path "*${version}/linux_amd64/hadoopfs.duckdb_extension")
if [ -z "$targets" ]; then
  mkdir -p "$dir/$version/linux_amd64"
  targets="$dir/$version/linux_amd64/hadoopfs.duckdb_extension"
fi
for t in $targets; do
  cp "$tmp/$name" "$t"
  echo "hadoopfs-pinned: $t <- $name"
done
rm -rf "$tmp"
