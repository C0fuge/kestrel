#!/bin/sh
# Assembles a plugins directory from built plugin modules, with a SHA256SUMS per plugin:
#   <out>/plugins/<name>/*.jar + SHA256SUMS
# The server finds it with -Darrowflight.plugins.dir=<out>/plugins (or FLIGHT_PLUGINS_DIR), or as
# ../plugins next to the server jar. Build first: mvn clean package (flight-plugin-<name>/target/plugin/<name>).
# usage: tools/make-plugin-dir.sh <out-dir> [plugin ...]     (default plugin: s3)
set -eu
if [ $# -lt 1 ]; then
    echo "usage: $0 <out-dir> [plugin ...]" >&2
    exit 2
fi
root=$(cd "$(dirname "$0")/.." && pwd)
out=$1
shift
[ $# -eq 0 ] && set -- s3
for name in "$@"; do
    src="$root/flight-plugin-$name/target/plugin/$name"
    if [ ! -d "$src" ] || ! ls "$src"/*.jar >/dev/null 2>&1; then
        echo "no built plugin in $src (run mvn clean package first)" >&2
        exit 1
    fi
    dest="$out/plugins/$name"
    rm -rf "$dest"
    mkdir -p "$dest"
    cp "$src"/*.jar "$dest"/
    (cd "$dest" && sha256sum -- *.jar > SHA256SUMS)
    echo "$dest: $(ls "$dest"/*.jar | wc -l) jars, $(du -sh "$dest" | cut -f1)"
done
