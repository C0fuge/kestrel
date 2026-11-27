#!/usr/bin/env bash
# Both variants execute this same recipe inside one immutable toolbox image.
set -euo pipefail
compiler=$1
common_flags=$2
phase=$3
case "$phase" in configure|build) ;; *) exit 2;; esac
umask 077
case "$compiler" in gcc) cc=gcc; cxx=g++;; clang) cc=clang; cxx=clang++;; *) exit 2;; esac
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export SOURCE_DATE_EPOCH=1790590000
export HOME=/tmp/native-home
mkdir -p "$HOME"
build=/source/build/release
mkdir -p "$build"
retain() {
  rc=$?
  trap - EXIT
  set +e
  /opt/python/cp312-cp312/bin/python3 /control/resources.py /evidence "$phase" exit --recipe-status "$rc"
  snapshot_rc=$?
  for file in CMakeCache.txt compile_commands.json; do
    if [ -f "$build/$file" ]; then
      cp "$build/$file" "/evidence/$phase-$file" || snapshot_rc=1
    fi
  done
  printf '%s\n' "$rc" > "/evidence/$phase-recipe-exit-status.txt"
  printf '%s\n' "$snapshot_rc" > "/evidence/$phase-retention-exit-status.txt"
  if [ "$rc" -ne 0 ]; then exit "$rc"; fi
  exit "$snapshot_rc"
}
trap retain EXIT
/opt/python/cp312-cp312/bin/python3 /control/resources.py /evidence "$phase" before
gcc_root=$(dirname "$(dirname "$(readlink -f "$(command -v gcc)")")")
gcc_libgcc=$(readlink -f "$(gcc -print-libgcc-file-name)")
gcc_install=$(dirname "$gcc_libgcc")
compiler_flags=$common_flags
if [ "$compiler" = clang ]; then
  compiler_flags+=" --no-default-config --gcc-install-dir=$gcc_install"
fi
if [ "$phase" = configure ]; then
  id > /evidence/container-identity.txt
  printf '%s\n' "$gcc_root" > /evidence/gcc-toolchain-root.txt
  printf '%s\n' "$gcc_install" > /evidence/gcc-install-dir.txt
  printf '%s\n' "$gcc_libgcc" > /evidence/gcc-libgcc-file.txt
  "$cc" --version > /evidence/c-compiler.txt 2>&1
  rpm -qa --qf '%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n' | sort > /evidence/packages.txt
  ld --version > /evidence/linker.txt
  cmake --version > /evidence/cmake-version.txt
  ninja --version > /evidence/ninja-version.txt
  "$JAVA_HOME/bin/java" -version > /evidence/build-java-version.txt 2>&1
  cmake_args=(-S /source -B "$build" -G Ninja -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_C_COMPILER="$cc" -DCMAKE_CXX_COMPILER="$cxx"
  -DCMAKE_CXX_COMPILER_EXTERNAL_TOOLCHAIN="$gcc_root"
  -DCMAKE_CXX_STANDARD=11 -DCMAKE_CXX_EXTENSIONS=ON
  -DCMAKE_CXX_FLAGS="$compiler_flags" -DCMAKE_C_FLAGS="$compiler_flags"
  -DCMAKE_CXX_FLAGS_RELEASE='-O3 -DNDEBUG' -DCMAKE_C_FLAGS_RELEASE='-O3 -DNDEBUG'
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF)
  printf '%q ' cmake "${cmake_args[@]}" > /evidence/configure-command.txt
  cmake "${cmake_args[@]}"
  cp "$build/compile_commands.json" /evidence/compile_commands.json
  /opt/python/cp312-cp312/bin/python3 /control/effective.py "$compiler" "$build/compile_commands.json" /evidence
  sha256sum "$build/compile_commands.json" > /evidence/preflight-compile-SHA256
  exit 0
fi
# The host requires both effective probes to agree before invoking either build.
test -s /evidence/effective.json
sha256sum -c /evidence/preflight-compile-SHA256
printf '%q ' cmake --build "$build" --parallel 2 --verbose > /evidence/build-command.txt
/usr/bin/time -v cmake --build "$build" --parallel 2 --verbose
library="$build/libduckdb_java.so_linux_amd64"
test -s "$library"
cp "$library" /evidence/libduckdb_java.so_linux_amd64
cp "$build/duckdb_jdbc.jar" /evidence/duckdb_jdbc.jar
cp "$build/duckdb_jdbc_tests.jar" /evidence/duckdb_jdbc_tests.jar
sha256sum /evidence/libduckdb_java.so_linux_amd64 /evidence/duckdb_jdbc.jar /evidence/duckdb_jdbc_tests.jar > /evidence/native-SHA256SUMS
readelf -n -d -V "$library" > /evidence/native-elf.txt
readelf -p .comment "$library" > /evidence/native-compiler-comments.txt
nm -D --defined-only "$library" > /evidence/native-exports.txt
ldd "$library" > /evidence/native-dependencies.txt
