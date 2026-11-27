#!/usr/bin/env bash
# Both variants execute this same recipe inside one immutable toolbox image.
set -euo pipefail
compiler=$1
common_flags=$2
case "$compiler" in gcc) cc=gcc; cxx=g++;; clang) cc=clang; cxx=clang++;; *) exit 2;; esac
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export SOURCE_DATE_EPOCH=1790590000
export HOME=/tmp
gcc_root=$(dirname "$(dirname "$(readlink -f "$(command -v gcc)")")")
extra=()
if [ "$compiler" = clang ]; then extra=("--gcc-toolchain=$gcc_root" -stdlib=libstdc++); fi
printf '%s\n' "$gcc_root" > /evidence/gcc-toolchain-root.txt
"$cc" --version > /evidence/c-compiler.txt 2>&1
"$cxx" --version > /evidence/cxx-compiler.txt 2>&1
"$cxx" "${extra[@]}" -v -E -x c++ /dev/null > /evidence/compiler-search-paths.txt 2>&1
"$cxx" "${extra[@]}" -print-file-name=libstdc++.so | xargs readlink -f > /evidence/libstdcxx.txt
rpm -qa --qf '%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n' | sort > /evidence/packages.txt
ld --version > /evidence/linker.txt
cmake --version > /evidence/cmake-version.txt
ninja --version > /evidence/ninja-version.txt
"$JAVA_HOME/bin/java" -version > /evidence/build-java-version.txt 2>&1
cat > /evidence/probe.cpp <<'EOF'
#include <bits/c++config.h>
#include "snappy_version.hpp"
EOF
# Intentional word splitting of the fixed, repository-owned common flags.
read -r -a flags <<< "$common_flags"
printf '%q ' "$cxx" "${extra[@]}" "${flags[@]}" -std=gnu++11 -dM -E -x c++ -I/source/src/duckdb/third_party/snappy /evidence/probe.cpp > /evidence/probe-command.txt
"$cxx" "${extra[@]}" "${flags[@]}" -std=gnu++11 -dM -E -x c++ -I/source/src/duckdb/third_party/snappy /evidence/probe.cpp > /evidence/macros.txt
build=/source/build/release
mkdir -p "$build"
retain() {
  rc=$?
  for file in CMakeCache.txt compile_commands.json; do
    [ ! -f "$build/$file" ] || cp "$build/$file" "/evidence/$file"
  done
  printf '%s\n' "$rc" > /evidence/recipe-exit-status.txt
}
trap retain EXIT
cmake_args=(-S /source -B "$build" -G Ninja -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_C_COMPILER="$cc" -DCMAKE_CXX_COMPILER="$cxx"
  -DCMAKE_CXX_COMPILER_EXTERNAL_TOOLCHAIN="$gcc_root"
  -DCMAKE_CXX_STANDARD=11 -DCMAKE_CXX_EXTENSIONS=ON
  -DCMAKE_CXX_FLAGS="$common_flags" -DCMAKE_C_FLAGS="$common_flags"
  -DCMAKE_CXX_FLAGS_RELEASE='-O3 -DNDEBUG' -DCMAKE_C_FLAGS_RELEASE='-O3 -DNDEBUG'
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF)
printf '%q ' cmake "${cmake_args[@]}" > /evidence/configure-command.txt
cmake "${cmake_args[@]}"
cp "$build/compile_commands.json" /evidence/compile_commands.json
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
