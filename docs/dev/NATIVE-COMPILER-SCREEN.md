# Isolated native compiler build and smoke

The manual `Native compiler build and smoke` workflow defaults to `preflight`:
download/verify sources, prepare both source trees, configure both toolchains and
verify the actual decoder compiler contracts. It does not compile the native engine
or run native smokes. CMake compiler-ability checks and preprocessing are part of
configuration. Only an explicit `build-smoke` dispatch can compile, after both
configurations pass the gate. The first reviewed cloud run must be `preflight`.
It changes no server dependency, deployment or default. There is no benchmark or
performance ranking. The original published GCC binary remains the fallback.
In `build-smoke`, the published Maven JAR, its checksum/POM and native library are
retained and smoke-tested as a third reference; its native SHA must match the stock
`b36f739cca01c475867e332e496da8c95bcac37478d55f40ac9b3149e30f285c`.
GCC rebuild versus Clang rebuild isolates the compiler within the controlled image;
adoption would also have to beat the unchanged stock artifact.

The pinned JDBC `v1.5.6.0` commit is
`a561d2ddadde231402411ce76112779bc34b57f0`. Its `src/duckdb` is vendored,
not a submodule; the source version records DuckDB `v1.5.6` / `069cc9f9b5`,
whose full upstream tag commit is `069cc9f9b5be802405797faecc284961b07c70ef`.
Before building, the runner retains both tag API responses, the immutable-commit
source archive and its SHA-256, and verifies the version and exact decoder bytes.
The archive hash is recorded from the downloaded bytes, not presented as an
independently published upstream checksum. A later reproduction must compare it.

Both builds use one toolbox image derived from the same digest-pinned
`manylinux_2_28_x86_64` image. Both compilers are installed once; the resulting
toolbox image ID and package versions are retained. Release `-O3 -DNDEBUG`,
C++11, libstdc++ with `_GLIBCXX_USE_CXX11_ABI=1`, generic x86-64 ISA, two compile
jobs and a 10 GiB container limit are common. Clang selects the same GCC toolchain
for headers and libstdc++. Containers run as the runner's explicit UID:GID with
image-derived passwd/group files, a private temporary HOME and every capability
dropped. They can write the runner-owned source/evidence mounts without DAC caps.

`effective.py` derives preprocessing, include tracing and library lookup from the
actual generated `snappy.cc` compile argv. It removes only compile-output/dependency
actions; it does not replace the source or its flags with a standalone probe. It
rejects conflicting ISA, optimization, C++ standard, ABI, decoder and standard-library
overrides, including later ones. Actual macros must prove baseline x86-64, C++11,
ABI 1 and GCC's old / Clang's new decoder. Both candidates must identify the same
libstdc++ linker input/hash and header version before either native build begins.
Normalized actual decoder argv must also match exactly across the pair, removing
only the compiler executable and the recorded Clang GCC-toolchain driver selector.
One-sided options such as `-fno-inline`, `-fno-tree-vectorize` or `--param` therefore
cannot hide behind unchanged preprocessor macros. Unexpected differences fail preflight.
The compile database is hashed and checked again by the build phase.
The toolbox is immutable within one run. Its additional RPMs are resolved during
the first cloud preflight; reproducing that toolchain across later runs requires
reusing/exporting its image or pinning those resolved package versions. The base
image digest alone is not claimed to pin future package-repository contents.

The common source adaptation extends upstream's GNU Linux linker-options block to
Clang, so both receive the same options and export version script. The official
jemalloc-enforcement patch is applied to both. All changes and their hashes are
recorded. This is not a claim that C++ extension ABI compatibility follows from a
matching version string. One `httpfs`/`iceberg` byte set is downloaded into the
shared directory, retained and hashed. Stock, GCC and Clang explicitly LOAD those
same signed files, without INSTALL or version/signature/metadata bypasses. Actual
mapped extension paths/hashes and unchanged shared files are required.

Java 21 smoke processes use only their selected stock or rebuilt JAR. They hash the
actual mapped JNI `.so`, check its source version, and retain flushed raw rows for
JDBC types, Arrow types, Snappy Parquet, cancellation plus connection reuse, and
httpfs reads. Arrow buffer bytes exclude transport framing; unavailable JDBC bytes
and batches are `-1`. The GCC process alone writes the retained common Parquet
corpus. GCC, stock and Clang read that exact file; its hash must stay unchanged. The
upstream JDBC test runner also executes three selected cheap test filters (connection,
execution exceptions and multiple statements). Full logs, source and binary hashes,
compiler/linker/package/JVM versions, build resource counters, ELF metadata,
query attempts, GC and privacy-filtered JFR files survive failures. No environment
or credential dump is collected.

Every smoke connection explicitly uses one native DuckDB thread, a 256 MiB native
memory limit and 256 MiB maximum temporary-file usage in a task-owned directory.
The upstream test entry point wraps the JDBC driver to apply and verify the same
limits for every test connection. JVM heap/direct limits are separate. Upstream
filters must produce a nonzero successful result for each intended method; exit 0
or an empty `OK` log is insufficient.

Cancellation uses a prepared streaming `sum(hash(i))` aggregate with fixed-size state,
not a cross product. Readiness requires observed positive native query progress or
two observations of the exact `DuckDBNative.duckdb_jdbc_execute` native frame on the
known query thread with positive thread CPU advance and an incomplete task. Stacks,
CPU/timestamps, observed progress, cancel request, interrupt outcome and executor
termination are retained. Unsupported range progress is not relabeled as supported.
The runner seals its partial raw evidence as `RAW-SHA256SUMS`. The always-run
workflow cleanup stops only the recorded randomly named task containers, then
seals the complete upload including cleanup evidence as `SHA256SUMS`.
Stock JDBC closes a statement after an execution error, including interruption;
the cancellation smoke therefore checks the same connection with a fresh statement.

Before any performance experiment, inspect the complete cloud build and smoke
proof, library/ELF baselines and loaded SHA for each variant. Exercise all actual
deployment extensions and relevant Arrow/lifetime/protocol tests; the small smoke
does not cover every extension operation or deployment. Then use the same GCC-written
data and measured hardware for paired, warmed, oracle-checked trials. Record raw
latencies, CPU, memory, JFR and native stacks. Runner CPU features do not establish
production ISA support, and compiler selection alone proves no speedup.

Primary sources: [upstream decoder selector](https://github.com/duckdb/duckdb/blob/v1.5.6/third_party/snappy/snappy_version.hpp),
[official JDBC Linux build](https://github.com/duckdb/duckdb-java/blob/v1.5.6.0/.github/composite-actions/linux-build-docker/action.yml),
[official JNI build configuration](https://github.com/duckdb/duckdb-java/blob/v1.5.6.0/CMakeLists.txt),
[concurrent native progress API](https://github.com/duckdb/duckdb-java/blob/a561d2ddadde231402411ce76112779bc34b57f0/src/main/java/org/duckdb/DuckDBPreparedStatement.java).
