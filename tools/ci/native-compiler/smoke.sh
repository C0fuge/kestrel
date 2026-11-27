#!/usr/bin/env bash
set -euo pipefail
evidence=$1
corpus=$2
control=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$control/../../.." && pwd)
test "$(java -XshowSettings:properties -version 2>&1 | sed -n 's/.*java.specification.version = //p')" = 21
mkdir "$evidence/smoke" "$evidence/smoke/classes" "$evidence/smoke/home"
smoke="$evidence/smoke"
java -version > "$smoke/java-version.txt" 2>&1
# No DuckDB Maven dependency: only the candidate's just-built JAR can load JNI.
mvn -B -ntp -f "$control/pom.xml" org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile="$smoke/dependency-classpath.txt" > "$smoke/dependencies.log" 2>&1
cp "$control/pom.xml" "$smoke/dependency-pom.xml"
python3 - "$smoke" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1])
dependencies=[]
for item in (root/'dependency-classpath.txt').read_text().strip().split(':'):
    path=Path(item)
    with path.open('rb') as source:
        dependencies.append({'file':path.name,'sha256':hashlib.file_digest(source,'sha256').hexdigest(),'bytes':path.stat().st_size})
with (root/'dependency-identities.json').open('x') as output:
    json.dump(dependencies,output,indent=2)
PY
cp="$evidence/duckdb_jdbc.jar:$(cat "$smoke/dependency-classpath.txt")"
package=pro/surpin/data/arrowflight/server
javac --release 21 -cp "$cp" -d "$smoke/classes" "$control/NativeCompilerSmoke.java" \
  "$repo/flight-server/src/test/java/$package/TpchChecksum.java" \
  "$repo/flight-server/src/test/java/$package/TpchArrowValues.java" > "$smoke/compile.log" 2>&1
jfr configure --input "$JAVA_HOME/lib/jfr/profile.jfc" --output "$smoke/smoke.jfc" \
  jdk.InitialEnvironmentVariable#enabled=false jdk.InitialSystemProperty#enabled=false \
  jdk.DirectBufferStatistics#enabled=true > "$smoke/jfr-configure.log" 2>&1
native_sha=$(sha256sum "$evidence/libduckdb_java.so_linux_amd64" | cut -d' ' -f1)
command=(java -Xmx512m -XX:MaxDirectMemorySize=512m
  "-XX:StartFlightRecording=filename=$smoke/smoke.jfr,dumponexit=true,settings=$smoke/smoke.jfc"
  "-Xlog:gc*,safepoint:file=$smoke/gc.log:time,uptime,level,tags:filecount=0"
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  -cp "$smoke/classes:$cp" pro.surpin.data.arrowflight.server.NativeCompilerSmoke "$smoke" "$native_sha" "$corpus" "$(basename "$evidence")")
printf '%q ' "${command[@]}" > "$smoke/java-command.txt"
rc=0
/usr/bin/time -v -o "$smoke/java-resources.txt" "${command[@]}" > "$smoke/java.log" 2>&1 || rc=$?
printf '%s\n' "$rc" > "$smoke/java-exit-status.txt"
jfr summary "$smoke/smoke.jfr" > "$smoke/jfr-summary.txt" 2>&1 || rc=1
jfr print --json --events jdk.InitialEnvironmentVariable,jdk.InitialSystemProperty "$smoke/smoke.jfr" > "$smoke/jfr-private-events.json" 2>&1 || rc=1
python3 - "$smoke" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1])
assert not json.loads((root/'jfr-private-events.json').read_text())['recording']['events']
PY
# Select cheap upstream JDBC checks; the complete suite includes packaging and large-data tests.
for test in test_connection test_execute_exception test_multiple_statements_execution; do
  command=(java -Xmx512m -cp "$evidence/duckdb_jdbc_tests.jar:$cp" org.duckdb.TestDuckDBJDBC "$test")
  printf '%q ' "${command[@]}" > "$smoke/upstream-$test-command.txt"
  status=0
  /usr/bin/time -v -o "$smoke/upstream-$test-resources.txt" "${command[@]}" > "$smoke/upstream-$test.log" 2>&1 || status=$?
  printf '%s\n' "$status" > "$smoke/upstream-$test-status.txt"
  [ "$status" -eq 0 ] || rc=1
done
exit "$rc"
