#!/usr/bin/env bash
# One cell of the stores service matrix (.github/workflows/service-stores.yml, lane 4): starts the
# real service as containers, loads TPC-H (tools/ci/service-tpch.py gen output in $TPCH), starts the
# server ($SRV: server.jar, libs/, plugins/) in a container with the profile, and runs
# tools/ci/service-tpch.py check (22 queries vs in-process DuckDB, Flight SQL metadata, a prepared
# query). Writes $RESULT (json). Test-only credentials, generated here (lakeFS: its documented
# quickstart key pair, a throw-away container).
# Usage: service-stores-cell.sh <service> <profile>
#   service: s3 | ozone | lakefs-branch | lakefs-commit | postgres | mysql
#   profile: balanced | speed
# Environment: SRV, EXT (DuckDB extension dir: v1.5.6/linux_amd64/*), TPCH, PY (python with duckdb,
# adbc), RESULT, WORK (scratch directory).
set -euo pipefail
service=$1
profile=$2
: "${SRV:?}" "${EXT:?}" "${TPCH:?}" "${PY:?}" "${RESULT:?}" "${WORK:?}"
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$WORK"
fail() { # writes a failed result without a check run
  printf '{"ok": false, "error": %s, "lines": []}\n' "$("$PY" -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")" > "$RESULT"
  echo "FAIL: $1"
  exit 1
}

key=$(openssl rand -hex 8)
secret=$(openssl rand -hex 16)
echo "::add-mask::$secret"
s3put() { # endpoint bucket-path: uploads the Parquet tree below <bucket-path>/tpch/
  AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$secret AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$1" s3 cp --recursive --only-show-errors "$TPCH/data/tpch" "s3://$2/tpch"
}
s3mb() {
  AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$secret AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$1" s3 mb "s3://$2"
}

# Server options per service: data directory, -D options, environment.
opts=()
envs=()
dataDir=/data
schema=tpch
mkdir -p "$WORK/data"
case "$service" in
  s3)
    # MinIO; two stores: the data directory on bucket "lake" and a named store "wh" on bucket "wh"
    # (holding a second copy, read by no catalog here: the named store must not disturb the
    # main one; the Iceberg cells read through it).
    image=""
    for candidate in minio/minio:RELEASE.2025-04-22T22-12-26Z quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z \
        cgr.dev/chainguard/minio:latest; do
      if docker pull -q "$candidate" > /dev/null 2>&1; then image=$candidate; break; fi
      echo "cannot pull $candidate"
    done
    [ -n "$image" ] || fail "no MinIO image could be pulled (minio/minio, quay.io/minio/minio, cgr.dev/chainguard/minio)"
    echo "MinIO image: $image"
    docker run -d --name svc --network host -e "MINIO_ROOT_USER=$key" -e "MINIO_ROOT_PASSWORD=$secret" \
      "$image" server /tmp/minio --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 > /dev/null
    for _ in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:9000/minio/health/ready && break; sleep 1; done
    curl -fsS -o /dev/null http://127.0.0.1:9000/minio/health/ready || fail "MinIO did not become ready"
    s3mb http://127.0.0.1:9000 lake && s3mb http://127.0.0.1:9000 wh
    s3put http://127.0.0.1:9000 lake/data && s3put http://127.0.0.1:9000 wh/copy
    dataDir=s3://lake/data
    opts+=(-Darrowflight.s3.endpoint=http://127.0.0.1:9000 -Darrowflight.s3.urlStyle=path
      -Darrowflight.s3.stores=wh -Darrowflight.s3.wh.root=s3://wh/copy -Darrowflight.s3.wh.endpoint=http://127.0.0.1:9000)
    envs+=(-e "FLIGHT_S3_ACCESS_KEY=$key" -e "FLIGHT_S3_SECRET_KEY=$secret"
      -e "FLIGHT_S3_WH_ACCESS_KEY=$key" -e "FLIGHT_S3_WH_SECRET_KEY=$secret")
    ;;
  ozone)
    # Apache Ozone's S3 gateway (non-secure: any key is accepted, so the keys only travel).
    "$here/ozone-s3.sh" apache/ozone:2.0.0 9000
    s3mb http://127.0.0.1:9000 lake
    s3put http://127.0.0.1:9000 lake/data
    dataDir=s3://lake/data
    opts+=(-Darrowflight.s3.endpoint=http://127.0.0.1:9000 -Darrowflight.s3.urlStyle=path)
    envs+=(-e "FLIGHT_S3_ACCESS_KEY=$key" -e "FLIGHT_S3_SECRET_KEY=$secret")
    ;;
  lakefs-branch|lakefs-commit)
    # lakeFS (local blockstore, quickstart: its documented example key pair); repository "tpch",
    # the data committed on branch main. The branch cell reads lakefs://tpch/main/data; the commit
    # cell reads the first commit's id after a second commit on main emptied nation, so it must
    # still answer from the first commit.
    key=AKIAIOSFOLQUICKSTART
    secret=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    docker run -d --name svc --network host treeverse/lakefs:1.50.0 run --quickstart > /dev/null
    for _ in $(seq 60); do curl -fsS -o /dev/null http://127.0.0.1:8000/_health && break; sleep 1; done
    curl -fsS -o /dev/null http://127.0.0.1:8000/_health || fail "lakeFS did not become ready"
    api() { curl -fsS -u "$key:$secret" -H 'Content-Type: application/json' "$@"; }
    api -X POST http://127.0.0.1:8000/api/v1/repositories \
      -d '{"name": "tpch", "storage_namespace": "local://tpch", "default_branch": "main"}' > /dev/null
    s3put http://127.0.0.1:8000 tpch/main/data
    commit=$(api -X POST http://127.0.0.1:8000/api/v1/repositories/tpch/branches/main/commits \
      -d '{"message": "tpch"}' | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["id"])')
    echo "first commit $commit"
    if [ "$service" = lakefs-commit ]; then
      # A later commit empties nation on main; the pinned commit must not see it.
      "$PY" -c "import duckdb,sys; duckdb.sql(\"COPY (SELECT * FROM read_parquet('$TPCH/data/tpch/nation/data.parquet') LIMIT 0) TO '$WORK/nation-empty.parquet' (FORMAT parquet)\")"
      AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$secret AWS_DEFAULT_REGION=us-east-1 \
        aws --endpoint-url http://127.0.0.1:8000 s3 cp --only-show-errors "$WORK/nation-empty.parquet" \
        s3://tpch/main/data/tpch/nation/data.parquet
      api -X POST http://127.0.0.1:8000/api/v1/repositories/tpch/branches/main/commits \
        -d '{"message": "empty nation"}' > /dev/null
      dataDir=lakefs://tpch/$commit/data
    else
      dataDir=lakefs://tpch/main/data
    fi
    opts+=(-Darrowflight.lakefs.endpoint=http://127.0.0.1:8000)
    envs+=(-e "FLIGHT_LAKEFS_ACCESS_KEY=$key" -e "FLIGHT_LAKEFS_SECRET_KEY=$secret")
    ;;
  postgres|mysql)
    # The jdbc plugin over a real database holding TPC-H (loaded from the generated CSV files).
    chmod -R a+rX "$TPCH/csv"
    ddl=$("$PY" - "$TPCH/reference.json" "$service" <<'EOF'
import json, sys
ref = json.load(open(sys.argv[1]))
out = []
for t, cols in ref["columns"].items():
    defs = ", ".join(f"{n} {'varchar(256)' if ty == 'VARCHAR' else ty.lower()}" for n, ty in cols)
    out.append(f"CREATE TABLE {t} ({defs});")
print("\n".join(out))
EOF
)
    tables=$("$PY" -c 'import json,sys; print(" ".join(json.load(open(sys.argv[1]))["columns"]))' "$TPCH/reference.json")
    if [ "$service" = postgres ]; then
      docker run -d --name svc --network host -v "$TPCH/csv:/csv:ro" -e POSTGRES_USER=tpch \
        -e "POSTGRES_PASSWORD=$secret" -e POSTGRES_DB=tpch postgres:16.4 > /dev/null
      for _ in $(seq 60); do docker exec svc pg_isready -h 127.0.0.1 -U tpch -d tpch > /dev/null 2>&1 && break; sleep 2; done
      sleep 3
      { echo "$ddl"; for t in $tables; do echo "COPY $t FROM '/csv/$t.csv' (FORMAT csv);"; done; echo "ANALYZE;"; } \
        | docker exec -i svc psql -v ON_ERROR_STOP=1 -q -h 127.0.0.1 -U tpch -d tpch
      list=$(for t in $tables; do printf 'public.%s,' "$t"; done)
      opts+=(-Darrowflight.jdbc.sources=pg -Darrowflight.jdbc.pg.type=postgres -Darrowflight.jdbc.pg.host=127.0.0.1
        -Darrowflight.jdbc.pg.port=5432 -Darrowflight.jdbc.pg.database=tpch -Darrowflight.jdbc.pg.user=tpch
        "-Darrowflight.jdbc.pg.tables=${list%,}")
      envs+=(-e "FLIGHT_JDBC_PG_PASSWORD=$secret")
      schema=pg
    else
      docker run -d --name svc --network host -v "$TPCH/csv:/csv:ro" -e MYSQL_USER=tpch \
        -e "MYSQL_PASSWORD=$secret" -e MYSQL_DATABASE=tpch -e "MYSQL_ROOT_PASSWORD=$secret" mysql:8.4 \
        --secure-file-priv=/csv > /dev/null
      for _ in $(seq 90); do docker exec svc mysqladmin ping -h 127.0.0.1 -uroot "-p$secret" --silent > /dev/null 2>&1 && break; sleep 2; done
      sleep 5
      docker exec svc mysql -h 127.0.0.1 -uroot "-p$secret" -e "GRANT FILE ON *.* TO 'tpch'@'%'" 2> /dev/null
      { echo "$ddl"
        for t in $tables; do
          echo "LOAD DATA INFILE '/csv/$t.csv' INTO TABLE $t FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n';"
        done; } | docker exec -i svc mysql -h 127.0.0.1 -utpch "-p$secret" tpch 2> >(grep -v 'Using a password' >&2)
      list=$(for t in $tables; do printf 'tpch.%s,' "$t"; done)
      opts+=(-Darrowflight.jdbc.sources=my -Darrowflight.jdbc.my.type=mysql -Darrowflight.jdbc.my.host=127.0.0.1
        -Darrowflight.jdbc.my.port=3306 -Darrowflight.jdbc.my.database=tpch -Darrowflight.jdbc.my.user=tpch
        "-Darrowflight.jdbc.my.tables=${list%,}")
      envs+=(-e "FLIGHT_JDBC_MY_PASSWORD=$secret")
      schema=my
    fi
    opts+=(-Dcatalog=jdbc)
    ;;
  *)
    fail "unknown service $service"
    ;;
esac

[ "$profile" = speed ] && opts+=(-DresourceProfile=speed)
docker run -d --name srv --network host --memory=10g -v "$SRV:/app:ro" -v "$EXT:/ext:ro" \
  -v "$WORK/data:/data:ro" -e FLIGHT_ALLOW_INSECURE=true "${envs[@]}" eclipse-temurin:21-jre \
  java -Xmx3g -XX:MaxDirectMemorySize=2g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -Dhazelcast.phone.home.enabled=false -Darrowflight.plugins.dir=/app/plugins \
  -Darrowflight.duckdb.extensionDir=/ext "${opts[@]}" -cp "/app/server.jar:/app/libs/*" \
  pro.surpin.data.arrowflight.server.HadoopArrowFlightServer --data-dir "$dataDir" --localhost 127.0.0.1 \
  --port 32010 --hosts 127.0.0.1 > /dev/null
for _ in $(seq 120); do docker logs srv 2>&1 | grep 'Server Arrow Flight SQL started' > /dev/null && break; sleep 1; done
if ! docker logs srv 2>&1 | grep 'Server Arrow Flight SQL started' > /dev/null; then
  docker logs srv 2>&1 | tail -n 60
  first=$(docker logs srv 2>&1 | grep -m 1 -E 'ERROR|Exception' || echo "no error line")
  fail "server did not start: $first"
fi
docker logs srv 2>&1 | grep -oE '(duckdb=sizing|resourceProfile) .*' | head -n 3 || true
rc=0
"$PY" -I "$here/service-tpch.py" check 127.0.0.1:32010 "$TPCH" "$schema" "$RESULT" || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "--- server log (errors)"
  docker logs srv 2>&1 | grep -E -A3 'ERROR|WARN.*(refus|fail)' | head -n 80 || true
fi
peak=$(docker exec srv cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)
[ -n "$peak" ] && echo "server container memory peak $((peak / 1048576)) MiB"
exit "$rc"
