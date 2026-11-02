#!/usr/bin/env bash
# Hive cell of service-security.yml: a real Hive (apache/hive image; HiveServer2 with an embedded
# Derby metastore) runs <hive dir>/ddl.sql of service-hive-data.py - external tables over the
# Parquet and text files, INSERT OVERWRITE into the ORC and Avro tables (Hive writes those files)
# and a view - then a standalone metastore on the same Derby database serves the server (one
# process on the database at a time, as hive-mm-writer.sh). <hive dir> must not lie below a
# directory whose name starts with "_" (Hive skips such paths), so not under $RUNNER_TEMP.
#
# Usage: service-hive.sh <image> <hive dir> <derby dir>
# Leaves container "hive-svc-hms" serving thrift://localhost:9083.
set -euo pipefail

image=$1
dir=$2
derby=$3
mkdir -p "$derby"
chmod -R a+rwX "$dir" "$derby"

opts="-Dhive.metastore.warehouse.dir=file://$dir/warehouse -Dmetastore.warehouse.dir=file://$dir/warehouse"
opts="$opts -Djavax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$derby/db;create=true"
# HADOOP_CLIENT_OPTS of the image carry -Xmx1G; the later -Xmx wins (Tez runs in HiveServer2 here).
writer="$opts -Xmx4g -Dhive.exec.compress.output=false -Dhive.stats.autogather=false"

docker run -d --name hive-svc-hs2 -v "$dir:$dir" -v "$derby:$derby" -e SERVICE_NAME=hiveserver2 \
  -e SERVICE_OPTS="$writer" "$image" > /dev/null
for _ in $(seq 120); do
  docker exec hive-svc-hs2 beeline -u jdbc:hive2://localhost:10000/ -e "SELECT 1" > /dev/null 2>&1 && break
  sleep 2
done

start=$(date +%s)
if ! docker exec hive-svc-hs2 beeline --silent=true -u jdbc:hive2://localhost:10000/ -f "$dir/ddl.sql" \
    > "$dir/beeline.out" 2> "$dir/beeline.err"; then
  echo "Hive failed on ddl.sql:"
  grep -E 'Error|FAILED|Exception' "$dir/beeline.err" | head -20
  docker logs hive-svc-hs2 2>&1 | tail -40
  exit 1
fi
echo "Hive ran ddl.sql in $(( $(date +%s) - start )) s"
docker exec hive-svc-hs2 beeline --silent=true --showHeader=false --outputformat=tsv2 \
  -u jdbc:hive2://localhost:10000/ -e "SELECT count(*) FROM tpch_orc.lineitem; SELECT count(*) FROM tpch_avro.lineitem;
SELECT count(*) FROM tpch_views.segment_lines" 2> /dev/null | sed 's/^/Hive count: /'
docker stop -t 30 hive-svc-hs2 > /dev/null
du -sh "$dir"/{parquet,text,orc,avro}

docker run -d --name hive-svc-hms -p 9083:9083 -v "$dir:$dir" -v "$derby:$derby" \
  -e SERVICE_NAME=metastore -e IS_RESUME=true -e SERVICE_OPTS="$opts" "$image" > /dev/null
# Probed inside the container: docker's port proxy accepts on the host port before the metastore
# listens (a server started then saw "Socket is closed by peer").
listening() { docker exec hive-svc-hms bash -c '(exec 3<> /dev/tcp/localhost/9083)' 2> /dev/null; }
for _ in $(seq 90); do
  listening && break
  sleep 2
done
listening || { docker logs hive-svc-hms | tail -50; exit 1; }
echo "metastore up"
