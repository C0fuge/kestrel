#!/usr/bin/env bash
# Lane 4, C4 step 3 (c): insert-only (MM) tables written by a real Hive 4 (HiveServer2 with an
# embedded Derby metastore, Docker Hub apache/hive), then a standalone metastore started on the same
# Derby database for the server test (HiveRealWriterIntegrationTest). HiveServer2 runs manual
# compactions itself, which needs direct access to the metastore database: hence one embedded
# Derby used in turn by HiveServer2 (writing) and the metastore (reading), never at the same time.
#
# Usage: hive-mm-writer.sh <image> <warehouse dir> <derby dir> <expected dir>
# Leaves container "hive-mm-hms" serving thrift://localhost:9083; writes <expected dir>/<table>.tsv
# (Hive's own answer to SELECT * per table, tab-separated, no header) and <expected dir>/layout.txt.
set -euo pipefail

image=$1
wh=$2
derby=$3
expected=$4
mkdir -p "$wh" "$derby" "$expected"
chmod 777 "$wh" "$derby"

opts="-Dhive.support.concurrency=true -Dhive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
opts="$opts -Dhive.metastore.warehouse.dir=file://$wh -Dmetastore.warehouse.dir=file://$wh"
opts="$opts -Djavax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$derby/db;create=true"
writer="$opts -Dhive.compactor.initiator.on=true -Dhive.compactor.worker.threads=1"
writer="$writer -Dhive.exec.dynamic.partition.mode=nonstrict"

docker run -d --name hive-mm-hs2 -v "$wh:$wh" -v "$derby:$derby" -e SERVICE_NAME=hiveserver2 \
  -e SERVICE_OPTS="$writer" "$image" > /dev/null
for _ in $(seq 90); do
  docker exec hive-mm-hs2 beeline -u jdbc:hive2://localhost:10000/ -e "SELECT 1" > /dev/null 2>&1 && break
  sleep 2
done

hive() {
  docker exec hive-mm-hs2 beeline --silent=true --showHeader=false --outputformat=tsv2 \
    -u jdbc:hive2://localhost:10000/ -e "$1" 2> "$expected/beeline.err"
}
# A statement that must fail (its write id is aborted).
fails() {
  if hive "$1" > /dev/null; then
    echo "expected a failure: $1"
    exit 1
  fi
}
mm="TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"

hive "CREATE DATABASE mmw"
# Parquet: inserts, a failed insert, major compaction, an insert after it.
hive "CREATE TABLE mmw.tp (i int, s string) STORED AS PARQUET $mm"
hive "INSERT INTO mmw.tp VALUES (1, 'a'), (2, 'b')"
hive "INSERT INTO mmw.tp VALUES (3, 'c')"
fails "INSERT INTO mmw.tp SELECT i + 100, s FROM mmw.tp WHERE assert_true(i < 0) IS NULL"
hive "INSERT INTO mmw.tp VALUES (4, 'd')"
hive "ALTER TABLE mmw.tp COMPACT 'major' AND WAIT"
hive "INSERT INTO mmw.tp VALUES (5, 'e')"
# Text: INSERT OVERWRITE replaces older writes; gzip output for the last insert.
hive "CREATE TABLE mmw.tt (i int, s string) STORED AS TEXTFILE $mm"
hive "INSERT INTO mmw.tt VALUES (1, 'a'), (2, 'b')"
hive "INSERT INTO mmw.tt VALUES (3, 'c')"
hive "INSERT OVERWRITE TABLE mmw.tt SELECT 10, 'x'"
hive "SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
INSERT INTO mmw.tt VALUES (11, 'y')"
# ORC, partitioned: dynamic partitions, a failed insert, INSERT OVERWRITE of one partition.
hive "CREATE TABLE mmw.tor (i int, s string) PARTITIONED BY (k string) STORED AS ORC $mm"
hive "INSERT INTO mmw.tor PARTITION (k) VALUES (1, 'a', 'p'), (2, 'b', 'q')"
hive "INSERT INTO mmw.tor PARTITION (k) VALUES (3, 'c', 'p'), (4, 'd', 'q')"
fails "INSERT INTO mmw.tor PARTITION (k = 'p') SELECT i + 100, s FROM mmw.tor WHERE assert_true(i < 0) IS NULL"
hive "INSERT OVERWRITE TABLE mmw.tor PARTITION (k = 'q') SELECT 20, 'o'"
# ORC: minor compaction of three writes.
hive "CREATE TABLE mmw.tm (i int, s string) STORED AS ORC $mm"
for i in 1 2 3; do
  hive "INSERT INTO mmw.tm VALUES ($i, 'm$i')"
done
hive "ALTER TABLE mmw.tm COMPACT 'minor' AND WAIT"
hive "INSERT INTO mmw.tm VALUES (4, 'm4')"

for t in tp tt tor tm; do
  hive "SELECT * FROM mmw.$t" | sort > "$expected/$t.tsv"
done
hive "SHOW COMPACTIONS" > "$expected/compactions.txt"
docker stop -t 30 hive-mm-hs2 > /dev/null
(cd "$wh" && find . | sort) > "$expected/layout.txt"

docker run -d --name hive-mm-hms -p 9083:9083 -v "$wh:$wh" -v "$derby:$derby" \
  -e SERVICE_NAME=metastore -e IS_RESUME=true -e SERVICE_OPTS="$opts" "$image" > /dev/null
for _ in $(seq 90); do
  (exec 3<> /dev/tcp/localhost/9083) 2> /dev/null && break
  sleep 2
done
(exec 3<> /dev/tcp/localhost/9083) 2> /dev/null || { docker logs hive-mm-hms | tail -50; exit 1; }
echo "Hive wrote:"
cat "$expected/layout.txt"
for t in tp tt tor tm; do
  echo "$t:"
  cat "$expected/$t.tsv"
done
