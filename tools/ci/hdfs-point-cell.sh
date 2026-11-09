#!/usr/bin/env bash
# Item 39 (c), lane 4: point / small queries on a single-node HDFS, closed loop, per resource profile.
#
# Usage: tools/ci/hdfs-point-cell.sh <balanced|speed> <input dir> <work dir> <clients> <seconds> <keys> [extra -D options]
#
# One NameNode and one DataNode (apache/hadoop, replication 1, no security), short-circuit reads when
# the image's native Hadoop library loads; the server runs in the DataNode's network namespace with its
# domain socket (as tools/ci/service-hdfs-cell.sh "plain") on CPUs 0-1 (--cpuset-cpus) with 6g; the
# load client (tools/ci/point-query-load.py) runs on the other CPUs. For each client count: q/s and
# latency percentiles, server CPU (docker stats, sampled), and at the largest client count three
# jcmd Thread.print dumps 3 s apart, summarised by tools/ci/thread-dump-summary.py.
# Writes <work>/result.txt (one line per client count), <work>/threads.txt, <work>/*.log.
set -u
profile=$1 in=$2 work=$3 clients=$4 seconds=$5 keys=$6 extra=${7:-}
image=${HADOOP_IMAGE:-apache/hadoop:3.4.1}
mkdir -p "$work"
res="$work/result.txt"; : > "$res"
fail() {
  echo "$profile|setup|$*" | tee -a "$res"
  for c in nn1 dn1 srv; do
    docker logs "$c" > "$work/$c.log" 2>&1 || true
    echo "--- $c (last lines)"; tail -n 25 "$work/$c.log" | cut -c1-400
  done
  exit 1
}
case "$profile" in balanced) opts="" ;; speed) opts="-DresourceProfile=speed" ;; *) fail "unknown profile $profile" ;; esac
opts="$opts $extra"

docker network create --subnet 172.31.0.0/24 hd > /dev/null 2>&1 || true
hd=/srv/hd
sudo rm -rf "$hd"; sudo install -d -o 1000 -g 1000 -m 755 "$hd" "$hd/nn1" "$hd/sock"
conf="$work/conf"; mkdir -p "$conf"
# DuckDB (hadoopfs, libhdfs3) reads short-circuit; the server's own Java client runs without the
# native Hadoop library (with it, this cell's JVM crashed in ShortCircuitCache: "NativeIO is not
# available", SIGSEGV), so its few footer reads go over TCP.
sc=off native=()
if docker run --rm "$image" hadoop checknative 2>&1 | grep -qE '^hadoop: +true'; then
  sc=on
fi
props=(fs.defaultFS hdfs://172.31.0.10:8020 dfs.replication 1 dfs.permissions.enabled false
  dfs.namenode.rpc-bind-host 0.0.0.0 dfs.namenode.http-bind-host 0.0.0.0 dfs.namenode.name.dir /hd/name
  dfs.datanode.data.dir /tmp/hdfs/dn dfs.namenode.datanode.registration.ip-hostname-check false)
[ "$sc" = on ] && props+=(dfs.client.read.shortcircuit true dfs.domain.socket.path /var/run/hdfs-sockets/dn)
{
  echo '<?xml version="1.0"?>'; echo '<configuration>'
  set -- "${props[@]}"
  while [ $# -ge 2 ]; do printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"; shift 2; done
  echo '</configuration>'
} > "$conf/hdfs-site.xml"
cp "$conf/hdfs-site.xml" "$conf/core-site.xml"
hconf=(-v "$conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" -v "$conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro")
sock=(); [ "$sc" = on ] && sock=(-v "$hd/sock:/var/run/hdfs-sockets")

docker rm -f nn1 dn1 srv > /dev/null 2>&1 || true
docker run -d --name nn1 --network hd --ip 172.31.0.10 --hostname nn1 "${hconf[@]}" -v "$hd/nn1:/hd/name" "$image" \
  bash -c 'hdfs namenode -format -nonInteractive -force > /tmp/format.log 2>&1 || { cat /tmp/format.log; exit 1; }; exec hdfs namenode' > /dev/null
docker run -d --name dn1 --network hd --ip 172.31.0.12 --hostname dn1 "${hconf[@]}" "${sock[@]}" "$image" hdfs datanode > /dev/null
report=""
for _ in $(seq 120); do
  report=$(docker exec nn1 hdfs dfsadmin -report 2>/dev/null || true)
  [[ $report == *"Live datanodes (1)"* && $report != *'Safe mode is ON'* ]] && break
  sleep 1
done
[[ $report == *"Live datanodes (1)"* ]] || fail "HDFS did not come up"
docker cp "$in/data" nn1:/tmp/upload > /dev/null
docker exec nn1 bash -c 'hdfs dfs -mkdir -p /data && hdfs dfs -put /tmp/upload/tpch /data/' || fail "HDFS upload failed"

echo "server options: ${opts} short-circuit=$sc" | tee "$work/notes.txt"
# shellcheck disable=SC2086
docker run -d --name srv --network container:dn1 "${sock[@]}" "${native[@]}" --cpuset-cpus=0,1 --memory=6g \
  -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
  -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
  -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
  sc-server /jdk/bin/java -Xmx2g -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -Dhazelcast.phone.home.enabled=false -DnumServers=1 $opts -Darrowflight.duckdb.extensionDir=/extdir \
  -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir hdfs://172.31.0.10:8020/data --localhost 172.31.0.12 --port 32010 --hazelcast-port 5701 \
  --hosts 172.31.0.12:5701 --metrics-enabled false > /dev/null
for _ in $(seq 180); do
  docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
  [ "$(docker inspect -f '{{.State.Running}}' srv)" = true ] || break
  sleep 1
done
docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started' \
  || fail "server did not start: $(docker logs srv 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
docker logs srv 2>&1 | grep -m 3 -oE 'duckdb=(threads|sizing|sharedDatabaseOpened) .*' | cut -c1-300 | tee -a "$work/notes.txt"

py="$RUNNER_TEMP/py/bin/python"
max=$(echo "$clients" | tr ',' '\n' | sort -n | tail -n 1)
status=0
"$py" -I tools/ci/point-query-load.py 172.31.0.12:32010 1 5 "$keys" > /dev/null 2>&1 || true # warm-up
IFS=',' read -ra counts <<< "$clients"
for c in "${counts[@]}"; do
  : > "$work/cpu-c$c.txt"
  ( sleep 4; for _ in $(seq 6); do docker stats --no-stream --format '{{.CPUPerc}}' srv >> "$work/cpu-c$c.txt"; sleep 1; done ) &
  sampler=$!
  dumper=""
  if [ "$c" = "$max" ]; then
    ( sleep $(( ${seconds%.*} / 3 )); for d in 1 2 3; do docker exec srv /jdk/bin/jcmd 1 Thread.print > "$work/dump-$d.txt" 2>&1; sleep 3; done ) &
    dumper=$!
  fi
  # Per-query HDFS work: NameNode opens (audit cmd=open, one per file a DuckDB or the Java client
  # opens) and DataNode short-circuit fd requests (one per block a client without the fds cached reads).
  opens0=$(docker logs nn1 2>&1 | grep -c 'cmd=open')
  fds0=$(docker logs dn1 2>&1 | grep -c 'op: REQUEST_SHORT_CIRCUIT_FDS')
  line=$(taskset -c 2,3 "$py" -I tools/ci/point-query-load.py 172.31.0.12:32010 "$c" "$seconds" "$keys" 2> "$work/client-c$c.log") || status=1
  wait "$sampler"; [ -n "$dumper" ] && wait "$dumper"
  opens=$(( $(docker logs nn1 2>&1 | grep -c 'cmd=open') - opens0 ))
  fds=$(( $(docker logs dn1 2>&1 | grep -c 'op: REQUEST_SHORT_CIRCUIT_FDS') - fds0 ))
  queries=$(echo "$line" | grep -oE 'queries=[0-9]+' | cut -d= -f2)
  per=$(awk -v o="$opens" -v f="$fds" -v q="${queries:-0}" 'BEGIN { if (q > 0) printf "opensPerQuery=%.2f fdRequestsPerQuery=%.2f", o / q, f / q; else print "opensPerQuery=? fdRequestsPerQuery=?" }')
  cpu=$(sed 's/%//' "$work/cpu-c$c.txt" | awk '{ s += $1; n++ } END { if (n) printf "%.0f", s / n; else print "?" }')
  echo "${LABEL:-$profile}|$line cpu=${cpu}% $per" | tee -a "$res"
done
python3 -I tools/ci/thread-dump-summary.py "$work"/dump-*.txt > "$work/threads.txt" 2>&1 || true
head -n 60 "$work/threads.txt"
for c in nn1 dn1 srv; do docker logs "$c" > "$work/$c.log" 2>&1 || true; done
docker rm -f srv dn1 nn1 > /dev/null 2>&1 || true
exit "$status"
