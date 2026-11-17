#!/usr/bin/env bash
# Item 39 (c), lane 4: point / small queries on a single-node HDFS, closed loop, per resource profile.
#
# Usage: tools/ci/hdfs-point-cell.sh <balanced|speed|upstream> <input dir> <work dir> <clients> <seconds> <keys> [extra -D options]
#
# One NameNode and one DataNode (apache/hadoop, replication 1, no security), short-circuit reads when
# the image's native Hadoop library loads; the server runs in the DataNode's network namespace with its
# domain socket (as tools/ci/service-hdfs-cell.sh "plain") on CPUs 0-1 (--cpuset-cpus) with 6g; the
# load client (tools/ci/point-query-load.py) runs on the other CPUs. For each client count: q/s and
# latency percentiles, server CPU (docker stats, sampled), and at the largest client count three
# jcmd Thread.print dumps 3 s apart, summarised by tools/ci/thread-dump-summary.py.
# Writes <work>/result.txt (one line per client count), <work>/threads.txt, <work>/*.log.
# Scan mode (item 54, SCAN_ROUNDS set): instead of the point load, tools/ci/scan-adbc.py reads
# `SELECT * FROM tpch.lineitem` in complete mode: one stream (x1), SCAN_CLIENTS streams at once and an
# export, SCAN_ROUNDS measured passes each, plus the server's per-thread CPU during one x1 pass
# (tools/ci/thread-cpu.py). SERVER_CPUS / CLIENT_CPUS (default 0,1 / 2,3) pin server and client.
# HDFS_MODE (item 54 step 2): plain (default, short-circuit when the native library loads), tcp (no
# short-circuit: every block over the DataNode's TCP port, no security) or sasl (Kerberos realm CI.TEST
# from an MIT KDC container, as tools/ci/service-hdfs-cell.sh sasl-direct, DataNode with
# dfs.data.transfer.protection=${SASL_PROTECTION:-authentication,privacy}, no short-circuit: DuckDB reads
# every block over SASL data transfer, hdfsSaslDataTransfer auto, like the stand's direct reads).
# Profile upstream (item 54 step 3): the upstream server of <input>/upapp (hdfs-point-perf.yml input
# upstream) with its defaults and only the command-line options scan-perf.yml starts it with.
set -u
profile=$1 in=$2 work=$3 clients=$4 seconds=$5 keys=$6 extra=${7:-}
image=${HADOOP_IMAGE:-apache/hadoop:3.4.1}
mkdir -p "$work"
res="$work/result.txt"; : > "$res"
fail() {
  echo "$profile|setup|$*" | tee -a "$res"
  for c in kdc nn1 dn1 srv; do
    docker logs "$c" > "$work/$c.log" 2>&1 || true
    echo "--- $c (last lines)"; tail -n 25 "$work/$c.log" | cut -c1-400
  done
  docker rm -f srv dn1 nn1 kdc > /dev/null 2>&1 || true
  exit 1
}
mode=${HDFS_MODE:-plain}
case "$mode" in plain|tcp|sasl) ;; *) fail "unknown HDFS_MODE $mode" ;; esac
app="$in/app"
srvArgs=(--hazelcast-port 5701 --hosts 172.31.0.12:5701 --metrics-enabled false)
upExtra=()
case "$profile" in
  balanced) opts="" ;;
  speed) opts="-DresourceProfile=speed" ;;
  upstream)
    app="$in/upapp"
    srvArgs=(--hosts 172.31.0.12)
    # Upstream's DuckDB finds no hdfs:// file system with /extdir alone (run 44172681969): hadoopfs also in DuckDB's
    # default extension directory and under the setting names this fork inherited.
    hext=/extdir/${DUCKDB_VERSION:-v1.5.6}/linux_amd64/hadoopfs.duckdb_extension
    upExtra=(-v "$in/extdir:/root/.duckdb/extensions:ro" -e "DUCKDB_HDFS_EXTENSION=$hext")
    opts="-DduckDbHdfsExtension=$hext -Darrowflight.duckdb.hdfs.extension=$hext"
    [ -f "$app/arrow-flight-server.jar" ] || fail "profile upstream needs the workflow input upstream (no $app)"
    ;;
  *) fail "unknown profile $profile" ;;
esac
opts="$opts $extra"

docker network create --subnet 172.31.0.0/24 hd > /dev/null 2>&1 || true
hd=/srv/hd
sudo rm -rf "$hd"; sudo install -d -o 1000 -g 1000 -m 755 "$hd" "$hd/nn1" "$hd/sock" "$hd/krb"
conf="$work/conf"; mkdir -p "$conf"
# DuckDB (hadoopfs, libhdfs3) reads short-circuit; the server's own Java client runs without the
# native Hadoop library (with it, this cell's JVM crashed in ShortCircuitCache: "NativeIO is not
# available", SIGSEGV), so its few footer reads go over TCP.
sc=off native=()
if [ "$mode" = plain ] && docker run --rm "$image" hadoop checknative 2>&1 | grep -qE '^hadoop: +true'; then
  sc=on
fi
props=(fs.defaultFS hdfs://172.31.0.10:8020 dfs.replication 1 dfs.permissions.enabled false
  dfs.namenode.rpc-bind-host 0.0.0.0 dfs.namenode.http-bind-host 0.0.0.0 dfs.namenode.name.dir /hd/name
  dfs.datanode.data.dir /tmp/hdfs/dn dfs.namenode.datanode.registration.ip-hostname-check false)
[ "$sc" = on ] && props+=(dfs.client.read.shortcircuit true dfs.domain.socket.path /var/run/hdfs-sockets/dn)
[ "$sc" = on ] || props+=(dfs.client.read.shortcircuit false)
if [ "$mode" = sasl ]; then
  p() { echo "$1/hdfs.ci@CI.TEST"; }
  props+=(hadoop.security.authentication kerberos hadoop.security.authorization false hadoop.rpc.protection authentication
    dfs.block.access.token.enable true
    dfs.namenode.kerberos.principal "$(p nn)" dfs.namenode.keytab.file /hd/krb/hdfs.keytab
    dfs.namenode.kerberos.internal.spnego.principal "$(p HTTP)"
    dfs.datanode.kerberos.principal "$(p dn)" dfs.datanode.keytab.file /hd/krb/hdfs.keytab
    dfs.web.authentication.kerberos.principal "$(p HTTP)" dfs.web.authentication.kerberos.keytab /hd/krb/hdfs.keytab
    ignore.secure.ports.for.testing true dfs.data.transfer.protection "${SASL_PROTECTION:-authentication,privacy}")
fi
{
  echo '<?xml version="1.0"?>'; echo '<configuration>'
  set -- "${props[@]}"
  while [ $# -ge 2 ]; do printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"; shift 2; done
  echo '</configuration>'
} > "$conf/hdfs-site.xml"
cp "$conf/hdfs-site.xml" "$conf/core-site.xml"
hconf=(-v "$conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" -v "$conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro")
sock=(); [ "$sc" = on ] && sock=(-v "$hd/sock:/var/run/hdfs-sockets")

docker rm -f nn1 dn1 srv kdc > /dev/null 2>&1 || true
kenv=() srvKrb=()
if [ "$mode" = sasl ]; then
  # The KDC of tools/ci/service-hdfs-cell.sh (MIT krb5 on ubuntu:24.04, bounded apt retries over three
  # mirrors); test-only principals and keytabs, made here.
  cat > "$conf/krb5.conf" <<'EOF'
[libdefaults]
  default_realm = CI.TEST
  dns_lookup_kdc = false
  dns_lookup_realm = false
  rdns = false
  dns_canonicalize_hostname = false
  udp_preference_limit = 1
  forwardable = true
  ticket_lifetime = 4h
[realms]
  CI.TEST = {
    kdc = 172.31.0.5:88
    admin_server = 172.31.0.5
  }
EOF
  cat > "$work/kdc.sh" <<'EOF'
set -e
export DEBIAN_FRONTEND=noninteractive
apt_opts="-o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30"
for t in 1 2 3; do
  [ "$t" != 2 ] || sed -i -e 's#http://archive.ubuntu.com#http://azure.archive.ubuntu.com#g' \
    -e 's#http://security.ubuntu.com#http://azure.archive.ubuntu.com#g' /etc/apt/sources.list.d/ubuntu.sources
  [ "$t" != 3 ] || sed -i -E 's#http://[a-z.]*ubuntu[.]com/ubuntu#http://mirrors.kernel.org/ubuntu#g' \
    /etc/apt/sources.list.d/ubuntu.sources
  echo "kdc: apt try $t $(date -u +%T)"
  timeout 240 apt-get $apt_opts update -qq && timeout 240 apt-get $apt_opts install -y -qq --no-install-recommends \
    krb5-kdc krb5-admin-server krb5-user > /dev/null && break
  [ "$t" -lt 3 ] || { echo "kdc: apt failed"; exit 1; }
  sleep 5
done
cp /conf/krb5.conf /etc/krb5.conf
mkdir -p /etc/krb5kdc
printf '%s\n' '[realms]' ' CI.TEST = {' '  master_key_type = aes256-cts-hmac-sha1-96' \
  '  supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal' ' }' > /etc/krb5kdc/kdc.conf
kdb5_util create -s -r CI.TEST -P "$KPASS" > /dev/null
for p in nn dn HTTP; do kadmin.local -q "addprinc -randkey $p/hdfs.ci@CI.TEST" > /dev/null; done
kadmin.local -q "addprinc -randkey flight@CI.TEST" > /dev/null
kadmin.local -q "ktadd -k /krb/hdfs.keytab nn/hdfs.ci@CI.TEST dn/hdfs.ci@CI.TEST HTTP/hdfs.ci@CI.TEST" > /dev/null
kadmin.local -q "ktadd -k /krb/flight.keytab flight@CI.TEST" > /dev/null
krb5kdc
for _ in $(seq 30); do kinit -kt /krb/hdfs.keytab -c /krb/nn.ccache nn/hdfs.ci@CI.TEST 2> /dev/null && break; sleep 1; done
chmod 644 /krb/*
touch /krb/ready
exec tail -F /var/log/krb5kdc.log /dev/null
EOF
  docker run -d --name kdc --network hd --ip 172.31.0.5 -e KPASS="$(openssl rand -hex 16)" -v "$conf:/conf:ro" \
    -v "$hd/krb:/krb" -v "$work/kdc.sh:/kdc.sh:ro" ubuntu:24.04 bash /kdc.sh > /dev/null
  for _ in $(seq 900); do [ -f "$hd/krb/ready" ] && break
    [ "$(docker inspect -f '{{.State.Running}}' kdc)" = true ] || break; sleep 1; done
  [ -f "$hd/krb/ready" ] || fail "KDC did not come up: $(docker logs kdc 2>&1 | grep '^kdc:' | tail -n 1)"
  hconf+=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb:/hd/krb:ro")
  kenv=(-e KRB5CCNAME=/hd/krb/nn.ccache)
  srvKrb=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb/flight.keytab:/krb/flight.keytab:ro"
    -e KRB5_CONFIG=/etc/krb5.conf -e KRB5CCNAME=/tmp/krb5cc_flight)
  opts="$opts -Djava.security.krb5.conf=/etc/krb5.conf -DhdfsKerberosPrincipal=flight@CI.TEST -DhdfsKerberosKeytab=/krb/flight.keytab"
fi
docker run -d --name nn1 --network hd --ip 172.31.0.10 --hostname nn1 "${hconf[@]}" -v "$hd/nn1:/hd/name" "$image" \
  bash -c 'hdfs namenode -format -nonInteractive -force > /tmp/format.log 2>&1 || { cat /tmp/format.log; exit 1; }; exec hdfs namenode' > /dev/null
docker run -d --name dn1 --network hd --ip 172.31.0.12 --hostname dn1 "${hconf[@]}" "${sock[@]}" "$image" hdfs datanode > /dev/null
report=""
for _ in $(seq 120); do
  report=$(docker exec "${kenv[@]}" nn1 hdfs dfsadmin -report 2>/dev/null || true)
  [[ $report == *"Live datanodes (1)"* && $report != *'Safe mode is ON'* ]] && break
  sleep 1
done
[[ $report == *"Live datanodes (1)"* ]] || fail "HDFS did not come up"
docker cp "$in/data" nn1:/tmp/upload > /dev/null
docker exec "${kenv[@]}" nn1 bash -c 'hdfs dfs -mkdir -p /data && hdfs dfs -put /tmp/upload/tpch /data/' || fail "HDFS upload failed"
if [ "$mode" = sasl ]; then
  # The NameNode container is not on the DataNode's host: its reads go over TCP; without SASL refused.
  probe=$(docker exec "${kenv[@]}" nn1 hdfs dfs -ls -R /data/tpch 2>/dev/null | awk '$1 ~ /^-/ {print $NF}' | head -n 1)
  if docker exec "${kenv[@]}" nn1 hdfs dfs -Ddfs.data.transfer.protection= -cat "$probe" > /dev/null 2>&1; then
    fail "the DataNode served a TCP read without SASL ($probe): data-transfer protection is not enforced"
  fi
  docker exec "${kenv[@]}" nn1 hdfs dfs -cat "$probe" > /dev/null 2> "$work/probe-sasl.log" \
    || fail "a SASL TCP read failed: $(grep -m 1 -E 'Exception|ERROR' "$work/probe-sasl.log" | cut -c1-200)"
fi

echo "server options: ${opts} hdfsMode=$mode short-circuit=$sc" | tee "$work/notes.txt"
# shellcheck disable=SC2086
docker run -d --name srv --network container:dn1 "${sock[@]}" "${native[@]}" "${upExtra[@]}" "${srvKrb[@]}" --cpuset-cpus="${SERVER_CPUS:-0,1}" --memory=6g \
  -v "$app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
  -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
  -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
  sc-server /jdk/bin/java -Xmx2g -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -Dhazelcast.phone.home.enabled=false -DnumServers=1 $opts -Darrowflight.duckdb.extensionDir=/extdir \
  -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir hdfs://172.31.0.10:8020/data --localhost 172.31.0.12 --port 32010 --hazelcast-port 5701 \
  "${srvArgs[@]}" > /dev/null
for _ in $(seq 180); do
  docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
  [ "$(docker inspect -f '{{.State.Running}}' srv)" = true ] || break
  sleep 1
done
docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started' \
  || fail "server did not start: $(docker logs srv 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
docker logs srv 2>&1 | grep -m 3 -oE 'duckdb=(threads|sizing|sharedDatabaseOpened) .*' | cut -c1-300 | tee -a "$work/notes.txt"
if [ "$profile" = upstream ]; then
  # How upstream reads HDFS (its DuckDB extension or the Java client) and its DuckDB settings, for the record.
  echo "${LABEL:-$profile}|upstream $(cat "$app/upstream-sha.txt" 2>/dev/null)" | tee -a "$res"
  docker logs srv 2>&1 | grep -iE 'hadoopfs|hdfs|threads|extension' | grep -v -iE 'warn.*native' | head -n 8 \
    | cut -c1-250 | sed "s/^/${LABEL:-$profile}|log /" | tee -a "$res"
fi
docker logs srv 2>&1 | grep -m 2 -oE 'hdfs=(shortCircuit|saslDataTransfer) .*' | cut -c1-300 | sed "s/^/${LABEL:-$profile}|/" | tee -a "$res"
if [ "$mode" = sasl ] && ! docker logs srv 2>&1 | grep -q 'hdfs=saslDataTransfer mode=auto direct=true'; then
  fail "sasl: the server did not log hdfs=saslDataTransfer mode=auto direct=true"
fi

py="$RUNNER_TEMP/py/bin/python"
if [ -n "${SCAN_ROUNDS:-}" ]; then
  status=0
  pid=$(docker inspect -f '{{.State.Pid}}' srv)
  # One unmeasured x1 pass while the per-thread CPU is sampled (seconds 3-13 of the read).
  ( sleep 3; python3 -I tools/ci/thread-cpu.py "$pid" 10 10 > "$work/thread-cpu.txt" 2>&1 ) &
  sampler=$!
  taskset -c "${CLIENT_CPUS:-2,3}" "$py" -I tools/ci/scan-adbc.py 172.31.0.12:32010 tpch.lineitem 0 1 \
    "$work/export.parquet" > "$work/client-profile.log" 2>&1 || true
  wait "$sampler"
  sed "s/^/${LABEL:-$profile}|thread-cpu x1 /" "$work/thread-cpu.txt" | tee -a "$res"
  taskset -c "${CLIENT_CPUS:-2,3}" "$py" -I tools/ci/scan-adbc.py 172.31.0.12:32010 tpch.lineitem "$SCAN_ROUNDS" \
    "${SCAN_CLIENTS:-4}" "$work/export.parquet" > "$work/client-scan.log" 2>&1 || status=1
  sed "s/^/${LABEL:-$profile}|/" "$work/client-scan.log" | tee -a "$res"
  # Best pass per shape (MB/s), the line the summary compares.
  for shape in x1 "x${SCAN_CLIENTS:-4}" export; do
    best=$(grep -E "^$shape r[1-9]" "$work/client-scan.log" | grep -v error | awk '{ print $10 }' | sort -n | tail -n 1)
    # Client CPU (% of one core, all client threads) of the measured passes: near 100 x the client's
    # CPU count means the client, not the server, caps the shape.
    cpu=$(grep -E "^cpu $shape r[1-9] " "$work/client-scan.log" | awk '{ print $5 }' | sort -n | tr '\n' ' ')
    echo "${LABEL:-$profile}|best $shape ${best:-?} MB/s (client cpu ${cpu:-?}% on ${CLIENT_CPUS:-2,3})" | tee -a "$res"
  done
  peak=$(cat "/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' srv).scope/memory.peak" 2>/dev/null || true)
  [ -n "$peak" ] && echo "${LABEL:-$profile}|server memory peak $(( peak / 1048576 )) MiB" | tee -a "$res"
  for c in kdc nn1 dn1 srv; do docker logs "$c" > "$work/$c.log" 2>&1 || true; done
  # DataNode-side transfer of the run (TCP block reads; short-circuit reads do not count here).
  echo "${LABEL:-$profile}|datanode $(grep -c 'op: HDFS_READ' "$work/dn1.log") TCP block reads (clienttrace op: HDFS_READ)" | tee -a "$res"
  # With -DstreamTimingLog=true: where each stream's time went (DuckDB / send / backpressure).
  grep -oE 'streamTiming .*' "$work/srv.log" | cut -c1-260 | head -n 12 | sed "s/^/${LABEL:-$profile}|/" | tee -a "$res"
  rm -f "$work/export.parquet"
  docker rm -f srv dn1 nn1 kdc > /dev/null 2>&1 || true
  exit "$status"
fi
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
  nnlines0=$(docker logs nn1 2>&1 | wc -l)
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
  # Which NameNode calls and which files: audit commands per query, opens per file (the table's
  # first file is the describe's sample file).
  docker logs nn1 2>&1 | tail -n +$(( nnlines0 + 1 )) > "$work/audit-c$c.txt"
  {
    printf '%s|c%s audit per query:' "${LABEL:-$profile}" "$c"
    grep -oE 'cmd=[A-Za-z]+' "$work/audit-c$c.txt" | sort | uniq -c \
      | awk -v q="${queries:-0}" '{ printf " %s=%.2f", substr($2, 5), (q > 0 ? $1 / q : 0) }'
    printf ' | opens per file:'
    grep 'cmd=open' "$work/audit-c$c.txt" | grep -oE 'src=[^ ]+' | sed 's|.*/data/tpch/||' | sort | uniq -c \
      | awk -v q="${queries:-0}" '{ printf " %s=%.2f", $2, (q > 0 ? $1 / q : 0) }'
    echo
  } | tee -a "$res"
done
# Q11 (item 39): a file rewritten in place is read fresh once the hadoopfs metadata cache TTL (30 s by
# default) has passed, with the external file cache and the footer cache warm. Last: it changes orders.
rewrite=$("$py" -I tools/ci/hdfs-rewrite-check.py 172.31.0.12:32010 "$in/data/tpch/orders/part-000.parquet" \
  "$work" "${REWRITE_WAIT_SECONDS:-35}" 2>&1 | tail -n 1)
echo "${LABEL:-$profile}|$rewrite" | tee -a "$res"
[[ $rewrite == *"fresh=true"* ]] || status=1
python3 -I tools/ci/thread-dump-summary.py "$work"/dump-*.txt > "$work/threads.txt" 2>&1 || true
head -n 60 "$work/threads.txt"
for c in nn1 dn1 srv; do docker logs "$c" > "$work/$c.log" 2>&1 || true; done
docker rm -f srv dn1 nn1 > /dev/null 2>&1 || true
exit "$status"
