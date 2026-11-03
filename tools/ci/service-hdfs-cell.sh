#!/usr/bin/env bash
# SERVICE MATRIX, HDFS part (core lane, STATUS item 25): one cell of service-hdfs.yml.
#
# Usage: tools/ci/service-hdfs-cell.sh <plain|ha|ha-krb|ha-krb-sasl> <balanced|speed> <input dir> <work dir> <server label>
#
# Topologies (apache/hadoop, one DataNode, replication 1, short-circuit configured when the image's
# native Hadoop library loads):
#   plain   one NameNode, data dir hdfs://172.31.0.10:8020/data
#   ha      HA nameservice ns1: two NameNodes (shared edits in a shared directory, manual failover),
#           nn1 standby and nn2 active, so a client must fail over; data dir hdfs://ns1/data
#   ha-krb  ha + Kerberos (an MIT KDC container, realm CI.TEST): NameNode and DataNode principals
#           from a keytab, block access tokens, DataNode without SASL data transfer protection
#           (hadoopfs has no SASL handshake; the stand runs privileged-port DataNodes, here
#           ignore.secure.ports.for.testing). The server logs in itself (hdfsKerberosPrincipal +
#           hdfsKerberosKeytab, no kinit) and writes the ticket cache the extension reads.
#   ha-krb-sasl  ha-krb + dfs.data.transfer.protection=authentication: the DataNode refuses data
#           transfer without SASL, which hadoopfs (libhdfs3) cannot speak, so only short-circuit reads
#           work (the maintainer's production cluster; STATUS item 26). The cell first shows that a
#           plain TCP read is refused and a SASL one passes.
# With short-circuit configured every cell requires that the server read no block over TCP: DataNode
# JMX BlocksRead stays 0 and the DataNode log shows REQUEST_SHORT_CIRCUIT_FDS (STATUS item 26); the
# server JVM gets the image's native Hadoop library so its Java client reads short-circuit too.
# The server runs in the DataNode's network namespace (the domain socket mounted), loads hadoopfs from
# -Darrowflight.duckdb.extensionDir only, reads the client configuration from /conf (class path and
# LIBHDFS3_CONF). Check: tools/ci/service-cluster-check.py check (22 TPC-H answers against the
# reference, metadata, a prepared query). Writes <work>/result.txt (topology|profile|pass/fail|first
# error) and <work>/notes.txt; exit code 0 = pass. Test-only principals and keytabs, made in the job.
set -u
topo=$1 profile=$2 in=$3 work=$4 label=${5:-server}
image=${HADOOP_IMAGE:-apache/hadoop:3.4.1}
res="$work/result.txt" notes="$work/notes.txt"
mkdir -p "$work"; : > "$notes"
note() { echo "$*" | tee -a "$notes"; }
finish() { # status, detail
  printf '%s|%s|%s|%s\n' "$topo" "$profile" "$1" "$(echo "$2" | head -n 1 | tr '|' '/' | cut -c1-300)" > "$res"
  for c in kdc nn1 nn2 dn1 srv; do docker logs "$c" > "$work/$c.log" 2>&1 || rm -f "$work/$c.log"; done
  [ "$1" = pass ]; exit $?
}

case "$topo" in plain|ha|ha-krb|ha-krb-sasl) ;; *) finish fail "unknown topology $topo" ;; esac
case "$profile" in balanced) opts="" ;; speed) opts="-DresourceProfile=speed" ;; *) finish fail "unknown profile $profile" ;; esac
krb=false; [[ $topo == ha-krb* ]] && krb=true
sasl=false; [ "$topo" = ha-krb-sasl ] && sasl=true
ha=false; [ "$topo" != plain ] && ha=true

docker network create --subnet 172.31.0.0/24 hd > /dev/null || finish fail "docker network not created"
hd=/srv/hd
sudo rm -rf "$hd"; sudo install -d -o 1000 -g 1000 -m 755 "$hd" "$hd/nn1" "$hd/nn2" "$hd/shared" "$hd/sock" "$hd/krb"
conf="$work/conf"; mkdir -p "$conf"

sc=off
if docker run --rm "$image" hadoop checknative 2>&1 | grep -qE '^hadoop: +true'; then sc=on; fi
native=()
if [ "$sc" = on ]; then
  # The server's own (Java) HDFS client reads short-circuit only with the native Hadoop library.
  docker create --name hlib "$image" > /dev/null && docker cp hlib:/opt/hadoop/lib/native "$work/hnative" > /dev/null
  docker rm hlib > /dev/null
  native=(-v "$work/hnative:/hnative:ro" -e LD_LIBRARY_PATH=/hnative)
  opts="$opts -Djava.library.path=/hnative"
fi
$sasl && [ "$sc" != on ] && finish fail "ha-krb-sasl needs short-circuit, but the image's native Hadoop library does not load"

# Properties: core (both files) and hdfs (hdfs-site.xml). The server's LIBHDFS3_CONF is hdfs-site.xml,
# so it carries the core keys too (libhdfs3 reads one file).
core=() hdfs=()
if $ha; then
  core+=(fs.defaultFS hdfs://ns1)
  hdfs+=(dfs.nameservices ns1 dfs.ha.namenodes.ns1 nn1,nn2
    dfs.namenode.rpc-address.ns1.nn1 172.31.0.10:8020 dfs.namenode.rpc-address.ns1.nn2 172.31.0.11:8020
    dfs.namenode.http-address.ns1.nn1 172.31.0.10:9870 dfs.namenode.http-address.ns1.nn2 172.31.0.11:9870
    dfs.namenode.shared.edits.dir file:///hd/shared dfs.ha.automatic-failover.enabled false
    dfs.ha.fencing.methods 'shell(/bin/true)'
    dfs.client.failover.proxy.provider.ns1 org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider)
  dataDir=hdfs://ns1/data
else
  core+=(fs.defaultFS hdfs://172.31.0.10:8020)
  dataDir=hdfs://172.31.0.10:8020/data
fi
hdfs+=(dfs.replication 1 dfs.permissions.enabled false dfs.namenode.rpc-bind-host 0.0.0.0
  dfs.namenode.http-bind-host 0.0.0.0 dfs.namenode.name.dir /hd/name dfs.datanode.data.dir /tmp/hdfs/dn
  dfs.namenode.datanode.registration.ip-hostname-check false)
if [ "$sc" = on ]; then
  hdfs+=(dfs.client.read.shortcircuit true dfs.domain.socket.path /var/run/hdfs-sockets/dn)
fi
principal() { echo "$1/hdfs.ci@CI.TEST"; }
if $krb; then
  core+=(hadoop.security.authentication kerberos hadoop.security.authorization false hadoop.rpc.protection authentication)
  hdfs+=(dfs.block.access.token.enable true
    dfs.namenode.kerberos.principal "$(principal nn)" dfs.namenode.keytab.file /hd/krb/hdfs.keytab
    dfs.namenode.kerberos.internal.spnego.principal "$(principal HTTP)"
    dfs.datanode.kerberos.principal "$(principal dn)" dfs.datanode.keytab.file /hd/krb/hdfs.keytab
    dfs.web.authentication.kerberos.principal "$(principal HTTP)" dfs.web.authentication.kerberos.keytab /hd/krb/hdfs.keytab
    ignore.secure.ports.for.testing true)
  $sasl && hdfs+=(dfs.data.transfer.protection authentication)
fi
xml() { # key value ...
  echo '<?xml version="1.0"?>'; echo '<configuration>'
  while [ $# -ge 2 ]; do printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"; shift 2; done
  echo '</configuration>'
}
xml "${core[@]}" > "$conf/core-site.xml"
xml "${core[@]}" "${hdfs[@]}" > "$conf/hdfs-site.xml"
cat > "$conf/krb5.conf" <<'EOF'
[libdefaults]
  default_realm = CI.TEST
  dns_lookup_kdc = false
  dns_lookup_realm = false
  rdns = false
  dns_canonicalize_hostname = false
  udp_preference_limit = 1
  forwardable = true
  ticket_lifetime = 2h
[realms]
  CI.TEST = {
    kdc = 172.31.0.5:88
    admin_server = 172.31.0.5
  }
EOF
note "HDFS $image, topology $topo ($([ "$ha" = true ] && echo 'nameservice ns1, nn1 standby / nn2 active' || echo 'one NameNode')$([ "$krb" = true ] && echo ', Kerberos realm CI.TEST'), 1 DataNode), short-circuit configured: $sc"

hconf=(-v "$conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" -v "$conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro")
kenv=()
if $krb; then
  # MIT KDC; keytabs and a NameNode-principal ticket cache (for the admin commands below) in $hd/krb.
  kpass=$(openssl rand -hex 16)
  cat > "$work/kdc.sh" <<EOF
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq && apt-get install -y -qq --no-install-recommends krb5-kdc krb5-admin-server krb5-user > /dev/null
cp /conf/krb5.conf /etc/krb5.conf
mkdir -p /etc/krb5kdc
printf '%s\n' '[realms]' ' CI.TEST = {' '  master_key_type = aes256-cts-hmac-sha1-96' \
  '  supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal' ' }' > /etc/krb5kdc/kdc.conf
kdb5_util create -s -r CI.TEST -P "\$KPASS" > /dev/null
for p in nn dn HTTP; do kadmin.local -q "addprinc -randkey \$p/hdfs.ci@CI.TEST" > /dev/null; done
kadmin.local -q "addprinc -randkey flight@CI.TEST" > /dev/null
kadmin.local -q "ktadd -k /krb/hdfs.keytab nn/hdfs.ci@CI.TEST dn/hdfs.ci@CI.TEST HTTP/hdfs.ci@CI.TEST" > /dev/null
kadmin.local -q "ktadd -k /krb/flight.keytab flight@CI.TEST" > /dev/null
krb5kdc
for _ in \$(seq 30); do kinit -kt /krb/hdfs.keytab -c /krb/nn.ccache nn/hdfs.ci@CI.TEST 2> /dev/null && break; sleep 1; done
chmod 644 /krb/*
touch /krb/ready
exec tail -F /var/log/krb5kdc.log /dev/null
EOF
  docker run -d --name kdc --network hd --ip 172.31.0.5 -e KPASS="$kpass" -v "$conf:/conf:ro" -v "$hd/krb:/krb" \
    -v "$work/kdc.sh:/kdc.sh:ro" ubuntu:24.04 bash /kdc.sh > /dev/null
  for _ in $(seq 180); do [ -f "$hd/krb/ready" ] && break; sleep 1; done
  [ -f "$hd/krb/ready" ] || { docker logs kdc 2>&1 | tail -n 20; finish fail "KDC did not come up"; }
  hconf+=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb:/hd/krb:ro")
  kenv=(-e KRB5CCNAME=/hd/krb/nn.ccache)
  note "KDC: MIT krb5 (ubuntu:24.04), principals nn/dn/HTTP of hdfs.ci and flight@CI.TEST (server keytab)"
fi

sock=()
[ "$sc" = on ] && sock=(-v "$hd/sock:/var/run/hdfs-sockets")
# nn1 formats its name directory (and the shared edits), waits until it is copied to nn2, then runs.
docker run -d --name nn1 --network hd --ip 172.31.0.10 --hostname nn1 "${hconf[@]}" -v "$hd/nn1:/hd/name" \
  -v "$hd/shared:/hd/shared" "$image" bash -c 'hdfs namenode -format -nonInteractive -force > /tmp/format.log 2>&1 \
    || { cat /tmp/format.log; exit 1; }; touch /hd/name/.formatted; while [ ! -f /hd/name/.go ]; do sleep 1; done; exec hdfs namenode' > /dev/null
for _ in $(seq 120); do [ -f "$hd/nn1/.formatted" ] && break; [ "$(docker inspect -f '{{.State.Running}}' nn1)" = true ] || break; sleep 1; done
[ -f "$hd/nn1/.formatted" ] || { docker logs nn1 2>&1 | tail -n 30; finish fail "NameNode format failed: $(docker logs nn1 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-200)"; }
if $ha; then
  sudo rm -rf "$hd/nn2"; sudo cp -a "$hd/nn1" "$hd/nn2"
  docker run -d --name nn2 --network hd --ip 172.31.0.11 --hostname nn2 "${hconf[@]}" -v "$hd/nn2:/hd/name" \
    -v "$hd/shared:/hd/shared" "$image" hdfs namenode > /dev/null
fi
sudo touch "$hd/nn1/.go"
docker run -d --name dn1 --network hd --ip 172.31.0.12 --hostname dn1 "${hconf[@]}" "${sock[@]}" "$image" hdfs datanode > /dev/null
admin=nn1
if $ha; then
  admin=nn2
  for _ in $(seq 120); do
    docker exec "${kenv[@]}" nn2 hdfs haadmin -getServiceState nn2 > /dev/null 2>&1 \
      && docker exec "${kenv[@]}" nn2 hdfs haadmin -getServiceState nn1 > /dev/null 2>&1 && break
    sleep 1
  done
  docker exec "${kenv[@]}" nn2 hdfs haadmin -transitionToActive nn2 2>&1 | tail -n 3
  state="nn1=$(docker exec "${kenv[@]}" nn2 hdfs haadmin -getServiceState nn1 2>&1 | tail -n 1) nn2=$(docker exec "${kenv[@]}" nn2 hdfs haadmin -getServiceState nn2 2>&1 | tail -n 1)"
  note "HA states: $state"
  [[ $state == *"nn1=standby"* && $state == *"nn2=active"* ]] || { docker logs nn2 2>&1 | tail -n 30; finish fail "HA transition failed: $state"; }
fi
for _ in $(seq 120); do
  report=$(docker exec "${kenv[@]}" "$admin" hdfs dfsadmin -report 2>/dev/null)
  [[ $report == *'Live datanodes (1)'* && $report != *'Safe mode is ON'* ]] && break
  sleep 1
done
if [[ $report != *'Live datanodes (1)'* ]]; then
  docker logs "$admin" 2>&1 | tail -n 20; docker logs dn1 2>&1 | tail -n 30
  finish fail "HDFS did not come up: $(docker logs dn1 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-200)"
fi
docker cp "$in/data" "$admin:/tmp/upload" > /dev/null
docker exec "${kenv[@]}" "$admin" bash -c 'hdfs dfs -mkdir -p /data && hdfs dfs -put /tmp/upload/tpch /data/' || finish fail "HDFS upload failed"
note "data: $(docker exec "${kenv[@]}" "$admin" hdfs dfs -count /data/tpch 2>/dev/null | awk '{print $2 " files, " $3 " bytes"}')"
if $sasl; then
  # The NameNode container is not on the DataNode's host: its reads go over TCP.
  probe=$(docker exec "${kenv[@]}" "$admin" hdfs dfs -ls -R /data/tpch 2>/dev/null | awk '$1 ~ /^-/ {print $NF}' | head -n 1)
  if docker exec "${kenv[@]}" "$admin" hdfs dfs -Ddfs.data.transfer.protection= -cat "$probe" > /dev/null 2> "$work/probe-plain.log"; then
    finish fail "the DataNode served a TCP read without SASL ($probe): data-transfer protection is not enforced"
  fi
  docker exec "${kenv[@]}" "$admin" hdfs dfs -cat "$probe" > /dev/null 2> "$work/probe-sasl.log" \
    || finish fail "a SASL TCP read failed: $(grep -m 1 -E 'Exception|ERROR' "$work/probe-sasl.log" | cut -c1-200)"
  note "SASL data transfer enforced: plain TCP read refused ($(grep -m 1 -oE '[A-Za-z.]*(Exception|Error)[^,]*' "$work/probe-plain.log" | cut -c1-150)), SASL read passes"
fi
blocksRead() { curl -fsS --max-time 5 "http://172.31.0.12:9864/jmx" 2>/dev/null | grep -m 1 -oE '"BlocksRead" : [0-9]+' | grep -oE '[0-9]+$'; }
sleep 12 # DataNode JMX values are cached for ~10 s
read0=$(blocksRead); fds0=$(docker logs dn1 2>&1 | grep -c 'op: REQUEST_SHORT_CIRCUIT_FDS')

srvKrb=()
if $krb; then
  srvKrb=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb/flight.keytab:/krb/flight.keytab:ro"
    -e KRB5_CONFIG=/etc/krb5.conf -e KRB5CCNAME=/tmp/krb5cc_flight)
  opts="$opts -Djava.security.krb5.conf=/etc/krb5.conf -DhdfsKerberosPrincipal=flight@CI.TEST -DhdfsKerberosKeytab=/krb/flight.keytab"
fi
note "server $label, options: ${opts:-(defaults)} -Darrowflight.duckdb.extensionDir=/extdir; 1 node, 2 CPUs / 6 GB"
# shellcheck disable=SC2086
docker run -d --name srv --network container:dn1 "${sock[@]}" "${srvKrb[@]}" "${native[@]}" --cpus=2 --memory=6g \
  -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
  -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
  -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
  sc-server /jdk/bin/java -Xmx2g -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -Dhazelcast.phone.home.enabled=false -DnumServers=1 $opts -Darrowflight.duckdb.extensionDir=/extdir \
  -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir "$dataDir" --localhost 172.31.0.12 --port 32010 --hazelcast-port 5701 \
  --hosts 172.31.0.12:5701 --metrics-enabled false > /dev/null
for _ in $(seq 180); do
  docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
  [ "$(docker inspect -f '{{.State.Running}}' srv)" = true ] || break
  sleep 1
done
if ! docker logs srv 2>&1 | grep -q 'Server Arrow Flight SQL started'; then
  docker logs srv 2>&1 | tail -n 40
  finish fail "server did not start: $(docker logs srv 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
fi
note "$(docker logs srv 2>&1 | grep -m 1 -oE 'duckdb=hdfsExtensionLoaded.*' || echo 'no duckdb=hdfsExtensionLoaded line')"
$krb && note "$(docker logs srv 2>&1 | grep -m 1 -oE 'hdfs=kerberosTicketCache.*' || echo 'no hdfs=kerberosTicketCache line')"
note "$(docker logs srv 2>&1 | grep -m 1 -oE 'hdfs=shortCircuit .*' || echo 'no hdfs=shortCircuit line')"
[ "$sc" = on ] && note "server JVM: $(docker logs srv 2>&1 | grep -m 1 -oE '(Loaded the native-hadoop library|Unable to load native-hadoop library)' || echo 'no native-hadoop line')"
sleep 3
py="$RUNNER_TEMP/py/bin/python"
export SC_DATANODES=172.31.0.12:9864
"$py" -I tools/ci/service-cluster-check.py check 172.31.0.12:32010 "$in" > "$work/client.log" 2>&1; rc=$?
cat "$work/client.log"
note "$(grep -m 1 '^reads total' "$work/client.log" || echo 'reads total: (none)')"
jmx=$(curl -fsS --max-time 5 "http://172.31.0.12:9864/jmx" 2>/dev/null | grep -oE '"(BlocksRead|ReadBlockOpNumOps|ReadsFromLocalClient|ReadsFromRemoteClient)" : [0-9]+' | tr -d '" ' | sort -u | tr '\n' ' ')
note "dn1 reads (TCP, from DataNode JMX): ${jmx:-(no JMX)}"
note "srv: refused $(docker logs srv 2>&1 | grep -c 'query=refused')"
sleep 12
read1=$(blocksRead); fds1=$(docker logs dn1 2>&1 | grep -c 'op: REQUEST_SHORT_CIRCUIT_FDS')
tcp=$(( ${read1:-0} - ${read0:-0} )) fds=$(( fds1 - fds0 ))
note "dn1 during the check: blocks read over TCP $tcp (JMX BlocksRead ${read0:-?} -> ${read1:-?}), short-circuit fd requests $fds"
result=$(grep -m 1 '^result ' "$work/client.log")
if [ "$rc" -eq 0 ] && [ "$result" = "result pass" ]; then
  if [ "$sc" = on ] && { [ -z "$read1" ] || [ "$tcp" -ne 0 ] || [ "$fds" -eq 0 ]; }; then
    finish fail "answers right, but short-circuit is configured and the server read $tcp blocks over TCP ($fds short-circuit fd requests)"
  fi
  finish pass ""
fi
detail=${result#result fail }
[ -n "$result" ] || detail="client failed: $(grep -m 1 -E 'Error|Exception' "$work/client.log")"
finish fail "$detail"
