#!/usr/bin/env bash
# SERVICE MATRIX, HDFS part (core lane, STATUS item 25): one cell of service-hdfs.yml.
#
# Usage: tools/ci/service-hdfs-cell.sh <plain|ha|ha-krb|ha-krb-sasl|sasl-cluster|sasl-direct> <balanced|speed> <input dir> <work dir> <server label>
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
#   sasl-cluster  ha-krb-sasl with 3 DataNodes and 3 servers (one in each DataNode's network namespace,
#           -DdistributedComplete=true; STATUS item 29): schemas tpch (replication 3: 22/22 on every
#           node), spread (replication 2) and split (one lineitem file, 1 MiB blocks, replication 1, no
#           DataNode holding all its blocks), plain (every tpch table renamed <name>_p, so that its plain
#           name is unique; lineitem_p / orders_p with 1 MiB blocks and replication 1, the rest replication
#           2: item 31, queries with plain names); tools/ci/service-hdfs-sasl-check.py: spread.lineitem
#           exact in parts and complete on every node, split.t read from node-local copies (item 30),
#           chunks (all lineitem files, 1 MiB blocks, replication 1: no node holds a file whole) full scan
#           and GROUP BY; under speed the copy cache is below one file, so scans go in batches (part 2b).
#           Under balanced a last stage runs one server whose copy disk (a tmpfs) is smaller than
#           chunks.lineitem: batches sized by the disk, a join needing every copy refused (part 2b).
#           ha-krb-sasl and sasl-cluster run with -DhdfsSaslDataTransfer=off (item 41): they keep checking
#           the short-circuit-only path with node-local copies, which hadoopfs 51b7220-afc6 no longer needs.
#   sasl-direct  sasl-cluster with dfs.data.transfer.protection=authentication,privacy (several QOPs, as on
#           the maintainer's real cluster; SASL_DIRECT_PROTECTION overrides it), hdfsSaslDataTransfer left at auto
#           (hadoopfs 51b7220-afc6+ speaks SASL data transfer: the start line must say direct=true) and
#           hdfsLocalCopyMaxBytes=0 (no node-local copies; item 41 (3)): the same 22/22 and
#           service-hdfs-sasl-check.py answers, split / chunks read from remote DataNodes directly, no
#           hdfs=localCopy line; TCP block reads are expected here. No later stages.
#   ha-krb, ha-krb-sasl (balanced): a last stage stops the server under load and starts one with
#           tpch.nation's files unreadable for the server's principal (item 37 (a), (b);
#           tools/ci/service-hdfs-deny-stop-check.py).
#   ha-krb-sasl, sasl-cluster, sasl-direct: a stage runs complete-mode point queries on point.orders_r
#           (tpch.orders in 8 key ranges) on one server and requires views=prunedFiles lines (item 42 (2);
#           tools/ci/service-hdfs-point-check.py).
# With short-circuit configured every cell requires that DuckDB (hadoopfs) read short-circuit: the
# DataNode log shows REQUEST_SHORT_CIRCUIT_FDS, and JMX BlocksRead (TCP) grows by at most the server's
# own Java footer reads, three blocks per file and node (planning footers, plus the view setup's footer
# stub of each table's first file since item 29; the Java client has no native Hadoop library in the
# server image; its reads speak SASL). In ha-krb-sasl a TCP read by hadoopfs cannot succeed at all.
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
  for c in kdc nn1 nn2 dn1 dn2 dn3 srv srv1 srv2 srv3 srvd; do docker logs "$c" > "$work/$c.log" 2>&1 || rm -f "$work/$c.log"; done
  [ "$1" = pass ]; exit $?
}

case "$topo" in plain|ha|ha-krb|ha-krb-sasl|sasl-cluster|sasl-direct) ;; *) finish fail "unknown topology $topo" ;; esac
case "$profile" in balanced) opts="" ;; speed) opts="-DresourceProfile=speed" ;; *) finish fail "unknown profile $profile" ;; esac
krb=false; [[ $topo == ha-krb* || $topo == sasl-* ]] && krb=true
sasl=false; [[ $topo == ha-krb-sasl || $topo == sasl-* ]] && sasl=true
n=1; [[ $topo == sasl-* ]] && n=3
# Item 41: sasl-direct reads non-local blocks over SASL data transfer (hadoopfs afc6+, auto); the
# other SASL topologies keep the short-circuit-only path.
direct=false; [ "$topo" = sasl-direct ] && direct=true
if $sasl && ! $direct; then opts="$opts -DhdfsSaslDataTransfer=off"; fi
# Item 42 (2): the point-query stage counts CatalogViews' views=prunedFiles debug lines.
if $sasl; then opts="$opts -Dorg.slf4j.simpleLogger.log.pro.surpin.data.arrowflight.server.services.CatalogViews=debug"; fi
dnIp() { echo "172.31.0.$(( 11 + $1 ))"; }   # DataNode i (and server i): 172.31.0.12, .13, .14
ha=false; [ "$topo" != plain ] && ha=true

docker network create --subnet 172.31.0.0/24 hd > /dev/null || finish fail "docker network not created"
hd=/srv/hd
sudo rm -rf "$hd"; sudo install -d -o 1000 -g 1000 -m 755 "$hd" "$hd/nn1" "$hd/nn2" "$hd/shared" "$hd/sock" "$hd/sock2" "$hd/sock3" "$hd/krb"
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
# Item 37 (a): one-server Kerberos cells check HDFS permissions (the NameNode's login nn, which
# uploads the data, is the superuser; the server's principal flight reads world-readable files).
perms=false; [[ $topo == ha-krb* ]] && perms=true
hdfs+=(dfs.replication 1 dfs.permissions.enabled $perms dfs.namenode.rpc-bind-host 0.0.0.0
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
  if $direct; then hdfs+=(dfs.data.transfer.protection "${SASL_DIRECT_PROTECTION:-authentication,privacy}")
  elif $sasl; then hdfs+=(dfs.data.transfer.protection authentication); fi
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
note "HDFS $image, topology $topo ($([ "$ha" = true ] && echo 'nameservice ns1, nn1 standby / nn2 active' || echo 'one NameNode')$([ "$krb" = true ] && echo ', Kerberos realm CI.TEST'), $n DataNode(s)), short-circuit configured: $sc"

hconf=(-v "$conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro" -v "$conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml:ro")
kenv=()
if $krb; then
  # MIT KDC; keytabs and a NameNode-principal ticket cache (for the admin commands below) in $hd/krb.
  kpass=$(openssl rand -hex 16)
  cat > "$work/kdc.sh" <<EOF
set -e
export DEBIAN_FRONTEND=noninteractive
# The Ubuntu mirror sometimes stalls (2026-11-10: KDC cells timed out with no output): bounded
# retries per request, a few whole tries, progress lines in the container log for the next failure.
apt_opts="-o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30"
for t in 1 2 3; do
  # Try 2 switches to the Azure mirror the runners' own apt uses (archive.ubuntu.com reset connections, 2026-11-10).
  [ "\$t" != 2 ] || sed -i -e 's#http://archive.ubuntu.com#http://azure.archive.ubuntu.com#g' \
    -e 's#http://security.ubuntu.com#http://azure.archive.ubuntu.com#g' /etc/apt/sources.list.d/ubuntu.sources
  # Try 3 uses kernel.org's full mirror of the archive (2026-11-11: runners could not resolve any *.ubuntu.com host).
  [ "\$t" != 3 ] || sed -i -E 's#http://[a-z.]*ubuntu[.]com/ubuntu#http://mirrors.kernel.org/ubuntu#g' \
    /etc/apt/sources.list.d/ubuntu.sources
  echo "kdc: apt try \$t \$(date -u +%T)"
  timeout 240 apt-get \$apt_opts update -qq && timeout 240 apt-get \$apt_opts install -y -qq --no-install-recommends \
    krb5-kdc krb5-admin-server krb5-user > /dev/null && break
  [ "\$t" -lt 3 ] || { echo "kdc: apt failed"; exit 1; }
  sleep 5
done
echo "kdc: packages installed \$(date -u +%T)"
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
  # Up to three apt tries of at most 2 x 240 s each (kdc.sh); normally ready in under a minute.
  for _ in $(seq 900); do [ -f "$hd/krb/ready" ] && break
    [ "$(docker inspect -f '{{.State.Running}}' kdc)" = true ] || break; sleep 1; done
  [ -f "$hd/krb/ready" ] || { docker logs kdc 2>&1 | tail -n 20
    finish fail "KDC did not come up: $(docker logs kdc 2>&1 | grep '^kdc:' | tail -n 1)"; }
  note "KDC ready: $(docker logs kdc 2>&1 | grep '^kdc:' | tr '\n' ' ')"
  hconf+=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb:/hd/krb:ro")
  kenv=(-e KRB5CCNAME=/hd/krb/nn.ccache)
  note "KDC: MIT krb5 (ubuntu:24.04), principals nn/dn/HTTP of hdfs.ci and flight@CI.TEST (server keytab)"
fi

sockOf() { [ "$sc" = on ] && echo "-v $hd/sock$([ "$1" -gt 1 ] && echo "$1"):/var/run/hdfs-sockets"; }
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
for i in $(seq "$n"); do
  # shellcheck disable=SC2046
  docker run -d --name "dn$i" --network hd --ip "$(dnIp "$i")" --hostname "dn$i" "${hconf[@]}" $(sockOf "$i") "$image" hdfs datanode > /dev/null
done
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
  [[ $report == *"Live datanodes ($n)"* && $report != *'Safe mode is ON'* ]] && break
  sleep 1
done
if [[ $report != *"Live datanodes ($n)"* ]]; then
  docker logs "$admin" 2>&1 | tail -n 20; docker logs dn1 2>&1 | tail -n 30
  finish fail "HDFS did not come up: $(docker logs dn1 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-200)"
fi
docker cp "$in/data" "$admin:/tmp/upload" > /dev/null
if [ "$n" -eq 1 ]; then
  docker exec "${kenv[@]}" "$admin" bash -c 'hdfs dfs -mkdir -p /data && hdfs dfs -put /tmp/upload/tpch /data/' || finish fail "HDFS upload failed"
else
  # tpch whole on every DataNode, spread whole on two of three, split's one file on no single one.
  splitFile=$(basename "$(ls "$in"/data/tpch/lineitem/*.parquet | head -n 1)")
  docker exec "${kenv[@]}" "$admin" bash -c "hdfs dfs -mkdir -p /data/spread /data/split/t /data/chunks \
    && hdfs dfs -D dfs.replication=3 -put /tmp/upload/tpch /data/ \
    && hdfs dfs -D dfs.replication=1 -D dfs.blocksize=1048576 -put /tmp/upload/tpch/lineitem /data/chunks/ \
    && hdfs dfs -D dfs.replication=2 -put /tmp/upload/tpch/* /data/spread/ \
    && hdfs dfs -D dfs.replication=1 -D dfs.blocksize=1048576 -put /tmp/upload/tpch/lineitem/$splitFile /data/split/t/ \
    && hdfs dfs -mkdir -p /data/plain \
    && for t in \$(ls /tmp/upload/tpch); do \
         case \$t in lineitem|orders) o='-D dfs.replication=1 -D dfs.blocksize=1048576' ;; *) o='-D dfs.replication=2' ;; esac; \
         hdfs dfs \$o -put /tmp/upload/tpch/\$t /data/plain/\${t}_p || exit 1; done" \
    || finish fail "HDFS upload failed"
  docker exec "${kenv[@]}" "$admin" hdfs fsck "/data/split/t/$splitFile" -files -blocks -locations > "$work/fsck-split.log" 2>&1
  blocks=$(grep -cE '^[0-9]+\. BP-' "$work/fsck-split.log")
  held=""
  for i in $(seq "$n"); do held="$held dn$i=$(grep -cE "^[0-9]+\. BP-.*$(dnIp "$i"):" "$work/fsck-split.log")"; done
  note "split/t/$splitFile: $blocks blocks, held by$held"
  [ "$blocks" -gt 1 ] || finish fail "split/t/$splitFile has $blocks block(s); the check needs blocks on several DataNodes"
  for i in $(seq "$n"); do
    [ "$(grep -cE "^[0-9]+\. BP-.*$(dnIp "$i"):" "$work/fsck-split.log")" -lt "$blocks" ] \
      || finish fail "dn$i holds every block of split/t/$splitFile (placement); rerun"
  done
fi
if $sasl; then
  # Item 42 (2): point.orders_r = tpch.orders in 8 files of one key range each (footer min/max tell
  # them apart), replication 2 of 3 in the clusters: some files whole on dn1, some only as copies.
  "$RUNNER_TEMP/py/bin/python" -I tools/ci/service-hdfs-point-check.py gen "$in/data/tpch/orders" "$work/point/orders_r" \
    || finish fail "item 42: point.orders_r files not written"
  docker cp "$work/point" "$admin:/tmp/upload-point" > /dev/null
  docker exec "${kenv[@]}" "$admin" bash -c "hdfs dfs -mkdir -p /data/point \
    && hdfs dfs -D dfs.replication=$([ "$n" -gt 1 ] && echo 2 || echo 1) -put /tmp/upload-point/orders_r /data/point/" \
    || finish fail "item 42: point.orders_r upload failed"
fi
files=$(docker exec "${kenv[@]}" "$admin" hdfs dfs -count /data 2>/dev/null | awk '{print $2}')
note "data: $(docker exec "${kenv[@]}" "$admin" hdfs dfs -count /data 2>/dev/null | awk '{print $2 " files, " $3 " bytes"}')"
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
blocksRead() { # TCP block reads of every DataNode, summed
  local t=0 v i
  for i in $(seq "$n"); do
    v=$(curl -fsS --max-time 5 "http://$(dnIp "$i"):9864/jmx" 2>/dev/null | grep -m 1 -oE '"BlocksRead" : [0-9]+' | grep -oE '[0-9]+$') || return 0
    [ -n "$v" ] || return 0
    t=$(( t + v ))
  done
  echo "$t"
}
fdRequests() { local t=0 i; for i in $(seq "$n"); do t=$(( t + $(docker logs "dn$i" 2>&1 | grep -c 'op: REQUEST_SHORT_CIRCUIT_FDS') )); done; echo "$t"; }
sleep 12 # DataNode JMX values are cached for ~10 s
read0=$(blocksRead); fds0=$(fdRequests)

srvKrb=()
if $krb; then
  srvKrb=(-v "$conf/krb5.conf:/etc/krb5.conf:ro" -v "$hd/krb/flight.keytab:/krb/flight.keytab:ro"
    -e KRB5_CONFIG=/etc/krb5.conf -e KRB5CCNAME=/tmp/krb5cc_flight)
  opts="$opts -Djava.security.krb5.conf=/etc/krb5.conf -DhdfsKerberosPrincipal=flight@CI.TEST -DhdfsKerberosKeytab=/krb/flight.keytab"
fi
srvName() { [ "$n" -eq 1 ] && echo srv || echo "srv$1"; }
peers=$(for i in $(seq "$n"); do printf '%s:5701,' "$(dnIp "$i")"; done); peers=${peers%,}
cpus=2 mem=6g xmx=2g
if [ "$n" -gt 1 ]; then
  cpus=1 mem=3g xmx=1g
  opts="$opts -DdistributedComplete=true"
  if $direct; then
    # Item 41 (3): copies off; every file a node lacks blocks of is read from the DataNodes directly.
    opts="$opts -DhdfsLocalCopyMaxBytes=0"
  elif [ "$profile" = speed ]; then
    # Item 30 part 2b: a cache smaller than every lineitem file (so smaller than a node's share of
    # chunks.lineitem, and than split.t): scans are copied in batches, every copy deleted after use.
    smallest=$(stat -c %s "$in"/data/tpch/lineitem/*.parquet | sort -n | head -n 1)
    # One endpoint per node and query, so a node's share of chunks (4 files, 3 nodes: some node
    # gets 2 or more) is one ticket larger than the cache.
    opts="$opts -DhdfsLocalCopyMaxBytes=$(( smallest * 3 / 4 )) -DendpointsPerServer=1"
  else
    # Item 30 part 2b (3): copies in hdfsLocalCopyDir (its own mount), not below the DuckDB temp root.
    opts="$opts -DhdfsLocalCopyDir=/copies"
  fi
fi
copyMount() { [ "$n" -gt 1 ] && [ "$profile" != speed ] || return 0; mkdir -p "$work/copies$1" && chmod 777 "$work/copies$1"; echo "-v $work/copies$1:/copies"; }
note "server $label, options: ${opts:-(defaults)} -Darrowflight.duckdb.extensionDir=/extdir; $n node(s), $cpus CPU(s) / $mem each"
for i in $(seq "$n"); do
# shellcheck disable=SC2086,SC2046
docker run -d --name "$(srvName "$i")" --network "container:dn$i" $(sockOf "$i") $(copyMount "$i") "${srvKrb[@]}" "${native[@]}" --cpus=$cpus --memory=$mem \
  -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
  -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
  -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
  sc-server /jdk/bin/java -Xmx$xmx -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
  -Dhazelcast.phone.home.enabled=false -DnumServers=$n $opts -Darrowflight.duckdb.extensionDir=/extdir \
  -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
  --data-dir "$dataDir" --localhost "$(dnIp "$i")" --port 32010 --hazelcast-port 5701 \
  --hosts "$peers" --metrics-enabled false > /dev/null
done
for i in $(seq "$n"); do
  c=$(srvName "$i")
  for _ in $(seq 180); do
    docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
    [ "$(docker inspect -f '{{.State.Running}}' "$c")" = true ] || break
    sleep 1
  done
  if ! docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started'; then
    docker logs "$c" 2>&1 | tail -n 40
    finish fail "server $c did not start: $(docker logs "$c" 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
  fi
done
srv=$(srvName 1)
note "$(docker logs "$srv" 2>&1 | grep -m 1 -oE 'duckdb=hdfsExtensionLoaded.*' || echo 'no duckdb=hdfsExtensionLoaded line')"
$krb && note "$(docker logs "$srv" 2>&1 | grep -m 1 -oE 'hdfs=kerberosTicketCache.*' || echo 'no hdfs=kerberosTicketCache line')"
note "$(docker logs "$srv" 2>&1 | grep -m 1 -oE 'hdfs=shortCircuit .*' || echo 'no hdfs=shortCircuit line')"
[ "$sc" = on ] && note "server JVM: $(docker logs "$srv" 2>&1 | grep -m 1 -oE '(Loaded the native-hadoop library|Unable to load native-hadoop library)' || echo 'no native-hadoop line')"
sleep 3
py="$RUNNER_TEMP/py/bin/python"
servers=$(for i in $(seq "$n"); do printf '%s:32010,' "$(dnIp "$i")"; done); servers=${servers%,}
SC_DATANODES=$(for i in $(seq "$n"); do printf '%s:9864,' "$(dnIp "$i")"; done); export SC_DATANODES=${SC_DATANODES%,}
"$py" -I tools/ci/service-cluster-check.py check "$servers" "$in" > "$work/client.log" 2>&1; rc=$?
cat "$work/client.log"
if [ "$n" -gt 1 ]; then
  t0=$SECONDS
  "$py" -I tools/ci/service-hdfs-sasl-check.py "$servers" "$in" "$splitFile" > "$work/sasl-check.log" 2>&1; rc2=$?
  cat "$work/sasl-check.log"
  note "sasl check: $(grep -m 1 '^result ' "$work/sasl-check.log" || echo 'no result line') in $(( SECONDS - t0 )) s ($($direct && echo 'direct SASL reads, no copies' || echo 'short-circuit + node-local copies'))"
  if $direct; then
    # Item 41: every node decided on direct reads (auto, afc6+), and none made a node-local copy.
    directNodes=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1 | grep -m 1 -c 'hdfs=saslDataTransfer mode=auto direct=true' || true; done | awk '{ s += $1 } END { print s + 0 }')
    directCopies=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=localCopy ' || true)
    note "item 41: $directNodes of $n node(s) read SASL DataNodes directly, local copies $directCopies; $(docker logs srv1 2>&1 | grep -m 1 -oE 'hdfs=saslDataTransfer .*' || echo 'no hdfs=saslDataTransfer line')"
    [ "$directNodes" -eq "$n" ] || finish fail "item 41: only $directNodes of $n node(s) logged hdfs=saslDataTransfer mode=auto direct=true"
    [ "$directCopies" -eq 0 ] || finish fail "item 41: hdfsLocalCopyMaxBytes=0 and direct reads, but $directCopies hdfs=localCopy line(s)"
  fi
  for i in $(seq "$n"); do
    note "srv$i: merged $(docker logs "srv$i" 2>&1 | grep -c 'distributedComplete=merged'), fallback $(docker logs "srv$i" 2>&1 | grep -c 'distributedComplete=fallback'), complete from copies $(docker logs "srv$i" 2>&1 | grep -c 'hdfs=completeFromCopies'), inventory $(docker logs "srv$i" 2>&1 | grep -m 1 -oE 'Registered [0-9]+ local Parquet' || echo '?')"
  done
  # Item 33 (D): distributedComplete endpoints must not read a subquery's table over TCP
  # (TPC-H q04's EXISTS: HdfsNetworkException under SASL) nor miss its schema on failover.
  netFail=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'HdfsNetworkException\|does not exist because schema' || true)
  note "item 33 (D): HdfsNetworkException / missing schema lines $netFail; $(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -m 1 -oE '(HdfsNetworkException|does not exist because schema).{0,160}' || echo 'none')"
  [ "$netFail" -eq 0 ] || finish fail "item 33 (D): a server read over TCP (HdfsNetworkException) or missed a schema on failover"
  batched=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=copyBatches' || true)
  note "scans read in batches of copies: $batched; $(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -m 1 -oE 'hdfs=copyBatches .*' || echo 'no hdfs=copyBatches line')"
  # Item 30 part 2b: aggregates (the chunks GROUP BY) answered per batch of copies and merged.
  aggBatched=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=copyBatches .*merge=aggregate' || true)
  aggMerged=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=copyBatches merged' || true)
  aggFallback=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=copyBatches merge=fallback' || true)
  # Item 30: DISTINCT / LIMIT scans (chunks) answered per batch of copies and merged too.
  rowsBatched=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=copyBatches .*merge=rows' || true)
  note "aggregates in batches of copies: $aggBatched, DISTINCT / LIMIT scans $rowsBatched, merged $aggMerged, fallback $aggFallback; $(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -m 1 -oE 'hdfs=copyBatches merged.*' || echo 'no merged line')"
  # Item 30 part 2b: per-query read lines; a footer-answered count(*) copies nothing.
  readLines=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=reads ' || true)
  copyReads=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -cE 'hdfs=reads .*localCopy=[1-9]' || true)
  footerOnly=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -cE 'hdfs=reads .*footerOnly=[1-9]' || true)
  note "hdfs=reads lines: $readLines, with local copies $copyReads, count(*) from footers without copies $footerOnly"
  if ! $direct && [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ] && { [ "$copyReads" -eq 0 ] || [ "$footerOnly" -eq 0 ]; }; then
    finish fail "expected hdfs=reads lines with local copies ($copyReads) and count(*) answered from footers without copies ($footerOnly)"
  fi
  if [ "$profile" = speed ] && ! $direct && [ "$batched" -eq 0 ] && [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ]; then
    finish fail "the cache is smaller than chunks.lineitem, but no scan was read in batches of copies"
  fi
  if [ "$profile" = speed ] && ! $direct && [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ] \
      && { [ "$aggMerged" -eq 0 ] || [ "$aggFallback" -ne 0 ]; }; then
    finish fail "the cache is smaller than chunks.lineitem, but the GROUP BY was not merged from batches of copies (merged $aggMerged, fallback $aggFallback)"
  fi
  if [ "$profile" = speed ] && ! $direct && [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ] && [ "$rowsBatched" -eq 0 ]; then
    finish fail "the cache is smaller than chunks.lineitem, but no DISTINCT / LIMIT scan was merged from batches of copies"
  fi
  # Item 30 part 2b: the free-space guard is on (minFreeBytes in the cache line) and never refuses on a runner's disk.
  guardLines=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=localCopyCache .*minFreeBytes=' || true)
  noSpace=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -c 'hdfs=localCopyNoSpace' || true)
  note "copy disk guard: $guardLines cache line(s) with minFreeBytes, $noSpace refusal(s) for lack of disk room"
  if ! $direct && [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ] && { [ "$guardLines" -eq 0 ] || [ "$noSpace" -ne 0 ]; }; then
    finish fail "copy disk guard: $guardLines cache line(s) with minFreeBytes (want > 0), $noSpace hdfs=localCopyNoSpace (want 0)"
  fi
  if [ "$profile" != speed ] && ! $direct; then
    # Every node copied into hdfsLocalCopyDir, and its directory there is gone once it stops.
    customDirs=$(for i in $(seq "$n"); do docker logs "srv$i" 2>&1 | grep -m 1 -c 'hdfs=localCopyCache dir=/copies/duckdb-' || true; done | awk '{ s += $1 } END { print s + 0 }')
    note "hdfsLocalCopyDir: $customDirs of $n node(s) copy into /copies; $(docker logs srv1 2>&1 | grep -m 1 -oE 'hdfs=localCopyCache .*' || echo 'no hdfs=localCopyCache line')"
    if [ "$rc2" -eq 0 ] && [ "$rc" -eq 0 ] && [ "$customDirs" -ne "$n" ]; then
      finish fail "hdfsLocalCopyDir=/copies set, but only $customDirs of $n node(s) copied there"
    fi
  fi
  if [ "$rc2" -ne 0 ] && [ "$rc" -eq 0 ]; then
    finish fail "sasl check: $(grep -m 1 '^result fail' "$work/sasl-check.log" | cut -c13- || echo 'client failed')"
  fi
fi
note "$(grep -m 1 '^reads total' "$work/client.log" || echo 'reads total: (none)')"
jmx=$(curl -fsS --max-time 5 "http://172.31.0.12:9864/jmx" 2>/dev/null | grep -oE '"(BlocksRead|ReadBlockOpNumOps|ReadsFromLocalClient|ReadsFromRemoteClient)" : [0-9]+' | tr -d '" ' | sort -u | tr '\n' ' ')
note "dn1 reads (TCP, from DataNode JMX): ${jmx:-(no JMX)}"
note "srv: refused $(docker logs "$srv" 2>&1 | grep -c 'query=refused')"
sleep 12
read1=$(blocksRead); fds1=$(fdRequests)
tcp=$(( ${read1:-0} - ${read0:-0} )) fds=$(( fds1 - fds0 ))
note "DataNodes during the check: blocks read over TCP $tcp (JMX BlocksRead ${read0:-?} -> ${read1:-?}), short-circuit fd requests $fds"
result=$(grep -m 1 '^result ' "$work/client.log")
# Item 30: files a node lacks blocks of are copied through the Java client, over TCP (SASL;
# hadoopfs cannot read over TCP there at all, so every TCP block is a Java-client read). Allow
# split's blocks once per node (footer stubs of it spanning several 1 MiB blocks) plus, for every
# copy, its size in MiB (1 MiB is the smallest block size of the cell; complete answers over
# spread copy whole tables since item 30 part 2).
copies=0; copyBlocks=0
if [ "$n" -gt 1 ]; then
  for i in $(seq "$n"); do
    c=$(docker logs "srv$i" 2>&1 | grep -c 'hdfs=localCopy ' || true)
    copies=$(( copies + c ))
    mib=$(docker logs "srv$i" 2>&1 | grep -oE 'hdfs=localCopy file=.* bytes=[0-9]+' | grep -oE '[0-9]+$' \
      | awk '{ s += int(($1 + 1048575) / 1048576) } END { print s + 0 }')
    copyBlocks=$(( copyBlocks + mib ))
  done
  [ "$copies" -eq 0 ] || copyBlocks=$(( copyBlocks + n * ${blocks:-0} + copies ))
  note "local copies: $copies file(s) (+$copyBlocks blocks allowed over TCP); $(for i in $(seq "$n"); do docker logs "srv$i" 2>&1; done | grep -m 1 -oE 'hdfs=reads .*localCopy=[1-9].*' || echo 'no hdfs=reads line with a local copy')"
fi
# Item 32: every query that succeeded on a node (an executionPath line with success=true) has an
# hdfs=reads line with its qid on that node (complete answers too, also those merged from peers or
# reading no table) - the benchmark summaries count reads per query. Prints "<missing> <queries>
# <first missing: container qid>".
missingReads() { # containers...
  local c
  for c in "$@"; do docker logs "$c" 2>&1 | sed "s/^/$c /"; done > "$work/reads-qid.log"
  awk '
    / executionPath=.* success=true/ { if (match($0, /qid=[^ ]+/)) { done[$1 " " substr($0, RSTART, RLENGTH)] = 1 } }
    / hdfs=reads / { if (match($0, /qid=[^ ]+/)) { reads[$1 " " substr($0, RSTART, RLENGTH)] = 1 } }
    END { m = 0; t = 0; first = "-"; for (q in done) { t++; if (!(q in reads)) { m++; if (first == "-") first = q } }
          print m, t, first }' "$work/reads-qid.log"
}
checkReadsPerQid() { # containers...
  local out missing total first
  out=$(missingReads "$@"); read -r missing total first _ <<< "$out"
  note "hdfs=reads per query ($*): $(( total - missing )) of $total queries with a line"
  if [ "$total" -eq 0 ] || [ "$missing" -ne 0 ]; then
    [ "$first" = - ] || grep -F "${first#* }" "$work/reads-qid.log" | grep "^${first%% *} " | head -n 5 | cut -c1-300
    finish fail "hdfs=reads per query: $missing of $total successful queries without an hdfs=reads line (first: $first)"
  fi
}
# Starts the one small-disk server <name> in dn1's network namespace with /copies = <dir> and
# the extra options <extra> (endpoints per server, room wait); fails the cell when it does not start.
startSmallDiskServer() {
  local name=$1 copies=$2 extra=$3
  # shellcheck disable=SC2086,SC2046
  docker run -d --name "$name" --network container:dn1 $(sockOf 1) -v "$copies:/copies" "${srvKrb[@]}" "${native[@]}" --cpus=2 --memory=4g \
    -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
    -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
    -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
    sc-server /jdk/bin/java -Xmx2g -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
    -Dhazelcast.phone.home.enabled=false ${opts//-DdistributedComplete=true/} -DnumServers=1 $extra \
    -Darrowflight.duckdb.extensionDir=/extdir \
    -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
    --data-dir "$dataDir" --localhost "$(dnIp 1)" --port 32010 --hazelcast-port 5701 \
    --hosts "$(dnIp 1):5701" --metrics-enabled false > /dev/null
  for _ in $(seq 180); do
    docker logs "$name" 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
    [ "$(docker inspect -f '{{.State.Running}}' "$name")" = true ] || break
    sleep 1
  done
  docker logs "$name" 2>&1 | grep -q 'Server Arrow Flight SQL started' \
    || { docker logs "$name" 2>&1 | grep -E 'ERROR|Exception|Caused by' | head -n 40 | cut -c1-400; false; } \
    || finish fail "small disk: server $name did not start: $(docker logs "$name" 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
}
# Item 30 part 2b (4): a copy disk smaller than a query's input. After the cluster checks (sasl-cluster,
# balanced only), the three servers stop and one server (numServers=1) starts in dn1's network
# namespace with hdfsLocalCopyDir on a host tmpfs of the largest lineitem file + 4 MiB plus the
# DEFAULT reserve (item 32: hdfsLocalCopyMinFreeBytes not set, so min(1 GiB, 10 % of the disk)),
# so smaller than chunks.lineitem; the cache limit stays at its default, only the disk bounds the
# copies. tools/ci/service-hdfs-smalldisk-check.py: scans and GROUP BY in parts go in batches sized by
# the disk's room; a complete join needing every copy is answered or refused by the guard (never
# "No space left"); complete one-table queries (scan, GROUP BY, plain names) go in disk-sized batches
# (item 31: hdfs=completeInCopyBatches; srvd runs with distributedComplete at its default, false, as on
# the maintainer's stand, so aggregates do not take the distributed path); the tmpfs is sampled and its peak use must stay below its size.
smallDisk() {
  local minFree largest total size dir peak sampler rc3 rc5 refused noSpace batches completeBatched i cacheLine
  largest=$(stat -c %s "$in"/data/tpch/lineitem/*.parquet | sort -n | tail -n 1)
  total=$(stat -c %s "$in"/data/tpch/lineitem/*.parquet | awk '{ s += $1 } END { print s + 0 }')
  # 90 % of the disk (the default reserve keeps 10 % free) holds the largest file + 4 MiB.
  size=$(( ((largest + 4 * 1048576) * 10 / 9 + 1048575) / 1048576 * 1048576 ))
  minFree=$(( size / 100 * 10 ))
  [ "$(( size - minFree ))" -lt "$total" ] || { note "small disk: $size bytes hold every lineitem file ($total); skipped"; return 0; }
  for i in $(seq "$n"); do docker stop -t 20 "srv$i" > /dev/null; done
  dir="$work/smalldisk"; mkdir -p "$dir"
  sudo mount -t tmpfs -o "size=$size,mode=1777" tmpfs "$dir" || finish fail "small disk: tmpfs not mounted"
  note "small disk: tmpfs $size bytes for copies (largest lineitem file $largest, all $total, minFreeBytes $minFree)"
  startSmallDiskServer srvd "$dir" "-DendpointsPerServer=1"
  sleep 3
  ( while :; do df -B1 --output=used "$dir" | tail -n 1; sleep 0.2; done ) > "$work/smalldisk-used.txt" 2>/dev/null &
  sampler=$!
  "$py" -I tools/ci/service-hdfs-smalldisk-check.py "$(dnIp 1):32010" "$in" > "$work/smalldisk-check.log" 2>&1; rc3=$?
  kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null
  cat "$work/smalldisk-check.log"
  peak=$(sort -n "$work/smalldisk-used.txt" | tail -n 1)
  refused=$(grep -c '^join refused' "$work/smalldisk-check.log" || true)
  noSpace=$(docker logs srvd 2>&1 | grep -c 'hdfs=localCopyNoSpace' || true)
  batches=$(docker logs srvd 2>&1 | grep -c 'hdfs=copyBatches' || true)
  completeBatched=$(docker logs srvd 2>&1 | grep -c 'hdfs=completeInCopyBatches table=' || true)
  # Item 34: a copy-disk refusal is one WARN line (result=noRoom), never an ERROR with a stack.
  noRoomStacks=$(docker logs srvd 2>&1 | grep -c 'LocalCopyCache\$NoRoomException' || true)
  if [ "$refused" -gt 0 ] && [ "$noRoomStacks" -gt 0 ]; then
    finish fail "small disk: a copy-disk refusal logged a NoRoomException stack ($noRoomStacks line(s)); want one WARN line"
  fi
  note "small disk: $(grep -m 1 '^result ' "$work/smalldisk-check.log" || echo 'no result line'); join $([ "$refused" -gt 0 ] && echo refused || echo answered), hdfs=localCopyNoSpace $noSpace, hdfs=copyBatches $batches, complete in batches $completeBatched, peak use ${peak:-?} of $size bytes; $(docker logs srvd 2>&1 | grep -m 1 -oE 'hdfs=localCopyNoSpace .*' || echo 'no refusal line')"
  # Item 33 speed cells: endpointsPerServer=4 on the same small disk, so the tickets of one
  # partial query copy at once; with hdfsLocalCopyRoomWaitMillis they wait for each other's room
  # instead of being refused (egret part-lineitem-sp / part-group-sp, exact answers, no refusal).
  docker stop -t 20 srvd > /dev/null
  startSmallDiskServer srvs "$dir" "-DendpointsPerServer=4 -DhdfsLocalCopyRoomWaitMillis=60000"
  sleep 3
  "$py" -I tools/ci/service-hdfs-smalldisk-check.py "$(dnIp 1):32010" "$in" speed > "$work/smalldisk-speed.log" 2>&1 &
  local checkPid=$!
  # Thread dump while tickets may wait for room (printed on failure; it found the wake-up loop of
  # CI 42090655236).
  sleep 20
  docker exec srvs /jdk/bin/jcmd 1 Thread.print > "$work/smalldisk-speed-threads.txt" 2>&1 || true
  wait "$checkPid"; rc5=$?
  cat "$work/smalldisk-speed.log"
  docker stop -t 20 srvs > /dev/null
  docker logs srvs > "$work/smalldisk-speed-server.log" 2>&1 || true
  if [ "$rc5" -ne 0 ]; then
    echo "--- speed server: copy lines (last 60) ---"
    grep -E 'hdfs=localCopy|hdfs=reads' "$work/smalldisk-speed-server.log" | tail -n 60 | cut -c1-300
    echo "--- speed server: threads in LocalCopyCache / blocked (dump at 20 s) ---"
    awk 'BEGIN { RS = "" } /LocalCopyCache|hdfs-local-copy|BLOCKED/' "$work/smalldisk-speed-threads.txt" | head -n 200 | cut -c1-200
  fi
  note "small disk speed cells: $(grep -m 1 '^result ' "$work/smalldisk-speed.log" || echo 'no result line'); hdfs=localCopyRoomWait $(docker logs srvs 2>&1 | grep -c 'hdfs=localCopyRoomWait ' || true), hdfs=localCopyRoomWaited $(docker logs srvs 2>&1 | grep -c 'hdfs=localCopyRoomWaited' || true), hdfs=localCopyNoSpace $(docker logs srvs 2>&1 | grep -c 'hdfs=localCopyNoSpace' || true); $(docker logs srvs 2>&1 | grep -m 1 -oE 'hdfs=localCopyCache .*' || echo 'no cache line')"
  [ "$rc5" -eq 0 ] || finish fail "small disk speed cells: $(grep -m 1 '^result fail' "$work/smalldisk-speed.log" | cut -c13- || echo 'client failed')"
  docker logs srvs 2>&1 | grep -q 'hdfs=localCopyRoomWaited' \
    || note "small disk speed cells: no ticket had to wait (no hdfs=localCopyRoomWaited line)"
  docker logs srvs 2>&1 | grep -q 'roomWaitMillis=60000' || finish fail "small disk speed cells: no roomWaitMillis=60000 in the cache line"
  docker logs srvs 2>&1 | grep -qiE 'No space left on device' && finish fail "small disk speed cells: 'No space left on device' in the server log"
  sudo umount "$dir" || true
  cacheLine=$(docker logs srvd 2>&1 | grep -m 1 -oE 'hdfs=localCopyCache .*' || true)
  note "small disk: ${cacheLine:-no hdfs=localCopyCache line}"
  [[ $cacheLine == *"reserve=default diskBytes=$size"* ]] && [[ $cacheLine == *"minFreeBytes=$(( size / 100 * 10 )) "* ]] \
    || finish fail "small disk: want the default reserve 10 % of the $size-byte disk in the cache line, got: ${cacheLine:-none}"
  [ "$rc3" -eq 0 ] || finish fail "small disk: $(grep -m 1 '^result fail' "$work/smalldisk-check.log" | cut -c13- || echo 'client failed')"
  [ -n "$peak" ] && [ "$peak" -lt "$size" ] || finish fail "small disk: copy disk peak use ${peak:-?} of $size bytes (filled)"
  [ "$batches" -gt 0 ] || finish fail "small disk: the copies of chunks.lineitem exceed the disk, but no hdfs=copyBatches line"
  docker logs srvd 2>&1 | grep -q 'hdfs=copyBatches .*merge=rewrite' \
    || finish fail "small disk: the HAVING / count(DISTINCT) query was not merged from copy batches (no merge=rewrite line)"
  [ "$completeBatched" -ge 15 ] || finish fail "small disk: $completeBatched of the 15 complete one-table queries over chunks / lineitem_p were read in batches of copies (hdfs=completeInCopyBatches lines); $(docker logs srvd 2>&1 | grep -m 1 -oE 'hdfs=completeInCopyBatches skipped .*' | cut -c1-300)"
  # Item 33 regression: one query at a time, so no copy stays pinned once a lease is released, and
  # a refusal (the join) never counts copies of other (finished) queries as in use.
  released=$(docker logs srvd 2>&1 | grep -oE 'hdfs=localCopyReleased .*' | tail -n 1)
  [[ $released == *" inUseBytes=0 "* ]] || finish fail "small disk: copies still pinned after the last query: ${released:-no hdfs=localCopyReleased line}"
  docker logs srvd 2>&1 | grep -E 'hdfs=localCopyNoSpace .*otherInUseBytes=[1-9]' | head -n 1 | grep -q . \
    && finish fail "small disk: a refusal counted copies of other queries as in use: $(docker logs srvd 2>&1 | grep -m 1 -oE 'hdfs=localCopyNoSpace .*otherInUseBytes=[1-9].*')"
  [ "$refused" -eq 0 ] || [ "$noSpace" -gt 0 ] || finish fail "small disk: join refused, but no hdfs=localCopyNoSpace line"
  docker logs srvd 2>&1 | grep -qiE 'No space left on device' && finish fail "small disk: 'No space left on device' in the server log"
  checkReadsPerQid srvd
  return 0
}
# Item 31 (7): direct memory of a distributed complete full scan. After the cluster checks
# (sasl-cluster, speed only) the three servers restart as on the maintainer's stand: speed defaults
# (4 endpoints per server, no small copy cache), distributedComplete=true, -XX:MaxDirectMemorySize=512m;
# tools/ci/service-hdfs-lowdirect-check.py runs SELECT * FROM tpch.lineitem in complete mode four times
# in a row on the first server (each answer merged from the endpoint answers of all nodes), exact
# every time, no OutOfMemoryError in any log.
lowDirect() {
  local lowOpts rc4 merged oom i c
  lowOpts=$(echo "$opts" | sed -E 's/ -DhdfsLocalCopyMaxBytes=[0-9]+//; s/ -DendpointsPerServer=1//')
  for i in $(seq "$n"); do docker stop -t 20 "srv$i" > /dev/null; done
  note "low direct memory: 3 servers, -XX:MaxDirectMemorySize=512m, options: $lowOpts"
  for i in $(seq "$n"); do
  # shellcheck disable=SC2086,SC2046
  docker run -d --name "srvm$i" --network "container:dn$i" $(sockOf "$i") "${srvKrb[@]}" "${native[@]}" --cpus=$cpus --memory=$mem \
    -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
    -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
    -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
    sc-server /jdk/bin/java -Xmx$xmx -XX:MaxDirectMemorySize=512m --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
    -Dhazelcast.phone.home.enabled=false -DnumServers=$n $lowOpts -Darrowflight.duckdb.extensionDir=/extdir \
    -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
    --data-dir "$dataDir" --localhost "$(dnIp "$i")" --port 32010 --hazelcast-port 5701 \
    --hosts "$peers" --metrics-enabled false > /dev/null
  done
  for i in $(seq "$n"); do
    c="srvm$i"
    for _ in $(seq 180); do
      docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
      [ "$(docker inspect -f '{{.State.Running}}' "$c")" = true ] || break
      sleep 1
    done
    docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started' \
      || finish fail "low direct memory: server $c did not start: $(docker logs "$c" 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
  done
  sleep 5
  "$py" -I tools/ci/service-hdfs-lowdirect-check.py "$(dnIp 1):32010" "$in" > "$work/lowdirect-check.log" 2>&1; rc4=$?
  cat "$work/lowdirect-check.log"
  for i in $(seq "$n"); do docker logs "srvm$i" > "$work/srvm$i.log" 2>&1 || true; done
  merged=$(grep -c 'distributedComplete=merged' "$work/srvm1.log" || true)
  oom=$(cat "$work"/srvm*.log | grep -c 'OutOfMemoryError' || true)
  note "low direct memory: $(grep -m 1 '^result ' "$work/lowdirect-check.log" || echo 'no result line'); merged $merged, OutOfMemoryError $oom; $(grep -m 1 -oE 'distributedComplete=merged endpoints=[0-9]+ elapsedMs=[0-9]+ peerBytes=[0-9]+' "$work/srvm1.log" || echo 'no merged line'); $(grep '^scan ' "$work/lowdirect-check.log" | tr '\n' ' ')"
  [ "$rc4" -eq 0 ] || finish fail "low direct memory: $(grep -m 1 '^result fail' "$work/lowdirect-check.log" | cut -c13- || echo 'client failed')"
  [ "$oom" -eq 0 ] || finish fail "low direct memory: OutOfMemoryError in a server log: $(cat "$work"/srvm*.log | grep -m 1 'OutOfMemoryError' | cut -c1-250)"
  [ "$merged" -ge 4 ] || finish fail "low direct memory: $merged of the 4 full scans were merged from endpoint answers (distributedComplete=merged lines)"
  checkReadsPerQid $(for i in $(seq "$n"); do echo "srvm$i"; done)
  return 0
}
# Item 36 (1): distributed complete on small copy disks, as on the maintainer's stand. After the low
# direct memory stage (sasl-cluster, speed only) the three servers restart with the speed defaults
# (4 endpoints per server), distributedComplete=true and hdfsLocalCopyDir=/lcopy, each on its own
# host tmpfs of the largest lineitem file + 4 MiB above the DEFAULT reserve (smaller than any node's
# share of chunks.lineitem). tools/ci/service-hdfs-dcsmalldisk-check.py runs eight complete queries
# over chunks.lineitem twice on the first server: every answer exact, and after each query no server
# holds a deleted copy file open (/proc/1/fd). No "No space left on device", no NoRoomException stack.
dcSmallDisk() {
  local dcOpts largest size rc6 i c merged perNode noSpace stacks
  largest=$(stat -c %s "$in"/data/tpch/lineitem/*.parquet | sort -n | tail -n 1)
  size=$(( ((largest + 4 * 1048576) * 10 / 9 + 1048575) / 1048576 * 1048576 ))
  dcOpts=$(echo "$opts" | sed -E 's/ -DhdfsLocalCopyMaxBytes=[0-9]+//; s/ -DendpointsPerServer=1//')
  dcOpts="$dcOpts -DhdfsLocalCopyDir=/lcopy"
  for i in $(seq "$n"); do docker stop -t 20 "srvm$i" > /dev/null 2>&1 || true; done
  note "dc small disk: 3 servers, a $size-byte tmpfs each for copies (largest lineitem file $largest), options: $dcOpts"
  for i in $(seq "$n"); do
  mkdir -p "$work/dcdisk$i"
  sudo mount -t tmpfs -o "size=$size,mode=1777" tmpfs "$work/dcdisk$i" || finish fail "dc small disk: tmpfs $i not mounted"
  # shellcheck disable=SC2086,SC2046
  docker run -d --name "srvc$i" --network "container:dn$i" $(sockOf "$i") -v "$work/dcdisk$i:/lcopy" "${srvKrb[@]}" "${native[@]}" --cpus=$cpus --memory=$mem \
    -v "$in/app:/app:ro" -v "$in/extdir:/extdir:ro" -v "$JAVA_HOME_21_X64:/jdk:ro" -v "$conf:/conf:ro" \
    -e FLIGHT_ALLOW_INSECURE=true -e DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true -e FLIGHT_PLUGINS_DIR=/app/plugins \
    -e LIBHDFS3_CONF=/conf/hdfs-site.xml -e LANG=C.UTF-8 \
    sc-server /jdk/bin/java -Xmx$xmx -XX:MaxDirectMemorySize=1g --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true \
    -Dhazelcast.phone.home.enabled=false -DnumServers=$n $dcOpts -Darrowflight.duckdb.extensionDir=/extdir \
    -cp "/conf:/app/arrow-flight-server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
    --data-dir "$dataDir" --localhost "$(dnIp "$i")" --port 32010 --hazelcast-port 5701 \
    --hosts "$peers" --metrics-enabled false > /dev/null
  done
  for i in $(seq "$n"); do
    c="srvc$i"
    for _ in $(seq 180); do
      docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
      [ "$(docker inspect -f '{{.State.Running}}' "$c")" = true ] || break
      sleep 1
    done
    docker logs "$c" 2>&1 | grep -q 'Server Arrow Flight SQL started' \
      || finish fail "dc small disk: server $c did not start: $(docker logs "$c" 2>&1 | grep -m 1 -E 'Exception|ERROR' | cut -c1-250)"
  done
  sleep 5
  "$py" -I tools/ci/service-hdfs-dcsmalldisk-check.py "$(dnIp 1):32010" "$in" "$(for i in $(seq "$n"); do printf 'srvc%s,' "$i"; done | sed 's/,$//')" \
    > "$work/dcsmalldisk-check.log" 2>&1; rc6=$?
  cat "$work/dcsmalldisk-check.log"
  if [ "$rc6" -ne 0 ]; then
    for i in $(seq "$n"); do
      echo "--- srvc$i: open files below /lcopy, copy lines (last 30) ---"
      docker exec "srvc$i" sh -c 'ls -l /proc/1/fd 2>/dev/null' | grep -F -- '-> /lcopy/' | head -n 20
      docker logs "srvc$i" 2>&1 | grep -E 'hdfs=localCopy|distributedComplete=' | tail -n 30 | cut -c1-300
    done
    docker exec srvc1 /jdk/bin/jcmd 1 Thread.print > "$work/dcsmalldisk-threads.txt" 2>&1 || true
  fi
  for i in $(seq "$n"); do docker stop -t 20 "srvc$i" > /dev/null; docker logs "srvc$i" > "$work/srvc$i.log" 2>&1 || true; done
  for i in $(seq "$n"); do sudo umount "$work/dcdisk$i" || true; done
  merged=$(grep -c 'distributedComplete=merged' "$work/srvc1.log" || true)
  perNode=$(grep -c 'distributedComplete=oneStreamPerNode' "$work/srvc1.log" || true)
  noSpace=$(cat "$work"/srvc*.log | grep -c 'hdfs=localCopyNoSpace' || true)
  stacks=$(cat "$work"/srvc*.log | grep -c 'LocalCopyCache\$NoRoomException' || true)
  note "dc small disk: $(grep -m 1 '^result ' "$work/dcsmalldisk-check.log" || echo 'no result line'); merged $merged, oneStreamPerNode $perNode, hdfs=localCopyNoSpace $noSpace; $(cat "$work"/srvc*.log | grep -m 1 -oE 'hdfs=localCopyNoSpace .*' | cut -c1-300 || echo 'no refusal line')"
  [ "$rc6" -eq 0 ] || finish fail "dc small disk: $(grep -m 1 '^result fail' "$work/dcsmalldisk-check.log" | cut -c13- || echo 'client failed')"
  [ "$stacks" -eq 0 ] || finish fail "dc small disk: $stacks NoRoomException stack line(s) in the server logs"
  cat "$work"/srvc*.log | grep -qiE 'No space left on device' && finish fail "dc small disk: 'No space left on device' in a server log"
  [ "$merged" -ge 1 ] || finish fail "dc small disk: no complete answer was merged from endpoint answers (no distributedComplete=merged line)"
  # shellcheck disable=SC2046
  checkReadsPerQid $(for i in $(seq "$n"); do echo "srvc$i"; done)
  return 0
}
# Item 42 (2): complete-mode point queries on point.orders_r read only the file their key can be in, also
# on SASL DataNodes read short-circuit only with node-local copies (the maintainer's stand: no
# views=prunedFiles line there, kept=1 files=8 with privileged DataNodes). One server without
# distributedComplete, as on the stand: srv itself in ha-krb-sasl; in the clusters the servers stop
# and srvq starts in dn1's network namespace (copy cache at its default) and stops again after.
pointPruning() {
  local name=$srv rcq lines kept1 kept0 saved
  if [ "$n" -gt 1 ]; then
    for i in $(seq "$n"); do docker stop -t 20 "srv$i" > /dev/null; done
    mkdir -p "$work/copiesq" && chmod 777 "$work/copiesq"
    saved=$opts
    opts=$(echo "$opts" | sed -E 's/ -DhdfsLocalCopyMaxBytes=[1-9][0-9]*//; s/ -DendpointsPerServer=1//')
    startSmallDiskServer srvq "$work/copiesq" ""
    opts=$saved
    name=srvq
    sleep 3
  fi
  "$py" -I tools/ci/service-hdfs-point-check.py check "$(dnIp 1):32010" "$work/point/orders_r" > "$work/point-check.log" 2>&1; rcq=$?
  cat "$work/point-check.log"
  docker logs "$name" > "$work/$name-point.log" 2>&1 || true
  [ "$n" -gt 1 ] && docker stop -t 20 srvq > /dev/null
  lines=$(grep -c 'views=prunedFiles table=point.orders_r' "$work/$name-point.log" || true)
  kept1=$(grep -c 'views=prunedFiles table=point.orders_r kept=1 files=8' "$work/$name-point.log" || true)
  kept0=$(grep -c 'views=prunedFiles table=point.orders_r kept=0 files=8' "$work/$name-point.log" || true)
  note "item 42 (2) point queries on $name: $(grep -m 1 '^result ' "$work/point-check.log" || echo 'no result line'); views=prunedFiles $lines (kept=1: $kept1, kept=0: $kept0), complete from copies $(grep -c 'hdfs=completeFromCopies' "$work/$name-point.log"), footer errors $(grep -c 'pruning=footerError' "$work/$name-point.log"); $(grep -m 1 -oE 'views=prunedFiles table=point.*' "$work/$name-point.log" || echo 'no views=prunedFiles line')"
  [ "$rcq" -eq 0 ] || finish fail "item 42 (2): $(grep -m 1 '^result fail' "$work/point-check.log" | cut -c13- || echo 'point client failed')"
  [ "$kept1" -ge 2 ] && [ "$kept0" -ge 1 ] \
    || finish fail "item 42 (2): point queries not pruned on SASL DataNodes (views=prunedFiles kept=1: $kept1 of 2, kept=0: $kept0 of 1)"
  return 0
}
# Item 37 (a), (b): a node stop under load, then a server started while tpch.nation's files are
# readable by their owner only (the server's principal flight is not the owner). Kerberos cells
# with one server, balanced only. Fails on: a load failure other than UNAVAILABLE, 'Filesystem
# closed', a Netty RejectedExecutionException or an allocator leak in the stopping server's log, a
# denied query answered as an internal error or a missing table, or the table not answering
# again (without a restart) once its files are readable.
denyAndStop() {
  local rcl rcd rcr rcv rcr2 stopLog bad
  "$py" -I tools/ci/service-hdfs-deny-stop-check.py load "$(dnIp 1):32010" 40 > "$work/stop-load.log" 2>&1 &
  local loader=$!
  sleep 10
  docker stop -t 25 "$srv" > /dev/null
  wait "$loader"; rcl=$?
  cat "$work/stop-load.log"
  docker logs "$srv" > "$work/srv-stop.log" 2>&1 || true
  stopLog=$(sed -n '/Shutting down server/,$p' "$work/srv-stop.log")
  bad=$(echo "$stopLog" | grep -cE 'Filesystem closed|RejectedExecutionException|Memory was leaked|Error closing' || true)
  note "item 37 (b) stop under load: $(grep -m 1 '^load: ' "$work/stop-load.log" || echo 'no load line'); $(echo "$stopLog" | grep -m 1 -oE 'shutdownDrain=(done|timeout).*' || echo 'no shutdownDrain line'); bad lines $bad, log lines after the stop began $(echo "$stopLog" | wc -l)"
  [ "$bad" -eq 0 ] || { echo "$stopLog" | grep -E 'Filesystem closed|RejectedExecutionException|Memory was leaked|Error closing' | head -n 10 | cut -c1-300; finish fail "item 37 (b): $bad 'Filesystem closed' / RejectedExecutionException / leak line(s) while the server stopped"; }
  echo "$stopLog" | grep -q 'shutdownDrain=started' || finish fail "item 37 (b): no shutdownDrain=started line (the stop did not drain through the gate)"
  [ "$rcl" -eq 0 ] || finish fail "item 37 (b): $(grep -m 1 '^result fail' "$work/stop-load.log" | cut -c13- || echo 'load client failed')"

  docker exec "${kenv[@]}" "$admin" hdfs dfs -chmod 600 '/data/tpch/nation/*' || finish fail "item 37 (a): chmod failed"
  mkdir -p "$work/copiesp" && chmod 777 "$work/copiesp"
  startSmallDiskServer srvp "$work/copiesp" ""
  sleep 3
  "$py" -I tools/ci/service-hdfs-deny-stop-check.py denied "$(dnIp 1):32010" > "$work/deny.log" 2>&1; rcd=$?
  cat "$work/deny.log"
  docker exec "${kenv[@]}" "$admin" hdfs dfs -chmod 644 '/data/tpch/nation/*' || finish fail "item 37 (a): chmod back failed"
  "$py" -I tools/ci/service-hdfs-deny-stop-check.py restored "$(dnIp 1):32010" > "$work/restored.log" 2>&1; rcr=$?
  cat "$work/restored.log"
  # Item 44 (2): revoke the directory of a table the server just read; the caches would answer
  # it for 30 s, the access recheck (hdfsAccessRecheckMs, 10 s) must deny it within 15 s.
  docker exec "${kenv[@]}" "$admin" hdfs dfs -chmod 700 /data/tpch/nation || finish fail "item 44 (2): chmod failed"
  "$py" -I tools/ci/service-hdfs-deny-stop-check.py revoked "$(dnIp 1):32010" 15 > "$work/revoked.log" 2>&1; rcv=$?
  cat "$work/revoked.log"
  docker exec "${kenv[@]}" "$admin" hdfs dfs -chmod 755 /data/tpch/nation || finish fail "item 44 (2): chmod back failed"
  "$py" -I tools/ci/service-hdfs-deny-stop-check.py restored "$(dnIp 1):32010" > "$work/restored2.log" 2>&1; rcr2=$?
  cat "$work/restored2.log"
  note "item 44 (2): $(grep -m 1 '^revoked: denied after' "$work/revoked.log" || echo 'no denial'), $(grep -m 1 '^result ' "$work/revoked.log" || echo '?'); granted again $(grep -m 1 '^result ' "$work/restored2.log" || echo '?')"
  docker stop -t 25 srvp > /dev/null; docker logs srvp > "$work/srvp.log" 2>&1 || true
  note "item 37 (a): denied $(grep -m 1 '^result ' "$work/deny.log" || echo '?'), restored $(grep -m 1 '^result ' "$work/restored.log" || echo '?'); views=createFailed $(grep -c 'views=createFailed' "$work/srvp.log"), views=created $(grep -c 'views=created ' "$work/srvp.log"), denied lines $(grep -cE 'query=denied|result=denied' "$work/srvp.log"); $(grep -m 1 -oE '(query|result)=denied.{0,200}' "$work/srvp.log" || echo 'no denied line')"
  [ "$rcd" -eq 0 ] || finish fail "item 37 (a): $(grep -m 1 '^result fail' "$work/deny.log" | cut -c13- || echo 'denied client failed')"
  [ "$rcr" -eq 0 ] || finish fail "item 37 (a): $(grep -m 1 '^result fail' "$work/restored.log" | cut -c13- || echo 'restored client failed')"
  [ "$rcv" -eq 0 ] || finish fail "item 44 (2): $(grep -m 1 '^result fail' "$work/revoked.log" | cut -c13- || echo 'revoked client failed')"
  [ "$rcr2" -eq 0 ] || finish fail "item 44 (2): $(grep -m 1 '^result fail' "$work/restored2.log" | cut -c13- || echo 'granted-again client failed')"
  grep -E ' ERROR ' "$work/srvp.log" | grep -qi 'nation' && finish fail "item 37 (a): an ERROR line names the denied table: $(grep -E ' ERROR ' "$work/srvp.log" | grep -i -m 1 nation | cut -c1-250)"
  return 0
}
bound=$(( 3 * n * ${files:-0} + copyBlocks ))
if [ "$rc" -eq 0 ] && [ "$result" = "result pass" ]; then
  # Item 41: with direct SASL reads hadoopfs reads remote blocks over TCP by design.
  if [ "$sc" = on ] && ! $direct && { [ -z "$read1" ] || [ "$tcp" -gt "$bound" ] || [ "$fds" -eq 0 ]; }; then
    finish fail "answers right, but short-circuit is configured and the server read $tcp blocks over TCP (Java footer + local copy bound $bound, $fds short-circuit fd requests)"
  fi
  if $sasl && [ "$n" -eq 1 ]; then checkReadsPerQid "$srv"; fi
  # shellcheck disable=SC2046
  # hdfs=reads lines (item 32): one per query, on the copy path and (item 41 follow-up) on the
  # direct SASL path, where they count files read node-local vs remote (direct=true).
  if [ "$n" -gt 1 ]; then checkReadsPerQid $(for i in $(seq "$n"); do echo "srv$i"; done); fi
  if $direct; then
    directLines=$(grep -c 'hdfs=reads files=[0-9]* direct=true' "$work/reads-qid.log" || true)
    remoteFiles=$(grep -oE 'hdfs=reads files=[0-9]+ direct=true nodeLocal=[0-9]+ remote=[0-9]+' "$work/reads-qid.log" | awk -F'remote=' '{ s += $2 } END { print s + 0 }')
    unknownFiles=$(grep -oE 'direct=true .* unknown=[0-9]+' "$work/reads-qid.log" | awk -F'unknown=' '{ s += $2 } END { print s + 0 }')
    note "item 41 direct reads: $directLines hdfs=reads direct=true line(s), remote files $remoteFiles, unknown $unknownFiles; $(grep -m 1 -oE 'hdfs=reads files=[1-9][0-9]* direct=true.*' "$work/reads-qid.log" || echo 'no direct line with files')"
    [ "$directLines" -ge 1 ] || finish fail "item 41: no hdfs=reads direct=true line on the direct SASL path"
    [ "$remoteFiles" -ge 1 ] || finish fail "item 41: no query read a remote file directly (remote=0 on every hdfs=reads direct=true line)"
  fi
  if $sasl; then pointPruning; fi
  if [ "$n" -gt 1 ] && ! $direct && [ "$profile" = balanced ]; then smallDisk; fi
  if [ "$n" -gt 1 ] && ! $direct && [ "$profile" = speed ]; then lowDirect; dcSmallDisk; fi
  if $krb && [ "$n" -eq 1 ] && [ "$profile" = balanced ]; then denyAndStop; fi
  finish pass ""
fi
detail=${result#result fail }
[ -n "$result" ] || detail="client failed: $(grep -m 1 -E 'Error|Exception' "$work/client.log")"
finish fail "$detail"
