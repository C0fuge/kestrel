#!/usr/bin/env bash
# Starts a single-node Apache Ozone (SCM, OM, one datanode, S3 gateway) as containers on a
# private network, with the S3 gateway published on localhost:<port>, and waits until it answers
# S3 requests. Non-secure cluster: the gateway accepts any access key (test-only, as moto).
# Replication ONE, small blocks/containers, so one datanode is enough.
# Usage: ozone-s3.sh <image> <port>
set -euo pipefail
image=$1
port=$2
docker network create ozone > /dev/null
conf=(
  -e OZONE-SITE.XML_ozone.om.address=om
  -e OZONE-SITE.XML_ozone.scm.names=scm
  -e OZONE-SITE.XML_ozone.scm.client.address=scm
  -e OZONE-SITE.XML_ozone.scm.block.client.address=scm
  -e OZONE-SITE.XML_ozone.scm.datanode.id.dir=/data/metadata
  -e OZONE-SITE.XML_ozone.metadata.dirs=/data/metadata
  -e OZONE-SITE.XML_hdds.datanode.dir=/data/hdds
  -e OZONE-SITE.XML_ozone.scm.container.size=256MB
  -e OZONE-SITE.XML_ozone.scm.block.size=4MB
  -e OZONE-SITE.XML_ozone.scm.datanode.ratis.volume.free-space.min=10MB
  -e OZONE-SITE.XML_hdds.datanode.volume.min.free.space=100MB
  -e OZONE-SITE.XML_ozone.scm.pipeline.creation.interval=5s
  -e OZONE-SITE.XML_ozone.scm.pipeline.owner.container.count=1
  -e OZONE-SITE.XML_ozone.datanode.pipeline.limit=1
  -e OZONE-SITE.XML_hdds.scm.safemode.min.datanode=1
  -e OZONE-SITE.XML_hdds.scm.wait.time.after.safemode.exit=5s
  -e OZONE-SITE.XML_hdds.heartbeat.interval=2s
  -e OZONE-SITE.XML_ozone.replication=1
  -e OZONE-SITE.XML_ozone.server.default.replication=1
  -e OZONE-SITE.XML_ozone.http.basedir=/tmp/ozone_http
  -e OZONE_CONF_DIR=/etc/hadoop
  -e no_proxy=om,scm,s3g,localhost,127.0.0.1
)
docker run -d --name ozone-scm --hostname scm --network ozone "${conf[@]}" \
  -e ENSURE_SCM_INITIALIZED=/data/metadata/scm/current/VERSION "$image" ozone scm > /dev/null
docker run -d --name ozone-om --hostname om --network ozone "${conf[@]}" \
  -e ENSURE_OM_INITIALIZED=/data/metadata/om/current/VERSION "$image" ozone om > /dev/null
docker run -d --name ozone-dn --hostname dn --network ozone "${conf[@]}" "$image" ozone datanode > /dev/null
docker run -d --name ozone-s3g --hostname s3g --network ozone "${conf[@]}" -p "$port:9878" \
  "$image" ozone s3g > /dev/null
# Ready when SCM has left safe mode and a key can be written and read (pipeline open).
for _ in $(seq 150); do
  if docker exec ozone-scm sh -c 'ozone admin safemode status | grep -q "out of safe mode" &&
      echo ok > /tmp/probe && { ozone sh volume info /probe > /dev/null || ozone sh volume create /probe; } &&
      { ozone sh bucket info /probe/probe > /dev/null || ozone sh bucket create /probe/probe; } &&
      ozone sh key put /probe/probe/k /tmp/probe && ozone sh key cat /probe/probe/k | grep -q ok' \
      > /dev/null 2>&1 \
      && curl -sS -o /dev/null "http://localhost:$port/"; then  # any HTTP answer (unsigned: 403)
    echo "ozone ready"
    exit 0
  fi
  sleep 2
done
echo "ozone did not become ready"
for c in ozone-scm ozone-om ozone-dn ozone-s3g; do echo "== $c"; docker logs "$c" 2>&1 | tail -40; done
exit 1
