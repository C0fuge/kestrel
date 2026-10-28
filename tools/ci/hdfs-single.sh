#!/usr/bin/env bash
# Lane 4 (catalogs and sources): a single-node HDFS for probes (apache/hadoop:3.4.1 from Docker Hub). One container
# on the host network runs a NameNode (localhost:8020) and a DataNode (localhost:9866), no security, replication 1.
# Usage: tools/ci/hdfs-single.sh [<local dir> <hdfs dir>]  - waits until HDFS leaves safe mode, then copies the
# local directory to the HDFS directory when given (permissions are off).
set -euo pipefail
image=apache/hadoop:3.4.1
docker rm -f hdfs > /dev/null 2>&1 || true
docker run -d --name hdfs --network host \
  -e CORE-SITE.XML_fs.defaultFS=hdfs://localhost:8020 \
  -e HDFS-SITE.XML_dfs.replication=1 \
  -e HDFS-SITE.XML_dfs.permissions.enabled=false \
  -e HDFS-SITE.XML_dfs.namenode.rpc-bind-host=0.0.0.0 \
  -e HDFS-SITE.XML_dfs.namenode.name.dir=/tmp/hdfs/nn \
  -e HDFS-SITE.XML_dfs.datanode.data.dir=/tmp/hdfs/dn \
  -e HDFS-SITE.XML_dfs.namenode.datanode.registration.ip-hostname-check=false \
  "$image" bash -c 'hdfs namenode -format -nonInteractive -force > /tmp/format.log 2>&1 \
    && hdfs --daemon start namenode && hdfs --daemon start datanode \
    && tail -F /opt/hadoop/logs/*.log' > /dev/null
for _ in $(seq 90); do
  # Captured first: grep -q closing the pipe early would fail the pipeline under pipefail.
  report=$(docker exec hdfs hdfs dfsadmin -report 2>/dev/null || true)
  if [[ $report == *'Live datanodes (1)'* && $report != *'Safe mode is ON'* ]]; then
    break
  fi
  sleep 1
done
docker exec hdfs hdfs dfsadmin -report | grep -E 'Live datanodes|^Name:'
if [ $# -ge 2 ]; then
  docker cp "$1" hdfs:/tmp/hdfs-upload
  docker exec hdfs bash -c 'hdfs dfs -mkdir -p "$1" && hdfs dfs -put -f /tmp/hdfs-upload/* "$1"/' _ "$2"
  docker exec hdfs hdfs dfs -ls "$2" | head -20
fi
