#!/usr/bin/env bash
# Item 43 (lane 3): 32 clients of point queries (point-query-load.py: point_orders, point_lineitem,
# count_filter) on node 1 of a service-cluster.yml cell (speed or speed-dc).
#
# Usage: tools/ci/service-cluster-point.sh <python> <keys> <cell> <out dir>
#
# Prints note lines; writes <out dir>/point.txt (the load's result line) and <out dir>/point.log.
# Exits 1 with the failure as its last line when the load gave no result, or when under speed-dc a
# point lookup on srv1 was merged or waited for peer-stream admission: with the default
# distributedCompletePointLookups=local they run on srv1 alone. This check is deterministic; the
# q/s of the speed and speed-dc cells (different runners) are compared in the workflow summary.
set -u
py=$1 keys=$2 cell=$3 out=$4
seconds=${POINT_SECONDS:-30}
l0=$(docker logs srv1 2>&1)
waits0=$(grep -c 'distributedComplete=admissionWait' <<< "$l0")
merged0=$(grep -c 'distributedComplete=merged ' <<< "$l0")
"$py" -I tools/ci/point-query-load.py 172.30.0.11:32010 4 5 "$keys" > /dev/null 2>&1 || true # warm-up
point=$("$py" -I tools/ci/point-query-load.py 172.30.0.11:32010 32 "$seconds" "$keys" 2> "$out/point.log"); prc=$?
echo "$point" > "$out/point.txt"
l=$(docker logs srv1 2>&1)
waits=$(( $(grep -c 'distributedComplete=admissionWait' <<< "$l") - waits0 ))
merged=$(( $(grep -c 'distributedComplete=merged ' <<< "$l") - merged0 ))
echo "point load c32 on srv1: ${point:-(no result: $(tail -n 1 "$out/point.log" | cut -c1-200))}"
echo "srv1 point load: admission waits $waits, merged $merged, merged on probation" \
    "$(grep -c 'distributedComplete=merged .*probation=true' <<< "$l"), probation over" \
    "$(grep -c 'distributedComplete=probationOver' <<< "$l")"
if [ "$prc" -ne 0 ] || [ -z "$point" ]; then
    echo "point load c32: ${point:-no result} $(tail -n 1 "$out/point.log" | cut -c1-150)"
    exit 1
fi
if [ "$cell" = speed-dc ] && { [ "$waits" -ne 0 ] || [ "$merged" -ne 0 ]; }; then
    echo "point load under dc: $merged merged, $waits admission waits on srv1 (expected 0: point lookups run locally)"
    exit 1
fi
