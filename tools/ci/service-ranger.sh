#!/usr/bin/env bash
# Ranger cell of service-security.yml: Apache Ranger admin 2.7.0 (the first release with amd64
# images) on PostgreSQL (apache/ranger-db, preset), without Solr (only the audit UI needs it).
# The images hard-code the host names: the admin's install.properties use db_host=ranger-db, its
# create-ranger-services.py talks to http://ranger:6080 and creates the Hive service dev_hive.
#
# Usage: service-ranger.sh start
#          starts both containers (the admin needs ~2 min; run this early, set up later);
#        service-ranger.sh setup <group> <databases...>
#          waits for the admin and service dev_hive, creates <group> and, per database, a select
#          policy on every table and column, a MASK_SHOW_LAST_4 mask on customer.c_phone and a row
#          filter o_orderstatus = 'F' on orders - all for <group> - and tags tpch_avro.nation with
#          EXPIRES_ON (expired) for the default tag policy of the Hive service's tag service.
# The admin listens on 127.0.0.1:6080 only. Its password is the image default (a throwaway
# container that never leaves the runner); it is masked in the job log.
set -euo pipefail

version=2.7.0
url=http://127.0.0.1:6080
password='rangerR0cks!'
echo "::add-mask::$password"

case "${1:-}" in
  start)
    docker network create ranger-svc > /dev/null
    docker run -d --name ranger-db --hostname ranger-db --network ranger-svc "apache/ranger-db:$version" > /dev/null
    docker run -d --name ranger --hostname ranger --network ranger-svc -p 127.0.0.1:6080:6080 \
      -e RANGER_VERSION="$version" -e RANGER_DB_TYPE=postgres "apache/ranger:$version" > /dev/null
    echo "ranger containers started"
    ;;
  setup)
    group=$2
    shift 2
    api() {
      local method=$1 path=$2 body=${3:-}
      curl -sS --fail-with-body -u "admin:$password" -H 'Content-Type: application/json' \
        -H 'Accept: application/json' -X "$method" "$url$path" ${body:+--data "$body"}
    }
    for _ in $(seq 150); do
      api GET /service/public/v2/api/service/name/dev_hive > /dev/null 2>&1 && break
      sleep 2
    done
    if ! api GET /service/public/v2/api/service/name/dev_hive > /dev/null; then
      echo "Ranger admin or service dev_hive not up:"
      docker logs ranger 2>&1 | tail -60
      exit 1
    fi
    echo "ranger admin up, service dev_hive present"
    # The image links dev_hive to tag service dev_tag, whose default policy EXPIRES_ON is a deny
    # (condition accessed-after-expiry) - Ranger creates it in every tag service. ranger-hive denies
    # only tables whose database, table or columns carry a tag of a restrictive tag policy, so the
    # policy stays; tag EXPIRES_ON with a past expiry_date goes on tpch_avro.nation (refused) and
    # tpch_avro.region stays untagged (read).
    tag=$(api GET /service/public/v2/api/service/name/dev_hive | jq -r '.tagService // ""')
    if [ -z "$tag" ]; then
      echo "dev_hive has no tag service; the tag checks need one"
      exit 1
    fi
    echo "dev_hive's tag service: $tag; its policies:"
    api GET "/service/public/v2/api/service/$tag/policy" \
      | jq -c '.[] | {id, name, resources, deny: (.denyPolicyItems | length),
          conditions: [.denyPolicyItems[]?.conditions[]?.type]}'
    # A tag, a resource and their map, as tag-sync would create them (each call prints Ranger's
    # error on failure).
    tagapi() {
      local out
      if ! out=$(api "$@"); then echo "Ranger tag REST $1 $2 failed: $out"; exit 1; fi
    }
    tagapi POST /service/tags/tags/ '{"guid":"svc-tag-expired","type":"EXPIRES_ON",
      "attributes":{"expiry_date":"2020/01/01"}}'
    tagapi POST /service/tags/resources/ '{"guid":"svc-res-avro-nation","serviceName":"dev_hive",
      "resourceElements":{"database":{"values":["tpch_avro"]},"table":{"values":["nation"]}}}'
    tagapi POST "/service/tags/tagresourcemaps/?tag-guid=svc-tag-expired&resource-guid=svc-res-avro-nation"
    # Ranger admin serves the new tags once its tag cache has caught up: wait for them (the server's
    # plugin downloads them at start).
    tags_file="${TMPDIR:-/tmp}/service-tags.json"
    for _ in $(seq 30); do
      api GET "/service/tags/download/dev_hive?lastKnownVersion=-1&pluginId=service-matrix" > "$tags_file" || true
      jq -e '[.tags[]? | select(.type == "EXPIRES_ON")] | length > 0' "$tags_file" > /dev/null 2>&1 && break
      sleep 2
    done
    jq -c '{tagVersion, tags: [.tags[]? | {type, attributes}],
        resources: [.serviceResources[]? | .resourceElements | map_values(.values)]}' "$tags_file" || cat "$tags_file"
    if ! jq -e '[.tags[]? | select(.type == "EXPIRES_ON")] | length > 0' "$tags_file" > /dev/null 2>&1; then
      echo "tag EXPIRES_ON not served for dev_hive"
      exit 1
    fi
    api POST /service/xusers/groups "{\"name\":\"$group\",\"description\":\"service matrix\"}" > /dev/null
    select_item="{\"groups\":[\"$group\"],\"accesses\":[{\"type\":\"select\",\"isAllowed\":true}]"
    for db in "$@"; do
      api POST /service/public/v2/api/policy "{\"service\":\"dev_hive\",\"name\":\"svc $db read\",
        \"policyType\":0,\"isEnabled\":true,\"resources\":{\"database\":{\"values\":[\"$db\"]},
        \"table\":{\"values\":[\"*\"]},\"column\":{\"values\":[\"*\"]}},\"policyItems\":[$select_item}]}" > /dev/null
      api POST /service/public/v2/api/policy "{\"service\":\"dev_hive\",\"name\":\"svc $db phone mask\",
        \"policyType\":1,\"isEnabled\":true,\"resources\":{\"database\":{\"values\":[\"$db\"]},
        \"table\":{\"values\":[\"customer\"]},\"column\":{\"values\":[\"c_phone\"]}},
        \"dataMaskPolicyItems\":[$select_item,\"dataMaskInfo\":{\"dataMaskType\":\"MASK_SHOW_LAST_4\"}}]}" > /dev/null
      api POST /service/public/v2/api/policy "{\"service\":\"dev_hive\",\"name\":\"svc $db orders filter\",
        \"policyType\":2,\"isEnabled\":true,\"resources\":{\"database\":{\"values\":[\"$db\"]},
        \"table\":{\"values\":[\"orders\"]}},
        \"rowFilterPolicyItems\":[$select_item,\"rowFilterInfo\":{\"filterExpr\":\"o_orderstatus = 'F'\"}}]}" > /dev/null
      echo "policies for $db created"
    done
    # What the server's plugin will download (no credentials needed): policy count and types.
    curl -sS --fail "$url/service/plugins/policies/download/dev_hive?lastKnownVersion=-1&pluginId=service-matrix" \
      | jq -c '{policyVersion, policies: [.policies[] | {name, policyType}],
          tagPolicies: [.tagPolicies.policies[]? | .name]}'
    ;;
  *)
    echo "usage: $0 start | setup <group> <databases...>" >&2
    exit 2
    ;;
esac
