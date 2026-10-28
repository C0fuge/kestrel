#!/usr/bin/env bash
# Creates the Iceberg REST conformance catalog in a fresh Apache Polaris (in-memory metastore,
# FILE storage allowed): catalog <catalog> with base location file://<warehouse dir>, and the
# root principal's role service_admin granted catalog_admin on it. Test-only credentials.
# Usage: polaris-setup.sh <polaris uri> <client id> <client secret> <catalog> <warehouse dir>
set -euo pipefail
uri=$1 id=$2 secret=$3 catalog=$4 dir=$5
for _ in $(seq 60); do
  # Up once it answers at all (401 without a token).
  status=$(curl -s -o /dev/null -w '%{http_code}' "$uri/api/catalog/v1/config" || true)
  [ "$status" != 000 ] && break
  sleep 2
done
token=$(curl -fsS "$uri/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d "client_id=$id" -d "client_secret=$secret" -d scope=PRINCIPAL_ROLE:ALL \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
auth=(-H "Authorization: Bearer $token" -H "Content-Type: application/json")
curl -fsS "${auth[@]}" -X POST "$uri/api/management/v1/catalogs" -d @- <<JSON
{"catalog": {"name": "$catalog", "type": "INTERNAL",
  "properties": {"default-base-location": "file://$dir"},
  "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file://$dir"]}}}
JSON
curl -fsS "${auth[@]}" -X PUT "$uri/api/management/v1/principal-roles/service_admin/catalog-roles/$catalog" \
  -d '{"catalogRole": {"name": "catalog_admin"}}'
echo "polaris catalog $catalog ready"
