#!/usr/bin/env bash
# Creates the Iceberg REST conformance catalog in a fresh Apache Polaris (in-memory metastore):
# catalog <catalog> with base location <warehouse> (a local directory, FILE storage allowed, or
# s3://bucket/path on an S3 endpoint without STS: Polaris reads and writes with its own AWS_*
# credentials and vends none), and the root principal's role service_admin granted
# catalog_admin on it. Test-only credentials.
# Usage: polaris-setup.sh <polaris uri> <client id> <client secret> <catalog> <warehouse> [s3 endpoint]
set -euo pipefail
uri=$1 id=$2 secret=$3 catalog=$4 warehouse=$5 endpoint=${6:-}
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
case "$warehouse" in
  s3://*)
    base=$warehouse
    storage="{\"storageType\": \"S3\", \"allowedLocations\": [\"$warehouse\"],
      \"roleArn\": \"arn:aws:iam::123456789012:role/conformance\", \"region\": \"us-east-1\",
      \"endpoint\": \"$endpoint\", \"pathStyleAccess\": true, \"stsUnavailable\": true}"
    ;;
  *)
    base="file://$warehouse"
    storage="{\"storageType\": \"FILE\", \"allowedLocations\": [\"$base\"]}"
    ;;
esac
curl -fsS "${auth[@]}" -X POST "$uri/api/management/v1/catalogs" -d @- <<JSON
{"catalog": {"name": "$catalog", "type": "INTERNAL",
  "properties": {"default-base-location": "$base"},
  "storageConfigInfo": $storage}}
JSON
curl -fsS "${auth[@]}" -X PUT "$uri/api/management/v1/principal-roles/service_admin/catalog-roles/$catalog" \
  -d '{"catalogRole": {"name": "catalog_admin"}}'
echo "polaris catalog $catalog ready"
