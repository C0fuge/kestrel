#!/usr/bin/env bash
# Starts Lakekeeper (with a PostgreSQL container, both on the host network; authentication off)
# and creates warehouse <warehouse> in bucket <bucket> under <key prefix> on an S3 endpoint
# without STS, with static keys (Lakekeeper vends none). Test-only credentials.
# Lakekeeper's metrics port moves to 9100: 9000 is the S3 endpoint.
# Usage: lakekeeper-setup.sh <image> <warehouse> <bucket> <key prefix> <s3 endpoint> <key> <secret>
set -euo pipefail
image=$1 warehouse=$2 bucket=$3 prefix=$4 endpoint=$5 key=$6 secret=$7
pg=$(openssl rand -hex 12)
docker run -d --name lakekeeper-pg --network host -e "POSTGRES_PASSWORD=$pg" \
  -e POSTGRES_DB=lakekeeper postgres:17-alpine
for _ in $(seq 60); do docker exec lakekeeper-pg pg_isready -U postgres -d lakekeeper && break; sleep 1; done
db="postgresql://postgres:$pg@localhost:5432/lakekeeper"
env=(-e "LAKEKEEPER__PG_DATABASE_URL_READ=$db" -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$db"
  -e "LAKEKEEPER__PG_ENCRYPTION_KEY=$(openssl rand -hex 16)" -e LAKEKEEPER__METRICS_PORT=9100)
docker run --rm --network host "${env[@]}" "$image" migrate
docker run -d --name catalog --network host "${env[@]}" "$image" serve
for _ in $(seq 60); do curl -fsS http://localhost:8181/health >/dev/null && break; sleep 2; done
curl -fsS -X POST http://localhost:8181/management/v1/bootstrap -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}'
curl -fsS -X POST http://localhost:8181/management/v1/warehouse -H 'Content-Type: application/json' -d @- <<JSON
{"warehouse-name": "$warehouse",
 "storage-profile": {"type": "s3", "bucket": "$bucket", "key-prefix": "$prefix", "region": "us-east-1",
   "endpoint": "$endpoint", "path-style-access": true, "flavor": "s3-compat", "sts-enabled": false},
 "storage-credential": {"type": "s3", "credential-type": "access-key",
   "aws-access-key-id": "$key", "aws-secret-access-key": "$secret"}}
JSON
echo
echo "lakekeeper warehouse $warehouse ready"
