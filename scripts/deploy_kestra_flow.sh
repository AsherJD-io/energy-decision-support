#!/usr/bin/env bash
set -euo pipefail

FLOW_FILE="${1:-orchestration/kestra/energy_dss_pipeline.yml}"
KESTRA_URL="${KESTRA_URL:-http://localhost:8087}"
KESTRA_TENANT="${KESTRA_TENANT:-main}"
KESTRA_USER="${KESTRA_USER:-admin@energy.local}"
KESTRA_PASSWORD="${KESTRA_PASSWORD:-Trailda7A}"

if [ ! -f "$FLOW_FILE" ]; then
  echo "Flow file not found: $FLOW_FILE" >&2
  exit 1
fi

FLOW_ID=$(awk '/^id:/ {print $2; exit}' "$FLOW_FILE")
NAMESPACE=$(awk '/^namespace:/ {print $2; exit}' "$FLOW_FILE")

if [ -z "${FLOW_ID}" ] || [ -z "${NAMESPACE}" ]; then
  echo "Could not parse id or namespace from ${FLOW_FILE}" >&2
  exit 1
fi

HTTP_CODE=$(
  curl --silent --show-error \
    -u "${KESTRA_USER}:${KESTRA_PASSWORD}" \
    -X PUT \
    "${KESTRA_URL}/api/v1/${KESTRA_TENANT}/flows/${NAMESPACE}/${FLOW_ID}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @"${FLOW_FILE}" \
    --output /tmp/kestra_deploy_response.json \
    --write-out "%{http_code}"
)

cat /tmp/kestra_deploy_response.json
echo
echo "HTTP_CODE=${HTTP_CODE}"

if [ "${HTTP_CODE}" -lt 200 ] || [ "${HTTP_CODE}" -ge 300 ]; then
  exit 1
fi

echo "Updated flow ${NAMESPACE}.${FLOW_ID} from ${FLOW_FILE} to ${KESTRA_URL}"
