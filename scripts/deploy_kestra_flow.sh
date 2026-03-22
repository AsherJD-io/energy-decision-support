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

if [ -z "${ENTSOE_API_TOKEN:-}" ]; then
  echo "ENTSOE_API_TOKEN is not set in the shell environment" >&2
  exit 1
fi

FLOW_ID=$(awk '/^id:/ {print $2; exit}' "$FLOW_FILE")
NAMESPACE=$(awk '/^namespace:/ {print $2; exit}' "$FLOW_FILE")

if [ -z "${FLOW_ID}" ] || [ -z "${NAMESPACE}" ]; then
  echo "Could not parse id or namespace from ${FLOW_FILE}" >&2
  exit 1
fi

python3 - <<'PY' "$FLOW_FILE" /tmp/kestra_flow_rendered.yml
import os
import sys
from pathlib import Path

src = Path(sys.argv[1]).read_text()
token = os.environ["ENTSOE_API_TOKEN"]
rendered = src.replace("${ENTSOE_API_TOKEN}", token)
Path(sys.argv[2]).write_text(rendered)
PY

STATUS_CODE=$(
  curl --max-time 15 --silent --output /tmp/kestra_flow_check.json --write-out "%{http_code}" \
    -u "${KESTRA_USER}:${KESTRA_PASSWORD}" \
    "${KESTRA_URL}/api/v1/${KESTRA_TENANT}/flows/${NAMESPACE}/${FLOW_ID}"
)

if [ "${STATUS_CODE}" = "200" ]; then
  METHOD="PUT"
  URL="${KESTRA_URL}/api/v1/${KESTRA_TENANT}/flows/${NAMESPACE}/${FLOW_ID}"
else
  METHOD="POST"
  URL="${KESTRA_URL}/api/v1/${KESTRA_TENANT}/flows"
fi

HTTP_CODE=$(
  curl --max-time 30 --silent --show-error \
    -u "${KESTRA_USER}:${KESTRA_PASSWORD}" \
    -X "${METHOD}" \
    "${URL}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @/tmp/kestra_flow_rendered.yml \
    --output /tmp/kestra_deploy_response.json \
    --write-out "%{http_code}"
)

cat /tmp/kestra_deploy_response.json
echo
echo "HTTP_CODE=${HTTP_CODE}"
echo "METHOD=${METHOD}"
echo "FLOW=${NAMESPACE}.${FLOW_ID}"

if [ "${HTTP_CODE}" -lt 200 ] || [ "${HTTP_CODE}" -ge 300 ]; then
  exit 1
fi

echo "Deployed flow from ${FLOW_FILE} to ${KESTRA_URL}"
