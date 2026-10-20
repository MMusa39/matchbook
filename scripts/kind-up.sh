#!/usr/bin/env bash
# Builds the images, creates a local kind cluster, installs the Helm chart and runs a smoke test.
# Requires: docker, kind, kubectl, helm.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=matchbook

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/kind-config.yaml
fi

docker compose -f deploy/compose.yaml build engine gateway marketdata posttrade
for svc in engine gateway marketdata posttrade; do
  kind load docker-image --name "$CLUSTER" "matchbook/$svc:local"
done

helm upgrade --install matchbook deploy/helm/matchbook --wait --timeout 10m
kubectl get pods -o wide

kubectl port-forward svc/matchbook-gateway 18080:8080 >/tmp/matchbook-pf.log 2>&1 &
PF=$!
trap 'kill $PF' EXIT
sleep 3
echo "--- smoke test: resting sell, crossing buy"
curl -sf -X POST localhost:18080/api/v1/orders -H 'X-API-Key: local-dev-key-1' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"kind-smoke-1","symbol":"AAPL","side":"SELL","type":"LIMIT","price":"250.00","quantity":"10"}'
echo
curl -sf -X POST localhost:18080/api/v1/orders -H 'X-API-Key: local-dev-key-2' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"kind-smoke-2","symbol":"AAPL","side":"BUY","type":"LIMIT","price":"250.00","quantity":"10"}'
echo
