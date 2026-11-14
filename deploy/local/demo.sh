#!/usr/bin/env bash
# Local Kubernetes demo of kafkafim (deploy/local/README.md).
#
#   deploy/local/demo.sh up      build the image, create the cluster, then one `helm install`
#                                of deploy/local/kafkafim-demo (Kafka, Elasticsearch, Grafana
#                                LGTM and kafkafim)
#   deploy/local/demo.sh smoke   send one record to demo.events and wait for it in Elasticsearch
#   deploy/local/demo.sh down    delete the kind cluster (Docker Desktop: the namespace)
#
# CLUSTER=kind (default) or CLUSTER=docker-desktop. SKIP_BUILD=1 reuses kafkafim:dev.
set -euo pipefail

CLUSTER="${CLUSTER:-kind}"
KIND_NAME="${KIND_NAME:-kafkafim-demo}"
NS=kafkafim-demo
IMAGE=kafkafim:dev
RELEASE=kafkafim
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

case "$CLUSTER" in
  kind) CONTEXT="kind-$KIND_NAME" ;;
  docker-desktop) CONTEXT=docker-desktop ;;
  *) echo "CLUSTER must be kind or docker-desktop, not '$CLUSTER'" >&2; exit 2 ;;
esac
k() { kubectl --context "$CONTEXT" -n "$NS" "$@"; }

need() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null || { echo "missing tool: $tool (see deploy/local/README.md)" >&2; exit 2; }
  done
}

up() {
  need docker kubectl helm
  if [[ "${SKIP_BUILD:-}" != 1 ]]; then
    (cd "$ROOT" && ./mvnw -B -ntp -q -DskipTests package && docker build -t "$IMAGE" .)
  fi

  if [[ "$CLUSTER" == kind ]]; then
    need kind
    kind get clusters 2>/dev/null | grep -x "$KIND_NAME" >/dev/null || kind create cluster --name "$KIND_NAME" --wait 120s
    kind load docker-image "$IMAGE" --name "$KIND_NAME"
  fi
  # Docker Desktop (kubeadm mode) runs on the same Docker engine, so it sees kafkafim:dev.

  # The whole demo is this one install: Kafka, Elasticsearch, Grafana LGTM and kafkafim.
  helm --kube-context "$CONTEXT" upgrade --install "$RELEASE" "$HERE/kafkafim-demo" \
    -n "$NS" --create-namespace --wait --timeout 10m
  k get pods
}

smoke() {
  need kubectl
  local id record now
  id="smoke-$(date +%s)"
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  record="{\"id\":\"$id\",\"region\":\"eu\",\"@timestamp\":\"$now\"}"
  echo "$record" | k exec -i deploy/kafka -c kafka -- /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server 127.0.0.1:19092 --topic demo.events
  echo "sent $record"
  for _ in $(seq 1 30); do
    local hits
    hits="$(k exec deploy/elasticsearch -- sh -c \
      "curl -fsS -u \"elastic:\$ELASTIC_PASSWORD\" 'http://127.0.0.1:9200/demo-eu-*/_count?q=_id:$id'" \
      2>/dev/null | sed -n 's/.*"count":\([0-9]*\).*/\1/p')" || hits=0   # retry on any hiccup
    if [[ "${hits:-0}" -ge 1 ]]; then
      echo "OK: $id is in Elasticsearch (demo-eu-*)"
      return 0
    fi
    sleep 2
  done
  echo "FAIL: $id did not reach Elasticsearch within 60s; see: kubectl --context $CONTEXT -n $NS logs deploy/kafkafim-demo" >&2
  return 1
}

down() {
  if [[ "$CLUSTER" == kind ]]; then
    need kind
    kind delete cluster --name "$KIND_NAME"
  else
    need kubectl helm
    helm --kube-context "$CONTEXT" uninstall "$RELEASE" -n "$NS" --ignore-not-found
    kubectl --context "$CONTEXT" delete namespace "$NS" --ignore-not-found
  fi
}

case "${1:-}" in
  up) up ;;
  smoke) smoke ;;
  down) down ;;
  *) echo "usage: $0 up|smoke|down" >&2; exit 2 ;;
esac
