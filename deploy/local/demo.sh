#!/usr/bin/env bash
# Local Kubernetes demo of kafkafim (deploy/local/README.md).
#
#   deploy/local/demo.sh up      build the image, create the cluster, deploy Kafka,
#                                Elasticsearch and the kafkafim chart
#   deploy/local/demo.sh smoke   send one record to demo.events and wait for it in Elasticsearch
#   deploy/local/demo.sh down    delete the kind cluster (Docker Desktop: the namespace)
#
# CLUSTER=kind (default) or CLUSTER=docker-desktop. SKIP_BUILD=1 reuses kafkafim:dev.
set -euo pipefail

CLUSTER="${CLUSTER:-kind}"
KIND_NAME="${KIND_NAME:-kafkafim-demo}"
NS=kafkafim-demo
IMAGE=kafkafim:dev
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

# A random password, created once per Secret; later runs keep the existing one.
ensure_secret() {
  local name="$1" username="$2"
  if ! k get secret "$name" >/dev/null 2>&1; then
    k create secret generic "$name" \
      --from-literal=username="$username" \
      --from-literal=password="$(openssl rand -hex 16)"
  fi
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

  kubectl --context "$CONTEXT" create namespace "$NS" --dry-run=client -o yaml \
    | kubectl --context "$CONTEXT" apply -f -
  ensure_secret kafka-demo-kafkafim kafkafim
  ensure_secret es-demo-kafkafim elastic

  k apply -f "$HERE/backends.yaml"
  k rollout status deploy/kafka --timeout=180s
  k rollout status deploy/elasticsearch --timeout=300s

  # kafkafim never creates topics (ADR-0021); this stands in for the team's provisioning.
  for topic in demo.events demo.events.kafkafim-dlq; do
    k exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:19092 \
      --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
  done

  helm --kube-context "$CONTEXT" upgrade --install kafkafim "$ROOT/charts/kafkafim" \
    -n "$NS" -f "$HERE/values-demo.yaml" --wait --timeout 5m
  k get pods
}

smoke() {
  need kubectl
  local id record now
  id="smoke-$(date +%s)"
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  record="{\"id\":\"$id\",\"region\":\"eu\",\"@timestamp\":\"$now\"}"
  echo "$record" | k exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh \
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
    helm --kube-context "$CONTEXT" uninstall kafkafim -n "$NS" --ignore-not-found
    kubectl --context "$CONTEXT" delete namespace "$NS" --ignore-not-found
  fi
}

case "${1:-}" in
  up) up ;;
  smoke) smoke ;;
  down) down ;;
  *) echo "usage: $0 up|smoke|down" >&2; exit 2 ;;
esac
