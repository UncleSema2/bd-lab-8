#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${NAMESPACE:-lab8}"
APP_IMAGE="${APP_IMAGE:-lab8-app:latest}"
DATAMART_IMAGE="${DATAMART_IMAGE:-lab8-datamart:latest}"
DATAMART_UI_PORT="${DATAMART_UI_PORT:-8090}"
RESET_K8S_RESOURCES="${RESET_K8S_RESOURCES:-1}"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

cd "${ROOT_DIR}"

reset_k8s_resources() {
  if [[ "${RESET_K8S_RESOURCES}" != "1" ]]; then
    echo "Skipping Kubernetes resource cleanup (RESET_K8S_RESOURCES=${RESET_K8S_RESOURCES})."
    return 0
  fi
  echo "Cleaning previous resources in namespace ${NAMESPACE}..."
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found
  for _ in $(seq 1 120); do
    kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || break
    sleep 2
  done
}

wait_for_deployment() {
  local name="$1"
  local timeout="${2:-300s}"
  echo "Waiting for deployment/${name} to be ready..."
  kubectl rollout status "deployment/${name}" -n "${NAMESPACE}" --timeout="${timeout}"
}

run_job() {
  echo "Deleting old spark-train job if present..."
  kubectl delete -f k8s/spark-job.yaml --ignore-not-found -n "${NAMESPACE}" >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    kubectl get job spark-train -n "${NAMESPACE}" >/dev/null 2>&1 || break
    sleep 2
  done
  echo "Submitting Spark training job..."
  kubectl apply -f k8s/spark-job.yaml
}

wait_for_job() {
  echo "Waiting for spark-train job to complete..."
  kubectl wait --for=condition=complete job/spark-train -n "${NAMESPACE}" --timeout=600s || {
    echo "Job failed or timed out. Logs:"
    kubectl logs -n "${NAMESPACE}" -l app=spark-train --tail=50 2>/dev/null || true
    kubectl describe job spark-train -n "${NAMESPACE}"
    exit 1
  }
}

if ! minikube status >/dev/null 2>&1; then
  echo "Starting minikube..."
  minikube start --driver=docker --memory=8192 --cpus=4
fi

echo "Using minikube context..."
kubectl config use-context minikube >/dev/null

reset_k8s_resources

echo "Preparing images inside minikube Docker daemon..."
eval "$(minikube docker-env)"
export DOCKER_API_VERSION="${DOCKER_API_VERSION:-1.44}"
build_if_needed() {
  local tag="$1"; shift
  if [[ "${REBUILD_IMAGES:-0}" == "1" ]] || ! docker image inspect "${tag}" >/dev/null 2>&1; then
    echo "Building ${tag}..."
    docker build --network=host -t "${tag}" "$@"
  else
    echo "Image ${tag} already present (set REBUILD_IMAGES=1 to force rebuild)."
  fi
}
build_if_needed "${APP_IMAGE}" .
build_if_needed "${DATAMART_IMAGE}" -f Dockerfile.datamart .

echo ""
echo "=== Step 2: model service ==="
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/model-config.yaml

echo "submitting model service job"
run_job
echo "Model service pod is running in k8s:"
kubectl get pods -n "${NAMESPACE}" -l app=spark-train

echo ""
echo "=== Step 3: data source service ==="
kubectl apply -f k8s/mssql.yaml
kubectl apply -f k8s/datamart.yaml

wait_for_deployment datamart 300s

echo "Data source is ready. Resubmitting spark-train job..."
run_job

# Step 4: datamart storage + rolling updates
echo ""
echo "=== Step 4: datamart storage + rolling updates ==="
wait_for_deployment mssql 300s

echo "Rolling update: restarting datamart to reconnect with SQL Server..."
kubectl rollout restart deployment/datamart -n "${NAMESPACE}"
wait_for_deployment datamart 300s

echo "Resubmitting Spark training job with full stack..."
run_job
wait_for_job

echo ""
echo "Training completed. Logs:"
kubectl logs -n "${NAMESPACE}" -l app=spark-train --tail=30

echo ""
echo "=== Verifying results in SQL Server ==="
MSSQL_POD="$(kubectl get pod -n "${NAMESPACE}" -l app=mssql -o jsonpath='{.items[0].metadata.name}')"
kubectl exec -n "${NAMESPACE}" "${MSSQL_POD}" -- /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "QwErTy123@!" -No -Q "USE food; SELECT name FROM sys.tables; SELECT 'predictions' AS tbl, COUNT(*) AS rows FROM food_predictions; SELECT metric, value FROM food_metrics;" 2>&1 || {
    echo "SQL Server query failed — checking datamart logs:"
    kubectl logs -n "${NAMESPACE}" deploy/datamart --tail=50
}

while true; do sleep 60; done
