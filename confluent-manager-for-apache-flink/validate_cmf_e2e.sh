#!/usr/bin/env bash
# validate_cmf_e2e.sh
# E2E validation of Confluent Manager for Apache Flink (CMF) on OpenShift (s390x).
# Creates/ensures: Environment, ComputePool, FlinkApplication (SQL datagen->print).
# Idempotent; safe to re-run. Does NOT create/delete the OpenShift project.

set -euo pipefail

# ------------- Tunables (override via env) -------------
NS="${NS:-confluent-platform}"
ENV_NAME="${ENV_NAME:-dev-ocp}"
POOL_NAME="${POOL_NAME:-pool-small}"
APP_NAME="${APP_NAME:-app-datagen-print}"

# Flink image & SA to run jobs (must be available/allowed by your cluster)
APP_IMG="${APP_IMG:-quay.io/tonyfieit75/flink:1.17-s390x-rocksdb}"
FLINK_SA="${FLINK_SA:-flink}"     # set to "default" if no dedicated SA
POLL_SECS="${POLL_SECS:-120}"     # total wait for app status

# ------------- Helpers -------------
red=$'\033[1;31m'; grn=$'\033[1;32m'; yel=$'\033[1;33m'; blu=$'\033[1;34m'; clr=$'\033[0m'
info(){ echo -e "${blu}[INFO]${clr} $*"; }
warn(){ echo -e "${yel}[WARN]${clr} $*"; }
ok(){   echo -e "${grn}[OK]${clr}   $*"; }
err(){  echo -e "${red}[ERR]${clr}  $*" >&2; }

need(){ command -v "$1" >/dev/null || { err "Missing '$1'"; exit 1; } }

need oc; need jq; need curl

# oc namespace exists?
if ! oc get ns "$NS" >/dev/null 2>&1; then
  err "Namespace '$NS' not found. Create it first: oc new-project $NS"
  exit 1
fi

info "Using namespace: ${NS} (context: $(oc config current-context 2>/dev/null || echo unknown))"

# Resolve Route -> HOST
HOST="$(oc -n "$NS" get route confluent-manager -o jsonpath='{.spec.host}' 2>/dev/null || true)"
if [[ -z "${HOST}" ]]; then
  err "Route 'confluent-manager' not found in ns '$NS'. Install CMF chart first."
  exit 1
fi
BASE="https://${HOST}"
CURL=(curl -sk)
JQ=(jq -r)

info "CMF OpenAPI exists? (${BASE}/v3/api-docs)"
if ! "${CURL[@]}" "${BASE}/v3/api-docs" >/dev/null 2>&1; then
  err "Cannot reach CMF at ${BASE}. Is the Deployment ready and Service/Route correct?"
  exit 1
fi

# ------------- Step 1: Environment -------------
info "[STEP 1] Ensure Environment '${ENV_NAME}' mapped to namespace '${NS}' ..."
# Try create; if error says exists, we continue.
create_env_body=$(cat <<JSON
{
  "name": "${ENV_NAME}",
  "kubernetesNamespace": "${NS}"
}
JSON
)
env_post="$("${CURL[@]}" -X POST "${BASE}/cmf/api/v1/environments" \
  -H 'Content-Type: application/json' \
  --data-binary @<(printf "%s" "${create_env_body}") 2>/dev/null || true)"

# If environment already exists, POST returns an error; ignore and list
"${CURL[@]}" "${BASE}/cmf/api/v1/environments" | jq -r '.items[].name' | grep -qx "${ENV_NAME}" \
  && ok "Environment present: ${ENV_NAME}" \
  || err "Failed to ensure environment (response: ${env_post})"

# ------------- Step 2: ComputePool (DEDICATED) -------------
info "[STEP 2] Ensure ComputePool '${POOL_NAME}' (DEDICATED)..."

po_body=$(cat <<JSON
{
  "apiVersion": "cmf.confluent.io/v1",
  "kind": "ComputePool",
  "metadata": { "name": "${POOL_NAME}" },
  "spec": {
    "type": "DEDICATED",
    "clusterSpec": {
      "jobManager":  { "replicas": 1 },
      "taskManager": { "replicas": 1 }
    }
  }
}
JSON
)

# Create if missing
if ! "${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools/${POOL_NAME}" \
     | jq -e . >/dev/null 2>&1; then
  "${CURL[@]}" -X POST "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools" \
    -H 'Content-Type: application/json' \
    --data-binary @<(printf "%s" "${po_body}") | jq . || true
fi

# Show phase (many CMF builds report "DEDICATED" as the phase value)
POOL_PHASE="$("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools/${POOL_NAME}" \
  | jq -r '.status.phase // empty' 2>/dev/null || true)"
[[ -n "$POOL_PHASE" ]] && info "ComputePool phase: ${POOL_PHASE}" || warn "Pool phase not reported by this build."

# ------------- Step 3: FlinkApplication (SQL datagen -> print, no pool ref) -------------
info "[STEP 3] Submitting FlinkApplication '${APP_NAME}' (datagen -> print) ..."

# Delete previous app (ignore error)
"${CURL[@]}" -X DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications/${APP_NAME}" >/dev/null 2>&1 || true

SQL_ONE_LINE="CREATE TABLE src (id BIGINT, name STRING) WITH ('connector'='datagen','rows-per-second'='3','fields.id.kind'='sequence','fields.id.start'='1','fields.name.length'='8'); CREATE TABLE sink (id BIGINT, name STRING) WITH ('connector'='print'); INSERT INTO sink SELECT id, name FROM src;"

post_app_variant() {
  local flink_ver="$1" sql_key="$2"
  local body status out
  body=$(cat <<JSON
{
  "apiVersion": "cmf.confluent.io/v1",
  "kind": "FlinkApplication",
  "metadata": { "name": "${APP_NAME}" },
  "spec": {
    "image": "${APP_IMG}",
    "flinkVersion": "${flink_ver}",
    "serviceAccount": "${FLINK_SA}",
    "flinkConfiguration": {
      "execution.checkpointing.interval": "15 s"
    },
    "taskManager": {
      "numberOfTaskSlots": 1,
      "resource": { "cpu": 1, "memory": "1024m" }
    },
    "jobManager": {
      "resource": { "cpu": 1, "memory": "1024m" }
    },
    "job": {
      "state": "running",
      "parallelism": 1,
      "upgradeMode": "stateless",
      "${sql_key}": $(jq -Rn --arg s "${SQL_ONE_LINE}" '$s')
    }
  }
}
JSON
)
  out="$("${CURL[@]}" -w '\n%{http_code}' -X POST \
          -H 'Content-Type: application/json' \
          --data-binary @<(printf "%s" "${body}") \
          "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications" 2>/dev/null)"
  status="$(echo "${out}" | tail -n1)"
  echo "${out}" | sed '$d' | jq . || true
  [[ "${status}" == "200" || "${status}" == "201" ]]
}

POST_OK=0
for ver in v1_19 v1_18 v1_17; do
  for sqlk in sqlScript sql; do
    info "Trying flinkVersion=${ver}, job.${sqlk}…"
    if post_app_variant "${ver}" "${sqlk}"; then
      ok "Application POST accepted with flinkVersion=${ver}, job.${sqlk}"
      POST_OK=1
      break 2
    fi
  done
done

if [[ "${POST_OK}" -ne 1 ]]; then
  err "FlinkApplication POST failed across tried variants. Inspect ${BASE}/v3/api-docs (FlinkApplicationSpec)."
  exit 1
fi

# ------------- Poll application status -------------
info "[STEP 4] Waiting for application to reach DEPLOYED/RUNNING (up to ${POLL_SECS}s)…"
deadline=$((SECONDS + POLL_SECS))
PHASE=""
while (( SECONDS < deadline )); do
  PHASE="$("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications/${APP_NAME}" \
    | jq -r '.status.phase // empty' 2>/dev/null || true)"
  DETAIL="$("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications/${APP_NAME}" \
    | jq -r '.status.detail // empty' 2>/dev/null || true)"
  [[ -n "${PHASE}" ]] && echo "  phase=${PHASE}${DETAIL:+  detail=${DETAIL}}"
  if [[ "${PHASE}" == "DEPLOYED" || "${PHASE}" == "RUNNING" ]]; then
    ok "Application is ${PHASE}"
    break
  fi
  if [[ "${PHASE}" == "FAILED" ]]; then
    err "Application FAILED. Detail: ${DETAIL}"
    exit 1
  fi
  sleep 3
done
[[ "${PHASE}" == "DEPLOYED" || "${PHASE}" == "RUNNING" ]] || warn "App not ready before timeout; current phase=${PHASE:-unknown}"

# ------------- Step 5: quick service/route checks -------------
info "[STEP 5] K8s service/endpoints sanity…"
oc -n "$NS" get deploy,svc -l app.kubernetes.io/name=confluent-manager-for-apache-flink || true
oc -n "$NS" get svc cmf-service -o wide 2>/dev/null || true
oc -n "$NS" get endpoints cmf-service -o yaml 2>/dev/null | sed -n '1,80p' || true

# ------------- Summary -------------
echo
ok "CMF E2E validation completed."
echo "Useful URLs:"
echo "  Route     : ${BASE}/"
echo "  Swagger UI: ${BASE}/swagger-ui/index.html"
echo "  OpenAPI   : ${BASE}/v3/api-docs"

