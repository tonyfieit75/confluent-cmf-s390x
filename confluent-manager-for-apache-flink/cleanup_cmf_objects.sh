#!/usr/bin/env bash
# cleanup_cmf_objects.sh
# Remove CMF resources created by validate_cmf_e2e.sh (keep the OpenShift project).
# Default targets:
#   - Environment:  dev-ocp
#   - ComputePool:  pool-small
#   - Applications: all in that Environment
#   - Statements :  all in that Environment
#
# Optional: --purge-shared will also delete cluster-scoped Catalogs (Kafka) and Secrets.

set -euo pipefail

# ---------- Defaults (override via env or flags) ----------
NS="${NS:-confluent-platform}"
ENV_NAME="${ENV_NAME:-dev-ocp}"
POOL_NAME="${POOL_NAME:-pool-small}"

PURGE_SHARED=0     # also delete /catalogs/* and /secrets/*
NONINTERACTIVE=0   # skip prompt

# ---------- Helpers ----------
red=$'\033[1;31m'; grn=$'\033[1;32m'; yel=$'\033[1;33m'; blu=$'\033[1;34m'; clr=$'\033[0m'
info(){ echo -e "${blu}[INFO]${clr} $*"; }
ok(){   echo -e "${grn}[OK]${clr}   $*"; }
warn(){ echo -e "${yel}[WARN]${clr} $*"; }
err(){  echo -e "${red}[ERR]${clr}  $*" >&2; }

need(){ command -v "$1" >/dev/null || { err "Missing '$1' in PATH"; exit 1; } }

usage() {
  cat <<EOF
Usage: $0 [--namespace NS] [--env ENV] [--pool POOL] [--purge-shared] [--yes]

  --namespace NS   OpenShift namespace (default: ${NS})
  --env ENV        CMF Environment name (default: ${ENV_NAME})
  --pool POOL      ComputePool name inside the ENV (default: ${POOL_NAME})
  --purge-shared   ALSO delete CMF cluster-scoped objects (catalogs, secrets)
  --yes            Non-interactive (no confirmation prompt)

Examples:
  $0
  NS=my-ns ENV_NAME=dev-ocp $0 --purge-shared --yes
EOF
}

# ---------- Args ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NS="$2"; shift 2 ;;
    --env)       ENV_NAME="$2"; shift 2 ;;
    --pool)      POOL_NAME="$2"; shift 2 ;;
    --purge-shared) PURGE_SHARED=1; shift ;;
    --yes|-y)    NONINTERACTIVE=1; shift ;;
    -h|--help)   usage; exit 0 ;;
    *) err "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

need oc; need jq; need curl

# ---------- Pre-checks ----------
if ! oc get ns "${NS}" >/dev/null 2>&1; then
  err "Namespace '${NS}' does not exist."
  exit 1
fi

HOST="$(oc -n "${NS}" get route confluent-manager -o jsonpath='{.spec.host}' 2>/dev/null || true)"
if [[ -z "${HOST}" ]]; then
  err "Route 'confluent-manager' not found in namespace '${NS}'."
  exit 1
fi
BASE="https://${HOST}"
CURL=(curl -sk)
JQ=(jq -r)

if ! "${CURL[@]}" "${BASE}/v3/api-docs" >/dev/null 2>&1; then
  err "CMF API unreachable at ${BASE}."
  exit 1
fi

info "Target namespace : ${NS}"
info "CMF base URL     : ${BASE}"
info "Environment      : ${ENV_NAME}"
info "ComputePool      : ${POOL_NAME}"
if (( PURGE_SHARED )); then
  warn "Shared objects (catalogs & secrets) will also be deleted."
fi

if (( NONINTERACTIVE == 0 )); then
  read -r -p "Proceed with cleanup? [y/N] " ans
  [[ "${ans}" =~ ^[Yy]$ ]] || { warn "Aborted."; exit 0; }
fi

# ---------- Helpers for CMF DELETEs ----------
delete_if_exists() {
  # $1: method (DELETE)
  # $2: url
  local method="$1" url="$2" code
  code="$("${CURL[@]}" -w '%{http_code}' -X "${method}" "${url}" -o /dev/null 2>/dev/null || true)"
  case "${code}" in
    200|202|204) ok "Deleted: ${url}" ;;
    404)         info "Not found (already gone): ${url}" ;;
    *)           warn "HTTP ${code}: ${url}" ;;
  esac
}

resource_exists() {
  # $1 url -> returns 0 if exists
  "${CURL[@]}" "$1" | "${JQ[@]}" . >/dev/null 2>&1
}

# ---------- 1) Statements ----------
info "Deleting Statements in env '${ENV_NAME}'..."
if resource_exists "${BASE}/cmf/api/v1/environments/${ENV_NAME}/statements"; then
  mapfile -t stmts < <("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/statements" \
    | jq -r '.items[].metadata.name' 2>/dev/null || true)
  if (( ${#stmts[@]} > 0 )); then
    for s in "${stmts[@]}"; do
      delete_if_exists DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}/statements/${s}"
    done
  else
    info "No statements found."
  fi
else
  info "Statements endpoint not found (skipping)."
fi

# ---------- 2) Applications ----------
info "Deleting Applications in env '${ENV_NAME}'..."
if resource_exists "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications"; then
  mapfile -t apps < <("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications" \
    | jq -r '.items[].metadata.name' 2>/dev/null || true)
  if (( ${#apps[@]} > 0 )); then
    for a in "${apps[@]}"; do
      delete_if_exists DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}/applications/${a}"
    done
  else
    info "No applications found."
  fi
else
  info "Applications endpoint not found (skipping)."
fi

# ---------- 3) Compute Pools ----------
info "Deleting Compute Pools in env '${ENV_NAME}'..."
if resource_exists "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools"; then
  # Prefer explicit pool first (if present), then any others
  if resource_exists "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools/${POOL_NAME}"; then
    delete_if_exists DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools/${POOL_NAME}"
  fi
  mapfile -t pools < <("${CURL[@]}" "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools" \
    | jq -r '.items[].metadata.name' 2>/dev/null || true)
  for p in "${pools[@]:-}"; do
    [[ "$p" == "$POOL_NAME" ]] && continue
    delete_if_exists DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}/compute-pools/${p}"
  done
else
  info "Compute-pools endpoint not found (skipping)."
fi

# ---------- 4) Environment ----------
info "Deleting Environment '${ENV_NAME}'..."
delete_if_exists DELETE "${BASE}/cmf/api/v1/environments/${ENV_NAME}"

# ---------- Optional: Shared (cluster-scoped) objects ----------
if (( PURGE_SHARED )); then
  # Kafka Catalogs
  info "Deleting Kafka Catalogs..."
  if resource_exists "${BASE}/cmf/api/v1/catalogs/kafka"; then
    mapfile -t cats < <("${CURL[@]}" "${BASE}/cmf/api/v1/catalogs/kafka" \
      | jq -r '.items[].metadata.name' 2>/dev/null || true)
    for c in "${cats[@]:-}"; do
      delete_if_exists DELETE "${BASE}/cmf/api/v1/catalogs/kafka/${c}"
    done
  else
    info "Catalogs endpoint not found (skipping)."
  fi

  # Secrets
  info "Deleting CMF Secrets..."
  if resource_exists "${BASE}/cmf/api/v1/secrets"; then
    mapfile -t secs < <("${CURL[@]}" "${BASE}/cmf/api/v1/secrets" \
      | jq -r '.items[].metadata.name' 2>/dev/null || true)
    for s in "${secs[@]:-}"; do
      delete_if_exists DELETE "${BASE}/cmf/api/v1/secrets/${s}"
    done
  else
    info "Secrets endpoint not found (skipping)."
  fi
fi

ok "Cleanup complete. OpenShift namespace '${NS}' was left intact."

