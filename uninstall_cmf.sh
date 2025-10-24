#!/usr/bin/env bash
# cleanup_cmf.sh
# Fully remove Confluent Manager for Apache Flink (CMF) from OpenShift.
#
# What it does:
# - Uninstalls Helm releases (you can specify multiple).
# - Deletes namespaced resources (+ Routes) by Helm labels and common app labels.
# - Deletes PVCs; optionally deletes bound PVs.
# - Optionally deletes cluster-scoped leftovers (ClusterRole/Binding).
# - Deletes the entire project/namespace at the end.
#
# Usage:
#   ./cleanup_cmf.sh [-n NAMESPACE] [--releases r1,r2] [--delete-pvs] [--purge-cluster-scope] [--force]
#
# Defaults:
#   NAMESPACE=confluent-platform
#   RELEASES="confluent-manager,cmf"     # common names seen earlier
#
# Examples:
#   ./cleanup_cmf.sh
#   ./cleanup_cmf.sh -n confluent-platform --force
#   ./cleanup_cmf.sh -n confluent-platform --delete-pvs --purge-cluster-scope --force
#
# Notes:
# - Requires: oc; helm optional (used if present).
# - Safe to re-run (idempotent).

set -euo pipefail

NAMESPACE="confluent-platform"
RELEASES_CSV="confluent-manager,cmf"
DELETE_PVS="false"
PURGE_CLUSTER_SCOPE="false"
FORCE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace) NAMESPACE="$2"; shift 2 ;;
    --releases)     RELEASES_CSV="$2"; shift 2 ;;
    --delete-pvs)   DELETE_PVS="true"; shift 1 ;;
    --purge-cluster-scope) PURGE_CLUSTER_SCOPE="true"; shift 1 ;;
    --force)        FORCE="true"; shift 1 ;;
    -h|--help)
      sed -n '1,120p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "[WARN] Unknown arg: $1"; shift ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] Missing: $1" >&2; exit 1; }; }
need oc
command -v helm >/dev/null 2>&1 || echo "[WARN] helm not found; will delete by labels only."

confirm() {
  if [[ "$FORCE" == "true" ]]; then return 0; fi
  read -r -p "$1 [y/N]: " ans
  [[ "${ans,,}" == "y" || "${ans,,}" == "yes" ]]
}

echo "[INFO] Namespace           : $NAMESPACE"
echo "[INFO] Candidate releases  : $RELEASES_CSV"
echo "[INFO] Delete PVs          : $DELETE_PVS"
echo "[INFO] Purge cluster-scope : $PURGE_CLUSTER_SCOPE"
echo "[INFO] Force               : $FORCE"

if ! oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Namespace '$NAMESPACE' does not exist. Nothing to clean in namespace."
fi

IFS=',' read -r -a RELEASES <<< "$RELEASES_CSV"

# Gather Helm releases that actually exist (if helm available)
EXISTING_REL=()
if command -v helm >/dev/null 2>&1 && oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  while IFS= read -r r; do
    [[ -n "$r" ]] && EXISTING_REL+=("$r")
  done < <(helm list -n "$NAMESPACE" -q | grep -E "^(($(printf '%s|' "${RELEASES[@]}" | sed 's/|$//')))$$" || true)
fi
echo "[INFO] Helm releases in '$NAMESPACE': ${EXISTING_REL[*]:-(none)}"

# Uninstall Helm releases
if [[ ${#EXISTING_REL[@]} -gt 0 ]]; then
  if confirm "[CONFIRM] Uninstall Helm releases (${EXISTING_REL[*]}) in namespace '$NAMESPACE'?"; then
    for r in "${EXISTING_REL[@]}"; do
      echo "[INFO] helm uninstall $r -n $NAMESPACE"
      helm uninstall "$r" -n "$NAMESPACE" || true
    done
  else
    echo "[INFO] Skipping helm uninstall."
  fi
fi

# Track PVCs before deletion to identify PVs later
PVC_NAMES_BEFORE=()
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  while IFS= read -r pvc; do PVC_NAMES_BEFORE+=("$pvc"); done < <(oc -n "$NAMESPACE" get pvc -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
fi

# Helper to delete by label selector
delete_labeled_ns() {
  local selector="$1"
  [[ -z "$selector" ]] && return 0
  echo "[INFO] Deleting namespaced resources with selector: $selector"
  oc -n "$NAMESPACE" delete all,cm,secret,sa,role,rolebinding,networkpolicy,pdb,hpa,ingress,route,jobs,cronjobs --selector "$selector" --ignore-not-found=true || true
  oc -n "$NAMESPACE" delete pvc --selector "$selector" --ignore-not-found=true || true
}

# Delete by Helm instance labels and common chart labels
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  for r in "${RELEASES[@]}"; do
    delete_labeled_ns "app.kubernetes.io/instance=${r}"
  done
  delete_labeled_ns "app=confluent-manager-for-apache-flink"
  delete_labeled_ns "app.kubernetes.io/name=confluent-manager-for-apache-flink"
fi

# Explicit best-effort deletes (names may vary in your chart)
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Deleting common CMF-named objects (best effort)"
  oc -n "$NAMESPACE" delete deploy,sts,svc,cm,secret,sa,role,rolebinding,pdb,hpa,ingress,route --ignore-not-found=true \
    confluent-manager-for-apache-flink || true

  # Known PVC used for SQLite if you created it
  oc -n "$NAMESPACE" delete pvc cmf-sqlite-pvc --ignore-not-found=true || true
fi

# Optionally delete bound PVs
get_bound_pvs() {
  local -a pvcs=("$@")
  local pv_list=()
  for pvc in "${pvcs[@]}"; do
    [[ -z "$pvc" ]] && continue
    local pv=$(oc get pv -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.claimRef.namespace}{" "}{.spec.claimRef.name}{"\n"}{end}' \
      | awk -v ns="$NAMESPACE" -v name="$pvc" '$2==ns && $3==name {print $1}')
    [[ -n "$pv" ]] && pv_list+=("$pv")
  done
  printf '%s\n' "${pv_list[@]}" | sort -u
}

PVS_TO_CONSIDER=()
if [[ ${#PVC_NAMES_BEFORE[@]} -gt 0 ]]; then
  while IFS= read -r pv; do PVS_TO_CONSIDER+=("$pv"); done < <(get_bound_pvs "${PVC_NAMES_BEFORE[@]}" || true)
fi

if [[ "$DELETE_PVS" == "true" && ${#PVS_TO_CONSIDER[@]} -gt 0 ]]; then
  if confirm "[CONFIRM] Delete bound PVs as well? (${PVS_TO_CONSIDER[*]})"; then
    for pv in "${PVS_TO_CONSIDER[@]}"; do
      echo "[INFO] Deleting PV $pv"
      oc delete pv "$pv" --ignore-not-found=true || true
    done
  else
    echo "[INFO] Skipping PV deletion."
  fi
fi

# Optionally purge cluster-scoped leftovers that commonly block helm installs
if [[ "$PURGE_CLUSTER_SCOPE" == "true" ]]; then
  echo "[INFO] Purging cluster-scoped leftovers (ClusterRole/Binding) that may carry wrong Helm ownership..."
  # Known names from the chart; extend if your chart creates other cluster objects
  CR_NAME="confluent-manager-for-apache-flink"

  # If any exist and are owned by a different release/namespace, delete them.
  for kind in clusterrole clusterrolebinding; do
    if oc get "$kind" "$CR_NAME" >/dev/null 2>&1; then
      rn=$(oc get "$kind" "$CR_NAME" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-name}' 2>/dev/null || echo "")
      rns=$(oc get "$kind" "$CR_NAME" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-namespace}' 2>/dev/null || echo "")
      if [[ -n "$rn" || -n "$rns" ]]; then
        echo "[INFO] $kind/$CR_NAME annotations: name='$rn' ns='$rns'"
      fi
      if confirm "[CONFIRM] Delete $kind/$CR_NAME ?"; then
        oc delete "$kind" "$CR_NAME" --ignore-not-found || true
      fi
    fi
  done

  # Also remove any cluster-scoped resources labeled for our candidate releases (best effort)
  for r in "${RELEASES[@]}"; do
    echo "[INFO] Deleting cluster-scoped resources labeled app.kubernetes.io/instance=${r} (best effort)"
    oc delete clusterrole,clusterrolebinding --selector "app.kubernetes.io/instance=${r}" --ignore-not-found=true || true
    oc delete clusterrole,clusterrolebinding --selector "meta.helm.sh/release-name=${r}" --ignore-not-found=true || true
  done
fi

# Finally delete the project/namespace
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  if confirm "[CONFIRM] Delete the entire project/namespace '$NAMESPACE'? This removes any remaining resources within it."; then
    echo "[INFO] Deleting project $NAMESPACE"
    oc delete project "$NAMESPACE" || oc delete namespace "$NAMESPACE" || true
    echo "[INFO] Waiting for project deletion to complete..."
    for i in {1..36}; do
      if ! oc get ns "$NAMESPACE" >/dev/null 2>&1; then
        echo "[INFO] Project $NAMESPACE deleted."
        break
      fi
      sleep 5
    done
  else
    echo "[INFO] Leaving project $NAMESPACE in place."
  fi
fi

echo "[DONE] CMF cleanup completed."

