#!/usr/bin/env bash
# install_cmf.sh
# Install Confluent Manager for Apache Flink (CMF) on OpenShift (s390x)
# - Namespace is REQUIRED to be 'confluent-platform' (created if missing)
# - Uses Helm chart at ./confluent-manager-for-apache-flink
# - Uses values file ./confluent-manager-for-apache-flink/values-ocp-s390x.yaml
# - Applies cmf-sqlite-pvc.yaml before Helm so embedded SQLite is persistent
#
# Usage:
#   ./install_cmf.sh [--create-route] [--release <name>] [--chart-dir <dir>] [--values <file>]
#
# Defaults:
#   release:     confluent-manager
#   namespace:   confluent-platform  (fixed by requirement)
#   chart-dir:   ./confluent-manager-for-apache-flink
#   values:      ./confluent-manager-for-apache-flink/values-ocp-s390x.yaml
#
# Optional:
#   --create-route   Create an OpenShift Route for the service if possible.

set -euo pipefail

RELEASE="confluent-manager"
NAMESPACE="confluent-platform"   # per requirement
CHART_DIR="./confluent-manager-for-apache-flink"
VALUES_FILE="${CHART_DIR}/values-ocp-s390x.yaml"
CREATE_ROUTE="false"

# --- parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --create-route) CREATE_ROUTE="true"; shift ;;
    --release)      RELEASE="$2"; shift 2 ;;
    --chart-dir)    CHART_DIR="$2"; shift 2 ;;
    --values)       VALUES_FILE="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,80p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "[WARN] Unknown arg: $1"; shift ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] Missing: $1" >&2; exit 1; }; }
need oc
need helm

echo "[INFO] Release:   $RELEASE"
echo "[INFO] Namespace: $NAMESPACE (will be created if absent)"
echo "[INFO] Chart dir: $CHART_DIR"
echo "[INFO] Values:    $VALUES_FILE"
echo "[INFO] Route:     ${CREATE_ROUTE}"

# --- sanity checks ---
[[ -d "$CHART_DIR" ]] || { echo "[ERR] Chart dir not found: $CHART_DIR"; exit 1; }
[[ -f "$VALUES_FILE" ]] || { echo "[ERR] Values file not found: $VALUES_FILE"; exit 1; }

# --- ensure namespace exists ---
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Namespace '$NAMESPACE' already exists."
else
  echo "[INFO] Creating namespace '$NAMESPACE'..."
  oc new-project "$NAMESPACE" >/dev/null
fi

# --- apply PVC for SQLite (if present) ---
PVC_FILE="${CHART_DIR}/cmf-sqlite-pvc.yaml"
if [[ -f "$PVC_FILE" ]]; then
  echo "[INFO] Applying PVC: $PVC_FILE"
  oc apply -n "$NAMESPACE" -f "$PVC_FILE"
else
  echo "[INFO] PVC file not found at ${PVC_FILE}; assuming chart will create or existingClaim is set."
fi

# --- helm lint (optional but helpful) ---
if helm lint "$CHART_DIR" -f "$VALUES_FILE" >/dev/null; then
  echo "[INFO] Helm lint: OK"
else
  echo "[WARN] Helm lint reported issues (continuing)."
fi

# --- install/upgrade ---
echo "[INFO] Installing/upgrading Helm release '$RELEASE' in namespace '$NAMESPACE'..."
set +e
helm upgrade --install "$RELEASE" \
  "$CHART_DIR" \
  -n "$NAMESPACE" \
  -f "$VALUES_FILE"
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  echo "[ERR] Helm upgrade/install failed (exit $rc)."
  echo "     Common causes:"
  echo "       - Leftover resources owned by a different Helm release (check ServiceAccount annotations)."
  echo "       - Try: oc get sa -n $NAMESPACE -o yaml | grep -A2 'meta.helm.sh/release-name'"
  exit $rc
fi

# --- wait for rollout ---
echo "[INFO] Waiting for Deployment(s) to become available..."
DEPLOYS=$(oc -n "$NAMESPACE" get deploy -l app.kubernetes.io/instance="$RELEASE" -o name 2>/dev/null || true)
if [[ -z "$DEPLOYS" ]]; then
  # Fallback: try by chart name label
  DEPLOYS=$(oc -n "$NAMESPACE" get deploy -l app.kubernetes.io/name=confluent-manager-for-apache-flink -o name 2>/dev/null || true)
fi

if [[ -z "$DEPLOYS" ]]; then
  echo "[WARN] No deployments found by labels. Showing all deployments in namespace:"
  oc -n "$NAMESPACE" get deploy
else
  for d in $DEPLOYS; do
    echo "[INFO] Rolling out: $d"
    oc -n "$NAMESPACE" rollout status "$d" --timeout=180s || true
  done
fi

# --- optionally create a Route ---
if [[ "$CREATE_ROUTE" == "true" ]]; then
  # find the first service for this release
  SVC=$(oc -n "$NAMESPACE" get svc -l app.kubernetes.io/instance="$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$SVC" ]]; then
    # fallback by chart name
    SVC=$(oc -n "$NAMESPACE" get svc -l app.kubernetes.io/name=confluent-manager-for-apache-flink -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  fi

  if [[ -n "$SVC" ]]; then
    echo "[INFO] Creating/exposing Route for service: $SVC"
    # create if not exists; otherwise patch does no harm
    if ! oc -n "$NAMESPACE" get route "$RELEASE" >/dev/null 2>&1; then
      # try to use named port 'http' if present; else default to 8080
      PORT_NAME=$(oc -n "$NAMESPACE" get svc "$SVC" -o jsonpath='{.spec.ports[0].name}' 2>/dev/null || echo "")
      if [[ -n "$PORT_NAME" ]]; then
        oc -n "$NAMESPACE" create route edge "$RELEASE" --service="$SVC" --port="$PORT_NAME" >/dev/null
      else
        oc -n "$NAMESPACE" create route edge "$RELEASE" --service="$SVC" --port=8080 >/dev/null || true
      fi
    fi
    HOST=$(oc -n "$NAMESPACE" get route "$RELEASE" -o jsonpath='{.spec.host}')
    echo "[INFO] Route: https://${HOST}"
  else
    echo "[WARN] Could not find service by label to create a Route."
  fi
fi

echo
echo "[OK] CMF install complete."
echo "    Namespace : $NAMESPACE"
echo "    Release   : $RELEASE"
echo
echo "Quick checks:"
echo "  oc -n $NAMESPACE get pods"
echo "  oc -n $NAMESPACE logs deploy/$(oc -n $NAMESPACE get deploy -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo '<deploy>') | head"
echo
echo "If you enabled a Route: open it in a browser or:"
echo "  curl -vk https://\$(oc -n $NAMESPACE get route $RELEASE -o jsonpath='{.spec.host}')/"

