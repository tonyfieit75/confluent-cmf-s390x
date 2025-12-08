#!/usr/bin/env bash
# install_cmf.sh
# Install Confluent Manager for Apache Flink (CMF) on OpenShift (s390x)
# - Namespace is REQUIRED to be 'confluent-platform'
# - Installs CMF via Helm + installs sample resources afterward
#
# Usage:
#   ./install_cmf.sh [--create-route] [--release <name>] [--chart-dir <dir>] [--values <file>]

set -euo pipefail

RELEASE="confluent-manager"
NAMESPACE="confluent-platform"
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
      sed -n '1,120p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "[WARN] Unknown arg: $1"; shift ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] Missing: $1">&2; exit 1; }; }
need oc
need helm

echo "[INFO] Release:   $RELEASE"
echo "[INFO] Namespace: $NAMESPACE"
echo "[INFO] Chart dir: $CHART_DIR"
echo "[INFO] Values:    $VALUES_FILE"
echo "[INFO] Route:     $CREATE_ROUTE"

[[ -d "$CHART_DIR" ]] || { echo "[ERR] Chart dir not found: $CHART_DIR"; exit 1; }
[[ -f "$VALUES_FILE" ]] || { echo "[ERR] Values file not found: $VALUES_FILE"; exit 1; }

# --- ensure namespace ---
if oc get ns "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Namespace '$NAMESPACE' already exists."
else
  echo "[INFO] Creating namespace '$NAMESPACE'..."
  oc new-project "$NAMESPACE" >/dev/null
fi

# --- apply PVC ---
PVC_FILE="${CHART_DIR}/cmf-sqlite-pvc.yaml"
if [[ -f "$PVC_FILE" ]]; then
  echo "[INFO] Applying PVC: $PVC_FILE"
  oc apply -n "$NAMESPACE" -f "$PVC_FILE"
else
  echo "[INFO] PVC file not found: $PVC_FILE"
fi

# --- helm lint ---
if helm lint "$CHART_DIR" -f "$VALUES_FILE" >/dev/null; then
  echo "[INFO] Helm lint OK"
else
  echo "[WARN] Helm lint issues detected (continuing)"
fi

# --- helm install/upgrade ---
echo "[INFO] Installing/upgrading Helm release..."
set +e
helm upgrade --install "$RELEASE" \
  "$CHART_DIR" \
  -n "$NAMESPACE" \
  -f "$VALUES_FILE"
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  echo "[ERR] Helm upgrade/install failed (exit $rc)"
  exit $rc
fi

# --- wait for rollout ---
echo "[INFO] Waiting for CMF deployment rollout..."
DEPLOYS=$(oc -n "$NAMESPACE" get deploy -l app.kubernetes.io/instance="$RELEASE" -o name 2>/dev/null || true)

if [[ -z "$DEPLOYS" ]]; then
  DEPLOYS=$(oc -n "$NAMESPACE" get deploy -l app.kubernetes.io/name=confluent-manager-for-apache-flink -o name 2>/dev/null || true)
fi

if [[ -z "$DEPLOYS" ]]; then
  echo "[WARN] No deployments found."
  oc -n "$NAMESPACE" get deploy
else
  for d in $DEPLOYS; do
    echo "[INFO] Rolling out: $d"
    oc -n "$NAMESPACE" rollout status "$d" --timeout=180s || true
  done
fi

# ============================================================
# 🚀 NEW SECTION: INSTALL SAMPLE CMF RESOURCES
# ============================================================
SAMPLE_DIR="./sample"

echo "[INFO] Installing CMF sample resources from: $SAMPLE_DIR"

SAMPLES=(
  "Sample-CMFRestClass.yaml"
  "cmfEnv.yaml"
  "cmfapp.yaml"
)

for s in "${SAMPLES[@]}"; do
  FILE="${SAMPLE_DIR}/${s}"
  if [[ -f "$FILE" ]]; then
    echo "[INFO] → Applying sample: $FILE"
    oc apply -n "$NAMESPACE" -f "$FILE"
  else
    echo "[WARN] Sample file missing: $FILE"
  fi
done

echo "[INFO] Sample CMF resources installed."

# ============================================================

# --- optionally create a route ---
if [[ "$CREATE_ROUTE" == "true" ]]; then
  echo "[INFO] Creating Route..."
  SVC=$(oc -n "$NAMESPACE" get svc -l app.kubernetes.io/instance="$RELEASE" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

  if [[ -n "$SVC" ]]; then
    if ! oc -n "$NAMESPACE" get route "$RELEASE" >/dev/null 2>&1; then
      PORT=$(oc -n "$NAMESPACE" get svc "$SVC" -o jsonpath='{.spec.ports[0].name}' 2>/dev/null || echo "")
      if [[ -n "$PORT" ]]; then
        oc -n "$NAMESPACE" create route edge "$RELEASE" --service="$SVC" --port="$PORT" >/dev/null
      else
        oc -n "$NAMESPACE" create route edge "$RELEASE" --service="$SVC" --port=8080 >/dev/null || true
      fi
    fi
    HOST=$(oc -n "$NAMESPACE" get route "$RELEASE" -o jsonpath='{.spec.host}')
    echo "[INFO] Route available at: https://${HOST}"
  else
    echo "[WARN] No service found to create Route."
  fi
fi

echo
echo "[OK] CMF installation + sample deployment complete."
echo "Namespace : $NAMESPACE"
echo "Release   : $RELEASE"
echo
echo "Quick checks:"
echo "  oc -n $NAMESPACE get pods"
echo "  oc -n $NAMESPACE get cmfrestclasses"
echo "  oc -n $NAMESPACE get cmfenvironments"
echo "  oc -n $NAMESPACE get cmfapplications"

