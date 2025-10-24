# Confluent Manager for Apache Flink® on OpenShift (s390x)

This repo contains a Helm chart and helper scripts to deploy **Confluent Manager for Apache Flink (CMF)** on **Red Hat OpenShift** (IBM Z **s390x**).
It packages a re-platformed CMF image for s390x plus OpenShift-friendly security settings (SCC-safe), persistence for CMF’s embedded SQLite, and an OpenShift Route for access.

> **What CMF does (high level)**
> CMF is a control plane for Apache Flink in Kubernetes/OpenShift. It provides:
>
> * REST API & UI to **create environments**, **compute pools**, **Flink applications**, and **SQL statements**.
> * **Lifecycle management** of Flink jobs (submit, observe, suspend).
> * **Metadata & catalog integration** (Kafka/Schema Registry catalogs).
> * **Works with the Flink Kubernetes Operator** to deploy workloads into your cluster.

---

## Contents

```
├── confluent-manager-for-apache-flink
│   ├── Chart.yaml
│   ├── cleanup_cmf_objects.sh        # delete CMF objects via REST (keeps namespace)
│   ├── cmf-route.yaml                # OpenShift Route to expose CMF
│   ├── cmf-sqlite-pvc.yaml           # PersistentVolumeClaim for embedded SQLite
│   ├── templates/                    # Helm manifests
│   │   ├── NOTES.txt
│   │   ├── _helpers.tpl
│   │   ├── clusterrole.yaml
│   │   ├── clusterrolebinding.yaml
│   │   ├── configmap.yaml
│   │   ├── deployment.yaml
│   │   ├── pvc.yaml
│   │   ├── service.yaml
│   │   └── serviceaccount.yaml
│   ├── validate_cmf_e2e.sh           # end-to-end API smoke tests
│   ├── values-ocp-s390x.yaml         # OCP+s390x defaults (nonroot, PVC, image)
│   └── values.yaml
├── install_cmf.sh                    # convenience installer (namespace-aware)
└── uninstall_cmf.sh                  # removes Helm release + route (+ optional PVC)
```

---

## Prerequisites

* OpenShift 4.x cluster access (`oc login …`)
* Helm v3
* **Flink Kubernetes Operator** installed (and permitted in your cluster)
* Project/namespace: `confluent-platform` (the scripts create it if missing)
* s390x CMF image available (defaults used below):

  * `quay.io/tonyfieit75/cp-cmf:2.0.3-s390x-ocp`
* Optional (for E2E tests): `jq`, `curl`

> **Security**: The chart runs **as an arbitrary OpenShift UID** (no fixed `runAsUser/fsGroup`) and drops all Linux capabilities, satisfying the **restricted** SCC.

---

## Quick Start (Install)

```bash
# 1) Create/select the project
oc new-project confluent-platform 2>/dev/null || oc project confluent-platform

# 2) Install CMF via Helm (uses values-ocp-s390x.yaml)
./install_cmf.sh
# or the raw helm command:
# helm upgrade --install confluent-manager ./confluent-manager-for-apache-flink \
#   -n confluent-platform -f confluent-manager-for-apache-flink/values-ocp-s390x.yaml

# 3) Create PVC for embedded SQLite (if your chart values don’t auto-create it)
oc apply -n confluent-platform -f confluent-manager-for-apache-flink/cmf-sqlite-pvc.yaml

# 4) Expose CMF with an OpenShift Route (ClusterIP Service -> HTTPS edge route)
oc apply -n confluent-platform -f confluent-manager-for-apache-flink/cmf-route.yaml
```

Check status:

```bash
oc -n confluent-platform get deploy,svc,pvc,route
oc -n confluent-platform logs deploy/confluent-manager-for-apache-flink --tail=100
```

Get the external URL:

```bash
HOST=$(oc -n confluent-platform get route confluent-manager -o jsonpath='{.spec.host}')
echo "CMF URL: https://$HOST/"
echo "Swagger: https://$HOST/swagger-ui/index.html"
```

---

## Minimal Validation (E2E)

You can run the scripted smoke test:

```bash
./confluent-manager-for-apache-flink/validate_cmf_e2e.sh
```

What it does:

1. **Environment** – ensures `dev-ocp` exists and maps to `confluent-platform`.
2. **Compute Pool** – ensures a small **DEDICATED** pool (1 JM / 1 TM).
3. **Flink Application** – submits a small **datagen → print** SQL job via CMF API (using the Flink image you set in values).
4. **Sanity checks** – confirms service/endpoints, prints useful URLs.

> If your cluster lacks a public Kafka/Schema Registry, the validator uses a self-contained **datagen/print** example.

---

## Manual API Peeks

```bash
HOST=$(oc -n confluent-platform get route confluent-manager -o jsonpath='{.spec.host}')

# OpenAPI / Swagger
curl -sk "https://$HOST/v3/api-docs" | jq '.info'
# UI:
# https://$HOST/swagger-ui/index.html

# List environments
curl -sk "https://$HOST/cmf/api/v1/environments" | jq .

# Create environment -> namespace mapping
curl -sk -X POST "https://$HOST/cmf/api/v1/environments" \
  -H 'Content-Type: application/json' \
  -d '{"name":"dev-ocp","kubernetesNamespace":"confluent-platform"}' | jq .

# Create a DEDICATED compute pool (Flink cluster under the hood)
cat > pool.json <<'JSON'
{
  "apiVersion": "cmf.confluent.io/v1",
  "kind": "ComputePool",
  "metadata": { "name": "pool-small" },
  "spec": {
    "type": "DEDICATED",
    "clusterSpec": {
      "jobManager":  { "replicas": 1 },
      "taskManager": { "replicas": 1 }
    }
  }
}
JSON

curl -sk -X POST "https://$HOST/cmf/api/v1/environments/dev-ocp/compute-pools" \
  -H 'Content-Type: application/json' --data-binary @pool.json | jq .
```

> **Tip:** The exact request/response schema is discoverable in Swagger (`/swagger-ui/index.html`). CMF will return helpful validation messages if a field name or API version is wrong.

---

## Uninstall / Cleanup

To remove the Helm release (keep the project):

```bash
./uninstall_cmf.sh
# or:
# helm -n confluent-platform uninstall confluent-manager
# oc -n confluent-platform delete route confluent-manager
# (optional) oc -n confluent-platform delete pvc cmf-sqlite-pvc
```

To **delete CMF objects via REST** (env, pools, apps, statements; keeps the namespace):

```bash
./confluent-manager-for-apache-flink/cleanup_cmf_objects.sh
# or purge shared catalogs/secrets too:
./confluent-manager-for-apache-flink/cleanup_cmf_objects.sh --purge-shared --yes
```

---

## Configuration (values excerpt)

`values-ocp-s390x.yaml` sets sane OCP defaults:

* **Image**: `quay.io/tonyfieit75/cp-cmf:2.0.3-s390x-ocp`
* **SCC-safe** security contexts (no fixed UID/GID, nonroot, drop ALL caps)
* **Service**: ClusterIP with a named targetPort (the Route references this)
* **PVC**: persistence for CMF’s embedded SQLite

Adjust these in `values-ocp-s390x.yaml` to point at your registries, storage class, or resource limits.

---

## Troubleshooting

* **Route 503 / “Application not available”**
  Ensure:

  * Service port `targetPort` **name** matches the Route’s `spec.port.targetPort`.
  * Endpoints show a Pod IP on **port 8080**.
  * Deployment is **READY 1/1** and logs show Tomcat started on port 8080.

* **SCC errors (RunAsUser/fsGroup/ranges)**
  Don’t set fixed IDs. This chart runs with OpenShift’s **arbitrary UID** model.
  Confirm `securityContext.runAsNonRoot: true` and **no** `runAsUser/fsGroup` in values/manifests.

* **ComputePool / Application validation**
  Use the Swagger UI to copy exact JSON. The CMF API is strict about:

  * `apiVersion` (e.g., `cmf.confluent.io/v1`)
  * allowed enum values (e.g., `type: "DEDICATED"`)
  * supported fields (no extra keys)

* **Flink not deploying**
  Ensure the **Flink Kubernetes Operator** is installed and has permissions in your namespace.

---

## License

This repo contains deployment assets and helper scripts.
Confluent Manager for Apache Flink and Apache Flink are licensed by their respective owners.

