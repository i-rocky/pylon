#!/bin/sh
set -eu

HELM_IMAGE=alpine/helm:4.3.0
helm_run() {
    docker run --rm -v "$PWD:/apps" -w /apps "$HELM_IMAGE" "$@"
}
trap 'rm -f .*-values.yaml.tmp' EXIT

helm_run lint deploy/helm/pylon

default=$(helm_run template pylon deploy/helm/pylon)
printf '%s' "$default" | grep -q '^kind: Secret$' || { echo "FAIL: default render has no Secret"; exit 1; }
printf '%s' "$default" | grep -q 'apps.json:' || { echo "FAIL: the Secret carries no apps.json"; exit 1; }
printf '%s' "$default" | grep -q '^kind: PodDisruptionBudget$' || { echo "FAIL: default render has no PDB"; exit 1; }
printf '%s' "$default" | grep -q 'minAvailable: 1' || { echo "FAIL: PDB minAvailable is not 1"; exit 1; }
printf '%s' "$default" | grep -q '^kind: ConfigMap$' && { echo "FAIL: a ConfigMap is still rendered"; exit 1; }

existing=$(helm_run template pylon deploy/helm/pylon --set existingSecret=my-secret)
printf '%s' "$existing" | grep -q '^kind: Secret$' && { echo "FAIL: existingSecret must not create a Secret"; exit 1; }
printf '%s' "$existing" | grep -q 'name: my-secret' || { echo "FAIL: existingSecret is not mounted"; exit 1; }

nopdb=$(helm_run template pylon deploy/helm/pylon --set podDisruptionBudget.enabled=false)
printf '%s' "$nopdb" | grep -q '^kind: PodDisruptionBudget$' && { echo "FAIL: PDB rendered while disabled"; exit 1; }

echo "OK: secret, pdb, existingSecret and no-configmap all render as specified"

bignum_values=".bignum-values.yaml.tmp"
cat > "$bignum_values" <<'YAML'
image:
  tag: 2147483648
config:
  workers: 2147483648
  memoryBudgetBytes: 2147483648
  shutdownPredrainsMs: 2147483648
  shutdownGraceMs: 2147483648
  redisPrefix: 2147483648
replicaCount: 2147483648
autoscaling:
  enabled: true
  minReplicas: 2147483648
  maxReplicas: 2147483648
  targetCPUUtilizationPercentage: 2147483648
  targetMemoryUtilizationPercentage: 2147483648
service:
  port: 2147483648
YAML

bignum=$(helm_run template pylon deploy/helm/pylon -f "$bignum_values")
printf '%s' "$bignum" | grep -A1 'name: PYLON_MEMORY_BUDGET_BYTES' | grep -q 'value: "2147483648"' || { echo "FAIL: memoryBudgetBytes did not render as a plain integer"; exit 1; }
printf '%s' "$bignum" | grep -q 'image: "ghcr.io/i-rocky/pylon:2147483648"' || { echo "FAIL: image.tag did not render as a plain integer"; exit 1; }
printf '%s' "$bignum" | grep -A1 'name: PYLON_REDIS_PREFIX' | grep -q 'value: "2147483648"' || { echo "FAIL: redisPrefix did not render as a plain integer"; exit 1; }
printf '%s' "$bignum" | grep -q 'e+' && { echo "FAIL: rendered chart contains scientific notation"; exit 1; }

echo "OK: large numeric overrides render as plain integers, no scientific notation"

pdbpercent=$(helm_run template pylon deploy/helm/pylon --set-string podDisruptionBudget.minAvailable=50%)
printf '%s' "$pdbpercent" | grep -q '^  minAvailable: 50%$' || { echo "FAIL: PDB minAvailable percentage override did not render exactly"; exit 1; }

echo "OK: PDB minAvailable percentage override renders exactly"

tag_values=".tagform-values.yaml.tmp"

cat > "$tag_values" <<'YAML'
image:
  tag: 20240115
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -q 'image: "ghcr.io/i-rocky/pylon:20240115"' || { echo "FAIL: integral numeric image.tag 20240115 did not render as a plain integer"; exit 1; }

cat > "$tag_values" <<'YAML'
image:
  tag: "007"
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -q 'image: "ghcr.io/i-rocky/pylon:007"' || { echo "FAIL: quoted numeric image.tag did not render exactly"; exit 1; }

cat > "$tag_values" <<'YAML'
image:
  tag: 1.10
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -q 'image: "ghcr.io/i-rocky/pylon:1.1"' || { echo "FAIL: fractional image.tag 1.10 did not render as 1.1"; exit 1; }

cat > "$tag_values" <<'YAML'
image:
  tag: 1.2.3
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -q 'image: "ghcr.io/i-rocky/pylon:1.2.3"' || { echo "FAIL: semver image.tag did not render exactly"; exit 1; }

cat > "$tag_values" <<'YAML'
image:
  tag: latest
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -q 'image: "ghcr.io/i-rocky/pylon:latest"' || { echo "FAIL: string image.tag did not render exactly"; exit 1; }

tag_default=$(helm_run template pylon deploy/helm/pylon)
printf '%s' "$tag_default" | grep -q 'image: "ghcr.io/i-rocky/pylon:0.5.0"' || { echo "FAIL: unset image.tag did not fall through to Chart.AppVersion"; exit 1; }

cat > "$tag_values" <<'YAML'
config:
  redisPrefix: 1.5
YAML
out=$(helm_run template pylon deploy/helm/pylon -f "$tag_values")
printf '%s' "$out" | grep -A1 'name: PYLON_REDIS_PREFIX' | grep -q 'value: "1.5"' || { echo "FAIL: fractional redisPrefix did not render exactly"; exit 1; }

echo "OK: every image.tag form and a fractional redisPrefix render their exact expected string"

strings_values=".strings-values.yaml.tmp"
cat > "$strings_values" <<'YAML'
replicaCount: two
config:
  workers: four
  memoryBudgetBytes: 2Gi
  shutdownPredrainsMs: 2s
  shutdownGraceMs: 10s
service:
  port: http
autoscaling:
  enabled: true
  minReplicas: one
  maxReplicas: ten
  targetCPUUtilizationPercentage: 70%
  targetMemoryUtilizationPercentage: 80%
YAML
strings=$(helm_run template pylon deploy/helm/pylon -f "$strings_values")
printf '%s' "$strings" | grep -q '^  replicas: two$' || { echo "FAIL: replicaCount 'two' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -A1 'name: PYLON_WORKERS' | grep -q 'value: "four"' || { echo "FAIL: workers 'four' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -A1 'name: PYLON_MEMORY_BUDGET_BYTES' | grep -q 'value: "2Gi"' || { echo "FAIL: memoryBudgetBytes '2Gi' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -A1 'name: PYLON_SHUTDOWN_PREDRAIN_MS' | grep -q 'value: "2s"' || { echo "FAIL: shutdownPredrainsMs '2s' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -A1 'name: PYLON_SHUTDOWN_GRACE_MS' | grep -q 'value: "10s"' || { echo "FAIL: shutdownGraceMs '10s' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q '^    - port: http$' || { echo "FAIL: service.port 'http' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q '^  minReplicas: one$' || { echo "FAIL: minReplicas 'one' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q '^  maxReplicas: ten$' || { echo "FAIL: maxReplicas 'ten' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q 'averageUtilization: 70%$' || { echo "FAIL: targetCPUUtilizationPercentage '70%' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q 'averageUtilization: 80%$' || { echo "FAIL: targetMemoryUtilizationPercentage '80%' was not passed through verbatim"; exit 1; }
printf '%s' "$strings" | grep -q 'value: "0"' && { echo "FAIL: an unparseable value was silently rendered as 0"; exit 1; }

fraction_values=".fraction-values.yaml.tmp"
cat > "$fraction_values" <<'YAML'
replicaCount: 2.5
YAML
fraction=$(helm_run template pylon deploy/helm/pylon -f "$fraction_values")
printf '%s' "$fraction" | grep -q '^  replicas: 2.5$' || { echo "FAIL: replicaCount 2.5 was truncated or altered instead of passed through"; exit 1; }

echo "OK: unparseable and fractional numeric values reach the manifest verbatim instead of becoming 0"
