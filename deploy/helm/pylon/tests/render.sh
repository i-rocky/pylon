#!/bin/sh
set -eu

HELM_IMAGE=alpine/helm:4.3.0
helm_run() {
    docker run --rm -v "$PWD:/apps" -w /apps "$HELM_IMAGE" "$@"
}

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

bignum_values="deploy/helm/pylon/tests/bignum-values.yaml.tmp"
trap 'rm -f "$bignum_values"' EXIT
cat > "$bignum_values" <<'YAML'
config:
  workers: 2147483648
  memoryBudgetBytes: 2147483648
  shutdownPredrainsMs: 2147483648
  shutdownGraceMs: 2147483648
replicaCount: 2147483648
autoscaling:
  enabled: true
  minReplicas: 2147483648
  maxReplicas: 2147483648
  targetCPUUtilizationPercentage: 2147483648
service:
  port: 2147483648
YAML

bignum=$(helm_run template pylon deploy/helm/pylon -f "$bignum_values")
printf '%s' "$bignum" | grep -A1 'name: PYLON_MEMORY_BUDGET_BYTES' | grep -q 'value: "2147483648"' || { echo "FAIL: memoryBudgetBytes did not render as a plain integer"; exit 1; }
printf '%s' "$bignum" | grep -q 'e+' && { echo "FAIL: rendered chart contains scientific notation"; exit 1; }

echo "OK: large numeric overrides render as plain integers, no scientific notation"

pdbpercent=$(helm_run template pylon deploy/helm/pylon --set-string podDisruptionBudget.minAvailable=50%)
printf '%s' "$pdbpercent" | grep -q '^  minAvailable: 50%$' || { echo "FAIL: PDB minAvailable percentage override did not render exactly"; exit 1; }

echo "OK: PDB minAvailable percentage override renders exactly"
