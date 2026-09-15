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
printf '%s' "$default" | grep -q 'terminationGracePeriodSeconds: 30' || { echo "FAIL: default shutdownPredrainsMs+shutdownGraceMs (12s, derived 18s) should floor terminationGracePeriodSeconds at 30"; exit 1; }

raised=$(helm_run template pylon deploy/helm/pylon --set config.shutdownGraceMs=30000)
printf '%s' "$raised" | grep -q 'terminationGracePeriodSeconds: 38' || { echo "FAIL: shutdownPredrainsMs 2000 + shutdownGraceMs 30000 (32s, exceeds the 30s floor) should derive terminationGracePeriodSeconds 38"; exit 1; }

zero=$(helm_run template pylon deploy/helm/pylon --set config.terminationGracePeriodSeconds=0)
printf '%s' "$zero" | grep -q 'terminationGracePeriodSeconds: 30' || { echo "FAIL: an explicit config.terminationGracePeriodSeconds=0 should derive (30s), same as unset"; exit 1; }

override=$(helm_run template pylon deploy/helm/pylon --set config.terminationGracePeriodSeconds=60)
printf '%s' "$override" | grep -q 'terminationGracePeriodSeconds: 60' || { echo "FAIL: an explicit, sufficient config.terminationGracePeriodSeconds override should be honored"; exit 1; }

atmin=$(helm_run template pylon deploy/helm/pylon --set config.terminationGracePeriodSeconds=13)
printf '%s' "$atmin" | grep -q 'terminationGracePeriodSeconds: 13' || { echo "FAIL: config.terminationGracePeriodSeconds=13 (exactly the minimum for the default 12s drain) should be honored, not rejected"; exit 1; }

if belowmin=$(helm_run template pylon deploy/helm/pylon --set config.terminationGracePeriodSeconds=12 2>&1); then
    echo "FAIL: config.terminationGracePeriodSeconds=12 (exactly one below the 13s minimum) should fail the render"; exit 1
fi
printf '%s' "$belowmin" | grep -q 'too small' || { echo "FAIL: the below-minimum failure doesn't name the problem"; exit 1; }

if negative=$(helm_run template pylon deploy/helm/pylon --set config.terminationGracePeriodSeconds=-5 2>&1); then
    echo "FAIL: config.terminationGracePeriodSeconds=-5 should fail the render, not silently derive"; exit 1
fi
printf '%s' "$negative" | grep -q -- '-5' || { echo "FAIL: the negative-override failure doesn't name the value"; exit 1; }

if unparseable=$(helm_run template pylon deploy/helm/pylon --set-string config.terminationGracePeriodSeconds=60s 2>&1); then
    echo "FAIL: config.terminationGracePeriodSeconds=60s should fail the render, not silently derive"; exit 1
fi
printf '%s' "$unparseable" | grep -q '60s' || { echo "FAIL: the unparseable-override failure doesn't name the value"; exit 1; }

echo "OK: secret, pdb, existingSecret, no-configmap and terminationGracePeriodSeconds derivation/floor/override/boundary/failure all render as specified"
