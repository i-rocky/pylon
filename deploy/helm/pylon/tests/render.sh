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
