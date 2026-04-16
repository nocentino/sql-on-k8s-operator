#!/bin/bash
# phase1-deploy.sh — Clean slate deploy of 3-sync AG and wait for bootstrap.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Phase 1: Cleanup ==="

# Patch any existing Retain-policy PVs to Delete so they are reclaimed with their PVCs.
for pv in $(kubectl get pv --no-headers 2>/dev/null | awk '{print $1}'); do
    policy=$(kubectl get pv "$pv" -o jsonpath='{.spec.persistentVolumeReclaimPolicy}' 2>/dev/null)
    if [ "$policy" = "Retain" ]; then
        kubectl patch pv "$pv" -p '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}' 2>/dev/null \
            && echo "  Patched PV $pv -> Delete"
    fi
done

kubectl delete sqlag --all --ignore-not-found 2>/dev/null || true
kubectl delete pvc -l app=mssql-ag --ignore-not-found 2>/dev/null || true
kubectl delete secret mssql-ag-secret --ignore-not-found 2>/dev/null || true

echo "Waiting for pods and PVs to terminate..."
kubectl wait pod -l app=mssql-ag --for=delete --timeout=90s 2>/dev/null || true
# Belt-and-suspenders: delete any Released/Failed PVs that local-path didn't reclaim
kubectl delete pv $(kubectl get pv --no-headers 2>/dev/null | grep -E "Released|Failed" | awk '{print $1}') \
    --ignore-not-found 2>/dev/null || true
sleep 5

echo ""
echo "=== Phase 1: Deploy operator (v0.25) ==="
cd "$REPO"
kubectl apply -f dist/install.yaml 2>&1 | grep -v "unchanged"
kubectl rollout status deployment/sql-on-k8s-operator-controller-manager \
    -n sql-on-k8s-operator-system --timeout=120s

echo ""
echo "=== Phase 1: Create secret and deploy 3-sync AG ==="
kubectl create secret generic mssql-ag-secret \
    --from-literal=SA_PASSWORD="YourStrong!Passw0rd" 2>/dev/null || true
kubectl apply -f config/samples/sql_v1alpha1_sqlserveravailabilitygroup_3sync.yaml

echo ""
echo "=== Phase 1: Wait for bootstrap (initializationComplete=true) ==="
for i in $(seq 1 90); do
    st=$(kubectl get sqlag mssql-ag -o jsonpath="{.status.initializationComplete}" 2>/dev/null || true)
    pods=$(kubectl get pods -l app=mssql-ag --no-headers 2>/dev/null | awk '{print $1,$3}' | tr '\n' ' ')
    echo "${i}0s: initializationComplete=${st}  pods: ${pods}"
    if [ "$st" = "true" ]; then
        echo ""
        echo "Bootstrap complete!"
        kubectl get pods -l app=mssql-ag -o wide
        exit 0
    fi
    sleep 10
done
echo "ERROR: bootstrap did not complete within 900s"
exit 1
