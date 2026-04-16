#!/bin/bash
# Wait for AG initialization to complete
echo "Waiting for AG initialization..."
for i in $(seq 1 60); do
  STATUS=$(kubectl get sqlag mssql-ag -n default -o jsonpath='{.status.initializationComplete}' 2>/dev/null)
  echo "  [$i] initializationComplete=$STATUS"
  if [ "$STATUS" = "true" ]; then
    echo "AG initialized!"
    break
  fi
  sleep 10
done
echo ""
echo "=== AG Status ==="
kubectl get sqlag mssql-ag -n default -o jsonpath='{.status}' 2>&1 | python3 -m json.tool
echo ""
echo "=== Pods ==="
kubectl get pods -n default -l app=mssql-ag -o wide
echo ""
echo "=== Verify preStop hook present ==="
kubectl get statefulset mssql-ag -n default -o jsonpath='{.spec.template.spec.containers[0].lifecycle}' | python3 -m json.tool
