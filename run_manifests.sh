#!/bin/sh
set -e
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
cd /Users/anocentino/Documents/GitHub/sql-on-k8s-operator
make manifests generate
echo "MANIFESTS_OK"
go build ./...
echo "BUILD_OK"
make lint-fix
echo "LINT_OK"
go test ./internal/... -count=1 2>&1 | tail -5
echo "TEST_DONE"
