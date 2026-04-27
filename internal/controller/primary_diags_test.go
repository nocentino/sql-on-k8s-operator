/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"errors"
	"testing"

	"github.com/anocentino/sql-on-k8s-operator/internal/sqlutil"
)

// TestEvaluatePrimaryDiagsResult covers the pure state-transition that drives
// Layer 2 (sp_server_diagnostics) gray-failure detection. This is the logic
// that must (a) never flip to unhealthy before N consecutive probe failures,
// (b) flip exactly on the Nth failure, (c) reset to 0 on any successful probe,
// and (d) treat a successful-but-unhealthy diagnostics response as unhealthy
// without polluting the counter.
func TestEvaluatePrimaryDiagsResult(t *testing.T) {
	// The healthy diagnostic sample all three components report state=1 (clean).
	healthy := sqlutil.ServerDiagnostics{System: true, Resource: true, QueryProcessing: true}

	// A system-level failure — sp_server_diagnostics reports state=3 for the
	// "system" component. With threshold="system", this must flip unhealthy.
	systemFailure := sqlutil.ServerDiagnostics{System: false, Resource: true, QueryProcessing: true}

	probeErr := errors.New("context deadline exceeded")

	tests := []struct {
		name          string
		currentCount  int32
		diag          sqlutil.ServerDiagnostics
		probeErr      error
		threshold     string
		wantNewCount  int32
		wantUnhealthy bool
	}{
		// --- Probe error path: counter math + cap + declaration at threshold ---
		{
			name:          "first probe failure -> counter=1, still healthy",
			currentCount:  0,
			probeErr:      probeErr,
			threshold:     "system",
			wantNewCount:  1,
			wantUnhealthy: false,
		},
		{
			name:          "second probe failure -> counter=2, still healthy",
			currentCount:  1,
			probeErr:      probeErr,
			threshold:     "system",
			wantNewCount:  2,
			wantUnhealthy: false,
		},
		{
			name:          "third probe failure -> counter=3, flips unhealthy",
			currentCount:  2,
			probeErr:      probeErr,
			threshold:     "system",
			wantNewCount:  maxPrimaryDiagsFailures,
			wantUnhealthy: true,
		},
		{
			name:          "counter already at cap stays at cap and stays unhealthy",
			currentCount:  maxPrimaryDiagsFailures,
			probeErr:      probeErr,
			threshold:     "system",
			wantNewCount:  maxPrimaryDiagsFailures,
			wantUnhealthy: true,
		},
		{
			name:          "counter beyond cap (shouldn't happen but safe) clamps to cap",
			currentCount:  maxPrimaryDiagsFailures + 5,
			probeErr:      probeErr,
			threshold:     "system",
			wantNewCount:  maxPrimaryDiagsFailures,
			wantUnhealthy: true,
		},

		// --- Probe success path: reset-on-success + threshold evaluation ---
		{
			name:          "successful healthy probe resets counter from 0",
			currentCount:  0,
			diag:          healthy,
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: false,
		},
		{
			name:          "successful healthy probe resets counter from 2",
			currentCount:  2,
			diag:          healthy,
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: false,
		},
		{
			name:          "successful healthy probe resets counter from cap",
			currentCount:  maxPrimaryDiagsFailures,
			diag:          healthy,
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: false,
		},
		{
			name:          "successful probe reporting system=3 is unhealthy at threshold=system",
			currentCount:  0,
			diag:          systemFailure,
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: true,
		},
		{
			name:          "successful probe with only resource=false is still healthy at threshold=system",
			currentCount:  0,
			diag:          sqlutil.ServerDiagnostics{System: true, Resource: false, QueryProcessing: true},
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: false,
		},
		{
			name:          "successful probe with resource=false is unhealthy at threshold=resource",
			currentCount:  0,
			diag:          sqlutil.ServerDiagnostics{System: true, Resource: false, QueryProcessing: true},
			threshold:     "resource",
			wantNewCount:  0,
			wantUnhealthy: true,
		},

		// --- Recovery pattern: fault then recovery must clear the counter ---
		{
			name:          "recovery after 2 failures: a single success resets to 0",
			currentCount:  2,
			diag:          healthy,
			threshold:     "system",
			wantNewCount:  0,
			wantUnhealthy: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCount, gotUnhealthy := evaluatePrimaryDiagsResult(tc.currentCount, tc.diag, tc.probeErr, tc.threshold)
			if gotCount != tc.wantNewCount {
				t.Errorf("newCount = %d, want %d", gotCount, tc.wantNewCount)
			}
			if gotUnhealthy != tc.wantUnhealthy {
				t.Errorf("unhealthy = %v, want %v", gotUnhealthy, tc.wantUnhealthy)
			}
		})
	}
}

// TestEvaluatePrimaryDiagsResult_ConsecutiveFailureSequence simulates the
// reconcile cadence a live cluster would experience: sequential probe failures
// that must increment the counter exactly once per call and trip unhealthy
// only on the third strike. Reinforces that the function is a pure state
// transition and composes correctly across multiple invocations.
func TestEvaluatePrimaryDiagsResult_ConsecutiveFailureSequence(t *testing.T) {
	probeErr := errors.New("context deadline exceeded")

	count := int32(0)
	for i := int32(1); i <= maxPrimaryDiagsFailures; i++ {
		newCount, unhealthy := evaluatePrimaryDiagsResult(count, sqlutil.ServerDiagnostics{}, probeErr, "system")
		if newCount != i {
			t.Fatalf("strike %d: newCount = %d, want %d", i, newCount, i)
		}
		wantUnhealthy := i >= maxPrimaryDiagsFailures
		if unhealthy != wantUnhealthy {
			t.Fatalf("strike %d: unhealthy = %v, want %v", i, unhealthy, wantUnhealthy)
		}
		count = newCount
	}

	// A successful probe arrives after the cap: counter must reset to 0 and
	// unhealthy must flip back to false.
	healthy := sqlutil.ServerDiagnostics{System: true, Resource: true, QueryProcessing: true}
	newCount, unhealthy := evaluatePrimaryDiagsResult(count, healthy, nil, "system")
	if newCount != 0 {
		t.Errorf("recovery: newCount = %d, want 0", newCount)
	}
	if unhealthy {
		t.Errorf("recovery: unhealthy = true, want false")
	}
}
