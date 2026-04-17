/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package sqlutil

import (
	"fmt"
	"strings"
)

// Well-known SQL Server error numbers observed in the AG failover path.
//
// Reference: the mssql-server-ha ag-helper binary handles these exact numbers.
const (
	// ErrCodeNotSynchronized is raised by ALTER AVAILABILITY GROUP ... FAILOVER
	// when the target replica was not synchronized with the primary at the time
	// of the failover. The operator never force-failovers; it retries on the
	// next reconcile cycle rather than risk data loss.
	ErrCodeNotSynchronized = 41142

	// ErrCodePreviousError is raised by ALTER AVAILABILITY GROUP ... SET (ROLE = SECONDARY)
	// when SQL Server's AG resource manager still has a latched "previous error"
	// from a stale primary connection. Recovery strategy in the operator:
	// retry with increasing delay, then escalate to ALTER AG OFFLINE and bilateral
	// endpoint restart.
	ErrCodePreviousError = 41104
)

// HasSQLError returns true when either the sqlcmd stdout or the wrapped error
// text contains the given SQL Server error number. sqlcmd prints error
// messages to stdout, not stderr, but the wrapped err returned by ExecSQL
// includes both streams, so scanning both covers every case.
//
// Prefer this helper over ad-hoc strings.Contains(..., "41142") so that all
// callers use the same matching semantics.
func HasSQLError(res ExecResult, err error, code int) bool {
	needle := fmt.Sprintf("%d", code)
	if strings.Contains(res.Stdout, needle) {
		return true
	}
	if err != nil && strings.Contains(err.Error(), needle) {
		return true
	}
	return false
}
