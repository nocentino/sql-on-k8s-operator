/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package sqlutil

import (
	"errors"
	"strings"
	"testing"
)

func TestSanitizeIdentifier(t *testing.T) {
	cases := []struct {
		in        string
		want      string
		wantPanic bool
	}{
		{"ag0", "[ag0]", false},
		{"MyAG", "[MyAG]", false},
		{"_under", "[_under]", false},
		{"a_b_1", "[a_b_1]", false},
		{"", "", true},
		{"1bad", "", true},
		{"has-dash", "", true},
		{"has space", "", true},
		{"has]bracket", "", true},
		{"has;semicolon", "", true},
		{"'quote", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.wantPanic && r == nil {
					t.Fatalf("expected panic for %q, got none", tc.in)
				}
				if !tc.wantPanic && r != nil {
					t.Fatalf("unexpected panic for %q: %v", tc.in, r)
				}
			}()
			got := sanitizeIdentifier(tc.in)
			if !tc.wantPanic && got != tc.want {
				t.Fatalf("sanitizeIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteBracketIdentifier(t *testing.T) {
	cases := []struct{ in, want string }{
		{"TPCC-5G", "[TPCC-5G]"},
		{"has]bracket", "[has]]bracket]"},
		{"multi]]brackets]", "[multi]]]]brackets]]]"},
		{"", "[]"},
	}
	for _, tc := range cases {
		if got := quoteBracketIdentifier(tc.in); got != tc.want {
			t.Errorf("quoteBracketIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEscapeSQLString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"simple", "simple"},
		{"it's", "it''s"},
		{"a''b", "a''''b"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := escapeSQLString(tc.in); got != tc.want {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFailoverSQLIsSafe(t *testing.T) {
	got := FailoverSQL("MyAG")
	if !strings.Contains(got, "[MyAG]") {
		t.Errorf("expected bracket-quoted AG name in output, got: %s", got)
	}
	if !strings.Contains(got, "external_cluster") {
		t.Errorf("expected session context in output, got: %s", got)
	}
}

func TestCreateEndpointSQLInterpolation(t *testing.T) {
	got := CreateEndpointSQL("AGEP", "cert_mssql_ag_0", 5022)
	if !strings.Contains(got, "[AGEP]") {
		t.Errorf("missing endpoint bracket-quote: %s", got)
	}
	if !strings.Contains(got, "[cert_mssql_ag_0]") {
		t.Errorf("missing cert bracket-quote: %s", got)
	}
	if !strings.Contains(got, "LISTENER_PORT = 5022") {
		t.Errorf("missing port: %s", got)
	}
}

func TestJoinDatabaseToAGAllowsHyphenatedDB(t *testing.T) {
	// Must not panic — hyphens are legal inside bracket-quoted SQL identifiers.
	got := JoinDatabaseToAGSQL("TPCC-5G", "ag0")
	if !strings.Contains(got, "[TPCC-5G]") {
		t.Errorf("expected bracket-quoted DB name, got: %s", got)
	}
	if !strings.Contains(got, "[ag0]") {
		t.Errorf("expected bracket-quoted AG name, got: %s", got)
	}
}

func TestHasSQLError(t *testing.T) {
	t.Run("nil-err-stdout-match", func(t *testing.T) {
		res := ExecResult{Stdout: "Msg 41142, Level 16"}
		if !HasSQLError(res, nil, ErrCodeNotSynchronized) {
			t.Fatal("expected true when stdout contains the error code")
		}
	})
	t.Run("err-message-match", func(t *testing.T) {
		err := errors.New("exec failed: 41104 something")
		if !HasSQLError(ExecResult{}, err, ErrCodePreviousError) {
			t.Fatal("expected true when err message contains the code")
		}
	})
	t.Run("no-match", func(t *testing.T) {
		err := errors.New("connection timeout")
		if HasSQLError(ExecResult{Stdout: "boom"}, err, ErrCodeNotSynchronized) {
			t.Fatal("expected false when neither source contains the code")
		}
	})
	t.Run("nil-err-no-stdout", func(t *testing.T) {
		if HasSQLError(ExecResult{}, nil, ErrCodePreviousError) {
			t.Fatal("expected false when neither err nor stdout set")
		}
	})
}
