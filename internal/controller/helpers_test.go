/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"slices"
	"testing"

	sqlv1alpha1 "github.com/anocentino/sql-on-k8s-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodNameForReplica(t *testing.T) {
	ag := &sqlv1alpha1.SQLServerAvailabilityGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "mssql-ag"},
	}
	for i, want := range []string{"mssql-ag-0", "mssql-ag-1", "mssql-ag-2"} {
		if got := podNameForReplica(ag, i); got != want {
			t.Errorf("podNameForReplica(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestBuildMSSQLConfDeterministic(t *testing.T) {
	conf := map[string]string{
		"memory.memorylimitmb": "4096",
		"network.tcpport":      "1433",
		"memory.memorymodel":   "conventional",
		"hadr.hadrenabled":     "1",
		"toplevel":             "value",
	}
	first := buildMSSQLConf(conf)
	for range 20 {
		got := buildMSSQLConf(conf)
		if got != first {
			t.Fatalf("buildMSSQLConf output changed between invocations\nfirst:\n%s\ngot:\n%s", first, got)
		}
	}
}

func TestBuildMSSQLConfGroupsSections(t *testing.T) {
	conf := map[string]string{
		"memory.memorylimitmb": "4096",
		"network.tcpport":      "1433",
		"memory.memorymodel":   "conventional",
	}
	got := buildMSSQLConf(conf)
	// Basic sanity: both [memory] and [network] headers must appear.
	for _, header := range []string{"[memory]", "[network]"} {
		if !containsLine(got, header) {
			t.Errorf("expected header %q in:\n%s", header, got)
		}
	}
}

func containsLine(s, needle string) bool {
	return slices.Contains(split(s, '\n'), needle)
}

func split(s string, sep byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}
