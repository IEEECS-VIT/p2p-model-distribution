package main

import (
	"os"
	"strings"
	"testing"
)

func TestParseFlags(t *testing.T) {
	// Silence flag package usage output for rejected cases.
	stderr := os.Stderr
	os.Stderr, _ = os.Open(os.DevNull)
	defer func() { os.Stderr = stderr }()

	o, err := parseFlags([]string{"-bootstrap", " 10.0.0.1:9000, ,10.0.0.2:9000", "-download", "abc", "-peer", "10.0.0.3:9000"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if len(o.bootstrap) != 2 || o.bootstrap[1] != "10.0.0.2:9000" || len(o.peers) != 1 {
		t.Fatalf("lists not parsed: %+v", o)
	}

	bad := [][]string{
		{"-chunk-kb", "0"},
		{"-chunk-kb", "9000"},
		{"-out", "x"},               // requires -download
		{"-download", "abc"},        // needs a way to find providers
		{"-log-level", "loud"},      // invalid level
		{"-seed", "a", "stray-arg"}, // unexpected positional argument
		{"-exit-after-download"},    // requires -download
		{"-timeout", "1m"},          // requires -download
	}
	for _, args := range bad {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("parseFlags(%q) succeeded, want error", args)
		}
	}
}

func TestVersionFlag(t *testing.T) {
	o, err := parseFlags([]string{"-version", "-chunk-kb", "0"})
	if err != nil || !o.showVersion {
		t.Fatalf("parseFlags(-version) = %+v, %v; want showVersion without validation errors", o, err)
	}

	orig := version
	version = "v9.9.9"
	defer func() { version = orig }()
	if got := versionString(); !strings.HasPrefix(got, "node v9.9.9 (commit ") {
		t.Fatalf("versionString() = %q", got)
	}
}
