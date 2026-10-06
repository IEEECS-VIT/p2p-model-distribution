package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Set at build time by release builds, e.g.
//
//	go build -ldflags "-X main.version=v1.2.3 -X main.commit=abc123 -X main.date=2026-01-02T15:04:05Z"
var (
	version = "dev"
	commit  = ""
	date    = ""
)

// versionString describes the running binary. When the build was not
// stamped (e.g. go build / go install), the commit and time Go embeds from
// the git checkout are used instead.
func versionString() string {
	c, d, dirty := commit, date, false
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if c == "" {
					c = s.Value
				}
			case "vcs.time":
				if d == "" {
					d = s.Value
				}
			case "vcs.modified":
				dirty = s.Value == "true" && commit == ""
			}
		}
	}
	if c == "" {
		c = "unknown"
	} else if len(c) > 12 {
		c = c[:12]
	}
	if dirty {
		c += "-dirty"
	}
	if d == "" {
		d = "unknown"
	}
	return fmt.Sprintf("p2pmd %s (commit %s, built %s, %s %s/%s)", version, c, d, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
