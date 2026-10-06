package main

import (
	"fmt"
	"os"
)

// console prints user-facing status lines. Colors and the in-place
// progress line are only used when stdout is a terminal and NO_COLOR is
// unset (https://no-color.org), so piped or logged output stays clean.
type console struct {
	color       bool
	tty         bool
	progressing bool
}

var ui = newConsole()

func newConsole() *console {
	info, err := os.Stdout.Stat()
	tty := err == nil && info.Mode()&os.ModeCharDevice != 0
	_, noColor := os.LookupEnv("NO_COLOR")
	return &console{tty: tty, color: tty && !noColor}
}

func (c *console) paint(code, s string) string {
	if !c.color {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func (c *console) bold(s string) string { return c.paint("1", s) }

func (c *console) line(tag, code, format string, args ...any) {
	c.endProgress()
	fmt.Printf("%s %s\n", c.paint(code, tag), fmt.Sprintf(format, args...))
}

func (c *console) infof(format string, args ...any)    { c.line("[info]", "36", format, args...) }
func (c *console) successf(format string, args ...any) { c.line("[ ok ]", "32", format, args...) }
func (c *console) warnf(format string, args ...any)    { c.line("[warn]", "33", format, args...) }

func (c *console) errorf(format string, args ...any) {
	c.endProgress()
	fmt.Fprintf(os.Stderr, "%s %s\n", c.paint("31", "[fail]"), fmt.Sprintf(format, args...))
}

func (c *console) header() {
	fmt.Println(c.paint("1;36", "P2P model distribution node"))
}

// progress renders download progress: an in-place line on a terminal,
// otherwise a line every 10%.
func (c *console) progress(done, total int) {
	if total == 0 {
		return
	}
	pct := done * 100 / total
	if c.tty {
		fmt.Printf("\r%s chunks %d/%d [%d%%]", c.paint("[....]", "33"), done, total, pct)
		c.progressing = true
		return
	}
	if prev := (done - 1) * 100 / total; done == total || pct/10 != prev/10 {
		fmt.Printf("[....] chunks %d/%d [%d%%]\n", done, total, pct)
	}
}

func (c *console) endProgress() {
	if c.progressing {
		fmt.Println()
		c.progressing = false
	}
}
