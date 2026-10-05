//go:build windows

package identity

import "os"

// checkKeyPermissions is a no-op on Windows. Windows has no Unix
// permission bits: Go reports every writable file as 0666, so the Unix
// check would reject every key. Access is governed by ACLs instead, and a
// key created under the user's profile is only accessible to that user
// (and administrators) by default.
func checkKeyPermissions(string, os.FileInfo) error {
	return nil
}
