//go:build !windows

package identity

import (
	"fmt"
	"os"
)

// checkKeyPermissions refuses a key file that group or others can access,
// as OpenSSH does for private keys.
func checkKeyPermissions(path string, info os.FileInfo) error {
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("identity key %s has permissions %#o; it must not be accessible by group or others (chmod 600)", path, perm)
	}
	return nil
}
