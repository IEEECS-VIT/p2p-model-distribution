package filemeta

import (
	"fmt"
	"strings"
	"unicode"
)

// maxFileNameLength matches the common filesystem limit for a single
// path component (ext4, APFS, NTFS).
const maxFileNameLength = 255

// ValidateFileName checks that name is safe to use as a single path
// component on any supported platform. FileName arrives inside manifests
// fetched from untrusted peers and is used to derive the default output
// path, so anything that could escape the target directory (separators,
// "..", absolute paths, drive letters) or confuse terminals and
// filesystems (control characters, NUL) is rejected rather than rewritten.
func ValidateFileName(name string) error {
	if name == "" {
		return fmt.Errorf("file name is empty")
	}
	if len(name) > maxFileNameLength {
		return fmt.Errorf("file name exceeds %d bytes", maxFileNameLength)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("file name %q is a relative path element", name)
	}
	if strings.ContainsAny(name, `/\:`) {
		return fmt.Errorf("file name %q contains a path separator", name)
	}
	for _, r := range name {
		if r == unicode.ReplacementChar || unicode.IsControl(r) {
			return fmt.Errorf("file name %q contains a control or invalid character", name)
		}
	}
	return nil
}
