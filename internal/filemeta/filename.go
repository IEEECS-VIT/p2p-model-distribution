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
// component on every supported platform (Linux, macOS and Windows). FileName arrives inside manifests
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
	// The rest keeps names valid on Windows, since a file shared from any
	// OS may be downloaded there.
	if strings.ContainsAny(name, `<>"|?*`) {
		return fmt.Errorf("file name %q contains a character not allowed on Windows", name)
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return fmt.Errorf("file name %q ends with a dot or space", name)
	}
	if isWindowsReservedName(name) {
		return fmt.Errorf("file name %q is a reserved device name on Windows", name)
	}
	for _, r := range name {
		if r == unicode.ReplacementChar || unicode.IsControl(r) {
			return fmt.Errorf("file name %q contains a control or invalid character", name)
		}
	}
	return nil
}

// isWindowsReservedName reports whether name is a Windows device name
// (CON, PRN, AUX, NUL, COM0-9, LPT0-9, including the superscript-digit
// variants), with or without an extension and in any case. Opening such a
// path on Windows refers to the device, not a file.
func isWindowsReservedName(name string) bool {
	base, _, _ := strings.Cut(name, ".")
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return true
	}
	if r := []rune(base); len(r) == 4 && (string(r[:3]) == "COM" || string(r[:3]) == "LPT") {
		return unicode.IsDigit(r[3]) || strings.ContainsRune("¹²³", r[3])
	}
	return false
}
