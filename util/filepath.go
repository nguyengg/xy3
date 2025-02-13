package util

import "path/filepath"

// StemAndExt is a variant of filepath.Ext that allows extended extension to be detected while also returning the stem.
//
// For example, `filepath.Ext("file.tar.gz")` would return ".gz", but `xy3.StemAndExt("file.tar.gz")` would return
// ".tar.gz" for the extension, "file" for the stem. This is useful when passed to OpenExclFile: "file-1.tar.gz" is more
// natural than "file.tar-1.gz".
//
// StemAndExt will only accept file extensions of 5 characters or less, so if there is no `.` in the last 6 characters,
// the returned ext will be empty string unlike filepath.Ext which will keep searching until the last path separator or
// `.` is found. That means longer extensions such as ".jfif-tbnl" or ".turbot" will not be found by StemAndExt but can
// be found by filepath.Ext.
func StemAndExt(path string) (stem, ext string) {
	n := len(path) - 1
	for i, j := n, max(0, n-6); i >= j; i-- {
		switch path[i] {
		case '\\', '/':
			stem = path[i+1:]
			return
		case '.':
			ext = path[i:] + ext
			path = path[:i]
			n = len(path)
			i, j = n, max(0, n-6)
			continue
		}
	}

	stem = filepath.Base(path)
	return
}
