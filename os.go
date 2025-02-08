package xy3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// OpenExclFile creates a new file for writing with the condition that the file did not exist prior to this call.
//
// The first string should be the stem of the filename, the second the extension. For example, the stem of
// "hello-world.txt" is "hello-world", its ext ".txt". But with "hello-world.txt.s3", filepath.Ext will think ".s3" is
// the ext while this method allows you to choose ".txt.s3" as extension instead. If you use ".txt.s3" as extension,
// the naming is more natural: it will be "hello-world-1.txt.s3" and "hello-world-2.txt.s3" instead of
// "hello-world.txt-1.s3". See StemAndExt for a variant of filepath.Ext that allows up to 4 characters to be counted as
// ext.
//
// The file is opened with flag `os.O_RDWR|os.O_CREATE|os.O_EXCL` and permission `0666`. Caller is responsible for
// closing the file upon a successful return. See MkExclDir for a dir equivalent.
func OpenExclFile(stem, ext string) (file *os.File, err error) {
	name := stem + ext
	for i := 0; ; {
		switch file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			name = strings.TrimSuffix(stem, ext) + "-" + strconv.Itoa(i) + ext
		default:
			return nil, fmt.Errorf("create file error: %w", err)
		}
	}
}

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

// MkExclDir creates a new child directory that did not exist prior to this invocation.
//
// Stem is the desired name of the directory. The actual directory that is created might have numeric suffixes such as
// stem-1, stem-2, etc. The return value "name" is the actual path to the newly created directory.
//
// The directory is created with perm `0755`.
func MkExclDir(parent, stem string) (name string, err error) {
	name = stem
	for i := 0; ; {
		switch err = os.Mkdir(filepath.Join(parent, name), 0755); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			name = stem + "-" + strconv.Itoa(i)
		default:
			return "", fmt.Errorf("create directory error: %w", err)
		}
	}
}
