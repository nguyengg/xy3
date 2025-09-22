package util

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// OpenExclFile creates a new file for writing with the condition that the file did not exist prior to this call.
//
// The first argument is the parent directory of the file to be created. The second argument is the stem of the file,
// the third the extension. For example, the stem of "hello-world.txt" is "hello-world", its ext ".txt". But with
// "hello-world.txt.s3", filepath.Ext will think ".s3" is the ext while this method allows you to choose ".txt.s3" as
// extension instead. If you use ".txt.s3" as extension, the naming is more natural: it will be "hello-world-1.txt.s3"
// or "hello-world-2.txt.s3" instead of "hello-world.txt-1.s3". See StemAndExt for a variant of filepath.Ext that allows
// up to 6 characters to be counted as ext.
//
// The file is opened with flag `os.O_RDWR|os.O_CREATE|os.O_EXCL`. Caller is responsible for closing the file upon a
// successful return. See MkExclDir for a dir equivalent.
func OpenExclFile(parent, stem, ext string, perm os.FileMode) (file *os.File, err error) {
	name := filepath.Join(parent, stem+ext)
	for i := 0; ; {
		switch file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			name = filepath.Join(parent, fmt.Sprintf("%s-%d%s", stem, i, ext))
		default:
			return nil, fmt.Errorf("create file error: %w", err)
		}
	}
}

// MkExclDir creates a new child directory that did not exist prior to this invocation.
//
// Stem is the desired name of the directory. The actual directory that is created might have numeric suffixes such as
// stem-1, stem-2, etc. The return value "name" is the actual path to the newly created directory.
func MkExclDir(parent, stem string, perm os.FileMode) (name string, err error) {
	name = filepath.Join(parent, stem)
	for i := 0; ; {
		switch err = os.Mkdir(name, perm); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			name = filepath.Join(parent, stem+"-"+strconv.Itoa(i))
		default:
			return "", fmt.Errorf("create directory error: %w", err)
		}
	}
}
