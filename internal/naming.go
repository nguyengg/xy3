package internal

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
// The first string should be the stem of the filename, the second the extension. If filename is "hello world.txt" then
// the stem is "hello world", ext is ".txt".
// See [os.O_EXCL]. Caller is responsible for closing the file upon a successful return.
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

// MkExclDir creates a new directory with the condition that the directory did not exist prior to this call.
//
// Stem is the desired name of the directory. The actual directory that is created might have numeric suffixes in its
// name which is the first return value.
func MkExclDir(stem string) (name string, err error) {
	name = stem
	for i := 0; ; {
		switch err = os.Mkdir(name, 0755); {
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

// SplitStemAndExt splits the given name into the stem and extension part.
//
// The extension starts at the final dot. If there is no dot, ext is empty string.
func SplitStemAndExt(name string) (stem string, ext string) {
	name = filepath.Base(name)
	ext = filepath.Ext(name)
	stem = strings.TrimSuffix(name, ext)
	return
}
