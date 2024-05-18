package internal

import (
	"errors"
	"fmt"
	"os"
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
