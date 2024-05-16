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
// See [os.O_EXCL]. Caller is responsible for closing the file upon a successful return.
func OpenExclFile(basename, ext string) (file *os.File, err error) {
	name := basename + ext
	for i := 0; ; {
		switch file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); {
		case err == nil:
			return
		case errors.Is(err, os.ErrExist):
			i++
			name = strings.TrimSuffix(basename, ext) + "-" + strconv.Itoa(i) + ext
		default:
			return nil, fmt.Errorf("create file error: %w", err)
		}
	}
}
