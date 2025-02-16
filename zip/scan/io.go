package scan

import (
	"io"
)

// cdReadSeeker delegates io.ReadSeeker calls to a proxy while keeping track of current offset.
// to be used by CentralDirectory.
type cdReadSeeker struct {
	io.ReadSeeker
	offset int64
}

func (r *cdReadSeeker) Read(p []byte) (n int, err error) {
	n, err = r.ReadSeeker.Read(p)
	r.offset += int64(n)
	return
}

func (r *cdReadSeeker) Seek(offset int64, whence int) (n int64, err error) {
	r.offset, err = r.ReadSeeker.Seek(offset, whence)
	return r.offset, err
}
