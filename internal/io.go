package internal

import (
	"fmt"
	"io"
)

// Sizer implements io.Writer that tallies that number of bytes written.
type Sizer struct {
	Size int64
}

func (s *Sizer) Write(p []byte) (n int, err error) {
	n = len(p)
	s.Size += int64(n)
	return
}

// ResettableReadSeeker saves the read offset of the given io.ReadSeeker, applies the callback, then resets the read
// offset.
func ResettableReadSeeker(r io.ReadSeeker, cb func(io.ReadSeeker) error) error {
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("save read offset error: %w", err)
	}

	if err = cb(r); err != nil {
		return err
	}

	if _, err = r.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("reset read offset error: %w", err)
	}

	return nil
}
