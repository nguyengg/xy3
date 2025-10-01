package util

import (
	"errors"
	"io"
	"strings"
)

type resetOnCloseReadSeeker struct {
	src    io.ReadSeeker
	offset int64
	err    error
}

// ResetOnCloseReadSeeker will reset the src io.ReadSeeker's read offset to the original value upon closing.
//
// Error from capturing the original read offset will be returned by the Read, Seek, and Close methods to prevent
// draining of the src io.ReadSeeker. Error from resetting the read offset will be returned only by the Close method.
func ResetOnCloseReadSeeker(src io.ReadSeeker) io.ReadSeekCloser {
	r := &resetOnCloseReadSeeker{src: src}
	r.offset, r.err = src.Seek(0, io.SeekCurrent)
	return r
}

func (r *resetOnCloseReadSeeker) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	return r.src.Read(p)
}

func (r *resetOnCloseReadSeeker) Seek(offset int64, whence int) (int64, error) {
	if r.err != nil {
		return r.offset, r.err
	}

	return r.src.Seek(offset, whence)
}

func (r *resetOnCloseReadSeeker) Close() error {
	if r.err != nil {
		return r.err
	}

	_, r.err = r.Seek(r.offset, io.SeekStart)
	return r.err
}

// WriteNoopCloser implements a no-op io.Closer for an io.Writer.
type WriteNoopCloser struct {
	io.Writer
}

func (w *WriteNoopCloser) Close() error {
	return nil
}

// ChainCloser makes sure all the close functions are called at least once and will return the first error that wraps
// subsequent errors.
//
// The order of wrapping assumes the first close function is the most important.
func ChainCloser(fn1 func() error, fn2 func() error, fns ...func() error) func() error {
	return func() error {
		err, err2 := fn1(), fn2()

		if err2 != nil && err == nil {
			err = err2
		}

		for _, fn := range fns {
			if err2 = fn(); err2 != nil && err == nil {
				err = err2
			}
		}

		return err
	}
}

type chainedError struct {
	cause, next error
}

func (c *chainedError) Error() string {
	next := c.next
	if next == nil {
		return c.cause.Error()
	}

	var sb strings.Builder
	sb.WriteString(c.cause.Error())

	for next != nil {
		var ce *chainedError
		if !errors.As(next, &ce) {
			sb.WriteString(", " + next.Error())
			break
		}

		sb.WriteString(", " + ce.cause.Error())
		next = ce.next
	}

	return sb.String()
}

func (c *chainedError) Unwrap() []error {
	if c.next == nil {
		return []error{c.cause}
	}

	return []error{c.cause, c.next}
}
