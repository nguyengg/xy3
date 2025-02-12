package s3reader

import (
	"errors"
	"io"
	"sync"

	"github.com/valyala/bytebufferpool"
)

// writer only writes to dst if there are contiguous available parts.
type writer struct {
	parts sync.Map

	// mu guards access to the single writer of dst (leader election).
	// any number of goroutines may be adding to parts, but only one goroutine can be writing to dst.
	mu              sync.Mutex
	dst             io.Writer
	written         int64
	nextPartToWrite int
}

func (w *writer) write(partNumber int, body io.Reader) error {
	bb := bytebufferpool.Get()
	if _, err := bb.ReadFrom(body); err != nil {
		return err
	}

	if w.parts.Store(partNumber, bb); !w.mu.TryLock() {
		return nil
	}
	defer w.mu.Unlock()

	for {
		v, ok := w.parts.LoadAndDelete(w.nextPartToWrite)
		if !ok {
			return nil
		}

		bb := v.(*bytebufferpool.ByteBuffer)
		n, err := bb.WriteTo(w.dst)
		bytebufferpool.Put(bb)
		w.written += n

		if err != nil {
			return err
		}

		w.nextPartToWrite++
	}
}

var errMissingPart = errors.New("missing part to write")

// drain should only be called once all the parts have been downloaded, and only writing to dst remains.
// if there is a gap in the parts, drain returns errMissingPart. otherwise, drain returns writer.err.
func (w *writer) drain() (written int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		v, ok := w.parts.LoadAndDelete(w.nextPartToWrite)
		if !ok {
			break
		}

		bb := v.(*bytebufferpool.ByteBuffer)
		n, err := bb.WriteTo(w.dst)
		bytebufferpool.Put(bb)
		w.written += n

		if err != nil {
			return w.written, err
		}

		w.nextPartToWrite++
	}

	w.parts.Range(func(_, _ any) bool {
		err = errMissingPart
		return false
	})

	return w.written, err
}

func (w *writer) close() {
	w.parts.Clear()
}
