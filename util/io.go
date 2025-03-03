package util

import (
	"context"
	"fmt"
	i0 "io"
)

// CopyBufferWithContext is a custom implementation of io.CopyBuffer that is cancellable via context.
//
// Similar to io.CopyBuffer, if buf is nil, a new buffer of size 32*1024 is created.
// Unlike io.CopyBuffer, it does not matter if src implements [io.WriterTo] or dst implements [io.ReaderFrom] because
// those interfaces do not support context.
//
// The context is checked for done status after every write. As a result, having too small a buffer may introduce too
// much overhead, while having a very large buffer may cause context cancellation to have a delayed effect.
func CopyBufferWithContext(ctx context.Context, dst i0.Writer, src i0.Reader, buf []byte) (written int64, err error) {
	if buf == nil {
		buf = make([]byte, 32*1024)
	}

	var nr, nw int
	var read int64
	for {
		nr, err = src.Read(buf)

		if nr > 0 {
			switch nw, err = dst.Write(buf[0:nr]); {
			case err != nil:
				return written, err
			case nr < nw:
				return written, i0.ErrShortWrite
			case nr != nw:
				return written, fmt.Errorf("invalid write: expected to write %d bytes, wrote %d bytes instead", nr, nw)
			}

			select {
			case <-ctx.Done():
				return written, ctx.Err()
			default:
				read += int64(nr)
				written += int64(nw)
			}
		}

		if err == i0.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
}
