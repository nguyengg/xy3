package internal

import (
	"context"
	"fmt"
	"io"
)

// CopyWithContext is a custom implementation of io.Copy that is cancellable.
func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (err error) {
	buf := make([]byte, 32*1024)

	var nr, nw int
	var read int64
	for {
		nr, err = src.Read(buf)

		if nr > 0 {
			switch nw, err = dst.Write(buf[0:nr]); {
			case err != nil:
				return err
			case nr < nw:
				return io.ErrShortWrite
			case nr != nw:
				return fmt.Errorf("invalid write: expected to write %d bytes, wrote %d bytes instead", nr, nw)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				read += int64(nr)
			}
		}

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
