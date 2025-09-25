package xy3

import (
	"compress/gzip"
	"fmt"
	"io"
)

type gzCodec struct {
}

func newGzipCompressor(dst io.Writer, opts *CompressOptions) (io.WriteCloser, error) {
	w, err := gzip.NewWriterLevel(dst, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer error: %w", err)
	}

	return w, nil
}

// implements decompressor.
var _ decompressor = &gzCodec{}

func (c *gzCodec) NewDecoder(src io.Reader) (r io.ReadCloser, err error) {
	if r, err = gzip.NewReader(src); err != nil {
		return nil, fmt.Errorf("create gzip reader error: %w", err)
	}
	return
}
