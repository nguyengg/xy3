package xy3

import (
	"fmt"
	"io"

	"github.com/ulikunitz/xz"
)

type xzCodec struct {
}

func newXzCompressor(dst io.Writer, opts *CompressOptions) (archiver, error) {
	w, err := xz.NewWriter(dst)
	if err != nil {
		return nil, fmt.Errorf("create xz writer error: %w", err)
	}

	return &tarCodec{wc: w}, nil
}

// implements decompressor.
var _ decompressor = &xzCodec{}

func (c *xzCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	r, err := xz.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("create xz reader error: %w", err)
	}

	return io.NopCloser(r), nil
}
