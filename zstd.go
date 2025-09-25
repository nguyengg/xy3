package xy3

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

type zstdCodec struct {
}

func newZstdCompressor(dst io.Writer, opts *CompressOptions) (io.WriteCloser, error) {
	zopts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression)}
	if opts.MaxConcurrency > 0 {
		zopts = append(zopts, zstd.WithEncoderConcurrency(opts.MaxConcurrency))
	}

	w, err := zstd.NewWriter(dst, zopts...)
	if err != nil {
		return nil, fmt.Errorf("create zstd writer error: %w", err)
	}

	return w, nil
}

// implements decompressor.
var _ decompressor = &zstdCodec{}

func (c *zstdCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	r, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("create zstd reader error: %w", err)
	}

	return &zstdDecoder{Decoder: r}, nil
}

type zstdDecoder struct {
	*zstd.Decoder
}

// Close adapts zstd.Decoder.Close which doesn't return error.
func (z *zstdDecoder) Close() error {
	z.Decoder.Close()
	return nil
}
