package compress

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type tarCompressor struct {
	concurrency int
	buf         []byte
	wc          io.WriteCloser
	tw          *tar.Writer // nil until NewFile is called at least once.
}

func (tc *tarCompressor) NewFile(src, dst string) error {
	if tc.tw == nil {
		tc.tw = tar.NewWriter(tc.wc)
	}

	fi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, src, err)
	}

	hdr, err := tar.FileInfoHeader(fi, dst)
	if err != nil {
		return fmt.Errorf(`create tar header for "%s" error: %w`, src, err)
	}
	hdr.Name = dst

	if err = tc.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf(`write tar header for "%s" error: %w`, src, err)
	}

	return nil
}

func (tc *tarCompressor) Write(p []byte) (int, error) {
	if tc.tw != nil {
		return tc.tw.Write(p)
	}

	return tc.wc.Write(p)
}

func (tc *tarCompressor) Close() (err error) {
	if tc.tw != nil {
		if err = tc.tw.Close(); err != nil {
			return fmt.Errorf("close tar writer error: %w", err)
		}
	}

	if err = tc.wc.Close(); err != nil {
		return fmt.Errorf("close compressor error: %w", err)
	}

	return nil
}

func newZSTDCompressor(dst io.Writer, opts *Options) (compressor, error) {
	ze, err := zstd.NewWriter(
		dst,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(opts.MaxConcurrency))
	if err != nil {
		return nil, fmt.Errorf("create zstd writer error: %w", err)
	}

	return &tarCompressor{
		concurrency: opts.MaxConcurrency,
		buf:         make([]byte, opts.BufferSize),
		wc:          ze,
	}, nil
}

func newGZIPCompressor(dst io.Writer, opts *Options) (compressor, error) {
	gw, err := gzip.NewWriterLevel(dst, gzip.DefaultCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer error: %w", err)
	}

	return &tarCompressor{
		concurrency: opts.MaxConcurrency,
		buf:         make([]byte, opts.BufferSize),
		wc:          gw,
	}, nil
}

var _ compressor = &tarCompressor{}
