package compress

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

type tarCompressor struct {
	wc io.WriteCloser
	tw *tar.Writer // nil until NewFile is called at least once.
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

func newZstdCompressor(dst io.Writer, opts *Options) (compressor, error) {
	w, err := zstd.NewWriter(
		dst,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(opts.MaxConcurrency))
	if err != nil {
		return nil, fmt.Errorf("create zstd writer error: %w", err)
	}

	return &tarCompressor{wc: w}, nil
}

func newGzipCompressor(dst io.Writer, opts *Options) (compressor, error) {
	w, err := gzip.NewWriterLevel(dst, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer error: %w", err)
	}

	return &tarCompressor{wc: w}, nil
}

func newXzCompressor(dst io.Writer, opts *Options) (compressor, error) {
	w, err := xz.NewWriter(dst)
	if err != nil {
		return nil, fmt.Errorf("create xz writer error: %w", err)
	}

	return &tarCompressor{wc: w}, nil
}

var _ compressor = &tarCompressor{}
