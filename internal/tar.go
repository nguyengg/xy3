package internal

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

type tarCodec struct {
	wc io.WriteCloser
	tw *tar.Writer // nil until NewFile is called at least once.
	ex func(io.Reader) iter.Seq2[ArchiveFile, error]
}

// compressor.
var _ compressor = &tarCodec{}

func (tc *tarCodec) AddFile(src, dst string) error {
	dst = filepath.ToSlash(dst)

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

func (tc *tarCodec) Write(p []byte) (int, error) {
	if tc.tw != nil {
		return tc.tw.Write(p)
	}

	return tc.wc.Write(p)
}

func (tc *tarCodec) Close() (err error) {
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

func newZstdCompressor(dst io.Writer, opts *CompressOptions) (compressor, error) {
	zopts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression)}
	if opts.MaxConcurrency > 0 {
		zopts = append(zopts, zstd.WithEncoderConcurrency(opts.MaxConcurrency))
	}

	w, err := zstd.NewWriter(dst, zopts...)
	if err != nil {
		return nil, fmt.Errorf("create zstd writer error: %w", err)
	}

	return &tarCodec{wc: w}, nil
}

func newGzipCompressor(dst io.Writer, opts *CompressOptions) (compressor, error) {
	w, err := gzip.NewWriterLevel(dst, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("create gzip writer error: %w", err)
	}

	return &tarCodec{wc: w}, nil
}

func newXzCompressor(dst io.Writer, opts *CompressOptions) (compressor, error) {
	w, err := xz.NewWriter(dst)
	if err != nil {
		return nil, fmt.Errorf("create xz writer error: %w", err)
	}

	return &tarCodec{wc: w}, nil
}

// extractor.
var _ extractor = &tarCodec{}

func (tc *tarCodec) Files(src io.Reader, open bool) (iter.Seq2[ArchiveFile, error], error) {
	return tc.ex(src), nil
}

func fromTarZstReader(src io.Reader) iter.Seq2[ArchiveFile, error] {
	return func(yield func(ArchiveFile, error) bool) {
		r, err := zstd.NewReader(src)
		if err != nil {
			yield(nil, fmt.Errorf("open zstd reader error: %w", err))
			return
		}

		defer r.Close()

		for e, err := range untar(r) {
			if !yield(e, err) || err != nil {
				return
			}
		}
	}
}

func fromTarGzipReader(src io.Reader) iter.Seq2[ArchiveFile, error] {
	return func(yield func(ArchiveFile, error) bool) {
		r, err := gzip.NewReader(src)
		if err != nil {
			yield(nil, fmt.Errorf("open gzip reader error: %w", err))
			return
		}

		for e, err := range untar(r) {
			if !yield(e, err) || err != nil {
				return
			}
		}

		if err = r.Close(); err != nil {
			yield(nil, err)
		}
	}
}

func fromTarXzReader(src io.Reader) iter.Seq2[ArchiveFile, error] {
	return func(yield func(ArchiveFile, error) bool) {
		r, err := xz.NewReader(src)
		if err != nil {
			yield(nil, fmt.Errorf("open xz reader error: %w", err))
			return
		}

		for e, err := range untar(r) {
			if !yield(e, err) || err != nil {
				return
			}
		}
	}
}

func untar(src io.Reader) iter.Seq2[ArchiveFile, error] {
	tr := tar.NewReader(src)

	return func(yield func(ArchiveFile, error) bool) {
		for {
			hdr, err := tr.Next()
			if err != nil {
				if err == io.EOF {
					return
				}

				yield(nil, fmt.Errorf("read next tar entry error: %w", err))
				return
			}

			if hdr.Typeflag == tar.TypeDir {
				// TODO support creating empty directories.
				continue
			}

			if !yield(&tarEntry{hdr: hdr, ReadCloser: io.NopCloser(tr)}, nil) {
				return
			}
		}
	}
}

type tarEntry struct {
	hdr *tar.Header
	io.ReadCloser
}

func (e *tarEntry) Name() string {
	return e.hdr.Name
}

func (e *tarEntry) FileInfo() os.FileInfo {
	return e.hdr.FileInfo()
}

func (e *tarEntry) FileMode() os.FileMode {
	return os.FileMode(e.hdr.Mode)
}
