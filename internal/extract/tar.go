package extract

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

type tarExtractor struct {
	f func(io.Reader) iter.Seq2[Entry, error]
}

func (t tarExtractor) Entries(src io.Reader, _ bool) (iter.Seq2[Entry, error], error) {
	return t.f(src), nil
}

func fromTarZstReader(src io.Reader) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
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

func fromTarGzipReader(src io.Reader) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
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

func fromTarXzReader(src io.Reader) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
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

func untar(src io.Reader) iter.Seq2[Entry, error] {
	tr := tar.NewReader(src)

	return func(yield func(Entry, error) bool) {
		for {
			hdr, err := tr.Next()
			if err != nil {
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
