package extract

import (
	"archive/zip"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/krolaw/zipstream"
)

func FromZipReader(src io.Reader) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		zr := zipstream.NewReader(src)

		for {
			fh, err := zr.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				yield(nil, fmt.Errorf("stream zip error: %w", err))
				return
			}

			if fh.FileInfo().IsDir() {
				// TODO support creating empty directories.
				continue
			}

			if !yield(&zipEntry{fh: fh, ReadCloser: io.NopCloser(zr)}, nil) {
				return
			}
		}
	}
}

func FromZipFile(src *os.File) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		fi, err := src.Stat()
		if err != nil {
			yield(nil, fmt.Errorf(`stat file "%s" error: %w`, src.Name(), err))
			return
		}

		zr, err := zip.NewReader(src, fi.Size())
		if err != nil {
			yield(nil, fmt.Errorf(`open zip file "%s" error: %w`, src.Name(), err))
			return
		}

		for _, f := range zr.File {
			if fi = f.FileInfo(); fi.IsDir() {
				// TODO support creating empty directories.
				continue
			}

			// we'll always open the file for reading for now. caller is responsible for closing it.
			rc, err := f.Open()
			if err != nil {
				yield(nil, fmt.Errorf(`open entry "%s" error: %w`, f.Name, err))
				return
			}

			if !yield(&zipEntry{fh: &f.FileHeader, ReadCloser: rc}, nil) {
				return
			}
		}
	}
}

type zipEntry struct {
	fh *zip.FileHeader
	io.ReadCloser
}

func (e *zipEntry) Name() string {
	return e.fh.Name
}

func (e *zipEntry) FileInfo() os.FileInfo {
	return e.fh.FileInfo()
}

func (e *zipEntry) FileMode() os.FileMode {
	return e.fh.Mode()
}
