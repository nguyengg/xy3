package xy3

import (
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/bodgit/sevenzip"
)

type sevenZipCodec struct {
}

// extractor.
var _ extractor = &sevenZipCodec{}

func (s *sevenZipCodec) Files(src io.Reader, open bool) (iter.Seq2[archiveFile, error], error) {
	if f, ok := src.(*os.File); ok {
		return from7zFile(f, open), nil
	}

	// TODO find an implementation of 7z reader that receives just io.Reader
	return nil, fmt.Errorf("7z archives must be opened as os.File")
}

func from7zFile(src *os.File, open bool) iter.Seq2[archiveFile, error] {
	return func(yield func(archiveFile, error) bool) {
		fi, err := src.Stat()
		if err != nil {
			yield(nil, fmt.Errorf(`stat file "%s" error: %w`, src.Name(), err))
			return
		}

		zr, err := sevenzip.NewReader(src, fi.Size())
		if err != nil {
			yield(nil, fmt.Errorf(`open 7z file "%s" error: %w`, src.Name(), err))
			return
		}

		for _, f := range zr.File {
			if fi = f.FileInfo(); fi.IsDir() {
				// TODO support creating empty directories.
				continue
			}

			var rc io.ReadCloser
			if open {
				rc, err = f.Open()
				if err != nil {
					yield(nil, fmt.Errorf(`open entry "%s" error: %w`, f.Name, err))
					return
				}
			}

			if !yield(&sevenZipEntry{fh: &f.FileHeader, ReadCloser: rc}, nil) {
				return
			}
		}
	}
}

type sevenZipEntry struct {
	fh *sevenzip.FileHeader
	io.ReadCloser
}

var _ archiveFile = &sevenZipEntry{}

func (e *sevenZipEntry) Name() string {
	return e.fh.Name
}

func (e *sevenZipEntry) FileInfo() os.FileInfo {
	return e.fh.FileInfo()
}

func (e *sevenZipEntry) FileMode() os.FileMode {
	return e.fh.Mode()
}
