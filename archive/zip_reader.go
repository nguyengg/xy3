package archive

import (
	"archive/zip"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/krolaw/zipstream"
)

// Zip implements Archiver for ZIP files.
type Zip struct {
}

var _ Archiver = Zip{}

func (z Zip) Open(src io.Reader) (iter.Seq2[File, error], error) {
	if f, ok := src.(*os.File); ok {
		return fromZipFile(f)
	}

	return fromZipReader(src)
}

func fromZipReader(src io.Reader) (iter.Seq2[File, error], error) {
	zr := zipstream.NewReader(src)

	return func(yield func(File, error) bool) {
		for {
			fh, err := zr.Next()
			if err == io.EOF {
				return
			}

			if !yield(&zipFile{
				FileHeader: fh,
				open: func() (io.ReadCloser, error) {
					return io.NopCloser(zr), nil
				},
			}, err) || err != nil {
				return
			}
		}
	}, nil
}

func fromZipFile(src *os.File) (iter.Seq2[File, error], error) {
	fi, err := src.Stat()
	if err != nil {
		return nil, fmt.Errorf(`stat file "%s" error: %w`, src.Name(), err)
	}

	zr, err := zip.NewReader(src, fi.Size())
	if err != nil {
		return nil, fmt.Errorf(`open zip file "%s" error: %w`, src.Name(), err)
	}

	return func(yield func(File, error) bool) {
		for _, zf := range zr.File {
			if !yield(&zipFile{
				FileHeader: &zf.FileHeader,
				open:       zf.Open,
			}, nil) {
				return
			}
		}
	}, nil
}

type zipFile struct {
	*zip.FileHeader
	open func() (io.ReadCloser, error)
}

var _ File = &zipFile{}

func (f *zipFile) Name() string {
	return f.FileHeader.Name
}

func (f *zipFile) Open() (io.ReadCloser, error) {
	return f.open()
}
