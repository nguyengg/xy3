package xy3

import (
	"archive/zip"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/krolaw/zipstream"
)

type zipCodec struct {
	zw *zip.Writer
	fw io.Writer // nil until NewFile is called at least once.
}

// archiver.
var _ archiver = &zipCodec{}

func (c *zipCodec) AddFile(src, dst string) error {
	dst = filepath.ToSlash(dst)

	fi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, src, err)
	}

	fh := &zip.FileHeader{
		Name:     strings.ReplaceAll(dst, "\\", "/"),
		Modified: fi.ModTime(),
	}
	fh.SetMode(fi.Mode())

	if c.fw, err = c.zw.CreateHeader(fh); err != nil {
		return fmt.Errorf(`create zip header for "%s" error: %w`, src, err)
	}

	return nil
}

func (c *zipCodec) Write(p []byte) (n int, err error) {
	if c.fw == nil {
		return 0, fmt.Errorf("AddFile has not been called")
	}

	return c.fw.Write(p)
}

func (c *zipCodec) Close() error {
	return c.zw.Close()
}

// extractor.
var _ extractor = &zipCodec{}

func (c *zipCodec) Files(src io.Reader, open bool) (iter.Seq2[archiveFile, error], error) {
	if f, ok := src.(*os.File); ok {
		return fromZipFile(f), nil
	}

	return fromZipReader(src), nil
}

func fromZipReader(src io.Reader) iter.Seq2[archiveFile, error] {
	return func(yield func(archiveFile, error) bool) {
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

func fromZipFile(src *os.File) iter.Seq2[archiveFile, error] {
	return func(yield func(archiveFile, error) bool) {
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

var _ archiveFile = &zipEntry{}

func (e *zipEntry) Name() string {
	return e.fh.Name
}

func (e *zipEntry) FileInfo() os.FileInfo {
	return e.fh.FileInfo()
}

func (e *zipEntry) FileMode() os.FileMode {
	return e.fh.Mode()
}
