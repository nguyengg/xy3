package internal

import (
	"archive/zip"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type zipCompressor struct {
	zw *zip.Writer
	fw io.Writer // nil until NewFile is called at least once.
}

func (c *zipCompressor) AddFile(src, dst string) error {
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

func (c *zipCompressor) Write(p []byte) (n int, err error) {
	if c.fw == nil {
		return 0, fmt.Errorf("NewFile has not been called")
	}

	return c.fw.Write(p)
}

func (c *zipCompressor) Close() error {
	return c.zw.Close()
}

func newZipCompressor(dst io.Writer, opts *CompressOptions) *zipCompressor {
	zw := zip.NewWriter(dst)
	zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})

	return &zipCompressor{zw: zw}
}

var _ compressor = &zipCompressor{}
