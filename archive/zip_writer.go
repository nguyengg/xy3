package archive

import (
	"archive/zip"
	"compress/flate"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/util"
)

type zipWriter struct {
	*zip.Writer
	root string
}

func (z Zip) Create(dst io.Writer, root string) (AddFunction, CloseFunction) {
	w := &zipWriter{
		Writer: zip.NewWriter(dst),
		root:   root,
	}
	w.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})

	return w.add, w.close
}

func (w *zipWriter) add(path string, fi os.FileInfo) (io.WriteCloser, error) {
	// can't use path.Join because that will Clean (i.e. remove the / suffix for directory).
	path = filepath.ToSlash(path)
	if w.root != "" {
		path = w.root + "/" + path
	}

	fh := &zip.FileHeader{
		Name:     path,
		Method:   zip.Deflate,
		Modified: fi.ModTime(),
	}
	fh.SetMode(fi.Mode())

	fw, err := w.CreateHeader(fh)
	if err != nil {
		return nil, fmt.Errorf("create zip file error: %w", err)
	}

	return &util.WriteNoopCloser{Writer: fw}, nil
}

func (w *zipWriter) close() error {
	return w.Close()
}
