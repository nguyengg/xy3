package archive

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/util"
)

type tarWriter struct {
	*tar.Writer
	root   string
	closer io.Closer
}

func (t Tar) Create(dst io.Writer, root string) (AddFunction, CloseFunction) {
	w := &tarWriter{
		Writer: tar.NewWriter(dst),
		root:   root,
	}

	return w.add, w.close
}

func (w *tarWriter) add(path string, fi os.FileInfo) (io.WriteCloser, error) {
	// can't use path.Join because that will Clean (i.e. remove the / suffix for directory).
	path = filepath.ToSlash(path)
	if w.root != "" {
		path = w.root + "/" + path
	}

	hdr, err := tar.FileInfoHeader(fi, path)
	if err != nil {
		return nil, err
	}
	hdr.Name = path

	if err = w.WriteHeader(hdr); err != nil {
		return nil, err
	}

	return &util.WriteNoopCloser{Writer: w}, nil
}

func (w *tarWriter) close() error {
	return w.Close()
}
