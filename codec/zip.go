package codec

import (
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/archive"
)

// ZipCompressor implements Compressor for ZIP files.
type ZipCompressor struct {
}

var _ Compressor = ZipCompressor{}

func (z ZipCompressor) NewArchive(dst io.Writer, root string) (add archive.AddFunction, closer archive.CloseFunction, err error) {
	add, closer = archive.Zip{}.Create(dst, filepath.Base(root))
	return
}

func (z ZipCompressor) New(dst io.Writer) (archive.AddFunction, error) {
	add, closer := archive.Zip{}.Create(dst, "")
	return func(path string, fi os.FileInfo) (io.WriteCloser, error) {
		w, err := add(path, fi)
		if err != nil {
			return nil, err
		}

		return &zipAdder{Writer: w, closer: closer}, nil
	}, nil
}

func (z ZipCompressor) Ext(archive bool) string {
	return ".zip"
}

func (z ZipCompressor) ContentType() string {
	return "application/zip"
}

type zipAdder struct {
	io.Writer
	closer archive.CloseFunction
}

func (z *zipAdder) Close() error {
	return z.closer()
}
