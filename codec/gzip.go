package codec

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/archive"
)

// GzipCodec implements Codec and Compressor for gzip compression algorithm.
type GzipCodec struct {
}

var _ Codec = GzipCodec{}

func (c GzipCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(src)
}

func (c GzipCodec) NewEncoder(dst io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(dst, gzip.BestCompression)
}

var _ Compressor = GzipCodec{}

func (c GzipCodec) NewArchive(dst io.Writer, root string) (add archive.AddFunction, closer archive.CloseFunction, err error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, nil, err
	}

	add, closer = archive.Tar{}.Create(enc, filepath.ToSlash(root))
	return add, wrapCloser(enc, closer), nil
}

func (c GzipCodec) New(dst io.Writer) (archive.AddFunction, error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, err
	}

	return func(path string, fi os.FileInfo) (io.WriteCloser, error) {
		return enc, err
	}, nil
}

func (c GzipCodec) Ext(archive bool) string {
	if archive {
		return ".tar.gz"
	}

	return ".gz"
}

func (c GzipCodec) ContentType() string {
	return "application/gzip"
}

func wrapCloser(c io.Closer, closer archive.CloseFunction) archive.CloseFunction {
	return func() (err error) {
		err = closer()

		if err != nil {
			_ = c.Close()
		} else {
			err = c.Close()
		}

		return
	}
}
