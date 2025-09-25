package codec

import (
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/archive"
	"github.com/ulikunitz/xz"
)

// xzCodec implements Codec and Compressor for xz compression algorithm.
type xzCodec struct {
}

var _ Codec = xzCodec{}

func (c xzCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	r, err := xz.NewReader(src)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(r), nil
}

func (c xzCodec) NewEncoder(dst io.Writer) (io.WriteCloser, error) {
	return xz.NewWriter(dst)
}

var _ Compressor = xzCodec{}

func (c xzCodec) NewArchive(dst io.Writer, root string) (add archive.AddFunction, closer archive.CloseFunction, err error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, nil, err
	}

	add, closer = archive.Tar{}.Create(enc, filepath.ToSlash(root))
	return add, wrapCloser(enc, closer), nil
}

func (c xzCodec) New(dst io.Writer) (archive.AddFunction, error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, err
	}

	return func(path string, fi os.FileInfo) (io.WriteCloser, error) {
		return enc, err
	}, nil
}

func (c xzCodec) Ext(archive bool) string {
	if archive {
		return ".tar.xz"
	}

	return ".xz"
}

func (c xzCodec) ContentType() string {
	return "application/x-xz"
}
