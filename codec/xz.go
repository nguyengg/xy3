package codec

import (
	"io"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/archive"
	"github.com/ulikunitz/xz"
)

// XzCodec implements Codec and Compressor for xz compression algorithm.
type XzCodec struct {
}

var _ Codec = XzCodec{}

func (c XzCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	r, err := xz.NewReader(src)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(r), nil
}

func (c XzCodec) NewEncoder(dst io.Writer) (io.WriteCloser, error) {
	return xz.NewWriter(dst)
}

var _ Compressor = XzCodec{}

func (c XzCodec) NewArchive(dst io.Writer, root string) (add archive.AddFunction, closer archive.CloseFunction, err error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, nil, err
	}

	add, closer = archive.Tar{}.Create(enc, filepath.ToSlash(root))
	return add, wrapCloser(enc, closer), nil
}

func (c XzCodec) New(dst io.Writer) (archive.AddFunction, error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, err
	}

	return func(path string, fi os.FileInfo) (io.WriteCloser, error) {
		return enc, err
	}, nil
}

func (c XzCodec) Ext(archive bool) string {
	if archive {
		return ".tar.xz"
	}

	return ".xz"
}

func (c XzCodec) ContentType() string {
	return "application/x-xz"
}
