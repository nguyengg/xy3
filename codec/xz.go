package codec

import (
	"io"

	"github.com/ulikunitz/xz"
)

// XzCodec implements Codec for xz compression algorithm.
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

func (c XzCodec) Ext() string {
	return ".xz"
}

func (c XzCodec) ContentType() string {
	return "application/x-xz"
}
