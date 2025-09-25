package codec

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// ZstdCodec implements Codec and Archiver for zstd compression algorithm.
type ZstdCodec struct{}

var _ Codec = ZstdCodec{}

func (c ZstdCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(src)
	return &zstdDecoder{dec}, err
}

type zstdDecoder struct {
	*zstd.Decoder
}

func (d *zstdDecoder) Close() error {
	d.Decoder.Close()
	return nil
}

func (c ZstdCodec) NewEncoder(dst io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
}

func (c ZstdCodec) Ext() string {
	return ".zst"
}

func (c ZstdCodec) ContentType() string {
	return "application/zstd"
}
