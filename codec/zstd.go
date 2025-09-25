package codec

import (
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/nguyengg/xy3/archive"
)

// ZstdCodec implements Codec and Compressor for zstd compression algorithm.
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

var _ Compressor = ZstdCodec{}

func (c ZstdCodec) NewArchive(dst io.Writer, root string) (add archive.AddFunction, closer archive.CloseFunction, err error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, nil, err
	}

	add, closer = archive.Tar{}.Create(enc, filepath.ToSlash(root))
	return add, wrapCloser(enc, closer), nil
}

func (c ZstdCodec) New(dst io.Writer) (archive.AddFunction, error) {
	enc, err := c.NewEncoder(dst)
	if err != nil {
		return nil, err
	}

	return func(path string, fi os.FileInfo) (io.WriteCloser, error) {
		return enc, err
	}, nil
}

func (c ZstdCodec) Ext(archive bool) string {
	if archive {
		return ".tar.zst"
	}

	return ".zst"
}

func (c ZstdCodec) ContentType() string {
	return "application/zstd"
}
