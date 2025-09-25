package codec

import (
	"compress/gzip"
	"io"
)

// GzipCodec implements Codec for gzip compression algorithm.
type GzipCodec struct {
}

var _ Codec = GzipCodec{}

func (c GzipCodec) NewDecoder(src io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(src)
}

func (c GzipCodec) NewEncoder(dst io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(dst, gzip.BestCompression)
}

func (c GzipCodec) Ext() string {
	return ".gz"
}

func (c GzipCodec) ContentType() string {
	return "application/gzip"
}
