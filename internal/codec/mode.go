package codec

import (
	"fmt"
	"io"
)

type Mode int

const (
	UNKNOWN Mode = iota
	ZSTD
	ZIP
	GZIP
)

func (m Mode) ContentType() string {
	switch m {
	case ZSTD:
		return "application/zstd"
	case ZIP:
		return "application/zip"
	case GZIP:
		return "application/gzip"
	default:
		panic(fmt.Sprintf("unknown mode: %v", m))
	}
}

func (m Mode) Ext() string {
	switch m {
	case ZSTD:
		return ".zst"
	case ZIP:
		return ".zip"
	case GZIP:
		return ".gz"
	default:
		panic(fmt.Sprintf("unknown mode: %v", m))
	}
}

func (m Mode) createCompressor(dst io.Writer, opts *CompressOptions) (compressor, error) {
	switch m {
	case ZSTD:
		return newZSTDCompressor(dst, opts)
	case ZIP:
		return newZIPCompressor(dst, opts), nil
	case GZIP:
		return newGZIPCompressor(dst, opts), nil
	default:
		return nil, fmt.Errorf("unknown mode: %v", m)
	}
}
