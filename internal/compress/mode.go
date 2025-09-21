package compress

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
	XZ
)

func (m Mode) ContentType() string {
	switch m {
	case ZSTD:
		return "application/zstd"
	case ZIP:
		return "application/zip"
	case GZIP:
		return "application/gzip"
	case XZ:
		return "application/x-xz"
	default:
		panic(fmt.Sprintf("unknown mode: %v", m))
	}
}

func (m Mode) Ext() string {
	switch m {
	case ZSTD:
		return ".tar.zst"
	case ZIP:
		return ".zip"
	case GZIP:
		return ".tar.gz"
	case XZ:
		return ".tar.xz"
	default:
		panic(fmt.Sprintf("unknown mode: %v", m))
	}
}

func (m Mode) createCompressor(dst io.Writer, opts *Options) (compressor, error) {
	switch m {
	case ZSTD:
		return newZstdCompressor(dst, opts)
	case ZIP:
		return newZipCompressor(dst, opts), nil
	case GZIP:
		return newGzipCompressor(dst, opts)
	case XZ:
		return newXzCompressor(dst, opts)
	default:
		return nil, fmt.Errorf("unknown mode: %v", m)
	}
}
