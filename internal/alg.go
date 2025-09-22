package internal

import (
	"fmt"
	"io"
)

// Algorithm indicates which algorithm to use for compression and decompression.
type Algorithm int

const (
	AlgorithmZstd Algorithm = iota
	AlgorithmZip
	AlgorithmGzip
	AlgorithmXz
)

func (m Algorithm) ContentType() string {
	switch m {
	case AlgorithmZstd:
		return "application/zstd"
	case AlgorithmZip:
		return "application/zip"
	case AlgorithmGzip:
		return "application/gzip"
	case AlgorithmXz:
		return "application/x-xz"
	default:
		panic(fmt.Sprintf("unknown algorithm: %v", m))
	}
}

func (m Algorithm) Ext() string {
	switch m {
	case AlgorithmZstd:
		return ".zst"
	case AlgorithmZip:
		return ".zip"
	case AlgorithmGzip:
		return ".gz"
	case AlgorithmXz:
		return ".xz"
	default:
		panic(fmt.Sprintf("unknown algorithm: %v", m))
	}
}

func (m Algorithm) ShouldTar() bool {
	switch m {
	case AlgorithmZstd:
		return true
	case AlgorithmZip:
		return false
	case AlgorithmGzip:
		return true
	case AlgorithmXz:
		return true
	default:
		panic(fmt.Sprintf("unknown algorithm: %v", m))
	}
}

func (m Algorithm) createCompressor(dst io.Writer, opts *CompressOptions) (compressor, error) {
	switch m {
	case AlgorithmZstd:
		return newZstdCompressor(dst, opts)
	case AlgorithmZip:
		return newZipCompressor(dst, opts), nil
	case AlgorithmGzip:
		return newGzipCompressor(dst, opts)
	case AlgorithmXz:
		return newXzCompressor(dst, opts)
	default:
		return nil, fmt.Errorf("unknown algorithm: %v", m)
	}
}
