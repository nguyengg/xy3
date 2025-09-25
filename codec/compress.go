package codec

import (
	"io"

	"github.com/nguyengg/xy3/archive"
)

// Compressor abstracts the compression logic.
type Compressor interface {
	// NewArchive creates a new compressor for an archive.
	//
	// ZIP will always create archives even when compressing a single file, while gzip, zstd, and xz can compress
	// files without creating a tarball.
	NewArchive(dst io.Writer, root string) (archive.AddFunction, archive.CloseFunction, error)

	// New creates a new compressor for a single file.
	//
	// ZIP will always create archives even when compressing a single file, while gzip, zstd, and xz can compress
	// files without creating a tarball.
	New(dst io.Writer) (archive.AddFunction, error)

	// Ext returns the extension of files (archive=false) or archives (with archive=true) compressed with this
	// compressor.
	Ext(archive bool) string

	// ContentType returns the content type of files compressed with this compressor.
	ContentType() string
}

// DefaultAlgorithmName is the name of the default compression algorithm.
const DefaultAlgorithmName = "zstd"

// NewCompressorFromAlgorithm returns a Compressor from the given algorithm name.
func NewCompressorFromAlgorithm(name string) (Compressor, bool) {
	switch name {
	case "gzip", "gz":
		return &gzipCodec{}, true
	case "zip":
		return &zipCompressor{}, true
	case "zstd":
		return &zstdCompressor{}, true
	case "xz":
		return &xzCodec{}, true
	default:
		return &zstdCompressor{}, false
	}
}
