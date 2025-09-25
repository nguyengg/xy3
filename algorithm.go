package xy3

import (
	"github.com/nguyengg/xy3/archive"
	"github.com/nguyengg/xy3/codec"
)

// DefaultAlgorithmName is the name of the default compression algorithm.
const DefaultAlgorithmName = "zstd"

// NewCompressorFromName returns an archive.Archiver from the given algorithm name.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func NewCompressorFromName(name string) archive.Archiver {
	switch name {
	case "gzip", "gz":
		return &archive.Tar{Codec: &codec.GzipCodec{}}
	case "zip":
		return &archive.Zip{}
	case "zstd":
		return &archive.Tar{Codec: &codec.ZstdCodec{}}
	case "xz":
		return &archive.Tar{Codec: &codec.XzCodec{}}
	default:
		return nil
	}
}
