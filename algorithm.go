package xy3

import (
	"strings"

	"github.com/nguyengg/xy3/archive"
	"github.com/nguyengg/xy3/codec"
)

// DefaultAlgorithmName is the name of the default compression algorithm.
const DefaultAlgorithmName = "zstd"

// NewCompressorFromName returns a compressor with the given algorithm name.
func NewCompressorFromName(algorithmName string) archive.Archiver {
	switch algorithmName {
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

// NewDecompressorFromName returns a decompressor for extracting from an archive with the given name.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func NewDecompressorFromName(name string) archive.Archiver {
	switch {
	case strings.HasSuffix(name, ".tar"):
		return &archive.Tar{}
	case strings.HasSuffix(name, ".tar.gz"):
		return &archive.Tar{Codec: &codec.GzipCodec{}}
	case strings.HasSuffix(name, ".tar.xz"):
		return &archive.Tar{Codec: &codec.XzCodec{}}
	case strings.HasSuffix(name, ".tar.zst"):
		return &archive.Tar{Codec: &codec.ZstdCodec{}}
	case strings.HasSuffix(name, ".7z"):
		return &archive.SevenZip{}
	case strings.HasSuffix(name, ".rar"):
		return &archive.Rar{}
	case strings.HasSuffix(name, ".zip"):
		return &archive.Zip{}
	default:
		return nil
	}
}

// NewDecoderFromExt returns a decoder for decompressing from files with the given file name extension.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func NewDecoderFromExt(ext string) codec.Codec {
	switch ext {
	case ".gz":
		return &codec.GzipCodec{}
	case ".xz":
		return &codec.XzCodec{}
	case ".zst":
		return &codec.ZstdCodec{}
	default:
		return nil
	}
}
