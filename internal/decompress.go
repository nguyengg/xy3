package internal

import (
	"context"
	"io"
)

// DecompressOptions customises Decompressor.Decompress.
type DecompressOptions struct {
}

type Decompressor interface {
	// Decompress decompresses the given io.Reader and writes contents to directory specified by dir.
	//
	// If src is an archive (either a Z file or a tarball), the contents will be automatically extracted to either
	// the directory named by "dir" argument or to a child directory depending on these conditions:
	//   1. If there is only one file, the file will be extracted to the "dir" directory under a unique name.
	//   2. If the files have a single root directory, they will be unwrapped and written to a unique directory
	//	whose name is derived from the stem passed into the DetectDecompressorFromName method.
	//   3. If the files do not have a single root directory, a unique root directory whose name is derived from the
	//	stem passed into the DetectDecompressorFromName method will be used.
	//
	// Essentially, Decompress will always create a unique file or directory.
	Decompress(ctx context.Context, src io.Reader, dir string) error
}

// DetectDecompressorFromName uses the extension of the file's name to determine decompression algorithm.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func DetectDecompressorFromName(stem, ext string) Decompressor {
	switch ext {
	case ".7z":
	case ".zip":
	case ".zst":
	case ".gz":
	case ".xz":
	default:
		return nil
	}

	return nil
}
