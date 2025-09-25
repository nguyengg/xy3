package codec

import (
	"io"
)

// Codec has methods to create compressor/encoder and decompressor/decoder.
type Codec interface {
	// NewDecoder creates a decoder to decompress contents from the given io.Reader.
	NewDecoder(src io.Reader) (io.ReadCloser, error)
	// NewEncoder creates an encoder to compress contents from the given io.Writer.
	NewEncoder(dst io.Writer) (io.WriteCloser, error)
}
