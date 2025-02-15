package z

import (
	"archive/zip"
	"io"
)

// FileHeader is a central directory file header that extends zip.FileHeader with additional information that can be
// used to open the file for reading.
type FileHeader struct {
	zip.FileHeader

	// DiskNumber is the disk number where file starts.
	//
	// Since floppy disks aren't a thing anymore, this field is most likely unused.
	DiskNumber uint16

	// Offset is the relative offset of local file header.
	//
	// This is the number of bytes between the start of the first disk on which the file occurs, and the start of
	// the local file header.
	//
	// See https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH).
	Offset int64

	opener   func() (io.Reader, error)
	writerTo func(io.Writer) (int64, error)
}

// fixedSizeCDFileHeader needs to be fixed size to work with binary.Read.
//
// https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH)
type fixedSizeCDFileHeader struct {
	Signature         uint32
	CreatorVersion    uint16
	ReaderVersion     uint16
	Flags             uint16
	Method            uint16
	ModifiedTime      uint16
	ModifiedDate      uint16
	CRC32             uint32
	CompressedSize    uint32
	UncompressedSize  uint32
	FileNameLength    uint16
	ExtraFieldLength  uint16
	FileCommentLength uint16
	DiskNumber        uint16
	InternalAttrs     uint16
	ExternalAttrs     uint32
	Offset            uint32
}

// Open returns a new io.Reader to the uncompressed file.
//
// It is safe to open concurrent files for read if the FileHeader was created using ScanFromReaderAt since they use
// io.ReaderAt under the hood.
func (f *FileHeader) Open() (io.Reader, error) {
	return f.opener()
}

// WriteTo reads and decompress content to the given dst.
//
// It is safe to open concurrent files for read if the FileHeader was created using ScanFromReaderAt since they use
// io.ReaderAt under the hood.
func (f *FileHeader) WriteTo(dst io.Writer) (int64, error) {
	return f.writerTo(dst)
}
