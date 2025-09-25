package archive

import (
	"io"
	"iter"
	"os"
)

// Archiver can read and write archives such as tar, zip, and 7z (read-only) files.
//
// All archiver implementations are not thread-safe by default.
type Archiver interface {
	// Open produces an iterator returning the files from the archive opened by the given io.Reader.
	//
	// The src io.Reader will be consumed by the end of the iterator.
	Open(src io.Reader) (iter.Seq2[File, error], error)

	// Create returns methods to write files to the archive being created by writing to the given io.Writer.
	//
	// If a root directory is given, it will become the root directory for all files added to the archive.
	//
	// The add function creates a new file in the archive with the given metadata and return the io.WriteCloser to
	// write the actual contents of the file. Calling add again implicitly closes out the previous file; not all
	// archive libraries support io.Close on writing individual files but add still returns io.WriteCloser just in
	// case.
	//
	// The close function should be called once all files have been added. After close is called, subsequent calls
	// to add and close will have undefined (and most likely wrong) behaviour.
	Create(dst io.Writer, root string) (add AddFunction, close CloseFunction, err error)

	// ArchiveExt returns the file name extension of the archives created with this compressor.
	ArchiveExt() string

	// ContentType returns the content type of the archives created with this compressor.
	ContentType() string
}

// AddFunction creates a new file in the archive.
type AddFunction func(path string, fi os.FileInfo) (io.WriteCloser, error)

// CloseFunction closes the writer.
type CloseFunction func() error

// File represents a file in an archive.
//
// The interface intentionally matches that of zip.File for simplicity.
type File interface {
	// Name returns the full name of the file in the archive.
	Name() string
	// FileInfo returns description about the file.
	FileInfo() os.FileInfo
	// Mode returns the file's mode.
	Mode() os.FileMode
	// Open opens the file for reading.
	Open() (io.ReadCloser, error)
}
