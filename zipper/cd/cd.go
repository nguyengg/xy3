package cd

import (
	"context"
	"errors"
	"io"
	"iter"
)

const (
	// DefaultMaxBytes is the default value of [Options.MaxBytes].
	DefaultMaxBytes int64 = 1 * 1024 * 1024
)

var (
	// ErrNoEOCDFound is returned if no EOCD signature was found.
	ErrNoEOCDFound = errors.New("end of central directory not found; most likely not a ZIP file")
	// ErrInvalidEOCD is returned if parsing EOCD encounters a validation error.
	ErrInvalidEOCD = errors.New("invalid EOCD")
)

// Options customises how the central directory is scanned.
type Options struct {
	// Ctx can be given to cancel the scanning after some time.
	Ctx context.Context

	// MaxBytes can be given to limit the number of bytes scanned.
	//
	// By default, DefaultMaxBytes is used. Set this to 0 or to the file size to force scanning the entire file.
	MaxBytes int64

	// KeepComment controls whether header's comment is kept or discarded.
	//
	// By default, the zero value discards comment fields from all returned records and headers.
	KeepComment bool
}

// Find scans backwards from the given io.ReadSeeker for the central directory.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
//
// The returned file headers MUST NOT be opened for read concurrently. Any concurrent [FileHeader.Open] or
// [FileHeader.WriteTo] will return an immediate error if it can detect this scenario. Only the file headers returned by
// FindFromReaderAt can safely [FileHeader.Open] or [FileHeader.WriteTo] in parallel. If src implements io.Closer, it is
// imperative src remains open for all subsequent reads.
func Find(src io.ReadSeeker, optFns ...func(*Options)) (EOCDRecord, iter.Seq2[FileHeader, error], error) {
	opts := &Options{
		Ctx:         context.Background(),
		MaxBytes:    0,
		KeepComment: false,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	r, err := findEOCD(src, opts)
	if err != nil {
		return r, nil, err
	}

	return r, func(yield func(FileHeader, error) bool) {

	}, nil
}

// FindFromReaderAt scans backwards from the given io.ReaderAt and size for the central directory (CD).
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
//
// The returned file headers can [FileHeader.Open] or [FileHeader.WriteTo] concurrently. If src implements io.Closer, it
// is imperative src remains open for all subsequent reads.
func FindFromReaderAt(src io.ReaderAt, size int64, optFns ...func(*Options)) (EOCDRecord, iter.Seq2[FileHeader, error], error) {
	opts := &Options{
		Ctx:         context.Background(),
		MaxBytes:    0,
		KeepComment: false,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	r, err := findEOCD(io.NewSectionReader(src, 0, size), opts)
	if err != nil {
		return r, nil, err
	}

	return r, func(yield func(FileHeader, error) bool) {

	}, nil
}
