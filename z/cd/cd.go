package cd

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-aws-commons/s3reader"
)

const (
	// DefaultMaxBytes is the default value of [Options.MaxBytes].
	DefaultMaxBytes int64 = 10 * 1024 * 1024
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
}

// FindCentralDirectory scans backwards from the given io.ReadSeeker for the central directory.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for the EOCD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
//
// The returned file headers cannot be opened for read concurrently. Only the file headers returned by
// FindCentralDirectoryFromReaderAt and FindCentralDirectoryFromS3 can be [FileHeader.Open] or [FileHeader.WriteTo]
// in parallel safely. If src implements io.Closer, it is imperative src remains open for all subsequent reads.
func FindCentralDirectory(src io.ReadSeeker, optFns ...func(*Options)) (EOCDRecord, iter.Seq2[FileHeader, error], error) {

}

// FindCentralDirectoryFromReaderAt scans backwards from the given io.ReaderAt for the central directory.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for the EOCD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
//
// The returned file headers can be [FileHeader.Open] or [FileHeader.WriteTo] concurrently. If src implements io.Closer,
// it is imperative src remains open for all subsequent reads.
func FindCentralDirectoryFromReaderAt(r io.ReaderAt, size uint64) (EOCDRecord, iter.Seq2[FileHeader, error], error) {

}

// FindCentralDirectoryFromS3 scans an S3 object backwards for the central directory.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for the EOCD.
//
// The method assumes the contents from the S3 object contains exactly 1 well-formatted ZIP archive. All bets are off
// otherwise.
//
// The returned file headers can be [FileHeader.Open] or [FileHeader.WriteTo] concurrently. If src implements io.Closer,
// it is imperative src remains open for all subsequent reads.
func FindCentralDirectoryFromS3(ctx context.Context, client *s3reader.GetAndHeadObjectClient, input *s3.GetObjectInput) (EOCDRecord, iter.Seq2[FileHeader, error], error) {

}
