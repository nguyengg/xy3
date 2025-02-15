package scan

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/valyala/bytebufferpool"
)

const (
	// DefaultMaxBytes is the default value of [CentralDirectoryOptions.MaxBytes].
	DefaultMaxBytes int64 = 1 * 1024 * 1024
)

var (
	// ErrConcurrentReadNotSupported is returned by [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] if
	// the underlying reader does not support concurrent reads.
	ErrConcurrentReadNotSupported = errors.New("concurrent read not supported")

	// ErrSeekNotSupported is returned by [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] if the
	// underlying reader does not implement io.ReadSeeker or and as a result cannot be used to retrace or reopen past
	// local files.
	ErrSeekNotSupported = errors.New("seek not supported")

	// ErrNoEOCDFound is returned if no EOCD signature was found.
	ErrNoEOCDFound = errors.New("end of central directory not found; most likely not a ZIP file")
)

// ReadableFileHeader provides methods to read a particular file within a ZIP archive.
type ReadableFileHeader interface {
	// Open returns a new io.Reader to the uncompressed file.
	//
	// Open cannot be used to concurrently read multiple files if the ReadableFileHeader was returned by scanning
	// with an io.Reader or io.ReadSeeker. ErrConcurrentReadNotSupported is returned if this is detected. If the
	// underlying reader is an io.ReaderAt, it is safe to open multiple files for read in parallel.
	//
	// If the underlying reader is an io.ReadSeeker or an io.ReaderAt, Open can also be called multiple times on the
	// same local file in any order. Otherwise, ErrSeekNotSupported is returned.
	Open() (io.Reader, error)

	// WriteTo writes the uncompressed file to dst.
	//
	// WriteTo cannot be used to concurrently read multiple files if the ReadableFileHeader was returned by scanning
	// with an io.Reader or io.ReadSeeker. ErrConcurrentReadNotSupported is returned if this is detected. If the
	// underlying reader is an io.ReaderAt, it is safe to open multiple files for read in parallel.
	//
	// If the underlying reader is an io.ReadSeeker or an io.ReaderAt, WriteTo can also be called multiple times on
	// the same local file in any order. Otherwise, ErrSeekNotSupported is returned.
	WriteTo(dst io.Writer) (int64, error)

	// FileHeader returns embedded zip.FileHeader for metadata needs.
	FileHeader() zip.FileHeader
}

// Forward scans forwards the given io.Reader for the local file headers.
//
// The headers are returned as an interator which is stopped if any error is encountered. Each header can be inspected
// for metadata or can be opened to read its uncompressed content using [ReadableFileHeader.Open] and
// [ReadableFileHeader.WriteTo].
//
// Because src is an io.Reader, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] cannot be used concurrently
// on multiple files lest ErrConcurrentReadNotSupported is returned. If src also implements io.Seeker,
// [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] can be called multiple times on the same local file or
// previous files that were returned by the iterator; ErrSeekNotSupported is returned otherwise.
func Forward(src io.Reader) iter.Seq2[ReadableFileHeader, error] {
	panic("implement me")
}

// ForwardWithReaderAt scans forwards the given io.ReaderAt for the local file headers.
//
// The headers are returned as an interator which is stopped if any error is encountered. Each header can be inspected
// for metadata or can be opened to read its uncompressed content using [ReadableFileHeader.Open] and
// [ReadableFileHeader.WriteTo].
//
// Because src is an io.ReaderAt, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] can be used concurrently
// on multiple files in any order.
func ForwardWithReaderAt(src io.ReaderAt) iter.Seq2[ReadableFileHeader, error] {
	panic("implement me")
}

// CentralDirectoryOptions customises how the central directory is scanned.
type CentralDirectoryOptions struct {
	// Ctx can be given to cancel the scanning prematurely.
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

// CentralDirectory scans backwards from the given io.ReadSeeker for the central directory file headers.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers
// (CentralDirectoryFileHeader), and any error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
// By default, only the last DefaultMaxBytes number of bytes are scanned. If an EOCD is not found in this range, it is
// most likely NOT a ZIP file. CentralDirectoryOptions can be used to change this limit, or pass a context.Context that
// can be used to cancel the scan.
//
// Because src is an io.ReadSeeker, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] cannot be used
// concurrently on multiple files lest ErrConcurrentReadNotSupported is returned. However, [ReadableFileHeader.Open] and
// [ReadableFileHeader.WriteTo] can be called multiple times on the same local file or previous files that were returned
// by the iterator since seek is supported.
func CentralDirectory(src io.ReadSeeker, optFns ...func(*CentralDirectoryOptions)) (EOCDRecord, iter.Seq2[ReadableFileHeader, error], error) {
	opts := &CentralDirectoryOptions{
		Ctx:         context.Background(),
		MaxBytes:    DefaultMaxBytes,
		KeepComment: false,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	r, err := findEOCD(src, opts)
	if err != nil {
		return r, nil, err
	}

	return r, func(yield func(ReadableFileHeader, error) bool) {
		if _, err := src.Seek(int64(r.CDOffset), io.SeekStart); err != nil {
			yield(nil, fmt.Errorf("next CD file header: set read offset to start of central directory (0x%x) error: %w", r.CDOffset, err))
		}

		var (
			// bufSrc wraps src to provide buffered read.
			bufSrc = bufio.NewReaderSize(src, 16*1024)
			// buf is the data slice to read 46 bytes which is the fixed-size part of the CD header.
			buf = make([]byte, 46)
		)

		for {
			switch readN, err := bufSrc.Read(buf); {
			case err != nil && !errors.Is(err, io.EOF):
				yield(nil, fmt.Errorf("read CD file header error: %w", err))
				return
			case readN >= 4 && bytes.Compare(buf[:4], eocdSigBytes) == 0:
				return
			case readN < 46:
				yield(nil, fmt.Errorf("read CD file header error: insufficient read: needs at least 46 bytes, got %d", readN))
				return
			}

			fh, err := parseCDFileHeader(([46]byte)(buf), bufSrc.Read)
			if err != nil {
				yield(nil, fmt.Errorf("read CD file header error: %w", err))
				return
			}

			// TODO support fh.Open and fh.WriteTo.

			if !yield(&fh, nil) {
				return
			}
		}
	}, nil
}

// CentralDirectoryWithReaderAt scans backwards from the given io.ReaderAt and size for the central directory (CD).
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers
// (CentralDirectoryFileHeader), and any error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
// By default, only the last DefaultMaxBytes number of bytes are scanned. If an EOCD is not found in this range, it is
// most likely NOT a ZIP file. CentralDirectoryOptions can be used to change this limit, or pass a context.Context that
// can be used to cancel the scan.
//
// Because src is an io.ReaderAt, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] can be used concurrently
// on multiple files in any order.
func CentralDirectoryWithReaderAt(src io.ReaderAt, size int64, optFns ...func(*CentralDirectoryOptions)) (EOCDRecord, iter.Seq2[ReadableFileHeader, error], error) {
	opts := &CentralDirectoryOptions{
		Ctx:         context.Background(),
		MaxBytes:    DefaultMaxBytes,
		KeepComment: false,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	r, err := findEOCD(io.NewSectionReader(src, 0, size), opts)
	if err != nil {
		return r, nil, err
	}

	return r, func(yield func(ReadableFileHeader, error) bool) {
		var (
			// bb is the dynamic read buffer that stores data from previous read operations.
			bb = bytebufferpool.Get()
			// buf is the fixed-size read buffer for every src.ReadAt. the result of this read will be
			// appended to bb which should never be longer than buf+46.
			buf = make([]byte, 16*1024)
			// offset is the next offset to use with src.ReadAt.
			offset = int64(r.CDOffset)
		)
		defer bytebufferpool.Put(bb)

		for {
			// if bb has enough bytes for the fixed-size part of the CD file header then use it.
			// if not, read the next batch.
			if bbLen := bb.Len(); bbLen < 46 {
				switch n, err := src.ReadAt(buf, offset); {
				case err != nil && !errors.Is(err, io.EOF):
					yield(nil, fmt.Errorf("read CD file header error: %w", err))
					return
				default:
					bb.B = append(bb.B, buf[:n]...)
					offset += int64(n)
				}
			}

			switch bbLen := bb.Len(); {
			case bbLen >= 4 && bytes.Compare(bb.B[:4], eocdSigBytes) == 0:
				return
			case bbLen < 46:
				yield(nil, fmt.Errorf("read CD file header error: insufficient read: needs at least 46 bytes, got %d", bbLen))
				return
			}

			fh, err := parseCDFileHeader(([46]byte)(bb.B[:46]), func(b []byte) (int, error) {
				nmkLen := len(b)
				bb.B = bb.B[46:]

				if nmkLen > bb.Len() {
					switch n, err := src.ReadAt(buf, offset); {
					case n < nmkLen || (err != nil && !errors.Is(err, io.EOF)):
						return n, err
					default:
						bb.B = append(bb.B, buf[:n]...)
						offset += int64(n)
					}
				}

				copy(b, bb.B[:nmkLen])
				bb.B = bb.B[nmkLen:]
				return nmkLen, nil
			})

			if err != nil {
				yield(nil, fmt.Errorf("read CD file header error: %w", err))
				return
			}

			// TODO support fh.Open and fh.WriteTo.

			if !yield(&fh, nil) {
				return
			}
		}
	}, nil
}
