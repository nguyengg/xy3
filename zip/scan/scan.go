package scan

import (
	"archive/zip"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
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

	// ZipFileHeader returns embedded zip.FileHeader for metadata needs.
	ZipFileHeader() zip.FileHeader
}

// Forward scans forwards the given io.Reader for ZIP local file headers.
//
// The headers are returned as an interator which is stopped if any error is encountered. Each header can be inspected
// for metadata or can be opened to read its uncompressed content using [ReadableFileHeader.Open] and
// [ReadableFileHeader.WriteTo]. If you only need to list all files in a ZIP archive, consider CentralDirectory which
// scans from end of stream for the central directory instead.
//
// Because src is an io.Reader, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] cannot be used concurrently
// on multiple files lest ErrConcurrentReadNotSupported is returned. If src also implements io.Seeker,
// [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] can be called multiple times on the same local file or
// previous files that were returned by the iterator; ErrSeekNotSupported is returned otherwise.
func Forward(src io.Reader) iter.Seq2[ReadableFileHeader, error] {
	return func(yield func(ReadableFileHeader, error) bool) {
		var (
			// bufSrc wraps src to provide buffered read.
			bufSrc = bufio.NewReaderSize(src, 16*1024)
			// buf is the data slice to read 30 bytes which is the fixed-size part of the local file header.
			buf = make([]byte, 30)
		)

		for {
			switch readN, err := bufSrc.Read(buf); {
			case err != nil && !errors.Is(err, io.EOF):
				yield(nil, fmt.Errorf("read file header error: %w", err))
				return
			case readN >= 4 && bytes.Compare(buf[:4], cdfhSigBytes) == 0:
				return
			case readN < 30:
				yield(nil, fmt.Errorf("read file header error: insufficient read: expected at least 46 bytes, got %d", readN))
				return
			}

			fh, err := unmarshalLocalFileHeader(([30]byte)(buf), bufSrc.Read)
			if err != nil {
				yield(nil, fmt.Errorf("read file header error: %w", err))
				return
			}

			// TODO support fh.Open and fh.WriteTo.

			if !yield(&fh, nil) {
				return
			}

			// right now we're just advancing pass the compressed data.
			switch n, err := io.Copy(io.Discard, io.LimitReader(src, int64(fh.CompressedSize64))); {
			case err != nil && !errors.Is(err, io.EOF):
				yield(nil, fmt.Errorf("read past file compressed data error: %w", err))
				return
			case uint64(n) < fh.CompressedSize64:
				yield(nil, fmt.Errorf("read past file compressed data error: insufficient read: expected at least %d bytes, got %d", fh.CompressedSize64, n))
				return
			}
		}
	}
}

// ForwardWithReaderAt scans forwards the given io.ReaderAt for ZIP local file headers.
//
// The headers are returned as an interator which is stopped if any error is encountered. Each header can be inspected
// for metadata or can be opened to read its uncompressed content using [ReadableFileHeader.Open] and
// [ReadableFileHeader.WriteTo]. If you only need to list all files in a ZIP archive, consider
// CentralDirectoryWithReaderAt which scans from end of stream for the central directory instead.
//
// Because src is an io.ReaderAt, [ReadableFileHeader.Open] and [ReadableFileHeader.WriteTo] can be used concurrently
// on multiple files in any order.
func ForwardWithReaderAt(src io.ReaderAt) iter.Seq2[ReadableFileHeader, error] {
	return func(yield func(ReadableFileHeader, error) bool) {
		var (
			// bb is the dynamic read/write buffer that stores data from previous read operations.
			bb = &bytes.Buffer{}
			// buf is the fixed-size read buffer for every src.ReadAt. the result of this read will be
			// appended to bb which should never be longer than len(buf)+30.
			buf = make([]byte, 16*1024)
			// offset is the next offset to use with src.ReadAt.
			offset int64
		)

		for {
			// if bb has enough bytes for the fixed-size part of the CD file header then use it.
			// if not, read the next batch.
			if bbLen := bb.Len(); bbLen < 30 {
				switch n, err := src.ReadAt(buf, offset); {
				case err != nil && !errors.Is(err, io.EOF):
					yield(nil, fmt.Errorf("read file header error: %w", err))
					return
				default:
					bb.Write(buf[:n])
					offset += int64(n)
				}
			}

			switch bbLen := bb.Len(); {
			case bbLen >= 4 && bytes.Compare(bb.Bytes()[:4], cdfhSigBytes) == 0:
				return
			case bbLen < 30:
				yield(nil, fmt.Errorf("read file header error: insufficient read: expected at least 30 bytes, got %d", bbLen))
				return
			}

			fh, err := unmarshalLocalFileHeader(([30]byte)(bb.Next(30)), func(b []byte) (int, error) {
				bLen := len(b)
				if bLen > bb.Len() {
					switch n, err := src.ReadAt(buf, offset); {
					case n < bLen || (err != nil && !errors.Is(err, io.EOF)):
						return n, err
					default:
						bb.Write(buf[:n])
						offset += int64(n)
					}
				}

				return copy(b, bb.Next(bLen)), nil
			})

			if err != nil {
				yield(nil, fmt.Errorf("read CD file header error: %w", err))
				return
			}

			// TODO support fh.Open and fh.WriteTo.

			if !yield(&fh, nil) {
				return
			}

			// right now we're just advancing pass the compressed data.
			offset += int64(fh.CompressedSize64)
		}
	}
}

// CentralDirectoryOptions customises how the central directory is scanned.
type CentralDirectoryOptions struct {
	// MaxBytes can be given to limit the number of bytes scanned.
	//
	// By default, DefaultMaxBytes is used. Set this to 0 or to the file size to force scanning the entire file.
	MaxBytes int64
}

// CentralDirectory scans from end of stream for ZIP central directory file headers.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
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
		MaxBytes: DefaultMaxBytes,
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
			// buf is the data slice to read 46 bytes which is the fixed-size part of the CD file header.
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
				yield(nil, fmt.Errorf("read CD file header error: insufficient read: expected at least 46 bytes, got %d", readN))
				return
			}

			fh, err := unmarshalCDFileHeader(([46]byte)(buf), bufSrc.Read)
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

// CentralDirectoryWithReaderAt scans from end of stream for ZIP central directory file headers.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
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
		MaxBytes: DefaultMaxBytes,
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
			// bb is the dynamic read/write buffer that stores data from previous read operations.
			bb = &bytes.Buffer{}
			// buf is the fixed-size read buffer for every src.ReadAt. the result of this read will be
			// appended to bb which should never be longer than len(buf)+46.
			buf = make([]byte, 16*1024)
			// offset is the next offset to use with src.ReadAt.
			offset = int64(r.CDOffset)
		)

		for {
			// if bb has enough bytes for the fixed-size part of the CD file header then use it.
			// if not, read the next batch.
			if bbLen := bb.Len(); bbLen < 46 {
				switch n, err := src.ReadAt(buf, offset); {
				case err != nil && !errors.Is(err, io.EOF):
					yield(nil, fmt.Errorf("read CD file header error: %w", err))
					return
				default:
					bb.Write(buf[:n])
					offset += int64(n)
				}
			}

			switch bbLen := bb.Len(); {
			case bbLen >= 4 && bytes.Compare(bb.Bytes()[:4], eocdSigBytes) == 0:
				return
			case bbLen < 46:
				yield(nil, fmt.Errorf("read CD file header error: insufficient read: expected at least 46 bytes, got %d", bbLen))
				return
			}

			fh, err := unmarshalCDFileHeader(([46]byte)(bb.Next(46)), func(b []byte) (int, error) {
				bLen := len(b)
				if bLen > bb.Len() {
					switch n, err := src.ReadAt(buf, offset); {
					case n < bLen || (err != nil && !errors.Is(err, io.EOF)):
						return n, err
					default:
						bb.Write(buf[:n])
						offset += int64(n)
					}
				}

				return copy(b, bb.Next(bLen)), nil
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
