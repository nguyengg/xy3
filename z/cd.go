package z

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/valyala/bytebufferpool"
)

const (
	// DefaultMaxBytes is the default value of [Options.MaxBytes].
	DefaultMaxBytes int64 = 1 * 1024 * 1024
)

var (
	// ErrNoEOCDFound is returned if no EOCD signature was found.
	ErrNoEOCDFound = errors.New("end of central directory not found; most likely not a ZIP file")
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

// Scan scans backwards from the given io.ReadSeeker for the central directory file headers.
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
// By default, only the last DefaultMaxBytes number of bytes are scanned. If an EOCD is not found in this range, it is
// most likely NOT a ZIP file. Options can be used to change this limit, or pass a context.Context that can be used to
// cancel the scan.
//
// The returned file headers MUST NOT be opened for read concurrently. Any concurrent [FileHeader.Open] or
// [FileHeader.WriteTo] will return an immediate error if it can detect this scenario. Only the file headers returned by
// ScanFromReaderAt can safely [FileHeader.Open] or [FileHeader.WriteTo] in parallel. If src implements io.Closer, it is
// imperative src remains open for all subsequent reads.
func Scan(src io.ReadSeeker, optFns ...func(*Options)) (EOCDRecord, iter.Seq2[FileHeader, error], error) {
	opts := &Options{
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

	return r, func(yield func(FileHeader, error) bool) {
		if _, err := src.Seek(int64(r.CDOffset), io.SeekStart); err != nil {
			yield(FileHeader{}, err)
		}

		var (
			br    = bufio.NewReaderSize(src, 16*1024)
			buf   = make([]byte, 46)
			readN int
		)

		for {
			fh := FileHeader{}

			if readN, err = br.Read(buf); err != nil && !errors.Is(err, io.EOF) {
				yield(fh, fmt.Errorf("next CD file header: read error: %w", err))
				return
			} else {
				if readN >= 4 && binary.LittleEndian.Uint32(buf[:4]) == 0x06054b50 {
					return
				}
				if readN < 46 {
					yield(fh, fmt.Errorf("next CD file header: read returns insufficient data, needs at least 46 bytes, got %d", readN))
					return
				}
			}

			fsfh := &fixedSizeCDFileHeader{}
			if err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, fsfh); err != nil {
				yield(fh, fmt.Errorf("next CD file header: parse error: %w", err))
				return
			}
			if fsfh.Signature != 0x02014b50 {
				yield(fh, fmt.Errorf("next CD file header: mismatched signature, got 0x%x, expected 0x%x", fsfh.Signature, 0x02014b50))
				return
			}
			fh = FileHeader{
				FileHeader: zip.FileHeader{
					CreatorVersion:     fsfh.CreatorVersion,
					ReaderVersion:      fsfh.ReaderVersion,
					Flags:              fsfh.Flags,
					Method:             fsfh.Method,
					Modified:           time.Time{},
					ModifiedTime:       fsfh.ModifiedTime,
					ModifiedDate:       fsfh.ModifiedDate,
					CRC32:              fsfh.CRC32,
					CompressedSize:     fsfh.CompressedSize,
					UncompressedSize:   fsfh.UncompressedSize,
					CompressedSize64:   uint64(fsfh.CompressedSize),
					UncompressedSize64: uint64(fsfh.UncompressedSize),
					ExternalAttrs:      fsfh.ExternalAttrs,
				},
				DiskNumber: fsfh.DiskNumber,
				Offset:     int64(fsfh.Offset),
			}
			fh.Modified = msDosTimeToTime(fh.ModifiedDate, fh.ModifiedTime)

			n, m, k := fsfh.FileNameLength, fsfh.ExtraFieldLength, fsfh.FileCommentLength
			nmkLen := int(n + m + k)
			if nmkLen > 0 {
				nmkBuf := make([]byte, nmkLen)
				if readN, err = br.Read(nmkBuf); err != nil {
					yield(fh, fmt.Errorf("next CD file header: read variable-size data: read error: %w", err))
					return
				} else if readN < nmkLen {
					yield(fh, fmt.Errorf("next CD file header: read variable-size data: read returns insufficient data, needs at least %d bytes, got %d", nmkLen, readN))
					return
				}
				fh.Name, fh.Comment, fh.Extra = string(nmkBuf[:n]), string(nmkBuf[n:n+m]), nmkBuf[n+m:n+m+k]
			}

			// TODO support fh.Open and fh.WriteTo.

			if !yield(fh, nil) {
				return
			}
		}
	}, nil
}

// ScanFromReaderAt scans backwards from the given io.ReaderAt and size for the central directory (CD).
//
// Returns the end-of-central-directory (EOCD) record, an iterator over the central directory file headers, and any
// error from searching for and parsing the EOCD/CD.
//
// The method assumes the contents from src contains exactly 1 well-formatted ZIP archive. All bets are off otherwise.
// By default, only the last DefaultMaxBytes number of bytes are scanned. If an EOCD is not found in this range, it is
// most likely NOT a ZIP file. Options can be used to change this limit, or pass a context.Context that can be used to
// cancel the scan.
//
// The returned file headers can [FileHeader.Open] or [FileHeader.WriteTo] concurrently. If src implements io.Closer, it
// is imperative src remains open for all subsequent reads.
func ScanFromReaderAt(src io.ReaderAt, size int64, optFns ...func(*Options)) (EOCDRecord, iter.Seq2[FileHeader, error], error) {
	opts := &Options{
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

	return r, func(yield func(FileHeader, error) bool) {
		bb := bytebufferpool.Get()
		defer bytebufferpool.Put(bb)

		var (
			buf    = make([]byte, 16*1024)
			offset = int64(r.CDOffset)
			readN  int
			err    error
		)

		for ; ; bb.Reset() {
			fh := FileHeader{}

			if readN, err = src.ReadAt(buf, offset); err != nil && !errors.Is(err, io.EOF) {
				yield(fh, fmt.Errorf("next CD file header: read error: %w", err))
				return
			} else {
				if readN >= 4 && binary.LittleEndian.Uint32(buf[:4]) == 0x06054b50 {
					return
				}
				if readN < 46 {
					yield(fh, fmt.Errorf("next CD file header: read returns insufficient data, needs at least 46 bytes, got %d", readN))
					return
				}
				bb.B = buf[:readN]
			}

			fsfh := &fixedSizeCDFileHeader{}
			if err = binary.Read(bytes.NewReader(bb.B[:46]), binary.LittleEndian, fsfh); err != nil {
				yield(fh, fmt.Errorf("next CD file header: parse error: %w", err))
				return
			}
			if fsfh.Signature != 0x02014b50 {
				yield(fh, fmt.Errorf("next CD file header: mismatched signature, got 0x%x, expected 0x%x", fsfh.Signature, 0x02014b50))
				return
			}
			fh = FileHeader{
				FileHeader: zip.FileHeader{
					CreatorVersion:     fsfh.CreatorVersion,
					ReaderVersion:      fsfh.ReaderVersion,
					Flags:              fsfh.Flags,
					Method:             fsfh.Method,
					Modified:           time.Time{},
					ModifiedTime:       fsfh.ModifiedTime,
					ModifiedDate:       fsfh.ModifiedDate,
					CRC32:              fsfh.CRC32,
					CompressedSize:     fsfh.CompressedSize,
					UncompressedSize:   fsfh.UncompressedSize,
					CompressedSize64:   uint64(fsfh.CompressedSize),
					UncompressedSize64: uint64(fsfh.UncompressedSize),
					ExternalAttrs:      fsfh.ExternalAttrs,
				},
				DiskNumber: fsfh.DiskNumber,
				Offset:     int64(fsfh.Offset),
			}
			fh.Modified = msDosTimeToTime(fh.ModifiedDate, fh.ModifiedTime)

			// 46 + n + m + k is the total number of bytes needed for the header. it's extremely unlikely
			// bufSize is less than 46 + n + m + k but just in case, wipe buffer to read and store nmk.
			bb.B, offset = bb.B[46:], offset+46
			n, m, k := fsfh.FileNameLength, fsfh.ExtraFieldLength, fsfh.FileCommentLength
			nmkLen := int(n + m + k)
			if nmkLen > bb.Len() {
				bb.B = make([]byte, nmkLen)
				if readN, err = src.ReadAt(bb.B, offset); err != nil {
					yield(fh, fmt.Errorf("next CD file header: read variable-size data: read error: %w", err))
					return
				} else if readN < nmkLen {
					yield(fh, fmt.Errorf("next CD file header: read variable-size data: read returns insufficient data, needs at least %d bytes, got %d", nmkLen, readN))
					return
				}
			}
			fh.Name, fh.Comment, fh.Extra = string(bb.B[:n]), string(bb.B[n:n+m]), bb.B[n+m:n+m+k]
			offset += int64(nmkLen)

			// TODO support fh.Open and fh.WriteTo.

			if !yield(fh, nil) {
				return
			}
		}
	}, nil
}

// msDosTimeToTime converts an MS-DOS date and time into a time.Time.
// The resolution is 2s.
// See: https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-dosdatetimetofiletime
//
// taken from https://go.dev/src/archive/zip/struct.go.
func msDosTimeToTime(dosDate, dosTime uint16) time.Time {
	return time.Date(
		// date bits 0-4: day of month; 5-8: month; 9-15: years since 1980
		int(dosDate>>9+1980),
		time.Month(dosDate>>5&0xf),
		int(dosDate&0x1f),

		// time bits 0-4: second/2; 5-10: minute; 11-15: hour
		int(dosTime>>11),
		int(dosTime>>5&0x3f),
		int(dosTime&0x1f*2),
		0, // nanoseconds

		time.UTC,
	)
}
