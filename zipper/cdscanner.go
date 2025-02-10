package zipper

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nguyengg/go-s3readseeker"
	"github.com/valyala/bytebufferpool"
)

// CDScanner provides methods to scan a zip file's central directory for information.
//
// CDScanner is not safe for use across multiple goroutine.
type CDScanner interface {
	// RecordCount returns the total number of records.
	RecordCount() int
	// Err returns the last non-error encountered.
	Err() error
	// Next returns the next zip file header.
	//
	// The boolean return value is false if there is no more file header to go over, or if there was an error.
	//
	// Don't mix Next and All as they use the same underlying io.ReadSeeker src.
	Next() (bool, zip.FileHeader)
	// All returns the remaining file headers as an iterator.
	//
	// Don't mix Next and All as they use the same underlying io.ReadSeeker src.
	All() iter.Seq[zip.FileHeader]
}

// ErrNoEOCDFound is returned by NewCDScanner if no EOCD was found.
var ErrNoEOCDFound = errors.New("end of central directory not found; most likely not a zip file")

// NewCDScanner reads from the given src to extract the zip.FileHeader records from the central directory.
//
// Returns an iterator over the zip.FileHeader entries and the expected record count. Any error will stop the iterator.
// If the src is not a zip file (due to missing end of central directory signature), the first and only entry will
// be `nil, ErrNoEOCDFound`.
func NewCDScanner(src io.ReadSeeker, size int64) (CDScanner, error) {
	buf := make([]byte, 1024)
	bufSize := int64(len(buf))
	offset, err := src.Seek(max(0, size-bufSize), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek EOCD error: %w", err)
	}

	recordCount := 0
	for {
		if _, err = src.Read(buf); err != nil {
			return nil, fmt.Errorf("read EOCD error: %w", err)
		}

		if i := bytes.LastIndex(buf, sigEOCD); i != -1 {
			recordCount = int(binary.LittleEndian.Uint16(buf[i+10 : i+12]))
			offset = int64(binary.LittleEndian.Uint32(buf[i+16 : i+20]))
			break
		}

		// we're already at start of file, there's no more bytes to read.
		if offset == 0 {
			return nil, ErrNoEOCDFound
		}

		if offset, err = src.Seek(max(0, offset-(bufSize-4)), io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek backwards for EOCD error: %w", err)
		}
	}

	return &cdScanner{
		src:         src,
		offset:      offset,
		recordCount: recordCount,
		err:         nil,
	}, nil
}

// NewCDScannerFromS3 reads from S3 instead.
func NewCDScannerFromS3(ctx context.Context, client s3readseeker.ReadSeekerClient, input *s3.GetObjectInput, optFns ...func(options *s3readseeker.ReadSeekerOptions)) (CDScanner, error) {
	s3object, err := s3readseeker.New(ctx, client, input, optFns...)
	if err != nil {
		return nil, err
	}

	return NewCDScanner(s3object, s3object.Size())
}

type cdScanner struct {
	src         io.ReadSeeker
	offset      int64
	recordCount int
	err         error
	eof         bool
}

func (s *cdScanner) RecordCount() int {
	return s.recordCount
}

func (s *cdScanner) Err() error {
	return s.err
}

func (s *cdScanner) Next() (ok bool, fh zip.FileHeader) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	if s.offset, s.err = s.src.Seek(s.offset, io.SeekStart); s.err != nil {
		s.err = fmt.Errorf("seek CDFH error: %w", s.err)
		return
	}

	if _, s.err = bb.ReadFrom(io.LimitReader(s.src, 46)); s.err != nil {
		s.err = fmt.Errorf("read CDFH error: %w", s.err)
		return
	}

	if j := bytes.Index(bb.B, sigCDFH); j != 0 {
		if j = bytes.Index(bb.B, sigEOCD); j == 0 {
			return
		}

		s.err = fmt.Errorf("invalid CDFH signature")
		return
	}

	n := int(bb.B[28]) | int(bb.B[29])<<8
	m := int(bb.B[30]) | int(bb.B[31])<<8
	k := int(bb.B[32]) | int(bb.B[33])<<8
	if _, s.err = bb.ReadFrom(io.LimitReader(s.src, int64(n+m+k))); s.err != nil {
		s.err = fmt.Errorf("read extra CDFH error: %w", s.err)
		return
	}

	s.offset += int64(len(bb.B))
	return true, toFileHeader(bb.B)
}

func (s *cdScanner) All() iter.Seq[zip.FileHeader] {
	return func(yield func(zip.FileHeader) bool) {
		bb := bytebufferpool.Get()
		defer bytebufferpool.Put(bb)

		if s.offset, s.err = s.src.Seek(s.offset, io.SeekStart); s.err != nil {
			s.err = fmt.Errorf("seek CDFH error: %w", s.err)
			return
		}

		for {
			if _, s.err = bb.ReadFrom(io.LimitReader(s.src, 46)); s.err != nil {
				s.err = fmt.Errorf("read CDFH error: %w", s.err)
				return
			}

			if j := bytes.Index(bb.B, sigCDFH); j != 0 {
				if j = bytes.Index(bb.B, sigEOCD); j == 0 {
					return
				}

				s.err = fmt.Errorf("invalid CDFH signature")
				return
			}

			n := int(bb.B[28]) | int(bb.B[29])<<8
			m := int(bb.B[30]) | int(bb.B[31])<<8
			k := int(bb.B[32]) | int(bb.B[33])<<8
			if _, s.err = bb.ReadFrom(io.LimitReader(s.src, int64(n+m+k))); s.err != nil {
				s.err = fmt.Errorf("read extra CDFH error: %w", s.err)
				return
			}

			s.offset += int64(len(bb.B))

			if !yield(toFileHeader(bb.B)) {
				return
			}

			bb.Reset()
		}
	}
}

// toFileHeader method panics if the data does not contain a full file header record.
//
// https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH)
func toFileHeader(data []byte) (fh zip.FileHeader) {
	fh = zip.FileHeader{
		CreatorVersion:     binary.LittleEndian.Uint16(data[4:6]),
		ReaderVersion:      binary.LittleEndian.Uint16(data[6:8]),
		Flags:              binary.LittleEndian.Uint16(data[8:10]),
		Method:             binary.LittleEndian.Uint16(data[10:12]),
		Modified:           time.Time{},
		ModifiedTime:       binary.LittleEndian.Uint16(data[12:14]),
		ModifiedDate:       binary.LittleEndian.Uint16(data[14:16]),
		CRC32:              binary.LittleEndian.Uint32(data[16:20]),
		CompressedSize64:   uint64(binary.LittleEndian.Uint32(data[20:24])),
		UncompressedSize64: uint64(binary.LittleEndian.Uint32(data[24:28])),
	}
	fh.Modified = msDosTimeToTime(fh.ModifiedDate, fh.ModifiedTime)

	n := int(data[28]) | int(data[29])<<8
	m := int(data[30]) | int(data[31])<<8
	k := int(data[32]) | int(data[33])<<8
	fh.Name = string(data[46 : 46+n])
	fh.Comment = string(data[46+n : 46+n+m])
	fh.Extra = data[46+n+m : 46+n+m+k]
	return
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

var (
	sigCDFH = make([]byte, 4)
	sigEOCD = make([]byte, 4)
)

func init() {
	binary.LittleEndian.PutUint32(sigCDFH, 0x02014b50)
	binary.LittleEndian.PutUint32(sigEOCD, 0x06054b50)
}
