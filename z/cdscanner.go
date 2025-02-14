package z

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/valyala/bytebufferpool"
)

// CDFileHeader extends zip.FileHeader with additional information from the central directory.
type CDFileHeader struct {
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
	Offset uint64
}

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
	Next() (bool, CDFileHeader)
	// All returns the remaining file headers as an iterator.
	//
	// Don't mix Next and All as they use the same underlying io.ReadSeeker src.
	All() iter.Seq[CDFileHeader]
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

	for {
		if _, err = src.Read(buf); err != nil {
			return nil, fmt.Errorf("read EOCD error: %w", err)
		}

		if i := bytes.LastIndex(buf, sigEOCD); i != -1 {
			if i+20 > len(buf) {
				return nil, fmt.Errorf("invalid EOCD")
			}

			cd := &cdScanner{src: src}
			cd.recordCount = int(binary.LittleEndian.Uint16(buf[i+10 : i+12]))
			cd.size = int(binary.LittleEndian.Uint16(buf[i+12 : i+14]))
			cd.offset = int64(binary.LittleEndian.Uint32(buf[i+16 : i+20]))
			return cd, nil
		}

		// we're already at start of file, there's no more bytes to read.
		if offset == 0 {
			return nil, ErrNoEOCDFound
		}

		if offset, err = src.Seek(max(0, offset-(bufSize-4)), io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek backwards for EOCD error: %w", err)
		}
	}
}

type cdScanner struct {
	src         io.ReadSeeker
	recordCount int
	size        int
	offset      int64
	err         error
	eof         bool
}

func (s *cdScanner) RecordCount() int {
	return s.recordCount
}

func (s *cdScanner) Size() int {
	return s.size
}

func (s *cdScanner) Err() error {
	return s.err
}

func (s *cdScanner) Next() (ok bool, fh CDFileHeader) {
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

func (s *cdScanner) All() iter.Seq[CDFileHeader] {
	return func(yield func(CDFileHeader) bool) {
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
func toFileHeader(data []byte) (fh CDFileHeader) {
	fh = CDFileHeader{
		FileHeader: zip.FileHeader{
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
			ExternalAttrs:      binary.LittleEndian.Uint32(data[38:42]),
		},
		DiskNumber: binary.LittleEndian.Uint16(data[34:46]),
		Offset:     uint64(binary.LittleEndian.Uint32(data[42:46])),
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
