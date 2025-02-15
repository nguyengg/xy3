package scan

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	lfhSig  = 0x04034b50
	cdfhSig = 0x02014b50
	eocdSig = 0x06054b50
)

var (
	lfhSigBytes  = putUint32(lfhSig)
	cdfhSigBytes = putUint32(cdfhSig)
	eocdSigBytes = putUint32(eocdSig)
)

func putUint32(v uint32) (b []byte) {
	b = make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

// CentralDirectoryFileHeader is a FileHeader parsed from the central directory of the ZIP file.
//
// See https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH).
type CentralDirectoryFileHeader struct {
	fh zip.FileHeader

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
}

func (f *CentralDirectoryFileHeader) FileHeader() zip.FileHeader {
	return f.fh
}

func (f *CentralDirectoryFileHeader) Open() (io.Reader, error) {
	panic("implement me")
}

// WriteTo reads and decompress content to the given dst.
//
// It is safe to open concurrent files for read if the CentralDirectoryFileHeader was created using ScanFromReaderAt since they use
// io.ReaderAt under the hood.
func (f *CentralDirectoryFileHeader) WriteTo(dst io.Writer) (int64, error) {
	panic("implement me")
}

// unmarshalCDFileHeader decodes the 46-byte slice as a CentralDirectoryFileHeader.
// read will always be called to retrieve the variable-size part of the header. if there is no variable-size part, read
// will be passed an empty slice.
func unmarshalCDFileHeader(b [46]byte, read func(b []byte) (int, error)) (fh CentralDirectoryFileHeader, err error) {
	data := &struct {
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
	}{}

	if bytes.Compare(cdfhSigBytes, b[:4]) != 0 {
		return fh, fmt.Errorf("mismatched signature, got 0x%x, expected 0x%x", b[:4], cdfhSigBytes)
	}

	if err = binary.Read(bytes.NewReader(b[:]), binary.LittleEndian, data); err != nil {
		return fh, fmt.Errorf("unmarshal error: %w", err)
	}

	fh = CentralDirectoryFileHeader{
		fh: zip.FileHeader{
			CreatorVersion:     data.CreatorVersion,
			ReaderVersion:      data.ReaderVersion,
			Flags:              data.Flags,
			Method:             data.Method,
			Modified:           time.Time{},
			ModifiedTime:       data.ModifiedTime,
			ModifiedDate:       data.ModifiedDate,
			CRC32:              data.CRC32,
			CompressedSize:     data.CompressedSize,
			UncompressedSize:   data.UncompressedSize,
			CompressedSize64:   uint64(data.CompressedSize),
			UncompressedSize64: uint64(data.UncompressedSize),
			ExternalAttrs:      data.ExternalAttrs,
		},
		DiskNumber: data.DiskNumber,
		Offset:     int64(data.Offset),
	}
	fh.fh.Modified = msDosTimeToTime(fh.fh.ModifiedDate, fh.fh.ModifiedTime)
	n, m, k := data.FileNameLength, data.ExtraFieldLength, data.FileCommentLength
	nmkLen := int(n + m + k)
	nmk := make([]byte, nmkLen)
	switch readN, err := read(nmk); {
	case err != nil && !errors.Is(err, io.EOF):
		return fh, fmt.Errorf("read variable-size data error: %w", err)
	case readN < nmkLen:
		return fh, fmt.Errorf("read variable-size data error: insufficient read: needs at least %d bytes, got %d", nmkLen, readN)
	default:
		fh.fh.Name, fh.fh.Comment, fh.fh.Extra = string(nmk[:n]), string(nmk[n:n+m]), nmk[n+m:n+m+k]
	}

	return fh, nil
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
