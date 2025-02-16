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

// localFileHeader is a ReadableFileHeader parsed from the local file headers of a ZIP file.
type localFileHeader struct {
	zip.FileHeader

	offset   int64
	readerAt io.ReaderAt
}

// unmarshalLocalFileHeader decodes the 30-byte slice as a localFileHeader.
// read will always be called to retrieve the variable-size part of the header. if there is no variable-size part, read
// will be passed an empty slice.
func unmarshalLocalFileHeader(b [30]byte, read func(b []byte) (int, error)) (fh localFileHeader, err error) {
	data := &struct {
		Signature        uint32
		ReaderVersion    uint16
		Flags            uint16
		Method           uint16
		ModifiedTime     uint16
		ModifiedDate     uint16
		CRC32            uint32
		CompressedSize   uint32
		UncompressedSize uint32
		FileNameLength   uint16
		ExtraFieldLength uint16
	}{}

	if bytes.Compare(lfhSigBytes, b[:4]) != 0 {
		return fh, fmt.Errorf("mismatched signature, got 0x%x, expected 0x%x", b[:4], lfhSigBytes)
	}

	if err = binary.Read(bytes.NewReader(b[:]), binary.LittleEndian, data); err != nil {
		return fh, fmt.Errorf("unmarshal error: %w", err)
	}

	fh = localFileHeader{
		FileHeader: zip.FileHeader{
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
		},
	}
	fh.Modified = msDosTimeToTime(fh.ModifiedDate, fh.ModifiedTime)
	n, m := data.FileNameLength, data.ExtraFieldLength
	nmLen := int(n + m)
	nm := make([]byte, nmLen)
	switch readN, err := read(nm); {
	case err != nil && !errors.Is(err, io.EOF):
		return fh, fmt.Errorf("read variable-size data error: %w", err)
	case readN < nmLen:
		return fh, fmt.Errorf("read variable-size data error: insufficient read: expected at least %d bytes, got %d", nmLen, readN)
	default:
		fh.Name, fh.Extra = string(nm[:n]), nm[n+m:]
	}

	return fh, nil
}

func (f *localFileHeader) Open() (io.Reader, error) {
	if f.readerAt != nil {
		return io.NewSectionReader(f.readerAt, f.offset, int64(f.CompressedSize64)), nil
	}

	//TODO implement me
	panic("implement me")
}

func (f *localFileHeader) WriteTo(dst io.Writer) (int64, error) {
	if f.readerAt != nil {
		return io.Copy(dst, io.NewSectionReader(f.readerAt, f.offset, int64(f.CompressedSize64)))
	}

	//TODO implement me
	panic("implement me")
}

func (f *localFileHeader) ZipFileHeader() zip.FileHeader {
	return f.FileHeader
}

// cdFileHeader is a ReadableFileHeader parsed from the central directory file headers of a ZIP file
type cdFileHeader struct {
	zip.FileHeader

	offset   int64
	readerAt io.ReaderAt
}

func (f *cdFileHeader) Open() (io.Reader, error) {
	if f.readerAt != nil {
		return io.NewSectionReader(f.readerAt, f.offset, int64(f.CompressedSize64)), nil
	}

	//TODO implement me
	panic("implement me")
}

// WriteTo reads and decompress content to the given dst.
//
// It is safe to open concurrent files for read if the cdFileHeader was created using ScanFromReaderAt since they use
// io.ReaderAt under the hood.
func (f *cdFileHeader) WriteTo(dst io.Writer) (int64, error) {
	if f.readerAt != nil {
		return io.Copy(dst, io.NewSectionReader(f.readerAt, f.offset, int64(f.CompressedSize64)))
	}

	//TODO implement me
	panic("implement me")
}

func (f *cdFileHeader) ZipFileHeader() zip.FileHeader {
	return f.FileHeader
}

// unmarshalCDFileHeader decodes the 46-byte slice as a cdFileHeader.
// read will always be called to retrieve the variable-size part of the header. if there is no variable-size part, read
// will be passed an empty slice.
func unmarshalCDFileHeader(b [46]byte, read func(b []byte) (int, error)) (fh cdFileHeader, err error) {
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

	fh = cdFileHeader{
		FileHeader: zip.FileHeader{
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
		offset: int64(data.Offset),
	}
	fh.Modified = msDosTimeToTime(fh.ModifiedDate, fh.ModifiedTime)
	n, m, k := data.FileNameLength, data.ExtraFieldLength, data.FileCommentLength
	nmkLen := int(n + m + k)
	nmk := make([]byte, nmkLen)
	switch readN, err := read(nmk); {
	case err != nil && !errors.Is(err, io.EOF):
		return fh, fmt.Errorf("read variable-size data error: %w", err)
	case readN < nmkLen:
		return fh, fmt.Errorf("read variable-size data error: insufficient read: expected at least %d bytes, got %d", nmkLen, readN)
	default:
		fh.Name, fh.Comment, fh.Extra = string(nmk[:n]), string(nmk[n:n+m]), nmk[n+m:n+m+k]
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
