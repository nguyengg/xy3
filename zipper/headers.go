package zipper

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nguyengg/xy3/s3reader"
)

// ErrNoEOCDFound is returned by ExtractZipHeaders if no EOCD was found.
var ErrNoEOCDFound = errors.New("end of central directory not found; most likely not a zip file")

// ExtractZipHeaders reads from the given src to extract the zip.FileHeader.
func ExtractZipHeaders(ctx context.Context, src io.ReadSeeker) (headers []zip.FileHeader, err error) {
	recordCount, off, err := findCDFH(ctx, src)
	if err != nil {
		return nil, err
	}

	data := make([]byte, 46)
	if _, err = src.Seek(off, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek CD from start error: %w", err)
	}

	for range recordCount {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if _, err = src.Read(data); err != nil {
			return nil, fmt.Errorf("read CDFH error")
		}

		if j := bytes.Index(data, sigCDFH); j != 0 {
			return nil, fmt.Errorf("invalid CDFH signature")
		}

		// https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header_(CDFH)
		header := zip.FileHeader{
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
		header.Modified = msDosTimeToTime(header.ModifiedDate, header.ModifiedTime)
		n := int(data[28]) | int(data[29])<<8
		m := int(data[30]) | int(data[31])<<8
		k := int(data[32]) | int(data[33])<<8

		nmkData := make([]byte, n+m+k)
		if _, err = src.Read(nmkData); err != nil {
			return headers, fmt.Errorf("seek next CDFH error")
		}

		header.Name = string(nmkData[:n])
		header.Comment = string(nmkData[n : n+m])
		header.Extra = nmkData[n+m:]
		headers = append(headers, header)
	}

	return
}

// ExtractZipHeadersFromS3 reads from S3 instead.
func ExtractZipHeadersFromS3(ctx context.Context, client s3reader.ReadSeekerClient, bucket, key string, optFns ...func(*s3reader.Options)) ([]zip.FileHeader, error) {
	s3object, err := s3reader.NewReaderSeeker(client, bucket, key, optFns...)
	if err != nil {
		return nil, fmt.Errorf("create S3 ReaderSeeker error: %w", err)
	}

	return ExtractZipHeaders(ctx, s3object)
}

// findCDFH reads from end of src to find the EOCD signature and then return:
// 1. The total number of central directory records
// 2. The offset of the start of central directory, relative to start of archive.
//
// TODO add support for ZIP64.
// See https://en.wikipedia.org/wiki/ZIP_(file_format)#End_of_central_directory_record_(EOCD).
func findCDFH(ctx context.Context, src io.ReadSeeker) (recordCount int, cdOffset int64, err error) {
	var (
		eocd = make([]byte, 1024)
		off  = -int64(len(eocd))
	)

	_, err = src.Seek(off, io.SeekEnd)
	if err != nil {
		return 0, 0, fmt.Errorf("seek EOCD from end error: %w", err)
	}

	for range 10 {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		if _, err = src.Read(eocd); err != nil {
			return 0, 0, fmt.Errorf("read EOCD error: %w", err)
		}

		if i := bytes.LastIndex(eocd, sigEOCD); i != -1 {
			recordCount = int(binary.LittleEndian.Uint16(eocd[i+10 : i+12]))
			cdOffset = int64(binary.LittleEndian.Uint32(eocd[i+16 : i+20]))
			return
		}

		if _, err = src.Seek(int64(-len(eocd)+4), io.SeekCurrent); err != nil {
			return 0, 0, fmt.Errorf("seek EOCD from current error: %w", err)
		}
	}

	return 0, 0, ErrNoEOCDFound
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
