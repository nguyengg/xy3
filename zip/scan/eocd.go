package scan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// EOCDRecord models the end of central directory record of a ZIP file.
//
// See https://en.wikipedia.org/wiki/ZIP_(file_format)#End_of_central_directory_record_(EOCD).
type EOCDRecord struct {
	// DiskNumber is number of this disk (or 0xffff for ZIP64).
	DiskNumber uint16
	// CDDiskOffset is disk where central directory starts (or 0xffff for ZIP64).
	CDDiskOffset uint16
	// CDCountOnDisk is then umber of central directory records on this disk (or 0xffff for ZIP64).
	CDCountOnDisk uint16
	// CDCount is the total number of central directory records (or 0xffff for ZIP64).
	CDCount uint16
	// CDSize is size of central directory (bytes) (or 0xffffffff for ZIP64).
	CDSize uint32
	// CDOffset is offset of start of central directory, relative to start of archive (or 0xffffffff for ZIP64).
	CDOffset uint32
	// Comment is the comment section of the EOCD.
	Comment string
}

// findEOCD searches the given src backwards for the EOCD record.
func findEOCD(src io.ReadSeeker, opts *CentralDirectoryOptions) (r EOCDRecord, err error) {
	var (
		// two buffers are used.
		// buf is the fixed-size read buffer to be used with src.Read.
		// b is the variable-sized read buffer that contains data from previous reads.
		// after buf is written to with src.Read, buf is prepended to b so that processing on b is easy.
		buf     = make([]byte, 16*1024)
		b       = make([]byte, 0)
		bufSize = int64(len(buf))
		offset  int64
	)

	// the first seek is only for the last 22 bytes so that we can get an accurate assessment of the file size
	// from the offset (size = offset + 22).
	if offset, err = src.Seek(-22, io.SeekEnd); err != nil {
		return r, fmt.Errorf("find EOCD: set read offset at -22 from end error: %w", err)
	}

	// if file is minuscule that can fit in readN then just read all of them at once.
	if offset+22 < bufSize {
		if offset, err = src.Seek(0, io.SeekStart); err != nil {
			return r, fmt.Errorf("find EOCD: set read offset at 0 from start error: %w", err)
		}
	} else if offset, err = src.Seek(-bufSize, io.SeekEnd); err != nil {
		return r, fmt.Errorf("find EOCD: set set read at %d from end error: %w", -bufSize, err)
	}

	for {
		switch n, err := src.Read(buf); {
		case err != nil && !errors.Is(err, io.EOF):
			return r, fmt.Errorf("find EOCD: read error: %w", err)
		default:
			b = append(make([]byte, n), b...)
			copy(b, buf[:n])

			if len(b) < 22 {
				return r, fmt.Errorf("find EOCD: insufficient read: need at least 22 bytes, got %d", n)
			}

			if i := bytes.LastIndex(b[:min(n+3, len(b))], eocdSigBytes); i != -1 {
				if r, err = unmarshalEOCDRecord(([22]byte)(b[i:i+22]), func(c []byte) (int, error) {
					return copy(c, b[i+22:]), nil
				}); err != nil {
					return r, fmt.Errorf("find ECOD: %w", err)
				}

				return r, nil
			}
		}

		// if we're already at start of file or at limit, stop reading.
		if offset == 0 || (opts.MaxBytes > 0 && int64(len(b)) >= opts.MaxBytes) {
			return r, ErrNoEOCDFound
		}

		// the trick is to make sure buf never overlaps comment by reducing buf size if needed.
		if offset < bufSize {
			buf = make([]byte, offset)
			offset = 0
		} else {
			offset -= bufSize
		}

		// move offset to prepare for next read.
		if offset, err = src.Seek(offset, io.SeekStart); err != nil {
			return r, fmt.Errorf("find EOCD: set read offset at %d from start error: %w", offset, err)
		}
	}
}

// unmarshalEOCDRecord decodes the 22-byte slice as a EOCDRecord.
// read will always be called to retrieve the variable-size part of the header. if there is no variable-size part, read
// will be passed an empty slice.
func unmarshalEOCDRecord(b [22]byte, read func(b []byte) (int, error)) (r EOCDRecord, err error) {
	data := &struct {
		Signature     uint32
		DiskNumber    uint16
		CDDiskOffset  uint16
		CDCountOnDisk uint16
		CDCount       uint16
		CDSize        uint32
		CDOffset      uint32
		CommentLength uint16
	}{}

	if bytes.Compare(eocdSigBytes, b[:4]) != 0 {
		return r, fmt.Errorf("mismatched signature, got 0x%x, expected 0x%x", b[:4], eocdSigBytes)
	}

	if err = binary.Read(bytes.NewReader(b[:]), binary.LittleEndian, data); err != nil {
		return r, fmt.Errorf("unmarshal error: %w", err)
	}

	r = EOCDRecord{
		DiskNumber:    data.DiskNumber,
		CDDiskOffset:  data.CDDiskOffset,
		CDCountOnDisk: data.CDCountOnDisk,
		CDCount:       data.CDCount,
		CDSize:        data.CDSize,
		CDOffset:      data.CDOffset,
	}

	comment := make([]byte, data.CommentLength)
	switch readN, err := read(comment); {
	case err != nil && !errors.Is(err, io.EOF):
		return r, fmt.Errorf("read variable-size data error: %w", err)
	case readN < int(data.CommentLength):
		return r, fmt.Errorf("read variable-size data error: insufficient read: needs at least %d bytes, got %d", data.CommentLength, readN)
	default:
		r.Comment = string(comment)
	}

	return r, nil
}
