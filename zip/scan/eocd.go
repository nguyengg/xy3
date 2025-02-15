package scan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/valyala/bytebufferpool"
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
		// bb contains two parts: the most recently read data from byte 0 to byte 16*1024 (or n, whichever is
		// smaller from the read call), and previously read data for the remaining bytes.
		bb = bytebufferpool.Get()
		// buf is the buffer for a single read which is then prepended to bb.B.
		buf = make([]byte, 16*1024)

		bufSize      = int64(len(buf))
		offset       int64
		readN, bbLen int
	)
	defer bytebufferpool.Put(bb)

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
		select {
		case <-opts.Ctx.Done():
			return r, opts.Ctx.Err()
		default:
		}

		// bb.Len() must be at least 22 bytes since that is the minimum EOCD size.
		if readN, err = src.Read(buf); err != nil && !errors.Is(err, io.EOF) {
			return r, fmt.Errorf("find EOCD: read error: %w", err)
		} else {
			// must copy to prevent byte slicing shenanigans since we'll be reusing buf.
			b := make([]byte, readN+bbLen)
			_ = copy(b, buf[:readN])
			_ = copy(b[readN:], bb.B)
			bb.B = b
			if bbLen = bb.Len(); bbLen < 22 {
				return r, fmt.Errorf("find EOCD: insufficient read: need at least 22 bytes, got %d", readN)
			}
		}

		// when searching for EOCD signature, start at n+3 if possible to avoid reading through previous data.
		if i := bytes.LastIndex(bb.B[:min(readN+3, bbLen)], eocdSigBytes); i != -1 {
			fsr := &fixedSizeEOCDRecord{}
			if err = binary.Read(bytes.NewReader(bb.B[i:i+22]), binary.LittleEndian, fsr); err != nil {
				return r, fmt.Errorf("find EOCD: parse error: %w", err)
			}

			r = EOCDRecord{
				DiskNumber:    fsr.DiskNumber,
				CDDiskOffset:  fsr.CDDiskOffset,
				CDCountOnDisk: fsr.CDCountOnDisk,
				CDCount:       fsr.CDCount,
				CDSize:        fsr.CDSize,
				CDOffset:      fsr.CDOffset,
			}
			if opts.KeepComment {
				if r.Comment = string(bb.B[i+22:]); len(r.Comment) != int(fsr.CommentLength) {
					return r, fmt.Errorf("find EOCD: mismatched comment size, expected %d, got %d", fsr.CommentLength, len(r.Comment))
				}
			}

			return
		}

		// if we're already at start of file or at limit, stop reading.
		if offset == 0 || (opts.MaxBytes > 0 && int64(bbLen) >= opts.MaxBytes) {
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

// fixedSizeEOCDRecord needs to be fixed size to work with binary.Read.
//
// https://en.wikipedia.org/wiki/ZIP_(file_format)#End_of_central_directory_record_(EOCD)
type fixedSizeEOCDRecord struct {
	Signature     uint32
	DiskNumber    uint16
	CDDiskOffset  uint16
	CDCountOnDisk uint16
	CDCount       uint16
	CDSize        uint32
	CDOffset      uint32
	CommentLength uint16
}
