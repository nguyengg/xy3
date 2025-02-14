package cd

import "encoding/binary"

// EOCDRecord models the end of central directory record of aa ZIP file.
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
	Comment []byte
}

var (
	sigEOCD = make([]byte, 4)
)

func init() {
	binary.LittleEndian.PutUint32(sigEOCD, 0x06054b50)
}

// parseEOCD parses the given byte slice for a valid EOCDRecord.
//
// Returns nil if the EOCD signature (0x06054b50) is not found. Returns an error if the EOCD is invalid.
func parseEOCD(data []byte) (*EOCDRecord, error) {

}
