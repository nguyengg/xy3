package scan

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"iter"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func collectCRC32sAndOffsets(it iter.Seq2[ReadableFileHeader, error]) (crc32s, offsets map[string]int64, err error) {
	crc32s = make(map[string]int64)
	offsets = make(map[string]int64)

	for fh, err := range it {
		if err != nil {
			return nil, nil, err
		}

		if v, ok := fh.(*localFileHeader); ok {
			crc32s[v.Name] = int64(v.CRC32)
			offsets[v.Name] = v.localHeaderOffset
			continue
		}
		if v, ok := fh.(*cdFileHeader); ok {
			crc32s[v.Name] = int64(v.CRC32)
			offsets[v.Name] = v.localHeaderOffset

			if _, err = v.Open(); err != nil {
				return nil, nil, err
			}

			// TODO see why WriteTo breaks the test.
			//if _, err = v.WriteTo(io.Discard); err != nil {
			//	return nil, nil, err
			//}

			continue
		}

		return nil, nil, fmt.Errorf("unknown type: %T", fh)
	}

	return
}

func TestScan(t *testing.T) {
	// the zip files in testdata should have fixed paths, CRC32sl, and offsets.
	tests := []struct {
		name            string
		testdata        string
		expectedCRC32s  map[string]int64
		expectedOffsets map[string]int64
	}{
		{
			name:     "default.zip",
			testdata: "../testdata/default.zip",
			expectedCRC32s: map[string]int64{
				"test/a.txt":              0x506d938f,
				"test/another/path/c.txt": 0xe434c8e8,
				"test/path/b.txt":         0xb9bb1847,
			},
			expectedOffsets: map[string]int64{
				"test/a.txt":              0x0,
				"test/another/path/c.txt": 0xc6,
				"test/path/b.txt":         0x245,
			},
		},
		{
			name:     "unwrap_root.zip",
			testdata: "../testdata/unwrap_root.zip",
			expectedCRC32s: map[string]int64{
				"a.txt":              0x506d938f,
				"another/path/c.txt": 0xe434c8e8,
				"path/b.txt":         0xb9bb1847,
			},
			expectedOffsets: map[string]int64{
				"a.txt":              0x0,
				"another/path/c.txt": 0xc1,
				"path/b.txt":         0x23b,
			},
		},
		{
			name:     "write_dir.zip",
			testdata: "../testdata/write_dir.zip",
			expectedCRC32s: map[string]int64{
				"test/":       0x0,
				"test/a.txt":  0x506d938f,
				"test/empty/": 0x0,
			},
			expectedOffsets: map[string]int64{
				"test/":       0x0,
				"test/a.txt":  0x3f,
				"test/empty/": 0x105,
			},
		},
		{
			name:     "write_dir_unwrap_root.zip",
			testdata: "../testdata/write_dir_unwrap_root.zip",
			expectedCRC32s: map[string]int64{
				"a.txt":  0x506d938f,
				"empty/": 0x0,
			},
			expectedOffsets: map[string]int64{
				"a.txt":  0x0,
				"empty/": 0xc1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.testdata)
			assert.NoErrorf(t, err, "Open(%s) error = %v", tt.testdata, err)
			defer f.Close()

			fi, err := f.Stat()
			assert.NoErrorf(t, err, "os.Stat(%s) error = %v", tt.testdata, err)

			_, headers, err := CentralDirectoryWithReaderAt(f, fi.Size())
			assert.NoErrorf(t, err, "CentralDirectoryWithReaderAt(...) error = %v", err)

			// we only care about the CRC32s and the offsets so pull that from the headers.
			crc32s, offsets, err := collectCRC32sAndOffsets(headers)
			assert.NoErrorf(t, err, "collectCRC32sAndOffsets(...) error = %v", err)
			assert.Equal(t, tt.expectedCRC32s, crc32s)
			assert.Equal(t, tt.expectedOffsets, offsets)

			// do same thing but with CentralDirectory.
			_, headers, err = CentralDirectory(f)
			assert.NoErrorf(t, err, "CentralDirectory(...) error = %v", err)
			crc32s, offsets, err = collectCRC32sAndOffsets(headers)
			assert.NoErrorf(t, err, "collectCRC32sAndOffsets(...) error = %v", err)
			assert.Equal(t, tt.expectedCRC32s, crc32s)
			assert.Equal(t, tt.expectedOffsets, offsets)

			// now for Forward.
			_, _ = f.Seek(0, 0)
			headers = Forward(f)
			crc32s, offsets, err = collectCRC32sAndOffsets(headers)
			assert.NoErrorf(t, err, "collectCRC32sAndOffsets(...) error = %v", err)
			assert.Equal(t, tt.expectedCRC32s, crc32s)
			assert.Equal(t, tt.expectedOffsets, offsets)

			// finally for ForwardWithReaderAt.
			headers = ForwardWithReaderAt(f)
			crc32s, offsets, err = collectCRC32sAndOffsets(headers)
			assert.NoErrorf(t, err, "collectCRC32sAndOffsets(...) error = %v", err)
			assert.Equal(t, tt.expectedCRC32s, crc32s)
			assert.Equal(t, tt.expectedOffsets, offsets)
		})
	}
}

func TestCdFileHeader_OpenWriteToWithReaderAt(t *testing.T) {
	// this will create a ZIP file with a local header that requires data descriptor.
	expected := make([]byte, 1024)
	_, _ = io.ReadFull(rand.Reader, expected)
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	w, _ := zw.Create("test")
	_, _ = w.Write(expected)
	_ = zw.Close()

	_, headers, err := CentralDirectoryWithReaderAt(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	assert.NoErrorf(t, err, "CentralDirectoryWithReaderAt(...) error = %v", err)

	for fh := range headers {
		// test Open first.
		r, err := fh.Open()
		assert.NoErrorf(t, err, "CdFileHeader.Open() error = %v", err)
		actual, err := io.ReadAll(r)
		assert.NoErrorf(t, err, "ReadAll(...) error = %v", err)
		assert.Equal(t, expected, actual)

		// test WriteTo next.
		newBuf := &bytes.Buffer{}
		_, err = fh.WriteTo(newBuf)
		assert.NoErrorf(t, err, "WriteTo(...) error = %v", err)
		assert.Equal(t, expected, newBuf.Bytes())
	}
}

func TestCdFileHeader_OpenWriteToWithReader(t *testing.T) {
	// this will create a ZIP file with a local header that requires data descriptor.
	expected := make([]byte, 1024)
	_, _ = io.ReadFull(rand.Reader, expected)
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	w, _ := zw.Create("test")
	_, _ = w.Write(expected)
	_ = zw.Close()

	_, headers, err := CentralDirectory(bytes.NewReader(buf.Bytes()))
	assert.NoErrorf(t, err, "CentralDirectory(...) error = %v", err)

	for fh := range headers {
		// test Open first.
		r, err := fh.Open()
		assert.NoErrorf(t, err, "CdFileHeader.Open() error = %v", err)
		actual, err := io.ReadAll(r)
		assert.NoErrorf(t, err, "ReadAll(...) error = %v", err)
		assert.Equal(t, expected, actual)

		// test WriteTo next.
		newBuf := &bytes.Buffer{}
		_, err = fh.WriteTo(newBuf)
		assert.NoErrorf(t, err, "WriteTo(...) error = %v", err)
		assert.Equal(t, expected, newBuf.Bytes())
	}
}
