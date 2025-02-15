package zipper

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test CompressFile by writing random bytes to a temp file, compress, extract, and compare the files.
func TestCompressFile(t *testing.T) {
	ctx := context.Background()

	name, data, err := createRandomTemp(1024)
	assert.NoErrorf(t, err, "createRandomTemp() error = %v", err)
	defer os.Remove(name)

	tests := []struct {
		name string
		opts []func(*CompressOptions)
	}{
		{
			name: "DefaultZipper",
			opts: nil,
		},
		{
			name: "WithBestCompression",
			opts: []func(*CompressOptions){WithBestCompression},
		},
		{
			name: "WithNoCompression",
			opts: []func(*CompressOptions){WithNoCompression},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// compress.
			archiveFile, err := os.CreateTemp("", "*.zip")
			assert.NoErrorf(t, err, "os.CreateTemp() error = %v", err)
			defer func(f *os.File) {
				_, _ = f.Close(), os.Remove(f.Name())
			}(archiveFile)

			err = CompressFile(ctx, name, archiveFile, tt.opts...)
			assert.NoErrorf(t, err, "CompressFile() error = %v", err)

			// open the archive up for inspection.
			r, err := zip.OpenReader(archiveFile.Name())
			assert.NoErrorf(t, err, "zip.OpenReader() error = %v", err)

			// there should be exactly one file.
			assert.Equalf(t, 1, len(r.File), "expected 1 file in archive, got %d", len(r.File))

			f := r.File[0]
			assert.Equalf(t, filepath.Base(name), f.Name, "expected file named %s in archive, got %s", filepath.Base(name), f.Name)
			rc, err := f.Open()
			assert.NoErrorf(t, err, "zip.File.Open() error = %v", err)

			var buf bytes.Buffer
			_, err = buf.ReadFrom(rc)
			assert.NoErrorf(t, err, "read from zip file error = %v", err)
			_ = rc.Close()

			assert.Equalf(t, data, buf.Bytes(), "data in archive is not identical to test data")
		})
	}
}

// createRandomTemp creates a temp file, writes random data to it, then return the file name.
func createRandomTemp(size int) (string, []byte, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		return "", nil, err
	}

	f, err := os.CreateTemp("", "")
	if err != nil {
		return "", nil, err
	}

	_, err = f.Write(data)
	_ = f.Close()
	return f.Name(), data, err
}
