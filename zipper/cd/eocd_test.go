package cd

import (
	"archive/zip"
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindEOCD_FromFile(t *testing.T) {
	tests := []struct {
		name     string
		testdata string
		expected *EOCDRecord
	}{
		{
			name:     "default.zip",
			testdata: "../testdata/default.zip",
			expected: &EOCDRecord{
				DiskNumber:    0,
				CDDiskOffset:  0,
				CDCountOnDisk: 3,
				CDCount:       3,
				CDSize:        258,
				CDOffset:      888,
				Comment:       nil,
			},
		},
		{
			name:     "unwrap_root.zip",
			testdata: "../testdata/unwrap_root.zip",
			expected: &EOCDRecord{
				DiskNumber:    0,
				CDDiskOffset:  0,
				CDCountOnDisk: 3,
				CDCount:       3,
				CDSize:        243,
				CDOffset:      873,
				Comment:       nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.testdata)
			assert.NoErrorf(t, err, "os.Open(%s) error = %v", tt.testdata, err)
			defer f.Close()

			r, err := findEOCD(f, &Options{Ctx: context.Background()})
			assert.NoErrorf(t, err, "findEOCD(%s) error = %v", tt.testdata, err)
			assert.Equal(t, tt.expected, r)
		})
	}
}

func TestFindEOCD_WithComment(t *testing.T) {
	// knowing the buffer size is 32*1024, this test creates a zip file with comment of exactly 32*1024 -4, -3, -2,
	// -1, and -0 character long to test finding the EOCD signature right on the edge.

	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	tests := []struct {
		name          string
		commentLength int
		expectedSize  int
	}{
		{
			name:          "32*1024 + 4",
			commentLength: 32*1024 + 4,
			expectedSize:  32794,
		},
		{
			name:          "32*1024 + 3",
			commentLength: 32*1024 + 3,
			expectedSize:  32793,
		},
		{
			name:          "32*1024 + 2",
			commentLength: 32*1024 + 2,
			expectedSize:  32792,
		},
		{
			name:          "32*1024 + 1",
			commentLength: 32*1024 + 1,
			expectedSize:  32791,
		},
		{
			name:          "32*1024",
			commentLength: 32 * 1024,
			expectedSize:  32790,
		},
		{
			name:          "32*1024 - 1",
			commentLength: 32*1024 - 1,
			expectedSize:  32789,
		},
		{
			name:          "32*1024 - 2",
			commentLength: 32*1024 - 2,
			expectedSize:  32788,
		},
		{
			name:          "32*1024 - 3",
			commentLength: 32*1024 - 3,
			expectedSize:  32787,
		},
		{
			name:          "32*1024 - 4",
			commentLength: 32*1024 - 4,
			expectedSize:  32786,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comment := make([]byte, tt.commentLength)
			for i := range tt.commentLength {
				comment[i] = alphabet[rand.IntN(len(alphabet))]
			}

			buf := &bytes.Buffer{}
			zw := zip.NewWriter(buf)

			err := zw.SetComment(string(comment))
			assert.NoErrorf(t, err, "SetComment(...) error = %v", err)

			err = zw.Close()
			assert.NoErrorf(t, err, "Close() error = %v", err)

			assert.Equalf(t, tt.expectedSize, buf.Len(), "Mismatched buffer size; got = %d, want = %d", buf.Len(), tt.expectedSize)

			r, err := findEOCD(bytes.NewReader(buf.Bytes()), &Options{
				Ctx:         context.Background(),
				KeepComment: true,
			})
			assert.NoErrorf(t, err, "findEOCD() error = %v", err)
			assert.Equal(t, comment, r.Comment)
		})
	}
}
