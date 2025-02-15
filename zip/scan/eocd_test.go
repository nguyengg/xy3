package scan

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindEOCD_FromFile(t *testing.T) {
	tests := []struct {
		name     string
		testdata string
		expected EOCDRecord
	}{
		{
			name:     "default.zip",
			testdata: "../testdata/default.zip",
			expected: EOCDRecord{
				DiskNumber:    0,
				CDDiskOffset:  0,
				CDCountOnDisk: 3,
				CDCount:       3,
				CDSize:        258,
				CDOffset:      888,
			},
		},
		{
			name:     "unwrap_root.zip",
			testdata: "../testdata/unwrap_root.zip",
			expected: EOCDRecord{
				DiskNumber:    0,
				CDDiskOffset:  0,
				CDCountOnDisk: 3,
				CDCount:       3,
				CDSize:        243,
				CDOffset:      873,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.testdata)
			assert.NoErrorf(t, err, "os.Open(%s) error = %v", tt.testdata, err)
			defer f.Close()

			r, err := findEOCD(f, &CentralDirectoryOptions{Ctx: context.Background()})
			assert.NoErrorf(t, err, "findEOCD(%s) error = %v", tt.testdata, err)
			assert.Equal(t, tt.expected, r)
		})
	}
}

func TestFindEOCD_WithComment(t *testing.T) {
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	tests := []struct {
		commentLength int
	}{
		{
			commentLength: 8 * 1024,
		},
		{
			commentLength: 16 * 1024,
		},
		{
			commentLength: 32 * 1024,
		},
		{
			commentLength: 48 * 1024,
		},
	}

	for _, tt := range tests {
		for _, delta := range []int{-4, -3, -2, -1, 0, 1, 2, 3, 4} {
			t.Run(fmt.Sprintf("%d with delta=%d", tt.commentLength, delta), func(t *testing.T) {
				n := tt.commentLength + delta
				comment := make([]byte, n)
				for i := range n {
					comment[i] = alphabet[rand.IntN(len(alphabet))]
				}

				buf := &bytes.Buffer{}
				zw := zip.NewWriter(buf)

				err := zw.SetComment(string(comment))
				assert.NoErrorf(t, err, "SetComment(...) error = %v", err)

				err = zw.Close()
				assert.NoErrorf(t, err, "Close() error = %v", err)
				assert.Equalf(t, tt.commentLength+22+delta, buf.Len(), "Mismatched buffer size; got = %d, want = %d", buf.Len(), tt.commentLength+22+delta)

				r, err := findEOCD(bytes.NewReader(buf.Bytes()), &CentralDirectoryOptions{
					Ctx:         context.Background(),
					KeepComment: true,
				})
				assert.NoErrorf(t, err, "findEOCD() error = %v", err)
				assert.Equal(t, string(comment), r.Comment)
			})
		}
	}
}
