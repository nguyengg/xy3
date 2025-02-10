package zipper

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCDScanner(t *testing.T) {
	// the zip files in testdata should have fixed attributes from parsing.
	tests := []struct {
		name     string
		testdata string
		expected map[string]uint64
	}{
		{
			name:     "default.zip",
			testdata: "testdata/default.zip",
			expected: map[string]uint64{
				"test/a.txt":              0x0,
				"test/another/path/c.txt": 0xc6,
				"test/path/b.txt":         0x245,
			},
		},
		{
			name:     "unwrap_root.zip",
			testdata: "testdata/unwrap_root.zip",
			expected: map[string]uint64{
				"a.txt":              0x0,
				"another/path/c.txt": 0xc1,
				"path/b.txt":         0x23b,
			},
		},
		{
			name:     "write_dir.zip",
			testdata: "testdata/write_dir.zip",
			expected: map[string]uint64{
				"test/":       0x0,
				"test/a.txt":  0x3f,
				"test/empty/": 0x105,
			},
		},
		{
			name:     "write_dir_unwrap_root.zip",
			testdata: "testdata/write_dir_unwrap_root.zip",
			expected: map[string]uint64{
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

			cd, err := NewCDScanner(f, fi.Size())
			assert.NoErrorf(t, err, "NewCDScanner(...) error = %v", err)

			// we only care about the offset so pull that from the headers.
			actual := make(map[string]uint64)
			for fh := range cd.All() {
				actual[fh.Name] = fh.Offset
			}
			assert.NoErrorf(t, cd.Err(), "All() error = %v", cd.Err())

			assert.Equal(t, tt.expected, actual)
		})
	}
}
