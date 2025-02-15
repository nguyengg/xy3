package scan

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCentralDirectory(t *testing.T) {
	// the zip files in testdata should have fixed attributes from parsing.
	tests := []struct {
		name     string
		testdata string
		expected map[string]int64
	}{
		{
			name:     "default.zip",
			testdata: "../testdata/default.zip",
			expected: map[string]int64{
				"test/a.txt":              0x0,
				"test/another/path/c.txt": 0xc6,
				"test/path/b.txt":         0x245,
			},
		},
		{
			name:     "unwrap_root.zip",
			testdata: "../testdata/unwrap_root.zip",
			expected: map[string]int64{
				"a.txt":              0x0,
				"another/path/c.txt": 0xc1,
				"path/b.txt":         0x23b,
			},
		},
		{
			name:     "write_dir.zip",
			testdata: "../testdata/write_dir.zip",
			expected: map[string]int64{
				"test/":       0x0,
				"test/a.txt":  0x3f,
				"test/empty/": 0x105,
			},
		},
		{
			name:     "write_dir_unwrap_root.zip",
			testdata: "../testdata/write_dir_unwrap_root.zip",
			expected: map[string]int64{
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

			// we only care about the offset so pull that from the headers.
			actual := make(map[string]int64)
			for fh, err := range headers {
				assert.NoErrorf(t, err, "headers error = %v", err)

				cdfh, ok := fh.(*CentralDirectoryFileHeader)
				assert.Truef(t, ok, "fh is not CentralDirectoryFileHeader; type = %T", fh)
				actual[cdfh.fh.Name] = cdfh.Offset
			}
			assert.Equal(t, tt.expected, actual)

			// do same thing but with a different method.
			_, headers, err = CentralDirectory(f)
			assert.NoErrorf(t, err, "Scan(...) error = %v", err)

			actual = make(map[string]int64)
			for fh, err := range headers {
				cdfh, ok := fh.(*CentralDirectoryFileHeader)
				assert.Truef(t, ok, "fh is not CentralDirectoryFileHeader; type = %T", fh)
				assert.NoErrorf(t, err, "headers error = %v", err)
				actual[cdfh.fh.Name] = cdfh.Offset
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}
