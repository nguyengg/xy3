package z

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtract(t *testing.T) {
	tests := []struct {
		name              string
		testdata          string
		useGivenDirectory bool
		noUnwrapRoot      bool
		expectedBaseDir   string
		expected          []string
	}{
		{
			// default.zip was created with UnwrapRoot=false, WriteDir=false
			// its content is:
			//	test/a.txt
			//	test/path/b.txt
			//	test/another/path/c.txt
			name:            "default.zip",
			testdata:        "testdata/default.zip",
			expectedBaseDir: "default",
			expected: []string{
				"default/a.txt",
				"default/path/b.txt",
				"default/another/path/c.txt",
			},
		},
		{
			name:              "default.zip with useGivenDirectory=true",
			testdata:          "testdata/default.zip",
			useGivenDirectory: true,
			// * will be replaced with tmpDir, showing that the given dir was used as output dir.
			expectedBaseDir: "*",
			expected: []string{
				"*/a.txt",
				"*/path/b.txt",
				"*/another/path/c.txt",
			},
		},
		{
			name:            "default.zip with noUnwrapRoot=true",
			testdata:        "testdata/default.zip",
			noUnwrapRoot:    true,
			expectedBaseDir: "default",
			expected: []string{
				// noUnwrapRoot means the test root directory is retained.
				"default/test/a.txt",
				"default/test/path/b.txt",
				"default/test/another/path/c.txt",
			},
		},
		{
			name:              "default.zip with useGivenDirectory=true and noUnwrapRoot=true",
			testdata:          "testdata/default.zip",
			useGivenDirectory: true,
			noUnwrapRoot:      true,
			expectedBaseDir:   "*",
			expected: []string{
				// noUnwrapRoot means the test root directory is retained.
				"*/test/a.txt",
				"*/test/path/b.txt",
				"*/test/another/path/c.txt",
			},
		},
		{
			// unwrap_root.zip was created with UnwrapRoot=true, WriteDir=false
			// its content is:
			//	a.txt
			//	path/b.txt
			//	another/path/c.txt
			name:            "unwrap_root.zip",
			testdata:        "testdata/unwrap_root.zip",
			expectedBaseDir: "unwrap_root",
			expected: []string{
				"unwrap_root/a.txt",
				"unwrap_root/path/b.txt",
				"unwrap_root/another/path/c.txt",
			},
		},
		{
			name:              "unwrap_root.zip with useGivenDirectory=true",
			testdata:          "testdata/unwrap_root.zip",
			useGivenDirectory: true,
			// * will be replaced with tmpDir, showing that the given dir was used as output dir.
			expectedBaseDir: "*",
			expected: []string{
				"*/a.txt",
				"*/path/b.txt",
				"*/another/path/c.txt",
			},
		},
		{
			name:            "unwrap_root.zip with noUnwrapRoot=true",
			testdata:        "testdata/unwrap_root.zip",
			noUnwrapRoot:    true,
			expectedBaseDir: "unwrap_root",
			expected: []string{
				// because unwrap_root.zip didn't have a root dir, it is identical to noUnwrapRoot=false
				"unwrap_root/a.txt",
				"unwrap_root/path/b.txt",
				"unwrap_root/another/path/c.txt",
			},
		},
		{
			name:              "unwrap_root.zip with useGivenDirectory=true and noUnwrapRoot=true",
			testdata:          "testdata/unwrap_root.zip",
			useGivenDirectory: true,
			noUnwrapRoot:      true,
			expectedBaseDir:   "*",
			expected: []string{
				// because unwrap_root.zip didn't have a root dir, it is identical to noUnwrapRoot=false
				"*/a.txt",
				"*/path/b.txt",
				"*/another/path/c.txt",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "")
			assert.NoErrorf(t, err, `MkdirTemp("", "") error = %v`, err)
			defer os.RemoveAll(tmpDir)

			actualDir, err := Extract(context.Background(), tt.testdata, tmpDir, func(options *ExtractOptions) {
				options.UseGivenDirectory = tt.useGivenDirectory
				options.NoUnwrapRoot = tt.noUnwrapRoot
			})
			assert.NoErrorf(t, err, `Extract(_, %s, %s) error = %v`, tt.testdata, tmpDir, err)

			expectedBaseDir := strings.ReplaceAll(tt.expectedBaseDir, "*", filepath.Base(tmpDir))
			assert.Equalf(t, expectedBaseDir, filepath.Base(actualDir), "Extract(_, %s, %s) did not return expected base dir; got %v, expected %v", tt.testdata, tmpDir, actualDir, expectedBaseDir)

			tt.expected = cleanAndSortRepl(tt.expected, tmpDir)
			actual := lsAndSort(actualDir, false)
			assert.Equalf(t, tt.expected, actual, "Extract(_, %s, %s) did not produce expected directory structure: got %v, want %v", tt.testdata, tmpDir, actualDir, tt.expected)
		})
	}
}

func TestExtract_WriteDir(t *testing.T) {
	tests := []struct {
		name              string
		testdata          string
		useGivenDirectory bool
		noUnwrapRoot      bool
		expectedBaseDir   string
		expected          []string
	}{
		{
			// testdata/write_dir.zip has this content:
			// test/empty/
			// test/a.txt
			name:            "write_dir.zip",
			testdata:        "testdata/write_dir.zip",
			expectedBaseDir: "write_dir",
			expected: []string{
				"write_dir/empty",
				"write_dir/a.txt",
			},
		},
		{
			name:              "write_dir.zip with useGivenDirectory=true",
			testdata:          "testdata/write_dir.zip",
			useGivenDirectory: true,
			// * will be replaced with tmpDir, showing that the given dir was used as output dir.
			expectedBaseDir: "*",
			expected: []string{
				"*/empty",
				"*/a.txt",
			},
		},
		{
			name:            "write_dir.zip with noUnwrapRoot=true",
			testdata:        "testdata/write_dir.zip",
			noUnwrapRoot:    true,
			expectedBaseDir: "write_dir",
			expected: []string{
				"write_dir/test",
				"write_dir/test/empty",
				"write_dir/test/a.txt",
			},
		},
		{
			name:              "write_dir.zip with useGivenDirectory=true and noUnwrapRoot=true",
			testdata:          "testdata/write_dir.zip",
			useGivenDirectory: true,
			noUnwrapRoot:      true,
			expectedBaseDir:   "*",
			expected: []string{
				"*/test",
				"*/test/empty",
				"*/test/a.txt",
			},
		},
		{
			// testdata/write_dir_unwrap_root.zip has this content:
			// empty/
			// a.txt
			name:            "write_dir_unwrap_root.zip",
			testdata:        "testdata/write_dir_unwrap_root.zip",
			expectedBaseDir: "write_dir_unwrap_root",
			expected: []string{
				"write_dir_unwrap_root/empty",
				"write_dir_unwrap_root/a.txt",
			},
		},
		{
			name:              "write_dir_unwrap_root.zip with useGivenDirectory=true",
			testdata:          "testdata/write_dir_unwrap_root.zip",
			useGivenDirectory: true,
			// * will be replaced with tmpDir, showing that the given dir was used as output dir.
			expectedBaseDir: "*",
			expected: []string{
				"*/empty",
				"*/a.txt",
			},
		},
		{
			name:            "write_dir_unwrap_root.zip with noUnwrapRoot=true",
			testdata:        "testdata/write_dir_unwrap_root.zip",
			noUnwrapRoot:    true,
			expectedBaseDir: "write_dir_unwrap_root",
			expected: []string{
				"write_dir_unwrap_root/empty",
				"write_dir_unwrap_root/a.txt",
			},
		},
		{
			name:              "write_dir_unwrap_root.zip with useGivenDirectory=true and noUnwrapRoot=true",
			testdata:          "testdata/write_dir_unwrap_root.zip",
			useGivenDirectory: true,
			noUnwrapRoot:      true,
			expectedBaseDir:   "*",
			expected: []string{
				// because write_dir_unwrap_root.zip didn't have a root dir, it is identical to noUnwrapRoot=false
				"*/empty",
				"*/a.txt",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "")
			assert.NoErrorf(t, err, `MkdirTemp("", "") error = %v`, err)
			defer os.RemoveAll(tmpDir)

			actualDir, err := Extract(context.Background(), tt.testdata, tmpDir, func(options *ExtractOptions) {
				options.UseGivenDirectory = tt.useGivenDirectory
				options.NoUnwrapRoot = tt.noUnwrapRoot
			})
			assert.NoErrorf(t, err, `Extract(_, %s, %s) error = %v`, tt.testdata, tmpDir, err)

			expectedBaseDir := strings.ReplaceAll(tt.expectedBaseDir, "*", filepath.Base(tmpDir))
			assert.Equalf(t, expectedBaseDir, filepath.Base(actualDir), "Extract(_, %s, %s) did not return expected base dir; got %v, expected %v", tt.testdata, tmpDir, actualDir, expectedBaseDir)

			tt.expected = cleanAndSortRepl(tt.expected, tmpDir)
			actual := lsAndSort(actualDir, true)
			assert.Equalf(t, tt.expected, actual, "Extract(_, %s, %s) did not produce expected directory structure: got %v, want %v", tt.testdata, tmpDir, actualDir, tt.expected)
		})
	}
}

func cleanAndSortRepl(paths []string, tmpDir string) []string {
	tmpDir = filepath.Base(tmpDir)

	for i, path := range paths {
		paths[i] = filepath.Clean(strings.ReplaceAll(path, "*", tmpDir))
		if strings.HasSuffix(path, "/") {
			paths[i] += "/"
		}
	}

	slices.Sort(paths)

	return paths
}

func lsAndSort(dir string, includeDir bool) []string {
	names := make([]string, 0)
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == dir {
			return err
		}

		if d.Type().IsRegular() || (includeDir && d.IsDir()) {
			names = append(names, rel(dir, path))
		}

		return nil
	}); err != nil {
		panic(err)
	}

	slices.Sort(names)

	return names
}
