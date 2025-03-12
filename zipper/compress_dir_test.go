package zipper

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressDir(t *testing.T) {
	// testdata/my-dir should look like this.
	//	my-dir/a.txt
	//	my-dir/path/b.txt
	//	my-dir/another/path/c.txt
	tests := []struct {
		name       string
		unwrapRoot bool
		writeDir   bool
		expected   []string
	}{
		{
			name:       "default",
			unwrapRoot: false,
			writeDir:   false,
			expected: []string{
				"my-dir/a.txt",
				"my-dir/path/b.txt",
				"my-dir/another/path/c.txt",
			},
		},
		{
			name:       "unwrapRoot=true",
			unwrapRoot: true,
			writeDir:   false,
			expected: []string{
				"a.txt",
				"path/b.txt",
				"another/path/c.txt",
			},
		},
		{
			name:       "writeDir=true",
			unwrapRoot: false,
			writeDir:   true,
			expected: []string{
				"my-dir/",
				"my-dir/a.txt",
				"my-dir/path/",
				"my-dir/path/b.txt",
				"my-dir/another/",
				"my-dir/another/path/",
				"my-dir/another/path/c.txt",
			},
		},
		{
			name:       "unwrapRoot=true and writeDir=true",
			unwrapRoot: true,
			writeDir:   true,
			expected: []string{
				"a.txt",
				"path/",
				"path/b.txt",
				"another/",
				"another/path/",
				"another/path/c.txt",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			err := CompressDir(context.Background(), "testdata/my-dir", &buf, func(options *CompressDirOptions) {
				options.UnwrapRoot = tt.unwrapRoot
				options.WriteDir = tt.writeDir
			})
			assert.NoErrorf(t, err, "CompressDir(_, %s, _) error = %v", "testdata/my-dir", err)

			zipReader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			assert.NoErrorf(t, err, "zip.NewReader(...) error = %v", err)

			tt.expected = cleanAndSort(tt.expected)
			actual := cleanAndSort(sortedNames(zipReader.File))
			assert.Equalf(t, tt.expected, actual, "CompressDir did not produce expected directory structure; got = %v, want = %v", actual, tt.expected)
		})
	}
}

func TestCompressDir_InMemory(t *testing.T) {
	name, expectedData, err := createRandomTemp(32)
	assert.NoErrorf(t, err, "createRandomTemp() error = %v", err)
	defer os.Remove(name)

	// the contents of the file don't matter. what matters is the directory structure.
	//	my-dir/a.txt
	//	my-dir/path/b.txt
	//	my-dir/another/path/c.txt
	tmpDir, err := os.MkdirTemp("", "")
	assert.NoErrorf(t, err, "MkdirTemp() error = %v", err)
	defer os.RemoveAll(tmpDir)

	err = fill(filepath.Join(tmpDir, "my-dir/a.txt"), expectedData)
	if err != nil {
		t.Error(err)
	}
	if err == nil {
		err = fill(filepath.Join(tmpDir, "my-dir/path/b.txt"), expectedData)
	}
	if err == nil {
		err = fill(filepath.Join(tmpDir, "my-dir/another/path/c.txt"), expectedData)
	}
	assert.NoErrorf(t, err, "fillRandom error = %v", err)

	tests := []struct {
		name       string
		unwrapRoot bool
		writeDir   bool
		expected   []string
	}{
		{
			name:       "default",
			unwrapRoot: false,
			writeDir:   false,
			expected: []string{
				"my-dir/a.txt",
				"my-dir/path/b.txt",
				"my-dir/another/path/c.txt",
			},
		},
		{
			name:       "unwrapRoot=true",
			unwrapRoot: true,
			writeDir:   false,
			expected: []string{
				"a.txt",
				"path/b.txt",
				"another/path/c.txt",
			},
		},
		{
			name:       "writeDir=true",
			unwrapRoot: false,
			writeDir:   true,
			expected: []string{
				"my-dir/",
				"my-dir/a.txt",
				"my-dir/path/",
				"my-dir/path/b.txt",
				"my-dir/another/",
				"my-dir/another/path/",
				"my-dir/another/path/c.txt",
			},
		},
		{
			name:       "unwrapRoot=true and writeDir=true",
			unwrapRoot: true,
			writeDir:   true,
			expected: []string{
				"a.txt",
				"path/",
				"path/b.txt",
				"another/",
				"another/path/",
				"another/path/c.txt",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			err = CompressDir(context.Background(), filepath.Join(tmpDir, "my-dir"), &buf, func(options *CompressDirOptions) {
				options.UnwrapRoot = tt.unwrapRoot
				options.WriteDir = tt.writeDir
			})
			assert.NoErrorf(t, err, "CompressDir(_, %s, _) error = %v", filepath.Join(tmpDir, "my-dir"), err)

			zipReader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			assert.NoErrorf(t, err, "zip.NewReader(...) error = %v", err)

			tt.expected = cleanAndSort(tt.expected)
			actual := cleanAndSort(sortedNames(zipReader.File))
			assert.Equalf(t, tt.expected, actual, "CompressDir did not produce expected directory structure; got = %v, want = %v", actual, tt.expected)

			// might as well verify the contents of the file.
			for _, f := range zipReader.File {
				if strings.HasSuffix(f.Name, "/") {
					continue
				}

				actualData := make([]byte, len(expectedData))
				r, err := f.Open()
				if err == nil {
					_, err = r.Read(actualData)
				}
				assert.NoErrorf(t, err, "read compressed data error = %v", err)
				assert.Equal(t, expectedData, actualData)
			}
		})
	}
}

func fill(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0777); err != nil {
		return err
	}

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0666)
	if err == nil {
		_, err = f.Write(data)
		_ = f.Close()
	}
	return err
}

func sortedNames(files []*zip.File) []string {
	names := make([]string, len(files))
	for i, file := range files {
		names[i] = file.FileHeader.Name
	}

	slices.Sort(names)
	return names
}

func cleanAndSort(paths []string) []string {
	for i, path := range paths {
		paths[i] = filepath.Clean(path)
		if strings.HasSuffix(path, "/") {
			paths[i] += "/"
		}
	}

	slices.Sort(paths)

	return paths
}
