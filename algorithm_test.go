package xy3

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDecompressorFromName(t *testing.T) {
	tests := []struct {
		name string
		file string
	}{
		// TODO: Add test cases.
		{
			name: "extract 7z",
			file: "testdata/test.7z",
		},
		{
			name: "extract rar",
			file: "testdata/test.rar",
		},
		{
			name: "extract tar.gz",
			file: "testdata/test.tar.gz",
		},
		{
			name: "extract tar.xz",
			file: "testdata/test.tar.xz",
		},
		{
			name: "extract tar.zst",
			file: "testdata/test.tar.zst",
		},
	}

	// test.txt
	expected := "Mr. Jock, TV quiz PhD, bags few lynx\n"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "*")
			assert.NoError(t, err)
			defer os.RemoveAll(dir)

			// name must be a directory that contains exactly one file named test.txt.
			name, err := Decompress(t.Context(), tt.file, dir)
			assert.NoError(t, err)

			data, err := os.ReadFile(filepath.Join(name, "test.txt"))
			assert.NoError(t, err)

			assert.Equalf(t, expected, string(data), "expectd=%s, actual=%s", expected, data)
		})
	}
}

func TestNewDecoderFromExt(t *testing.T) {
	tests := []struct {
		name string
		file string
	}{
		// TODO: Add test cases.
		{
			name: "decode gz",
			file: "testdata/test.txt.gz",
		},
		{
			name: "decode xz",
			file: "testdata/test.txt.xz",
		},
		{
			name: "decode zstd",
			file: "testdata/test.txt.zst",
		},
	}

	// test.txt
	expected := "Mr. Jock, TV quiz PhD, bags few lynx\n"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "*")
			assert.NoError(t, err)
			defer os.RemoveAll(dir)

			// name must be the file test.txt itself.
			name, err := Decompress(t.Context(), tt.file, dir, func(opts *DecompressOptions) {
				opts.NoExtract = true
			})
			assert.NoError(t, err)
			assert.Equalf(t, "test.txt", filepath.Base(name), "expected extracted file name to be test.txt, got %s", name)

			data, err := os.ReadFile(name)
			assert.NoError(t, err)

			assert.Equalf(t, expected, string(data), "expectd=%s, actual=%s", expected, data)
		})
	}
}
