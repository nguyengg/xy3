package xy3

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nguyengg/xy3/archive"
	"github.com/nguyengg/xy3/codec"
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

func TestNewCompressorFromName_Dispatch(t *testing.T) {
	tests := []struct {
		name    string
		alg     string
		wantNil bool
		wantExt string
	}{
		{"zstd resolves to tar+zstd", "zstd", false, ".tar.zst"},
		{"gzip resolves to tar+gzip", "gzip", false, ".tar.gz"},
		{"gz alias also resolves", "gz", false, ".tar.gz"},
		{"xz resolves to tar+xz", "xz", false, ".tar.xz"},
		{"zip resolves to plain zip", "zip", false, "zip"},
		{"unknown returns nil", "brotli", true, ""},
		{"empty returns nil", "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewCompressorFromName(tt.alg)
			if tt.wantNil {
				assert.Nil(t, got)
				return
			}
			assert.NotNil(t, got)
			assert.Equal(t, tt.wantExt, got.ArchiveExt())
		})
	}
}

func TestNewCompressorFromName_TarCodecWiring(t *testing.T) {
	// each tar-based compressor must actually carry the matching Codec instance.
	tests := []struct {
		alg          string
		wantCodecExt string
	}{
		{"zstd", ".zst"},
		{"gzip", ".gz"},
		{"gz", ".gz"},
		{"xz", ".xz"},
	}
	for _, tt := range tests {
		t.Run(tt.alg, func(t *testing.T) {
			got := NewCompressorFromName(tt.alg)
			tarComp, ok := got.(*archive.Tar)
			assert.True(t, ok, "expected *archive.Tar for %q", tt.alg)
			assert.NotNil(t, tarComp.Codec)
			cd, ok := tarComp.Codec.(codec.Codec)
			assert.True(t, ok)
			assert.Equal(t, tt.wantCodecExt, cd.Ext())
		})
	}
}

func TestNewDecompressorFromName_Dispatch(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantNil bool
	}{
		{"tar.gz", "backup.tar.gz", false},
		{"tar.xz", "backup.tar.xz", false},
		{"tar.zst", "backup.tar.zst", false},
		{"plain tar", "backup.tar", false},
		{"zip", "backup.zip", false},
		{"7z", "backup.7z", false},
		{"rar", "backup.rar", false},
		{"unknown", "backup.br", true},
		{"empty", "", true},
		// ordering test: .tar.gz must NOT be picked up by the plain .tar case first.
		{"tar.gz not matched as plain tar", "archive.tar.gz", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDecompressorFromName(tt.file)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
			}
		})
	}
}

func TestNewDecoderFromExt_Dispatch(t *testing.T) {
	tests := []struct {
		name    string
		ext     string
		wantNil bool
	}{
		{"gz", ".gz", false},
		{"xz", ".xz", false},
		{"zst", ".zst", false},
		{"unknown", ".br", true},
		{"empty", "", true},
		{"missing leading dot", "gz", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDecoderFromExt(tt.ext)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
			}
		})
	}
}
