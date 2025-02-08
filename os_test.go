package xy3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStemAndExt(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantStem string
		wantExt  string
	}{
		// TODO: Add test cases.
		{
			name:     "test.txt",
			path:     "C:\\Users\\test.txt",
			wantStem: "test",
			wantExt:  ".txt",
		},
		{
			name:     "test.tar.gz",
			path:     "/path/to/test.tar.gz",
			wantStem: "test",
			wantExt:  ".tar.gz",
		},
		{
			name:     "test.mhtml.s3",
			path:     "/path/to/test.mhtml.s3",
			wantStem: "test",
			wantExt:  ".mhtml.s3",
		},
		{
			name:     "test.jfif-tbnl",
			path:     "/path/to/test.jfif-tbnl",
			wantStem: "test.jfif-tbnl",
			wantExt:  "",
		},
		{
			name:     "ab",
			path:     "/path/to/ab",
			wantStem: "ab",
			wantExt:  "",
		},
		{
			// different between this and above is that this code path ends up using filepath.Base while
			// the one above does not. but they produce the same stem and ext.
			name:     "ab via filepath.Base",
			path:     "ab",
			wantStem: "ab",
			wantExt:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStem, gotExt := StemAndExt(tt.path)
			assert.Equalf(t, gotStem, tt.wantStem, "StemAndExt() gotStem = %v, want %v", gotStem, tt.wantStem)
			assert.Equalf(t, gotExt, tt.wantExt, "StemAndExt() gotExt = %v, want %v", gotExt, tt.wantExt)
		})
	}
}
