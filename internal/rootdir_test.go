package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindRootDir(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantRoot string
	}{
		{
			name: "simple root",
			args: []string{
				"test/a.txt",
				"test/path/b.txt",
				"test/another/path/c.txt",
			},
			wantRoot: "test/",
		},
		{
			name: "no root",
			args: []string{
				"a.txt",
				"path/b.txt",
				"another/path/c.txt",
			},
			wantRoot: "",
		},
		{
			name: "long root",
			args: []string{
				"test/path/to/a.txt",
				"test/path/to/a.txt",
				"test/path/to/a.txt",
			},
			wantRoot: "test/",
		},
		{
			name: "window paths",
			args: []string{
				"test\\a.txt",
				"test\\path\\b.txt",
				"test\\another\\path\\c.txt",
			},
			wantRoot: "test/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names := make([]string, 0)
			gotRoot, fn := RootDir(""), NewZipRootDirFinder()
			for _, name := range tt.args {
				names = append(names, name)
				gotRoot, _ = fn(name)
			}

			assert.Equalf(t, RootDir(tt.wantRoot), gotRoot, "NewZipRootDirFinder() got = %v, want = %v", gotRoot, tt.wantRoot)

			gotRoot = FindZipRootDir(names)
			assert.Equalf(t, RootDir(tt.wantRoot), gotRoot, "FindZipRootDir(%v) got = %v, want = %v", tt.args, gotRoot, tt.wantRoot)
		})
	}
}
