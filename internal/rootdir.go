package internal

import (
	"path/filepath"
	"regexp"
	"strings"
)

var sep = regexp.MustCompile(`[\\/]`)

// RootDir can be used to remove the root prefix of a path.
type RootDir string

// Join trims the path then joins the paths with filepath.Join.
func (r RootDir) Join(base, path string) string {
	return filepath.Join(base, strings.TrimPrefix(path, string(r)))
}

// FindZipRootDir returns the common root directory of the given file names in a ZIP archive.
//
// Given these three names (ZIP file paths must always be relative and using `/` as separator):
//
//	test/a.txt
//	test/path/b.txt
//	test/another/path/c.txt
//
// The common root directory of those files is `test`. The returned value is empty if the given files have no common
// root directory.
func FindZipRootDir(names []string) (rootDir RootDir) {
	fn := NewZipRootDirFinder()

	var ok bool
	for _, name := range names {
		rootDir, ok = fn(name)
		if !ok {
			break
		}
	}

	return
}

// NewZipRootDirFinder returns a function that can be passed the file names to compute the common root.
//
// NewZipRootDirFinder is a functional variant of FindZipRootDir. It returns the current root dir and a boolean
// indicating whether there is a common root so far. As soon as the returned boolean value is false, the search can stop
// since there is no common root and subsequent calls will keep returning `"", false`.
func NewZipRootDirFinder() func(string) (rootDir RootDir, hasRoot bool) {
	noRoot, root := false, ""

	return func(name string) (RootDir, bool) {
		if noRoot {
			return "", false
		}

		paths := sep.Split(name, 2)
		if len(paths) == 1 {
			// this is a file at top level so there is no root for sure.
			noRoot = true
			return "", false
		}

		switch root {
		case paths[0]:
		case "":
			root = paths[0]
		default:
			noRoot = true
			return "", false
		}

		return RootDir(root), true
	}
}
