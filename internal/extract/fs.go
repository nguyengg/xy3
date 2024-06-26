package extract

import (
	"context"
	"github.com/nguyengg/xy3/internal"
	"io"
	"io/fs"
	"log"
	"strings"
)

// FSExtractor uses archiver.FileSystem to perform extraction so it can work on a lot more types of archives.
type FSExtractor struct {
	Name   string
	In     fs.FS
	logger *log.Logger
}

// Extract extracts contents from the archive and writes to a newly created directory.
func (x *FSExtractor) Extract(ctx context.Context) (string, error) {
	topLevelDir, size, err := x.topLevelDir(ctx)
	if err != nil {
		return "", err
	}

	var stem string
	switch {
	case strings.HasSuffix(x.Name, ".tar.xz"):
		stem = strings.TrimSuffix(x.Name, ".tar.xz")
	default:
		stem, _ = internal.SplitStemAndExt(x.Name)
	}

	output, pathFn, err := createOutputDir(topLevelDir, stem)
	if err != nil {
		return "", err
	}

	x.logger.Printf(`extracting to "%s"`, output)
	bar := internal.DefaultBytes(size, "extracting")

	if err = fs.WalkDir(x.In, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return err
		}

		f, err := x.In.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		w, err := createExclFile(pathFn(path), d.Type().Perm())
		if err != nil {
			return err
		}
		defer w.Close()

		if err = internal.CopyWithContext(ctx, io.MultiWriter(w, bar), f); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}); err == nil {
		_ = bar.Close()
	}

	return output, err
}

// topLevelDir returns the top-level directory that is ancestor to all files in the archive.
//
// See ZipExtractor.topLevelDir.
//
// The method also returns the number of files and total size in bytes in the archive.
func (x *FSExtractor) topLevelDir(ctx context.Context) (root string, size int64, err error) {
	err = fs.WalkDir(x.In, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}

		size += fi.Size()

		switch paths := strings.SplitN(path, "/", 2); {
		case len(paths) == 1:
			// no root dir so must create one.
			fallthrough
		case root != "" && root != paths[0]:
			root = ""
			return nil
		default:
			root = paths[0]
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	})

	return
}
