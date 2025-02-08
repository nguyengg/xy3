package extract

import (
	"archive/zip"
	"context"
	"io"
	"log"
	"strings"

	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
)

// ZipExtractor can only extract ZIP files.
type ZipExtractor struct {
	Name   string
	In     *zip.ReadCloser
	logger *log.Logger
}

// Extract extracts contents from the ZIP archive and writes to a newly created directory.
func (x *ZipExtractor) Extract(ctx context.Context) (string, error) {
	topLevelDir, uncompressedSize, err := x.topLevelDir(ctx)
	if err != nil {
		return "", err
	}

	stem, _ := xy3.StemAndExt(x.Name)
	output, pathFn, err := createOutputDir(topLevelDir, stem)
	if err != nil {
		return "", err
	}

	x.logger.Printf(`extracting to "%s"`, output)
	bar := internal.DefaultBytes(int64(uncompressedSize), "extracting")

	for _, f := range x.In.File {
		if f.FileInfo().IsDir() {
			continue
		}

		w, err := createExclFile(pathFn(f.Name), f.Mode().Perm())
		if err != nil {
			return output, err
		}

		r, err := f.Open()
		if err != nil {
			_ = w.Close()
			return output, err
		}

		err = xy3.CopyBufferWithContext(ctx, io.MultiWriter(w, bar), r, nil)
		_, _ = w.Close(), r.Close()
		if err != nil {
			return output, err
		}

		select {
		case <-ctx.Done():
			return output, ctx.Err()
		default:
		}
	}

	_ = bar.Close()
	return output, nil
}

// topLevelDir returns the top-level directory that is ancestor to all files in the archive.
//
// This exists only if all files in the archive has the same top-level directory. If at least two files don't share the
// same top-level directory, return an empty string. If the archive contains only one file but the file does not belong
// to any directory, an empty string is also returned.
func (x *ZipExtractor) topLevelDir(ctx context.Context) (root string, uncompressedSize uint64, err error) {
	for _, f := range x.In.File {
		if f.FileInfo().IsDir() {
			continue
		}

		uncompressedSize += f.UncompressedSize64

		switch paths := strings.SplitN(f.Name, "/", 2); {
		case len(paths) == 1:
			// no root dir so must create one.
			fallthrough
		case root != "" && root != paths[0]:
			root = ""
			return
		default:
			root = paths[0]
		}

		select {
		case <-ctx.Done():
			return "", uncompressedSize, ctx.Err()
		default:
		}
	}

	return
}
