package extract

import (
	"archive/zip"
	"context"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3/internal"
	"golang.org/x/time/rate"
	"log"
	"strings"
	"time"
)

// ZipExtractor can only extract ZIP files.
type ZipExtractor struct {
	Name string
	In   *zip.ReadCloser
}

// Extract extracts contents from the ZIP archive and writes to a newly created directory.
func (x *ZipExtractor) Extract(ctx context.Context) (string, error) {
	topLevelDir, err := x.topLevelDir(ctx)
	if err != nil {
		return "", err
	}

	stem, _ := internal.SplitStemAndExt(x.Name)
	output, pathFn, err := createOutputDir(topLevelDir, stem)
	if err != nil {
		return "", err
	}

	sometimes := rate.Sometimes{Interval: 5 * time.Second}
	n := len(x.In.File)

	for i, f := range x.In.File {
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

		err = copyWithProgress(ctx, w, r, i, n, f.Name, int64(f.UncompressedSize64))
		_, _ = w.Close(), r.Close()
		if err != nil {
			return output, err
		}

		select {
		case <-ctx.Done():
			return output, ctx.Err()
		default:
			sometimes.Do(func() {
				log.Printf(`[%d/%d] (%s) %s`, i+1, n, humanize.Bytes(f.UncompressedSize64), f.Name)
			})
		}
	}

	return output, nil
}

// topLevelDir returns the top-level directory that is ancestor to all files in the archive.
//
// This exists only if all files in the archive has the same top-level directory. If at least two files don't share the
// same top-level directory, return an empty string. If the archive contains only one file but the file does not belong
// to any directory, an empty string is also returned.
func (x *ZipExtractor) topLevelDir(ctx context.Context) (root string, err error) {
	for _, f := range x.In.File {
		if f.FileInfo().IsDir() {
			continue
		}

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
			return "", ctx.Err()
		default:
		}
	}

	return
}
