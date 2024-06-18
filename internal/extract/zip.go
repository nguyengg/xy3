package extract

import (
	"archive/zip"
	"context"
	"fmt"
	"github.com/nguyengg/xy3/internal"
	"github.com/schollz/progressbar/v3"
	"io"
	"os"
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
	topLevelDir, uncompressedSize, err := x.topLevelDir(ctx)
	if err != nil {
		return "", err
	}

	stem, _ := internal.SplitStemAndExt(x.Name)
	output, pathFn, err := createOutputDir(topLevelDir, stem)
	if err != nil {
		return "", err
	}

	// equivalent to progressbar.DefaultBytes but with higher OptionThrottle to reduce flickering.
	bar := progressbar.NewOptions64(int64(uncompressedSize),
		progressbar.OptionSetDescription("extracting"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(1*time.Second),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			_, _ = fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true))

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

		err = copyWithContext(ctx, io.MultiWriter(w, bar), r)
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
