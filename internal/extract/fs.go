package extract

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/nguyengg/xy3/internal"
	"golang.org/x/time/rate"
	"io/fs"
	"log"
	"strings"
	"time"
)

// FSExtractor uses archiver.FileSystem to perform extraction so it can work on a lot more types of archives.
type FSExtractor struct {
	Name string
	In   fs.FS
}

// Extract extracts contents from the archive and writes to a newly created directory.
func (x *FSExtractor) Extract(ctx context.Context) (string, error) {
	topLevelDir, n, err := x.topLevelDir(ctx)
	if err != nil {
		return "", err
	}

	stem, _ := internal.SplitStemAndExt(x.Name)
	output, pathFn, err := createOutputDir(topLevelDir, stem)
	if err != nil {
		return "", err
	}
	log.Printf("topLevelDir=%s, output=%s", topLevelDir, output)

	sometimes := rate.Sometimes{Interval: 5 * time.Second}
	i := 0

	err = fs.WalkDir(x.In, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return err
		}

		fi, err := d.Info()
		if err != nil {
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

		if err = copyWithProgress(ctx, w, f, i, n, path, fi.Size()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			i++
			sometimes.Do(func() {
				log.Printf(`[%d/%d] (%s) %s`, i, n, humanize.Bytes(uint64(fi.Size())), path)
			})
			return nil
		}
	})

	return output, err
}

// topLevelDir returns the top-level directory that is ancestor to all files in the archive.
//
// See ZipExtractor.topLevelDir.
//
// The method also returns the number of files in the archive.
func (x *FSExtractor) topLevelDir(ctx context.Context) (root string, n int, err error) {
	err = fs.WalkDir(x.In, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		n++

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
