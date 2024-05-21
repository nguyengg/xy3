package upload

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"github.com/nguyengg/xy3/internal"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, logger *log.Logger, root string) (name string, checksum string, err error) {
	base := filepath.Base(root)

	// a new file will always be created, and if the operation fails, the file will be auto deleted.
	out, err := internal.OpenExclFile(base, ".zip")
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	name = out.Name()
	defer func() {
		if _ = out.Close(); err != nil {
			_ = os.Remove(name)
			name = ""
		}
	}()

	// use MultiWriter to compress and compute checksum at the same time.
	h := internal.NewHasher()
	w := zip.NewWriter(io.MultiWriter(out, h))
	w.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})
	defer func(w *zip.Writer) {
		_ = w.Close()
	}(w)

	// report compress progress every few seconds.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// to be able to report progress, we'll do filepath.WalkDir twice, the first time to tally up the number of files,
	// the second time to actually perform the archiving.
	fc := 0
	if err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return filepath.SkipAll
		case <-ticker.C:
			logger.Printf("found %d files to be compressed so far", fc)
		default:
		}

		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return err
		}

		fc++
		return nil
	}); err != nil {
		return
	}

	cc := 0
	if err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return filepath.SkipAll
		case <-ticker.C:
			logger.Printf("compressed %d/%d files so far", cc, fc)
		default:
		}

		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return err
		}

		src, err := os.Open(path)
		if err != nil {
			return fmt.Errorf(`open file "%s" error: %w`, path, err)
		}
		defer func(f *os.File) {
			_ = f.Close()
		}(src)

		p, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf(`determine file "%s" rel path error: %w`, path, err)
		}
		dst, err := w.Create(filepath.Join(base, p))
		if _, err = io.Copy(dst, src); err != nil {
			return fmt.Errorf(`archive file "%s", error: %w`, path, err)
		}

		cc++
		return nil
	}); err != nil {
		return
	}

	if err = ctx.Err(); err != nil {
		return
	}

	checksum = h.SumToChecksumString(nil)
	logger.Printf("done compressing %d/%d files", cc, fc)
	return
}
