package zipper

import (
	"context"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/internal"
	"github.com/schollz/progressbar/v3"
)

// ProgressReporter is called to provide update on compressing individual files.
//
//   - src: path of the file being added to the archive
//   - dst: relative path of the file in the archive
//   - written: number of bytes of the file specified by src that has been read and written to archive so far
//   - done: is true only when the file has been read and written in its entirety
//
// The method will be called at least once for every file being compressed. If the file is small enough to fit into one
// read (see DefaultBufferSize), then the method is called exactly once with `done` being true.
type ProgressReporter func(src, dst string, written int64, done bool)

// DefaultProgressReporter is the default report that only reports upon a file being successfully added to archive.
//
// Specifically, after file `path/to/a` is added, [log.Printf] will print `added path/to/a to archive`.
func DefaultProgressReporter(src, dst string, written int64, done bool) {
	if done {
		log.Printf(`added "%s" to archive`, dst)
	}
}

// NewDirectoryProgressReporter creates a progress reporter intended to be used for compressing a directory.
//
// Specifically, the new progress reporter is aware of how many files are there to be compressed by doing a preflight
// filepath.WalkDir (also cancellable), and for each file being compressed, the reporter is aware of the total number of
// bytes for that file. If the initial filepath.WalkDir fails, its error wil be returned.
//
//   - src: path of the file being added to the archive
//   - dst: relative path of the file in the archive
//   - written: number of bytes of the file that has been read and written to archive so far
//   - size: the total number of bytes of the file being compressed. Can be -1 if os.Stat fails.
//   - done: is true only when the file has been read and written in its entirety (written==size)
//   - wc: the number of files that has been written to archive so far
//   - fc: the total number of files to be written to archive
func NewDirectoryProgressReporter(ctx context.Context, root string, reporter func(src, dst string, written, size int64, done bool, wc, fc int)) (ProgressReporter, error) {
	sizes := make(map[string]int64)
	var wc, fc int

	return func(src, dst string, written int64, done bool) {
			size, ok := sizes[src]
			if !ok {
				fi, err := os.Stat(src)
				if err != nil {
					sizes[src] = -1
					size = -1
				} else {
					size = fi.Size()
				}
			}

			if done {
				wc++
				delete(sizes, src)
			}

			reporter(src, dst, written, size, done, wc, fc)
		}, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			select {
			case <-ctx.Done():
				// ctx.Err is not supposed to return nil here if ctx.Done() is closed.
				if err = ctx.Err(); err == nil {
					return filepath.SkipAll
				}
				return err
			default:
				break
			}

			if err != nil || d.IsDir() || !d.Type().IsRegular() {
				return err
			}

			fc++
			return nil
		})
}

// NewProgressBarReporter creates a progress report that uses the specified progressbar.ProgressBar.
//
// If the given progress bar is nil, it will be created with progressbar.DefaultBytes.
func NewProgressBarReporter(ctx context.Context, root string, bar *progressbar.ProgressBar) (ProgressReporter, error) {
	n, size, err := CountDirContents(ctx, root)
	if err != nil {
		return nil, err
	}

	if bar == nil {
		bar = internal.DefaultBytes(size, "compressing")
	} else {
		bar.ChangeMax64(size)
	}

	var totalWritten int64
	var previousSrc string
	return func(src, dst string, written int64, done bool) {
		if previousSrc != src {
			totalWritten = 0
			previousSrc = src
		}

		if _, totalWritten = bar.Add64(written-totalWritten), written; done {
			if n--; n == 0 {
				_ = bar.Close()
			}
		}
	}, nil
}

// CountDirContents uses WalkRegularFiles to count all regular files and returns the total size of those files as well.
func CountDirContents(ctx context.Context, root string) (n int, size int64, err error) {
	err = WalkRegularFiles(ctx, root, func(path string, d fs.DirEntry) error {
		n++

		fi, err := d.Info()
		if err != nil {
			return err
		}

		size += fi.Size()
		return nil
	})
	return
}
