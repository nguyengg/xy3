package zipper

import (
	"context"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

// ProgressReporter is called to provide update on compressing individual files or extracting from an archive.
//
// If being used with CompressFile or CompressDir:
//   - src: path of the file being added to the archive
//   - dst: path of the file in the archive
//   - written: number of bytes of the file specified by src that has been read and written to dst so far
//   - done: is true only when the file specified by src has been read and written to archive in its entirety
//
// If being used with Extract:
//   - src: path of the file in archive being extracted
//   - dst: path (relative to output directory) of the file on filesystem
//   - written: number of bytes of the file specified by src that has been read and written to dst so far
//   - done: is true only when the file specified by src has been read and written to filesystem in its entirety
//
// The method will be called at least once for every file being processed. If the file is small enough to fit into one
// read (see DefaultBufferSize), then the method is called exactly once with `done` being true.
type ProgressReporter func(src, dst string, written int64, done bool)

// DefaultProgressReporter is the default progress reporter that only logs upon a file being successfully added to
// archive.
func DefaultProgressReporter(src, dst string, written int64, done bool) {
	if done {
		log.Printf(`%s => %s`, src, dst)
	}
}

// NoOpProgressReporter can be used to turn off progress reporting.
func NoOpProgressReporter(src, dst string, written int64, done bool) {
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

// WalkRegularFiles is a specialisation of filepath.WalkDir that applies the callback only to regular files.
//
// This is the same method that CompressDir uses to find the files for compressing.
func WalkRegularFiles(ctx context.Context, root string, fn func(path string, d fs.DirEntry) error) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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

		switch {
		case err != nil, d.IsDir(), !d.Type().IsRegular():
			return err
		default:
			return fn(path, d)
		}
	})
}

// CreateWriter creates an io.WriteCloser for the given src and dst.
//
// When Write is called, ProgressReport is called with the src and dst given here, while the number of bytes written
// is a rolling sum created with initial value 0. Close will call ProgressReport again passing the total number of bytes
// written and done flag being true. Close will never return a non-nil error.
func (r ProgressReporter) CreateWriter(src, dst string) io.WriteCloser {
	return &progressReporterWriter{r, src, dst, 0}
}

type progressReporterWriter struct {
	ProgressReporter
	src, dst string
	written  int64
}

func (w *progressReporterWriter) Write(data []byte) (int, error) {
	n := len(data)
	w.written += int64(n)
	w.ProgressReporter(w.src, w.dst, w.written, false)
	return n, nil
}

func (w *progressReporterWriter) Close() error {
	w.ProgressReporter(w.src, w.dst, w.written, true)
	return nil
}
