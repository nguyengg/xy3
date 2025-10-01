package zipper

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	commons "github.com/nguyengg/go-aws-commons"
)

// CompressDirOptions customises CompressDir.
type CompressDirOptions struct {
	CompressOptions

	// UnwrapRoot determines whether all compressed files are under a single root directory hierarchy or not.
	UnwrapRoot bool

	// WriteDir will write directory entries to the archive.
	WriteDir bool
}

// NewWriterWithDeflateLevel is a CompressOptions.NewWriter option.
//
// See [flate.NewWriter] on the acceptable level, for example [flate.BestCompression].
func NewWriterWithDeflateLevel(level int) func(w io.Writer) *zip.Writer {
	return func(w io.Writer) *zip.Writer {
		zw := zip.NewWriter(w)
		zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(w, level)
		})
		return zw
	}
}

// CompressDir compresses a directory recursively to the archive opened as io.Writer.
//
// See CompressDirOptions for customisation options. For example, if the directory (specified by "dir" argument) is
// "my-dir" and contains:
//
//	my-dir/a.txt
//	my-dir/path/b.txt
//	my-dir/another/path/c.txt
//
// By default, the archive content looks like this:
//
//	my-dir/a.txt
//	my-dir/path/b.txt
//	my-dir/another/path/c.txt
//
// If CompressDirOptions.UnwrapRoot is true, the archive content looks like this:
//
//	a.txt
//	path/b.txt
//	another/path/c.txt
//
// If CompressDirOptions.WriteDir is true and CompressDirOptions.UnwrapRoot is false, the archive content become:
//
//	my-dir/
//	my-dir/a.txt
//	my-dir/path/
//	my-dir/path/b.txt
//	my-dir/another/
//	my-dir/another/path/
//	my-dir/another/path/c.txt
//
// If both CompressDirOptions.WriteDir and CompressDirOptions.UnwrapRoot are true, the archive content become:
//
//	a.txt
//	path/
//	path/b.txt
//	another/
//	another/path/
//	another/path/c.txt
//
// This function is a wrapper around [DefaultZipper.CompressDir].
func CompressDir(ctx context.Context, dir string, dst io.Writer, optFns ...func(*CompressDirOptions)) error {
	opts := &CompressDirOptions{
		CompressOptions: CompressOptions{
			ProgressReporter: DefaultProgressReporter,
			BufferSize:       DefaultBufferSize,
			NewWriter:        zip.NewWriter,
		},
		UnwrapRoot: false,
		WriteDir:   false,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	zipWriter := opts.NewWriter(dst)
	defer zipWriter.Close()

	// archivePath is the function to add basename of dir (UnwrapRoot=false) or not (UnwrapRoot=true).
	archivePath := func(path string) (name string, err error) {
		name, err = filepath.Rel(dir, path)
		return
	}
	if !opts.UnwrapRoot {
		base := filepath.Base(dir)
		archivePath = func(path string) (name string, err error) {
			name, err = filepath.Rel(dir, path)
			return filepath.Join(base, name), err
		}
	}

	buf := make([]byte, opts.BufferSize)
	pr := opts.ProgressReporter

	return filepath.WalkDir(dir, func(srcPath string, d fs.DirEntry, err error) error {
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

		var fi os.FileInfo

		switch {
		case err != nil:
			return fmt.Errorf("walk dir error: %w", err)

		case d.IsDir():
			if !opts.WriteDir {
				return nil
			}

			fi, err = d.Info()
			if err != nil {
				return fmt.Errorf("describe directory (path=%s) error: %w", srcPath, err)
			}

			dstPath, err := archivePath(srcPath)
			if err != nil {
				return fmt.Errorf("compute directory (path=%s) name in archive error: %w", srcPath, err)
			} else if dstPath == "." {
				return nil
			}

			if _, err = zipWriter.CreateHeader(fileHeader(fi, dstPath+"/")); err != nil {
				return fmt.Errorf("create zip record (path=%s) for directory (path=%s) error: %w", dstPath, srcPath, err)
			}

			if pr != nil {
				pr(rel(dir, srcPath), dstPath, 0, true)
			}

			return nil

		case d.Type().IsRegular():
			fi, err = d.Info()
			if err != nil {
				return fmt.Errorf("describe file (path=%s) error: %w", srcPath, err)
			}

			src, err := os.Open(srcPath)
			if err != nil {
				return fmt.Errorf("open file (path=%s) error: %w", srcPath, err)
			}
			defer src.Close()

			dstPath, err := archivePath(srcPath)
			if err != nil {
				return fmt.Errorf("compute file (path=%s) name in archive error: %w", dstPath, err)
			}

			f, err := zipWriter.CreateHeader(fileHeader(fi, dstPath))
			if err != nil {
				return fmt.Errorf("create zip record (name=%s) for file (path=%s) error: %w", dstPath, srcPath, err)
			}

			if pr == nil {
				if _, err = commons.CopyBufferWithContext(ctx, f, src, buf); err != nil {
					return fmt.Errorf("add file (path=%s) to archive file (name=%s) error: %w", srcPath, dstPath, err)
				}

				return nil
			}

			w := pr.CreateWriter(rel(dir, srcPath), dstPath)
			if _, err = commons.CopyBufferWithContext(ctx, io.MultiWriter(f, w), src, buf); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}

				return fmt.Errorf("add file (path=%s) to archive file (name=%s) error: %w", srcPath, dstPath, err)
			}

			return w.Close()

		default:
			return nil
		}
	})
}

// rel is a smarter filepath.Rel that returns the original path if fails.
func rel(basepath, path string) string {
	v, err := filepath.Rel(basepath, path)
	if err != nil {
		return path
	}

	return filepath.Join(filepath.Base(basepath), v)
}
