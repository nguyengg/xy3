package compress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/util"
	"github.com/nguyengg/xy3/zipper"
)

type Options struct {
	Mode           Mode
	MaxConcurrency int
	BufferSize     int
}

// CompressDir recursively compresses the given directory.
//
// The archive will have a single root directory that is the given dir argument so that extracting the archive will put
// all files under the same directory.
func CompressDir(ctx context.Context, dir string, dst io.Writer, optFns ...func(options *Options)) error {
	opts := &Options{
		Mode:           ZSTD,
		MaxConcurrency: 5,
		BufferSize:     64 * 1024,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	pbr, err := zipper.NewProgressBarReporter(ctx, dir, nil)
	if err != nil {
		return fmt.Errorf("create progress bar reporter error: %w", err)
	}

	comp, err := opts.Mode.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	buf := make([]byte, opts.BufferSize)

	err = filepath.WalkDir(dir, func(srcPath string, d fs.DirEntry, err error) error {
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
		case err != nil:
			return fmt.Errorf("walk dir error: %w", err)

		case d.Type().IsRegular():
			src, err := os.Open(srcPath)
			if err != nil {
				return fmt.Errorf("open file (path=%s) error: %w", srcPath, err)
			}
			defer src.Close()

			dstPath, err := filepath.Rel(dir, srcPath)
			if err == nil {
				err = comp.NewFile(srcPath, dstPath)
			}
			if err != nil {
				return fmt.Errorf("compute file (path=%s) name in archive error: %w", dstPath, err)
			}

			pbw := pbr.CreateWriter(rel(dir, srcPath), dstPath)
			if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, pbw), buf); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}

				return fmt.Errorf("add file (path=%s) to archive file (name=%s) error: %w", srcPath, dstPath, err)
			}

			return pbw.Close()

		default:
			return nil
		}
	})
	if err == nil {
		err = comp.Close()
	}
	if err != nil {
		return fmt.Errorf("compress error: %w", err)
	}

	return nil
}

// compressor can be used for both compressing a single file or a directory.
type compressor interface {
	// NewFile indicates subsequent calls to Write will be for compressing the file specified by src argument.
	//
	// The dst argument indicates the desired name of the file in archive. If this method is never called, the
	// compressor can be used to compress a single file.
	NewFile(src, dst string) error
	io.WriteCloser
}

// rel is a smarter filepath.Rel that returns the original path if fails.
func rel(basepath, path string) string {
	v, err := filepath.Rel(basepath, path)
	if err != nil {
		return path
	}

	return filepath.Join(filepath.Base(basepath), v)
}
