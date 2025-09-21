package compress

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
	"github.com/nguyengg/xy3/zipper"
)

// Options customises Compress.
type Options struct {
	// Mode indicates which compression algorithm to use.
	Mode Mode
	// MaxConcurrency customises the concurrency level.
	// Applicable only for compression libraries that support it (e.g. zstd).
	MaxConcurrency int
}

// Compress recursively compresses the given named file or directory.
//
// If name is a directory, the resulting archive will have a single root directory that is the basename of the name
// argument so that extracting the archive will put all files under the same directory.
//
// If name is a file, it will still be tar-ed if the mode does not support metadata out of the box.
func Compress(ctx context.Context, name string, dst io.Writer, optFns ...func(options *Options)) error {
	opts := &Options{
		Mode:           ZSTD,
		MaxConcurrency: 5,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	// check if is file.
	fi, err := os.Stat(name)
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, name, err)
	}
	if !fi.IsDir() {
		return compressFile(ctx, name, fi.Size(), dst, opts)
	}

	pbr, err := zipper.NewProgressBarReporter(ctx, name, nil)
	if err != nil {
		return fmt.Errorf("create progress bar reporter error: %w", err)
	}

	comp, err := opts.Mode.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	buf := make([]byte, 32*1024)

	err = filepath.WalkDir(name, func(srcPath string, d fs.DirEntry, err error) error {
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

			dstPath, err := filepath.Rel(name, srcPath)
			if err == nil {
				err = comp.NewFile(srcPath, dstPath)
			}
			if err != nil {
				return fmt.Errorf("compute file (path=%s) name in archive error: %w", dstPath, err)
			}

			pbw := pbr.CreateWriter(rel(name, srcPath), dstPath)
			if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, pbw), buf); err != nil {
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

func compressFile(ctx context.Context, name string, size int64, dst io.Writer, opts *Options) error {
	src, err := os.Open(name)
	if err != nil {
		return fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	base := filepath.Base(name)
	bar := internal.DefaultBytes(size, "compressing "+base)
	defer bar.Close()

	comp, err := opts.Mode.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	if err = comp.NewFile(name, base); err != nil {
		return fmt.Errorf(`create file "%s" in archive error: %w`, base, err)
	}
	if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, bar), nil); err != nil {
		return fmt.Errorf(`compress file "%s" error: %w`, base, err)
	}

	if err = comp.Close(); err != nil {
		return fmt.Errorf("close compressor error: %w", err)
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
