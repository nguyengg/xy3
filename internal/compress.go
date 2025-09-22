package internal

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/util"
	"github.com/schollz/progressbar/v3"
)

// CompressOptions customises Compress.
type CompressOptions struct {
	// Algorithm indicates which compression algorithm to use.
	//
	// Default to DefaultAlgorithm.
	Algorithm Algorithm

	// MaxConcurrency customises the concurrency level.
	//
	// Applicable only for compression libraries that support it (e.g. zstd). The zero value indicates no specific
	// setting and the encoder should use default.
	MaxConcurrency int

	progressBar *progressbar.ProgressBar
}

// DefaultAlgorithm is the default compression algorithm.
const DefaultAlgorithm = AlgorithmZstd

const defaultBufferSize = 32 * 1024

// CompressDir recursively compresses the given named directory.
func CompressDir(ctx context.Context, dir string, dst io.Writer, optFns ...func(options *CompressOptions)) error {
	opts := &CompressOptions{
		Algorithm: DefaultAlgorithm,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	if opts.progressBar != nil {
		defer opts.progressBar.Close()
	}

	comp, err := opts.Algorithm.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	base := filepath.Base(dir)
	createPath := func(path string) (name string, err error) {
		name, err = filepath.Rel(dir, path)
		return filepath.Join(base, name), err
	}

	buf := make([]byte, defaultBufferSize)

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

			dstPath, err := createPath(srcPath)
			if err == nil {
				err = comp.AddFile(srcPath, dstPath)
			}
			if err != nil {
				return fmt.Errorf("compute file (path=%s) name in archive error: %w", dstPath, err)
			}

			if opts.progressBar != nil {
				if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, opts.progressBar), buf); err != nil {
					return fmt.Errorf("add file (path=%s) to archive file (name=%s) error: %w", srcPath, dstPath, err)
				}
			} else if _, err = util.CopyBufferWithContext(ctx, comp, src, buf); err != nil {
				return fmt.Errorf("add file (path=%s) to archive file (name=%s) error: %w", srcPath, dstPath, err)
			}

			return nil

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

// Compress compresses the given io.Reader.
func Compress(ctx context.Context, src io.Reader, dst io.Writer, optFns ...func(options *CompressOptions)) error {
	opts := &CompressOptions{
		Algorithm: DefaultAlgorithm,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	comp, err := opts.Algorithm.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	if opts.progressBar != nil {
		if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, opts.progressBar), nil); err != nil {
			return fmt.Errorf("compress error: %w", err)
		}
	} else if _, err = util.CopyBufferWithContext(ctx, comp, src, nil); err != nil {
		return fmt.Errorf("compress error: %w", err)
	}

	if err = comp.Close(); err != nil {
		return fmt.Errorf("close compressor error: %w", err)
	}

	return nil
}

// WithCompressDirProgressBar creates a progress bar for CompressDir.
func WithCompressDirProgressBar(dir string) func(*CompressOptions) {
	var (
		n    int
		size int64
	)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil, d.IsDir(), !d.Type().IsRegular():
			return err
		default:
			fi, err := d.Info()
			if err != nil {
				return err
			}

			n++
			size += fi.Size()
			return nil
		}
	})

	return func(opts *CompressOptions) {
		if err == nil {
			opts.progressBar = DefaultBytes(size, fmt.Sprintf(`compressing "%s"`, filepath.Base(dir)))
		}
	}
}

// WithCompressProgressBar creates a progress bar for Compress.
func WithCompressProgressBar(name string) func(*CompressOptions) {
	var size int64
	if fi, err := os.Stat(name); err == nil {
		size = fi.Size()
	}

	return func(opts *CompressOptions) {
		if size > 0 {
			opts.progressBar = DefaultBytes(size, fmt.Sprintf(`compressing "%s"`, filepath.Base(name)))
		}
	}
}

// compressor can be used for both compressing a single file or a directory.
type compressor interface {
	io.WriteCloser

	// AddFile indicates subsequent calls to Write will be for compressing the file specified by src argument.
	//
	// The dst argument indicates the desired name of the file in archive. If this method is never called, the
	// compressor can be used to compress a single file.
	AddFile(src, dst string) error
}
