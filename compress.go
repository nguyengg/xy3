package xy3

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/util"
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
}

// DefaultAlgorithm is the default compression algorithm for CompressDir and Compress.
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

	bar, _ := compressDirProgressBar(dir)
	if bar != nil {
		defer bar.Close()
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

			if bar != nil {
				if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, bar), buf); err != nil {
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

	bar, _ := compressFileProgressBar(src)
	defer func() {
		if bar != nil {
			_ = bar.Close()
		}
	}()

	comp, err := opts.Algorithm.createCompressor(dst, opts)
	if err != nil {
		return fmt.Errorf("create compressor error: %w", err)
	}

	if bar != nil {
		if _, err = util.CopyBufferWithContext(ctx, comp, io.TeeReader(src, bar), nil); err != nil {
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

func compressDirProgressBar(dir string) (io.WriteCloser, error) {
	var size int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil, d.IsDir(), !d.Type().IsRegular():
			return err
		default:
			fi, err := d.Info()
			if err != nil {
				return err
			}

			size += fi.Size()
			return nil
		}
	})
	if err == nil {
		return tspb.DefaultBytes(size, fmt.Sprintf(`compressing "%s"`, filepath.Base(dir))), nil
	}

	return nil, err
}

func compressFileProgressBar(r io.Reader) (io.WriteCloser, error) {
	if f, ok := r.(*os.File); ok {
		fi, err := f.Stat()
		if err == nil {
			return tspb.DefaultBytes(fi.Size(), fmt.Sprintf(`compressing "%s"`, filepath.Base(f.Name()))), nil
		}

		return nil, err
	}

	return nil, nil
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
