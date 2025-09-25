package xy3

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/codec"
	"github.com/nguyengg/xy3/util"
)

// CompressOptions customises Compress.
type CompressOptions struct {
	// Algorithm indicates which compression algorithm to use.
	//
	// Default to codec.DefaultAlgorithmName.
	Algorithm string

	// MaxConcurrency customises the concurrency level.
	//
	// Applicable only for compression libraries that support it (e.g. zstd). The zero value indicates no specific
	// setting and the encoder should use default.
	MaxConcurrency int
}

const defaultBufferSize = 32 * 1024

// CompressDir compresses the given root directory.
func CompressDir(ctx context.Context, dir string, dst io.Writer, optFns ...func(options *CompressOptions)) (err error) {
	opts := &CompressOptions{
		Algorithm: DefaultAlgorithmName,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	comp := NewCompressorFromName(opts.Algorithm)
	add, closer, err := comp.Create(dst, filepath.Base(dir))
	if err != nil {
		return fmt.Errorf("create %s compressor error: %w", opts.Algorithm, err)
	}

	bar, err := compressDirProgressBar(dir)
	if err != nil {
		return err
	}

	buf := make([]byte, defaultBufferSize)

	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
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
			src, err := os.Open(path)
			if err != nil {
				return fmt.Errorf(`open file "%s" error: %w`, path, err)
			}
			defer src.Close()

			fi, err := src.Stat()
			if err != nil {
				return fmt.Errorf(`stat file "%s" error: %w`, path, err)
			}

			w, err := add(path, fi)
			if err != nil {
				return fmt.Errorf(`create archive file "%s" error: %w`, path, err)
			}

			if _, err = util.CopyBufferWithContext(ctx, w, io.TeeReader(src, bar), buf); err != nil {
				_ = w.Close()
				return fmt.Errorf(`write archive file "%s" error: %w`, path, err)
			}

			if err = w.Close(); err != nil {
				return fmt.Errorf(`close archive file "%s" error: %w`, path, err)
			}

			return nil

		default:
			return nil
		}
	})
	if err == nil {
		if err = closer(); err == nil {
			_ = bar.Close()
		}
	}
	if err != nil {
		return fmt.Errorf(`compress directory "%s" error: %w`, dir, err)
	}

	return nil
}

// Compress compresses a single file given by the io.Reader.
//
// The os.FileInfo argument is required if the compression algorithm is an archiver like ZIP and 7z.
func Compress(ctx context.Context, src io.Reader, fi os.FileInfo, dst io.Writer, optFns ...func(options *CompressOptions)) error {
	opts := &CompressOptions{
		Algorithm: DefaultAlgorithmName,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	comp := NewCompressorFromName(opts.Algorithm)

	var bar io.WriteCloser
	if fi != nil {
		bar = tspb.DefaultBytes(fi.Size(), fmt.Sprintf(`compressing "%s"`, fi.Name()))
	} else {
		bar = tspb.DefaultBytes(-1, "compressing")
	}

	// if the compressor implements codec.Codec then use that interface directly.
	if c, ok := comp.(codec.Codec); ok {
		w, err := c.NewEncoder(dst)
		if err != nil {
			return fmt.Errorf("create encoder error: %w", err)
		}

		if _, err = util.CopyBufferWithContext(ctx, w, io.TeeReader(src, bar), nil); err != nil {
			_ = w.Close()
			return err
		}

		if err = w.Close(); err == nil {
			_ = bar.Close()
		}

		return err
	}

	// archiver requires FileInfo to be given.
	if fi == nil {
		return fmt.Errorf("%s compressor requires a named file", opts.Algorithm)
	}

	add, closer, err := comp.Create(dst, "")
	if err != nil {
		return fmt.Errorf("create %s compressor error: %w", opts.Algorithm, err)
	}

	w, err := add(fi.Name(), fi)
	if err != nil {
		return err
	}

	closer = util.ChainCloser(w.Close, closer)

	if _, err = util.CopyBufferWithContext(ctx, w, io.TeeReader(src, bar), nil); err != nil {
		_ = closer()
		return err
	}

	if err = closer(); err != nil {
		return err
	}

	_ = bar.Close()
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
	if err != nil {
		return nil, err
	}

	return tspb.DefaultBytes(size, fmt.Sprintf(`compressing "%s"`, filepath.Base(dir))), nil
}

// archiver compresses a directory recursively.
type archiver interface {
	io.WriteCloser

	// AddFile indicates subsequent calls to Write will be for compressing the file specified by src argument.
	//
	// The dst argument indicates the desired name of the file in archive. If this method is never called, the
	// archiver can be used to compress a single file.
	AddFile(src, dst string) error
}
