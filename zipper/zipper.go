package zipper

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	// DefaultBufferSize is the default value for [Zipper.BufferSize], which is 32 KiB.
	DefaultBufferSize = 32 * 1024
)

// Zipper is used to recursively compress (with zip) a directory or file with progress report and cancellable context.
//
// The zero value is ready for use.
type Zipper struct {
	// ProgressReporter controls how progress is reported.
	//
	// By default, DefaultProgressReporter is used, which logs `added path/to/file to archive` after each file has
	// been successfully added to the archive.
	ProgressReporter ProgressReporter

	// BufferSize is the length of the buffer being used for copying/adding files to the archive.
	//
	// BufferSize indirectly controls how frequently ProgressReporter is called; after each copy is done,
	// ProgressReporter is called once.
	//
	// Default to DefaultBufferSize.
	BufferSize int

	// NewZipWriterFn allows customization of the zip.Writer being used.
	//
	// Default to a [zip.Writer] that uses [flate.DefaultCompression] for balance.
	NewZipWriterFn func(w io.Writer) *zip.Writer
}

// New returns a new Zipper with customisation options.
func New(optFns ...func(*Zipper)) *Zipper {
	z := &Zipper{
		ProgressReporter: DefaultProgressReporter,
		BufferSize:       DefaultBufferSize,
		NewZipWriterFn:   zip.NewWriter,
	}
	for _, fn := range optFns {
		fn(z)
	}

	return z
}

// NoCompressionZipWriter uses a [zip.Writer] that registers [flate.NoCompression] as its compressor.
func NoCompressionZipWriter() func(*Zipper) {
	return func(z *Zipper) {
		z.NewZipWriterFn = func(w io.Writer) *zip.Writer {
			zw := zip.NewWriter(w)
			zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
				return flate.NewWriter(w, flate.NoCompression)
			})
			return zw
		}
	}
}

// BestCompressionZipWriter uses a [zip.Writer] that registers [flate.BestCompression] as its compressor.
func BestCompressionZipWriter() func(*Zipper) {
	return func(z *Zipper) {
		z.NewZipWriterFn = func(w io.Writer) *zip.Writer {
			zw := zip.NewWriter(w)
			zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
				return flate.NewWriter(w, flate.NoCompression)
			})
			return zw
		}
	}
}

// CompressFile compresses a single file to the archive opened as io.Writer.
func (z Zipper) CompressFile(ctx context.Context, name string, dst io.Writer) error {
	sf, err := os.Open(name)
	if err != nil {
		return err
	}

	var zw *zip.Writer
	if z.NewZipWriterFn != nil {
		zw = z.NewZipWriterFn(dst)
	} else {
		zw = zip.NewWriter(dst)
	}
	defer zw.Close()

	p := filepath.Base(name)
	zf, err := zw.Create(p)
	if err == nil {
		err = z.copy(ctx, zf, sf, name, p)
	}

	return err
}

// CompressDir compresses a directory recursively to the archive opened as io.Writer.
//
// JunkRoot determines whether all compressed files are under a single root directory hierarchy or not.
//
// For example, when compressing directory named "test" (root="test") with JunkRoot being false, all files in that
// directory and their children will be added to archive using these paths:
//
//	test/a.txt
//	test/path/b.txt
//	test/another/path/c.txt
//
// If junkRoot is true, the files will use these paths:
//
//	a.txt
//	path/b.txt
//	another/path/c.txt
func (z Zipper) CompressDir(ctx context.Context, root string, dst io.Writer, junkRoot bool) error {
	zw := z.NewZipWriterFn(dst)
	defer zw.Close()

	base := filepath.Base(root)
	mkpath := func(path string) string {
		return filepath.Join(base, path)
	}
	if junkRoot {
		mkpath = func(path string) string {
			return path
		}
	}

	return WalkRegularFiles(ctx, root, func(path string, d fs.DirEntry) error {
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		if path, err = filepath.Rel(root, path); err != nil {
			return err
		}
		path = mkpath(path)
		zf, err := zw.Create(path)
		if err == nil {
			err = z.copy(ctx, zf, src, src.Name(), path)
		}

		return err
	})
}

// copy is an implementation of io.Copy that is cancellable and also provides progress report which is useful for large
// files.
func (z Zipper) copy(ctx context.Context, w io.Writer, r io.Reader, src, dst string) (err error) {
	buf := make([]byte, 32*1024)

	var nr, nw int
	var written int64
	for {
		nr, err = r.Read(buf)

		if nr > 0 {
			switch nw, err = w.Write(buf[0:nr]); {
			case err != nil:
				return err
			case nr < nw:
				return io.ErrShortWrite
			case nr != nw:
				return fmt.Errorf("invalid write: expected to write %d bytes, wrote %d bytes instead", nr, nw)
			}

			written += int64(nw)

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if z.ProgressReporter != nil {
					z.ProgressReporter(src, dst, written, false)
				}
			}
		}

		if err == io.EOF {
			z.ProgressReporter(src, dst, written, true)
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// WalkRegularFiles is a specialisation of filepath.WalkDir that applies the callback only to regular files.
//
// This is the same method that Zipper.CompressDir will use to compress files.
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
