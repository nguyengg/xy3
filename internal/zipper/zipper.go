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

// Zipper is used to recursively compress (with zip) a directory or file with progress report and cancellable context.
type Zipper struct {
	// JunkRoot determines whether all compressed files are under a single root directory hierarchy or not.
	//
	// For example, when compressing directory named "test" with JunkRoot being false by default, all files in that
	// directory and their children will be added to archive using these paths:
	// 		test/a.txt
	//		test/path/b.txt
	//		test/another/path/c.txt
	//
	// If JunkRoot is true, the files will use these paths:
	//		a.txt
	//		path/b.txt
	//		another/path/c.txt
	JunkRoot bool

	// ProgressReporter controls how progress is reported.
	ProgressReporter ProgressReporter

	// NewWriter allows customization of the zip.Writer being used.
	//
	// Default to BestCompressionZipWriter.
	NewWriter func(w io.Writer) *zip.Writer
}

// New creates a new Zipper with default settings.
//
// By default, log.Printf will be used to provide progress update only on each file being added to the archive.
func New() *Zipper {
	return &Zipper{
		ProgressReporter: DefaultProgressReporter,
		NewWriter:        BestCompressionZipWriter,
	}
}

// NoCompressionZipWriter returns a zip.Writer that registers flate.NoCompression as its compression.
func NoCompressionZipWriter(w io.Writer) *zip.Writer {
	zw := zip.NewWriter(w)
	zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.NoCompression)
	})
	return zw
}

// BestCompressionZipWriter returns a zip.Writer that registers flate.BestCompression as its compression.
func BestCompressionZipWriter(w io.Writer) *zip.Writer {
	zw := zip.NewWriter(w)
	zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(w, flate.BestCompression)
	})
	return zw
}

// CompressFile compresses a single file to the archive opened as io.Writer.
func (z Zipper) CompressFile(ctx context.Context, name string, dst io.Writer) error {
	sf, err := os.Open(name)
	if err != nil {
		return err
	}

	zw := z.NewWriter(dst)
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
// If JunkRoot is true, the files will use these paths:
//
//	a.txt
//	path/b.txt
//	another/path/c.txt
func (z Zipper) CompressDir(ctx context.Context, root string, dst io.Writer, junkRoot bool) error {
	zw := z.NewWriter(dst)
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

		if err != nil || d.IsDir() || !d.Type().IsRegular() {
			return err
		}

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
