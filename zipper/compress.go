package zipper

import (
	"archive/zip"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	commons "github.com/nguyengg/go-aws-commons"
)

const (
	// DefaultBufferSize is the default value for [Compressor.BufferSize], which is 32 KiB.
	DefaultBufferSize = 32 * 1024
)

// CompressOptions customises CompressFile.
type CompressOptions struct {
	// ProgressReporter controls how progress is reported.
	//
	// By default, DefaultProgressReporter is used.
	ProgressReporter ProgressReporter

	// BufferSize is the length of the buffer being used for copying/adding files to the archive.
	//
	// BufferSize indirectly controls how frequently ProgressReporter is called; after each copy is done,
	// ProgressReporter is called once.
	//
	// Default to DefaultBufferSize.
	BufferSize int

	// NewWriter allows customization of the zip.Writer being used.
	//
	// Default to a [zip.NewWriter].
	NewWriter func(w io.Writer) *zip.Writer
}

// WithNoCompression uses a [zip.Writer] that registers [flate.NoCompression] as its compressor.
func WithNoCompression(options *CompressOptions) {
	options.NewWriter = func(w io.Writer) *zip.Writer {
		zw := zip.NewWriter(w)
		zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(w, flate.NoCompression)
		})
		return zw
	}
}

// WithBestCompression uses a [zip.Writer] that registers [flate.BestCompression] as its compressor.
func WithBestCompression(options *CompressOptions) {
	options.NewWriter = func(w io.Writer) *zip.Writer {
		zw := zip.NewWriter(w)
		zw.RegisterCompressor(zip.Deflate, func(w io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(w, flate.BestCompression)
		})
		return zw
	}
}

// CompressFile compresses a single file (specified by name) to the archive opened as io.Writer.
func CompressFile(ctx context.Context, name string, dst io.Writer, optFns ...func(*CompressOptions)) error {
	opts := &CompressOptions{
		ProgressReporter: DefaultProgressReporter,
		BufferSize:       DefaultBufferSize,
		NewWriter:        zip.NewWriter,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	zipWriter := opts.NewWriter(dst)
	defer zipWriter.Close()

	src, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("open src file error: %w", err)
	}
	defer src.Close()

	fi, err := src.Stat()
	if err != nil {
		return fmt.Errorf("describe src file error: %w", err)
	}

	f, err := zipWriter.CreateHeader(fileHeader(fi, fi.Name()))
	if err != nil {
		return fmt.Errorf("create zip file header error: %w", err)
	}

	buf := make([]byte, opts.BufferSize)
	pr := opts.ProgressReporter
	if pr == nil {
		if _, err = commons.CopyBufferWithContext(ctx, f, src, buf); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			return fmt.Errorf("add src file to archive error: %w", err)
		}

		return nil
	}

	w := pr.CreateWriter(name, fi.Name())
	if _, err = commons.CopyBufferWithContext(ctx, io.MultiWriter(f, w), src, buf); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}

		return fmt.Errorf("add src file to archive error: %w", err)
	}

	return w.Close()
}

func fileHeader(fi os.FileInfo, name string) *zip.FileHeader {
	fh := &zip.FileHeader{
		Name:     strings.ReplaceAll(name, "\\", "/"),
		Modified: fi.ModTime(),
	}
	fh.SetMode(fi.Mode())
	return fh
}
