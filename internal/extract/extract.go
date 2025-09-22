package extract

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/nguyengg/xy3/util"
	"github.com/schollz/progressbar/v3"
)

const defaultBufferSize = 32 * 1024

var ErrUnknownArchiveExtension = errors.New("unknown archive extension")

// EntriesFromExt uses the extension of the file's name to determine decompression algorithm.
//
// TODO use http.DetectContentType() instead of relying on file extension.
// TODO implement unwrap root by using two passes.
//
// Returns ErrUnknownArchiveExtension if the extension is not supported.
func EntriesFromExt(src io.Reader, ext string) (iter.Seq2[Entry, error], error) {
	switch ext {
	case ".7z":
		if f, ok := src.(*os.File); ok {
			return From7zFile(f), nil
		}

		// TODO find an implementation of 7z reader that receives just io.Reader
		return nil, fmt.Errorf("7z archives must be opened as os.File")
	case ".zip":
		if f, ok := src.(*os.File); ok {
			return FromZipFile(f), nil
		}

		return FromZipReader(src), nil
	case ".tar.zst":
		return FromTarZstReader(src), nil
	case ".tar.gz":
		return FromTarGzipReader(src), nil
	default:
		return nil, ErrUnknownArchiveExtension
	}
}

// Options customises Extract.
type Options struct {
	// ProgressBar if given will be used to provide progress report.
	ProgressBar *progressbar.ProgressBar
}

// Extract uses the extension of the file's name to determine decompression algorithm (see EntriesFromExt).
//
// The dir argument is the parent directory to create extracted files.
//
// Returns ErrUnknownArchiveExtension if the extension is not supported.
func Extract(ctx context.Context, src io.Reader, ext, dir string, optFns ...func(*Options)) error {
	opts := &Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	files, err := EntriesFromExt(src, ext)
	if err != nil {
		return err
	}

	buf := make([]byte, defaultBufferSize)

	for f, err := range files {
		if err != nil {
			return err
		}

		// TODO support creating directories as well

		path, fi := filepath.Join(dir, f.Name()), f.FileInfo()

		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			_ = f.Close()
			return fmt.Errorf(`create path to file "%s" error: %w`, path, err)
		}

		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, f.FileMode())
		if err != nil {
			_ = f.Close()
			return fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		if opts.ProgressBar != nil {
			_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(w, opts.ProgressBar), f, buf)
		} else {
			_, err = util.CopyBufferWithContext(ctx, w, f, buf)
		}

		_, _ = w.Close(), f.Close()
		if err != nil {
			return fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}

		if err = os.Chtimes(path, time.Time{}, fi.ModTime()); err != nil {
			return fmt.Errorf(`change mod time of "%s" error: %w"`, path, err)
		}
	}

	return nil
}

// Entry represents an opened file in the archive to be extracted.
type Entry interface {
	Name() string
	FileInfo() os.FileInfo
	FileMode() os.FileMode
	io.ReadCloser
}
