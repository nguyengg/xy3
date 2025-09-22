package extract

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
	"github.com/schollz/progressbar/v3"
)

const defaultBufferSize = 32 * 1024

// Options customises Extractor.Extract.
type Options struct {
	// ProgressBar if given will be used to provide progress report.
	ProgressBar *progressbar.ProgressBar
}

// Extractor provides methods to extract contents from an archive using iterator.
type Extractor interface {
	// Entries produces an iterator returning the archive entries.
	//
	// The src io.Reader will be consumed by the end of the iterator.
	Entries(src io.Reader) (iter.Seq2[Entry, error], error)
	// Extract extracts contents of the archive to the dir directory.
	Extract(ctx context.Context, src io.Reader, dir string, optFns ...func(*Options)) error
}

type entriesExtractor interface {
	Entries(src io.Reader) (iter.Seq2[Entry, error], error)
}

// DetectExtractorFromExt uses the extension of the file's name to determine decompression algorithm.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func DetectExtractorFromExt(ext string) Extractor {
	switch ext {
	case ".7z":
		return &baseExtractor{sevenZipExtractor{}}
	case ".zip":
		return &baseExtractor{zipExtractor{}}
	case ".tar.zst":
		return &baseExtractor{tarExtractor{fromTarZstReader}}
	case ".tar.gz":
		return &baseExtractor{tarExtractor{fromTarGzipReader}}
	default:
		return nil
	}
}

type baseExtractor struct {
	entriesExtractor
}

func (e *baseExtractor) Extract(ctx context.Context, src io.Reader, dir string, optFns ...func(*Options)) error {
	opts := &Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	var rootDir internal.RootDir
	if rs, ok := src.(io.ReadSeeker); ok {
		files, err := e.entriesExtractor.Entries(rs)
		if err != nil {
			return err
		}

		rootFinder := internal.NewZipRootDirFinder()

		for f, err := range files {
			if err != nil {
				return err
			}

			if rootDir, ok = rootFinder(f.Name()); !ok {
				break
			}
		}

		if _, err = rs.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek start error: %w", err)
		}
	}

	files, err := e.entriesExtractor.Entries(src)
	if err != nil {
		return err
	}

	buf := make([]byte, defaultBufferSize)

	for f, err := range files {
		if err != nil {
			return err
		}

		// TODO support creating directories as well

		path, fi := rootDir.Join(dir, f.Name()), f.FileInfo()

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
