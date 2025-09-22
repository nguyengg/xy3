package internal

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/nguyengg/xy3/util"
	"github.com/ulikunitz/xz"
)

// DecompressOptions customises Decompressor.Decompress.
type DecompressOptions struct {
}

type Decompressor interface {
	// Decompress decompresses the given io.Reader and writes to dst io.Writer.
	//
	// Intended to be used for decompressing-only without extracting from archives.
	Decompress(ctx context.Context, src io.Reader, dst io.Writer, optFns ...func(*DecompressOptions)) error

	// Extract treats the given io.Reader as an archive to decompress and extract its contents to directory
	// specified by dir.
	//
	// Essentially, Decompress will always create a unique file or directory.
	Extract(ctx context.Context, src io.Reader, dir string, optFns ...func(*DecompressOptions)) error
}

// Decompress decompresses and optionally extracts the named file or archive to the given parent directory.
//
// If the file specified by "name" is an archive, the returned "target" string will be the name of the directory
// containing extracted contents. If the file "name" is not an archive, "target" will be the name of the decompressed
// file.
//
// TODO use http.DetectContentType() instead of relying on file extension.
func Decompress(ctx context.Context, name, dir string, optFns ...func(*DecompressOptions)) (target string, err error) {
	opts := &DecompressOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	stem, ext := util.StemAndExt(name)
	src, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	var (
		// only one of these values will be non-nil.
		ex  extractor
		dec io.ReadCloser
	)

	switch ext {
	case ".tar.gz":
		ex = &tarCodec{ex: fromTarGzipReader}
	case ".tar.xz":
		ex = &tarCodec{ex: fromTarXzReader}
	case ".tar.zst":
		ex = &tarCodec{ex: fromTarZstReader}
	case ".7z":
		ex = &sevenZipCodec{}
	case ".zip":
		ex = &zipCodec{}
	case ".zst":
		r, err := zstd.NewReader(src)
		if err != nil {
			return "", fmt.Errorf("create zstd reader error: %w", err)
		}
		defer r.Close()

		dec = io.NopCloser(r)
	case ".gz":
		dec, err = gzip.NewReader(src)
		if err != nil {
			return "", fmt.Errorf("create gzip reader error: %w", err)
		}
	case ".xz":
		r, err := xz.NewReader(src)
		if err != nil {
			return "", fmt.Errorf("create xz reader error: %w", err)
		}

		dec = io.NopCloser(r)
	default:
		return "", fmt.Errorf("unsupported extension: %v", ext)
	}

	// just decompressing.
	if dec != nil {
		dst, err := util.OpenExclFile(dir, stem, ext, 0666)
		if err != nil {
			return "", fmt.Errorf("create output file error: %w", err)
		}

		bar := DefaultBytes(-1, fmt.Sprintf(`decompressing "%s"`, filepath.Base(name)))
		_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(dst, bar), dec, nil)
		_ = bar.Close()
		if err == nil {
			err = dec.Close()
		}
		if err != nil {
			_ = os.Remove(dst.Name())
			return "", fmt.Errorf(`decompress file "%s" error: %w`, name, err)
		}

		return dst.Name(), nil
	}

	// the contents of the archive will be extracted into a unique directory.
	// if unsuccessful, this output directory will be deleted.
	target, err = util.MkExclDir(dir, stem, 0755)
	if err != nil {
		return "", fmt.Errorf("create output directory error: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(target)
		}
	}()

	// extracting will start with finding root dir.
	var rootDir RootDir
	files, err := ex.Files(src, false)
	if err != nil {
		return "", nil
	}
	if rootDir, err = findRootDir(ctx, files); err != nil {
		return "", err
	}
	if _, err = src.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek start error: %w", err)
	}

	// now go through the archive files again, this time opening each file for reading.
	files, err = ex.Files(src, true)
	if err != nil {
		return "", err
	}

	bar := DefaultBytes(-1, fmt.Sprintf(`decompressing "%s"`, filepath.Base(name)))
	defer bar.Close()

	buf := make([]byte, defaultBufferSize)

	for f, err := range files {
		if err != nil {
			return "", err
		}

		// TODO support creating directories as well

		path, fi := rootDir.Join(target, f.Name()), f.FileInfo()

		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			_ = f.Close()
			return "", fmt.Errorf(`create path to file "%s" error: %w`, path, err)
		}

		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, f.FileMode())
		if err != nil {
			_ = f.Close()
			return "", fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(w, bar), f, buf)

		_, _ = w.Close(), f.Close()
		if err != nil {
			return "", fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}

		if err = os.Chtimes(path, time.Time{}, fi.ModTime()); err != nil {
			return "", fmt.Errorf(`change mod time of "%s" error: %w"`, path, err)
		}
	}

	success = true

	return target, nil
}

func findRootDir(ctx context.Context, files iter.Seq2[ArchiveFile, error]) (rootDir RootDir, err error) {
	var (
		rootFinder = NewZipRootDirFinder()
		ok         bool
	)

	for f, err := range files {
		if err != nil {
			return "", err
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			if rootDir, ok = rootFinder(f.Name()); !ok {
				return rootDir, nil
			}
		}

	}

	return
}

// extractor abstracts methods to browse and extract contents from an archive.
type extractor interface {
	// Files produces an iterator returning the archive entries.
	//
	// The src io.Reader will be consumed by the end of the iterator.
	Files(src io.Reader, open bool) (iter.Seq2[ArchiveFile, error], error)
}

// ArchiveFile represents a file in an archive.
type ArchiveFile interface {
	Name() string
	FileInfo() os.FileInfo
	FileMode() os.FileMode
	io.ReadCloser
}
