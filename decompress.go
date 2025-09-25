package xy3

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

// DecompressOptions customises Decompress.
type DecompressOptions struct {
	// NoExtract if true will only decompress archives without extracting their contents.
	NoExtract bool
}

// Decompress decompresses and optionally extracts the named file or archive to the given parent directory.
//
// If the file specified by "name" is an archive, the returned "target" string will be the name of the directory
// containing extracted contents. If the file "name" is not an archive, "target" will be the name of the decompressed
// file.
func Decompress(ctx context.Context, name, dir string, optFns ...func(*DecompressOptions)) (target string, err error) {
	opts := &DecompressOptions{}
	for _, fn := range optFns {
		fn(opts)
	}

	// TODO use http.DetectContentType() instead of relying on file extension.
	if opts.NoExtract {
		basename := filepath.Base(name)
		ext := filepath.Ext(name)
		stem := strings.TrimSuffix(basename, ext)
		switch ext {
		case ".gz":
			ext = filepath.Ext(stem)
			stem = strings.TrimSuffix(stem, ext)
			return decompress(ctx, name, dir, stem, ext, &gzCodec{})
		case ".xz":
			ext = filepath.Ext(stem)
			stem = strings.TrimSuffix(stem, ext)
			return decompress(ctx, name, dir, stem, ext, &xzCodec{})
		case ".zst":
			ext = filepath.Ext(stem)
			stem = strings.TrimSuffix(stem, ext)
			return decompress(ctx, name, dir, stem, ext, &zstdCodec{})
		default:
			return "", fmt.Errorf("no support for decompression of files with extension %s", ext)
		}
	}

	switch stem, ext := util.StemAndExt(name); ext {
	case ".tar.gz":
		return extract(ctx, name, dir, stem, &tarCodec{dec: &gzCodec{}})
	case ".tar.xz":
		return extract(ctx, name, dir, stem, &tarCodec{dec: &xzCodec{}})
	case ".tar.zst":
		return extract(ctx, name, dir, stem, &tarCodec{dec: &zstdCodec{}})
	case ".7z":
		return extract(ctx, name, dir, stem, &sevenZipCodec{})
	case ".zip":
		return extract(ctx, name, dir, stem, &zipCodec{})
	default:
		return "", fmt.Errorf("no support for extraction from files with extension %s", ext)
	}
}

func decompress(ctx context.Context, name, dir, stem, ext string, dec decompressor) (string, error) {
	src, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	// stat file for the size to get progress report.
	fi, err := src.Stat()
	if err != nil {
		return "", fmt.Errorf(`stat file "%s" error: %w`, name, err)
	}
	bar := tspb.DefaultBytes(fi.Size(), fmt.Sprintf(`decompressing "%s"`, filepath.Base(name)))

	r, err := dec.NewDecoder(io.TeeReader(src, bar))
	if err != nil {
		return "", err
	}
	defer r.Close()

	dst, err := util.OpenExclFile(dir, stem, ext, 0666)
	if err != nil {
		return "", fmt.Errorf("create output file error: %w", err)
	}

	_, err = util.CopyBufferWithContext(ctx, dst, r, nil)
	_ = dst.Close()
	if err == nil {
		err, _ = r.Close(), bar.Close()
	}
	if err != nil {
		_ = os.Remove(dst.Name())
		return "", fmt.Errorf(`decompress file "%s" error: %w`, name, err)
	}

	return dst.Name(), nil
}

type decompressor interface {
	NewDecoder(io.Reader) (io.ReadCloser, error)
}

func extract(ctx context.Context, name, dir, stem string, ex extractor) (string, error) {
	src, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	// decompress and extract archive contents into a unique directory.
	// if unsuccessful, this output directory will be deleted.
	target, err := util.MkExclDir(dir, stem, 0755)
	if err != nil {
		return "", fmt.Errorf("create output directory error: %w", err)
	}

	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(target)
		}
	}()

	// extracting will start with finding root dir. since we're already looking through all the files to find
	// root dir, let's tally up the count and total uncompressed size of total regular files for better progress
	// report.
	var (
		rootDir          internal.RootDir
		uncompressedSize int64
	)
	if err = util.ResettableReadSeeker(src, func(r io.ReadSeeker) error {
		files, err := ex.Files(r, false)
		if err != nil {
			return err
		}
		if rootDir, uncompressedSize, err = findRootDir(ctx, files); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("find root dir error: %w", err)
	}

	bar := tspb.DefaultBytes(uncompressedSize, fmt.Sprintf(`extracting "%s"`, filepath.Base(name)))
	defer bar.Close()

	// now go through the archive files again, this time opening each file for reading.
	files, err := ex.Files(src, true)
	if err != nil {
		return "", err
	}

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

		_, err = util.CopyBufferWithContext(ctx, w, io.TeeReader(f, bar), buf)
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

func findRootDir(ctx context.Context, files iter.Seq2[archiveFile, error]) (rootDir internal.RootDir, uncompressedSize int64, err error) {
	var (
		rootFinder = internal.NewZipRootDirFinder()
		ok         = true
	)

	for f, err := range files {
		if err != nil {
			return rootDir, uncompressedSize, err
		}

		select {
		case <-ctx.Done():
			return rootDir, uncompressedSize, ctx.Err()
		default:
			if ok {
				rootDir, ok = rootFinder(f.Name())
			}

			if f.FileMode().IsRegular() {
				uncompressedSize += f.FileInfo().Size()
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
	Files(src io.Reader, open bool) (iter.Seq2[archiveFile, error], error)
}

// archiveFile represents a file in an archive.
type archiveFile interface {
	Name() string
	FileInfo() os.FileInfo
	FileMode() os.FileMode
	io.ReadCloser
}
