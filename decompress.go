package xy3

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	commons "github.com/nguyengg/go-aws-commons"
	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/archive"
	"github.com/nguyengg/xy3/internal"
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

	if opts.NoExtract {
		return decompress(ctx, name, dir)
	}

	return extract(ctx, name, dir)
}

func decompress(ctx context.Context, name, dir string) (string, error) {
	// use the file name extension to detect a codec.
	cd := NewDecoderFromExt(filepath.Ext(name))
	if cd == nil {
		return "", fmt.Errorf(`no supported decompression algorithm for file "%s"`, filepath.Base(name))
	}

	// the name of the output file will be the original with the codec ext trimmed off.
	stem, ext := commons.StemExt(strings.TrimSuffix(name, cd.Ext()))
	dst, err := commons.OpenExclFile(dir, stem, ext, 0666)
	if err != nil {
		return "", fmt.Errorf("create output file error: %w", err)
	}
	defer dst.Close()

	src, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	bar := tspb.DefaultBytesWriter(src, `decompressing "{basename}"`)
	defer bar.Close()

	r, err := cd.NewDecoder(io.TeeReader(src, bar))
	if err != nil {
		return "", fmt.Errorf("create decoder error: %w", err)
	}

	closer := internal.ChainCloser(r.Close, src.Close, dst.Close)

	if _, err = commons.CopyBufferWithContext(ctx, dst, r, nil); err != nil {
		_, _ = closer(), os.Remove(dst.Name())
		return "", fmt.Errorf(`decompress file "%s" error: %w`, name, err)
	}

	if err = closer(); err != nil {
		return "", fmt.Errorf(`complete writing to file "%s" error: %w`, name, err)
	}

	_ = bar.Finish()
	return dst.Name(), nil
}

func extract(ctx context.Context, name, dir string) (string, error) {
	// use the file's base name to detect a decompressor.
	arc := NewDecompressorFromName(filepath.Base(name))
	if arc == nil {
		return "", fmt.Errorf(`no supported decompression algorithm for file "%s"`, filepath.Base(name))
	}

	// decompress and extract contents into a unique directory.
	stem, _ := commons.StemExt(strings.TrimSuffix(name, arc.ArchiveExt()))
	target, err := commons.MkExclDir(dir, stem, 0755)
	if err != nil {
		return "", fmt.Errorf("create output directory error: %w", err)
	}

	// if unsuccessful, this output directory will be deleted.
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(target)
		}
	}()

	// first pass to find root dir and uncompressed size for progress report.
	rootDir, uncompressedSize, err := findRootDir(ctx, name, arc)
	if err != nil {
		return "", fmt.Errorf("find root dir error: %w", err)
	}

	bar := tspb.DefaultBytes(uncompressedSize, fmt.Sprintf(`extracting "%s"`, internal.TruncateRightWithSuffix(filepath.Base(name), 15, "...")))
	defer bar.Close()

	// now go through the archive files again, this time opening each file for reading.
	src, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	files, err := arc.Open(src)
	if err != nil {
		return "", fmt.Errorf(`read archive "%s" error: %w`, name, err)
	}

	buf := make([]byte, 32*1024)

	for f, err := range files {
		if err != nil {
			return "", err
		}

		name, fi := f.Name(), f.FileInfo()
		if fi.IsDir() || strings.HasSuffix(name, "/") {
			// TODO support creating directories as well
			continue
		}

		r, err := f.Open()
		if err != nil {
			return "", fmt.Errorf(`open archive file "%s" error: %w`, f.Name(), err)
		}

		path := rootDir.Join(target, name)
		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			_ = r.Close()
			return "", fmt.Errorf(`create path to file "%s" error: %w`, path, err)
		}

		w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, f.Mode())
		if err != nil {
			_ = r.Close()
			return "", fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		closer := internal.ChainCloser(w.Close, r.Close)

		if _, err = commons.CopyBufferWithContext(ctx, w, io.TeeReader(r, bar), buf); err != nil {
			_ = closer()
			return "", fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}

		if err = closer(); err != nil {
			return "", fmt.Errorf(`complete writing to file "%s" error: %w`, path, err)
		}

		if err = os.Chtimes(path, time.Time{}, fi.ModTime()); err != nil {
			return "", fmt.Errorf(`change mod time of "%s" error: %w"`, path, err)
		}
	}

	success = true
	_ = bar.Finish()
	return target, nil
}

// findRootDir inspects the named archive and return the root dir (if exits).
//
// And since we're already looking through all the files to find root dir, let's tally up the count and total
// uncompressed size of total regular files for better progress report.
func findRootDir(ctx context.Context, name string, archiver archive.Archiver) (rootDir internal.RootDir, uncompressedSize int64, err error) {
	src, err := os.Open(name)
	if err != nil {
		return "", 0, fmt.Errorf(`open file "%s" error: %w`, name, err)
	}
	defer src.Close()

	files, err := archiver.Open(src)
	if err != nil {
		return "", 0, fmt.Errorf(`read archive "%s" error: %w`, name, err)
	}

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

			if f.Mode().IsRegular() {
				uncompressedSize += f.FileInfo().Size()
			}
		}

	}

	return
}
