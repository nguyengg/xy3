package z

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

// ExtractOptions is an opaque struct for customising Extract.
type ExtractOptions struct {
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

	// UseGivenDirectory will extract files directly to the dir argument passed to Extract.
	//
	// See Extract for more information on the interaction between UseGivenDirectory and NoUnwrapRoot.
	UseGivenDirectory bool

	// NoUnwrapRoot turns off root unwrapping feature.
	//
	// See Extract for more information on the interaction between UseGivenDirectory and NoUnwrapRoot.
	NoUnwrapRoot bool

	// NoOverwrite will ignore files that already exist at the target directory.
	//
	// By default, Extract will overwrite existing files. If NoOverwrite is true, those files will be skipped.
	NoOverwrite bool
}

// Extract recursively extracts the named archive to the given parent directory.
//
// Returns the name of the output directory which can be different from the argument "dir".
//
// See ExtractOptions for customisation options. For example, if the archive ("default.zip") has file like this:
//
//	test/a.txt
//	test/path/b.txt
//	test/another/path/c.txt
//
// Using "my-dir" as the dir argument, if "my-dir/test" already exists, the extracted directory looks like this:
//
//	my-dir/test-1/a.txt
//	my-dir/test-1/path/b.txt
//	my-dir/test-1/another/path/c.txt
//
// If "my-dir/test" didn't exist, the extracted directory looks like this:
//
//	my-dir/test/a.txt
//	my-dir/test/path/b.txt
//	my-dir/test/another/path/c.txt
//
// If the content of the archive ("no-root.zip") did not have a common top-level directory ("root" directory) like this:
//
//	a.txt
//	path/b.txt
//	another/path/c.txt
//
// The name of the archive ("no-root.zip") will be used to create a new directory:
//
//	my-dir/no-root/a.txt
//	my-dir/no-root/path/b.txt
//	my-dir/no-root/another/path/c.txt
//
// If "my-archive" already exists, "my-archive-1", "my-archive-2", etc. will be created.
//
// If [ExtractOptions.UseGivenDirectory] is true, the dir argument is used as the root directory to extract files to.
// Extract is able to create dir if it didn't exist as a directory prior to invocation.
//
// If [ExtractOptions.NoUnwrapRoot] is true, the common root directory in archive will be created in the extracted
// directory. This flag is only meaningful if all files in the archive content are under one common top-level directory
// ("root" directory). For example, the "no-root.zip" example above has no common root because a.txt exists at the top
// level while b.txt and c.txt shares no common path.
//
// Using "default.zip" example, if [ExtractOptions.NoUnwrapRoot] is true and [ExtractOptions.UseGivenDirectory] is true,
// the extracted directory for would become:
//
//	my-dir/test/a.txt
//	my-dir/test/path/b.txt
//	my-dir/test/another/path/c.txt
//
// If [ExtractOptions.NoUnwrapRoot] is true and [ExtractOptions.UseGivenDirectory] is false, however, the extracted
// directory becomes:
//
//	my-dir/default/a.txt
//	my-dir/default/path/b.txt
//	my-dir/default/another/path/c.txt
//
// In other words, because [ExtractOptions.UseGivenDirectory] is false, "default" (or "default-1", "default-2") was
// created as the output directory. So long as [ExtractOptions.UseGivenDirectory] is false, the default settings will
// always try to extract to a newly created directory to avoid conflicts.
//
// Note: the definition of root is limited to only the top-level directory. Even if the archive may have a longer common
// root, in this example the archive is still considered to have only "test" as the common root:
//
//	test/path/to/a.txt
//	test/path/to/b.txt
//	test/path/to/c.txt
//
// This is because most users will compress a directory named "test" wishing to retain the directory structure inside
// "test", but when extracting they don't necessarily want "test" to exist.
func Extract(ctx context.Context, src, dir string, optFns ...func(*ExtractOptions)) (string, error) {
	opts := &ExtractOptions{
		ProgressReporter: DefaultProgressReporter,
		BufferSize:       DefaultBufferSize,
	}
	for _, fn := range optFns {
		fn(opts)
	}

	zipReader, err := zip.OpenReader(src)
	if err != nil {
		return "", fmt.Errorf("open zip error: %w", err)
	}

	// determine the output directory from options.
	if !opts.UseGivenDirectory {
		stem := strings.TrimSuffix(filepath.Base(src), filepath.Ext(src))
		name := filepath.Join(dir, stem)
	mkdirLoop:
		for i := 0; ; {
			switch err = os.Mkdir(name, 0755); {
			case err == nil:
				dir = name
				break mkdirLoop
			case errors.Is(err, os.ErrExist):
				i++
				name = filepath.Join(dir, stem+"-"+strconv.Itoa(i))
			default:
				return "", fmt.Errorf("create output directory error: %w", err)
			}
		}
	}

	// determine whether the file's path in archive is trimmed (unwrapping its root).
	var rootDir internal.RootDir
	if !opts.NoUnwrapRoot {
		ok, rootFinder := false, internal.NewZipRootDirFinder()
		for _, f := range zipReader.File {
			if rootDir, ok = rootFinder(f.Name); !ok {
				break
			}
		}
	}

	// start walking through the files to extract them.
	buf := make([]byte, opts.BufferSize)
	pr := opts.ProgressReporter
	for _, f := range zipReader.File {
		select {
		case <-ctx.Done():
			return dir, ctx.Err()
		default:
			break
		}

		name := f.Name
		path := rootDir.Join(dir, name)

		fi := f.FileInfo()
		if fi.IsDir() {
			if err = os.MkdirAll(path, f.Mode().Perm()); err != nil {
				return dir, fmt.Errorf("create directory (path=%s) error: %w", path, err)
			}

			continue
		}

		perm := fi.Mode().Perm()
		if err = os.MkdirAll(filepath.Dir(path), perm); err != nil {
			return dir, fmt.Errorf("create parent directories to file (path=%s) error: %w", path, err)
		}

		flag := os.O_WRONLY | os.O_CREATE | os.O_EXCL
		if !opts.NoOverwrite {
			flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		}

		dst, err := os.OpenFile(path, flag, perm)
		if err != nil {
			if opts.NoOverwrite && os.IsExist(err) {
				continue
			}

			return dir, fmt.Errorf("create file (path=%s) error: %w", path, err)
		}

		src, err := f.Open()
		if err != nil {
			_ = dst.Close()
			return dir, fmt.Errorf("open file (name=%s) in archive error: %w", name, err)
		}

		if pr == nil {
			_, err = util.CopyBufferWithContext(ctx, dst, src, buf)
		} else {
			w := pr.createWriter(name, rel(dir, dst.Name()))
			_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(dst, w), src, buf)
			if err == nil {
				w.done()
			}
		}

		_, _ = dst.Close(), src.Close()
		if err != nil {
			return dir, fmt.Errorf("extract file (name%s) in archive to file (path=%s) error: %w", name, path, err)
		}
	}

	return dir, nil
}
