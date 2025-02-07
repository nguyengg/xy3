package upload

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/zipper"
	"github.com/ulikunitz/xz"
)

type compressor struct {
	ContentType string
	Ext         string
	Compress    func(ctx context.Context, dst io.Writer, src string) (err error)
}

var (
	Zip = compressor{
		ContentType: "application/zip",
		Ext:         ".zip",
		Compress: func(ctx context.Context, dst io.Writer, root string) (err error) {
			z := zipper.New()
			z.ProgressReporter, err = zipper.NewProgressBarReporter(ctx, root, nil)
			if err == nil {
				err = z.CompressDir(ctx, root, dst, false)
			}
			return
		},
	}
	XZ = compressor{
		ContentType: "application/x-xz",
		Ext:         ".tar.xz",
		Compress: func(ctx context.Context, dst io.Writer, root string) error {
			base := filepath.Base(root)

			_, size, err := zipper.CountDirContents(ctx, root)
			if err != nil {
				return err
			}

			bar := internal.DefaultBytes(size, "compressing")

			xzw, err := xz.NewWriter(dst)
			if err != nil {
				return err
			}

			tw := tar.NewWriter(xzw)

			if err = zipper.WalkRegularFiles(ctx, root, func(path string, d fs.DirEntry) error {
				fi, err := d.Info()
				if err != nil {
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

				if err = tw.WriteHeader(&tar.Header{
					Name: filepath.Join(base, path),
					Size: fi.Size(),
					Mode: 0600,
				}); err == nil {
					err = internal.CopyWithContext(ctx, io.MultiWriter(tw, bar), src)
				}

				return err
			}); err != nil {
				return err
			}

			if err = tw.Close(); err == nil {
				err = xzw.Close()
			}

			return err
		},
	}
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// All files in the archive include root's basename in its path, meaning the top-level file of the archive output is
// the root directory itself.
func (c *Command) compress(ctx context.Context, root string) (name string, contentType *string, err error) {
	base := filepath.Base(root)

	z := Zip
	if c.XZ {
		z = XZ
	}

	contentType = &z.ContentType

	// a new file will always be created, and if the operation fails, the file will be auto deleted.
	out, err := internal.OpenExclFile(base, z.Ext)
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	name = out.Name()
	defer func() {
		if _ = out.Close(); err != nil {
			_ = os.Remove(name)
			name = ""
		}
	}()

	err = z.Compress(ctx, out, root)
	return
}
