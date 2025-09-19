package extract

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3/util"
)

func Extract(ctx context.Context, src io.Reader, name string) error {
	buf := make([]byte, 64*1024)

	// TODO use http.DetectContentType() instead of relying on file extension.
	_, ext := util.StemAndExt(name)
	switch ext {
	case ".zip":
		for zr := zipstream.NewReader(src); ; {
			fh, err := zr.Next()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return fmt.Errorf("stream zip error: %w", err)
			}

			path := fh.Name

			fi := fh.FileInfo()
			if fi.IsDir() {
				if err = os.MkdirAll(path, 0755); err != nil {
					return fmt.Errorf(`create dir "%s" error: %w`, path, err)
				}
				continue
			}

			if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return fmt.Errorf(`create path to file "%s" error: %w`, path, err)
			}

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fi.Mode())
			if err != nil {
				return fmt.Errorf(`create file "%s" error: %w`, path, err)
			}

			_, err = util.CopyBufferWithContext(ctx, f, zr, buf)
			_ = f.Close()
			if err != nil {
				return fmt.Errorf(`write to file "%s" error: %w`, path, err)
			}
		}
	case ".tar.zst":
		zr, err := zstd.NewReader(src)
		if err != nil {
			return fmt.Errorf("open zstd reader error: %w", err)
		}

		defer zr.Close()
		return untar(ctx, zr, buf)
	case ".tar.gz":
		gr, err := gzip.NewReader(src)
		if err != nil {
			return fmt.Errorf("open gzip reader error: %w", err)
		}

		defer gr.Close()
		return untar(ctx, gr, buf)

	default:
		return fmt.Errorf("unknown extension: %v", ext)
	}
}

func untar(ctx context.Context, src io.Reader, buf []byte) error {
	tr := tar.NewReader(src)

	for {
		hdr, err := tr.Next()
		if err != nil {
			return fmt.Errorf("read next tar entry error: %w", err)
		}

		path := hdr.Name

		if hdr.Typeflag == tar.TypeDir {
			if err = os.MkdirAll(path, 0755); err != nil {
				return fmt.Errorf(`create dir "%s" error: %w`, path, err)
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf(`create path to file "%s" error: %w`, path, err)
		}

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		_, err = util.CopyBufferWithContext(ctx, f, tr, buf)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}
	}
}
