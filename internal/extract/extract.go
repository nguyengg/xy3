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
	"github.com/nguyengg/xy3/util"
)

func Extract(ctx context.Context, src io.Reader, name string) error {
	buf := make([]byte, 32*1024)

	// TODO use http.DetectContentType() instead of relying on file extension.
	_, ext := util.StemAndExt(name)
	switch ext {
	case ".7z":
		if f, ok := src.(*os.File); ok {
			return extract7zFile(ctx, f, buf)
		}

		// TODO find an implementation of 7z reader that receives just io.Reader
		return fmt.Errorf("7z archives must be opened as os.File")
	case ".zip":
		if f, ok := src.(*os.File); ok {
			return extractZipFile(ctx, f, buf)
		}

		return extractZip(ctx, src, buf)
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

		err = untar(ctx, gr, buf)
		if err == nil {
			err = gr.Close()
		}

		return err
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

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(hdr.Mode))
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
