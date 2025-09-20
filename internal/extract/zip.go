package extract

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/krolaw/zipstream"
	"github.com/nguyengg/xy3/util"
)

func extractZip(ctx context.Context, src io.Reader, buf []byte) error {
	zr := zipstream.NewReader(src)

	for {
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

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fi.Mode())
		if err != nil {
			return fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		_, err = util.CopyBufferWithContext(ctx, f, zr, buf)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}
	}
}

func extractZipFile(ctx context.Context, src *os.File, buf []byte) error {
	fi, err := src.Stat()
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, src.Name(), err)
	}

	zr, err := zip.NewReader(src, fi.Size())
	if err != nil {
		return fmt.Errorf(`open zip file "%s" error: %w`, src.Name(), err)
	}

	for _, zf := range zr.File {
		path := zf.Name

		fi := zf.FileInfo()
		if fi.IsDir() {
			if err = os.MkdirAll(path, 0755); err != nil {
				return fmt.Errorf(`create dir "%s" error: %w`, path, err)
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf(`create path to file "%s" error: %w`, path, err)
		}

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fi.Mode())
		if err != nil {
			return fmt.Errorf(`create file "%s" error: %w`, path, err)
		}

		w, err := zf.Open()
		if err != nil {
			_ = f.Close()
			return fmt.Errorf(`open file "%s" in archive error: %w`, path, err)
		}

		_, err = util.CopyBufferWithContext(ctx, f, w, buf)
		_, _ = w.Close(), f.Close()
		if err != nil {
			return fmt.Errorf(`write to file "%s" error: %w`, path, err)
		}
	}

	return nil
}
