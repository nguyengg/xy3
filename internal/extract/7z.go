package extract

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bodgit/sevenzip"
	"github.com/nguyengg/xy3/util"
)

func extract7zFile(ctx context.Context, src *os.File, buf []byte) error {
	fi, err := src.Stat()
	if err != nil {
		return fmt.Errorf(`stat file "%s" error: %w`, src.Name(), err)
	}

	zr, err := sevenzip.NewReader(src, fi.Size())
	if err != nil {
		return fmt.Errorf(`open 7z file "%s" error: %w`, src.Name(), err)
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
