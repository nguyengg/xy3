package upload

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
)

// prepare validates the file (specified by "name" argument) to be uploaded.
//
// If the file is a directory, an archive will be created to recursively compress all files in the directory, and the
// name of the archive is returned as the filename parameter. Otherwise, the returned filename parameter will be
// identical to the name argument.
func (c *Command) prepare(ctx context.Context, name string) (f *os.File, size int64, contentType *string, err error) {
	// name can either be a file or a directory, so use stat to determine what to do.
	fi, err := os.Stat(name)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("describe file error: %w", err)
	}

	switch {
	case fi.IsDir():
		c.logger.Printf(`start compressing "%s"`, name)

		f, contentType, err = c.compress(ctx, name)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("compress directory error: %w", err)
		}

		if fi, err = f.Stat(); err != nil {
			_, _ = f.Close(), os.Remove(f.Name())
			return nil, 0, nil, fmt.Errorf("check compressed file size error: %w", err)
		}

		return f, fi.Size(), contentType, nil

	case fi.Mode().IsRegular():
		if f, err = os.Open(name); err != nil {
			return nil, 0, nil, fmt.Errorf("open file error: %w", err)
		}

		data := make([]byte, 512)
		if _, err = f.Read(data); err != nil {
			_ = f.Close()
			return nil, 0, nil, fmt.Errorf("read first 512 bytes error: %w", err)
		}

		if v := http.DetectContentType(data); v != "application/octet-stream" {
			contentType = &v
		}

		if _, err = f.Seek(0, 0); err != nil {
			_ = f.Close()
			return nil, 0, nil, fmt.Errorf("reset read offset error: %w", err)
		}

		return f, fi.Size(), contentType, err
	default:
		return nil, 0, nil, errors.New("not a regular file")
	}
}
