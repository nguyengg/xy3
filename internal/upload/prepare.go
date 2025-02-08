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
func (c *Command) prepare(ctx context.Context, name string) (filename string, size int64, contentType *string, err error) {
	filename = name

	// name can either be a file or a directory, so use stat to determine what to do.
	fi, err := os.Stat(filename)
	if err != nil {
		err = fmt.Errorf("describe file error: %w", err)
		return
	}
	size = fi.Size()

	switch {
	case fi.IsDir():
		c.logger.Printf("start archiving")

		filename, contentType, err = c.compress(ctx, name)
		if err != nil {
			err = fmt.Errorf("compress directory error: %w", err)
			return
		}

		if fi, err = os.Stat(filename); err == nil {
			size = fi.Size()
		}
		return
	case !fi.Mode().IsRegular():
		err = errors.New("not a regular file")
		return

	}

	// at this point, it's a regular file so read the first 512 bytes to detect its content type.
	var file *os.File
	if file, err = os.Open(filename); err != nil {
		err = fmt.Errorf("open file error: %w", err)
		return
	}
	data := make([]byte, 512)
	_, err = file.Read(data)
	if _ = file.Close(); err != nil {
		err = fmt.Errorf("read first 512 bytes error: %w", err)
		return
	}
	if v := http.DetectContentType(data); v != "application/octet-stream" {
		contentType = &v
	}

	return
}
