package upload

import (
	"context"
	"errors"
	"fmt"
	"github.com/nguyengg/xy3/internal"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// preflight validates the file (specified by "name" argument) to be uploaded.
//
// If the file is a directory, an archive will be created to recursively compress all files in the directory, and the
// name of the archive is returned as the filename parameter. Otherwise, the returned filename parameter will be
// identical to the name argument.
func (c *Command) preflight(ctx context.Context, logger *log.Logger, name string) (filename, ext string, size int64, checksum string, contentType *string, err error) {
	filename, ext = name, filepath.Ext(name)

	// name can either be a file or a directory, so use stat to determine what to do.
	fi, err := os.Stat(name)
	if err != nil {
		err = fmt.Errorf("describe file error: %w", err)
		return
	}

	if fi.IsDir() {
		filename, ext, checksum, contentType, err = c.compress(ctx, logger, name)
		if err != nil {
			err = fmt.Errorf("compress directory error: %w", err)
			return
		}

		// in the case of compression, we do want to stat the file again to get the size.
		if fi, err = os.Stat(filename); err != nil {
			err = fmt.Errorf("describe archive error: %w", err)
		} else {
			size = fi.Size()
		}
		return
	}

	if !fi.Mode().IsRegular() {
		err = errors.New("not a regular file")
		return
	}

	// at this point, it's a regular file so compute its checksum here.
	var file *os.File
	if file, err = os.Open(name); err != nil {
		err = fmt.Errorf("open file error: %w", err)
		return
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)

	data := make([]byte, 512)
	if _, err = file.Read(data); err != nil {
		err = fmt.Errorf("read first 512 bytes error: %w", err)
		return
	}
	if v := http.DetectContentType(data); v != "application/octet-stream" {
		contentType = &v
	}

	h := internal.NewHasher()
	_, err = h.Write(data)
	if err != nil {
		_, err = io.Copy(h, file)
	}
	if err != nil {
		err = fmt.Errorf("compute hash error: %w", err)
		return
	}

	checksum = h.SumToChecksumString(nil)
	return
}
