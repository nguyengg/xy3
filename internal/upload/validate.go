package upload

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/dustin/go-humanize"
	"io"
	"net/http"
	"os"
)

// validate will open a file for reading. Caller is responsible for closing the file unless an error is returned.
func (c *Command) validate(name string) (file *os.File, size int64, checksum string, contentType *string, err error) {
	fi, err := os.Stat(name)
	if err != nil {
		err = fmt.Errorf("describe file error: %v", err)
		return
	}

	if fi.IsDir() {
		err = fmt.Errorf("is a directory")
		return
	}

	size = fi.Size()
	switch {
	case size > maxUploadSize:
		err = fmt.Errorf("size (%d - %s) is larger than limit (%d - %s)",
			size, humanize.Bytes(uint64(size)),
			maxUploadSize, humanize.Bytes(uint64(maxUploadSize)))
		return
	case size == 0:
		err = fmt.Errorf("empty file")
		return
	}

	// in the first pass of the file, compute the SHA-384 checksum as well as finding the content type since S3 does
	// not do a good job of detecting 7z files.
	file, err = os.Open(name)
	if err != nil {
		err = fmt.Errorf("open file error: %w", err)
		return
	}
	defer func() {
		if err != nil && file != nil {
			_ = file.Close()
			file = nil
		}
	}()

	data := make([]byte, 512)
	h := sha512.New384()
	if _, err = file.Read(data); err != nil {
		err = fmt.Errorf("read first 512 bytes error: %w", err)
		return
	}
	if v := http.DetectContentType(data); v != "application/octet-stream" {
		contentType = &v
	}

	h.Write(data)
	if _, err = io.Copy(h, file); err != nil {
		err = fmt.Errorf("compute hash error: %w", err)
		return
	}

	checksum = "sha384-" + base64.StdEncoding.EncodeToString(h.Sum(nil))

	if _, err = file.Seek(0, 0); err != nil {
		err = fmt.Errorf("reset file reader error: %w", err)
		return
	}

	return
}
