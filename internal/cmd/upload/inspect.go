package upload

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/nguyengg/go-aws-commons/tspb"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

// inspect computes the content type and checksum of the given file.
//
// On success, the returned file is ready for read at offset 0.
func (c *Command) inspect(ctx context.Context, name string) (f *os.File, size int64, contentType *string, checksum string, err error) {
	if f, err = os.Open(name); err != nil {
		return nil, 0, nil, "", fmt.Errorf(`open file "%s" error: %w`, name, err)
	}

	if fi, err := f.Stat(); err != nil {
		return nil, 0, nil, "", fmt.Errorf(`stat file "%s" error: %w`, name, err)
	} else {
		size = fi.Size()
	}

	checksummer := internal.DefaultChecksum()
	bar := tspb.DefaultBytes(size, fmt.Sprintf(`computing checksum "%s"`, filepath.Base(name)))

	// read first 512 bytes to detect content type.
	// if this won't produce a usable content type then let S3 decides it (which is probably going to be "binary/octet-stream").
	data := make([]byte, 512)
	if n, err := f.Read(data); err != nil {
		_ = f.Close()
		return nil, 0, nil, "", fmt.Errorf("read first 512 bytes error: %w", err)
	} else {
		_, _ = checksummer.Write(data[:n])
		_, _ = bar.Write(data[:n])
	}

	if v := http.DetectContentType(data); v != "application/octet-stream" {
		contentType = &v
	}

	_, err = util.CopyBufferWithContext(ctx, io.MultiWriter(checksummer, bar), f, nil)
	_ = bar.Close()
	if err != nil {
		_ = f.Close()
		return nil, 0, nil, "", fmt.Errorf("compute checksum error: %w", err)
	}

	if _, err = f.Seek(0, 0); err != nil {
		_ = f.Close()
		return nil, 0, nil, "", fmt.Errorf("seek start error: %w", err)
	}

	checksum = checksummer.SumToString(nil)
	return
}
