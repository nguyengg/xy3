package upload

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/internal/compress"
	"github.com/nguyengg/xy3/util"
)

// compress creates a new archive and compresses all files recursively starting at root.
//
// On success, the returned file is ready for read at offset 0.
func (c *Command) compress(ctx context.Context, root string) (f *os.File, size int64, contentType *string, checksum string, err error) {
	mode := compress.ZSTD

	f, err = util.OpenExclFile(".", filepath.Base(root), mode.Ext(), 0666)
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	sizer := &sizer{}
	checksummer := util.DefaultChecksum()
	bar := internal.DefaultBytes(-1, fmt.Sprintf(`compressing "%s"`, filepath.Base(root)))

	if err, _ = compress.Compress(ctx, root, io.MultiWriter(f, sizer, checksummer, bar), func(opts *compress.Options) {
		opts.Mode = mode
		opts.MaxConcurrency = c.MaxConcurrency
	}), bar.Close(); err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return nil, 0, nil, "", err
	}

	return f, sizer.size, aws.String(mode.ContentType()), checksummer.SumToString(nil), nil
}

type sizer struct {
	size int64
}

func (s *sizer) Write(p []byte) (n int, err error) {
	n = len(p)
	s.size += int64(n)
	return
}
