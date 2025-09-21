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
func (c *Command) compress(ctx context.Context, root string) (f *os.File, contentType *string, err error) {
	mode := compress.ZSTD

	f, err = util.OpenExclFile(".", filepath.Base(root), mode.Ext(), 0666)
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	bar := internal.DefaultBytes(-1, filepath.Base(root))
	if err, _ = compress.Compress(ctx, root, io.MultiWriter(f, bar), func(opts *compress.Options) {
		opts.Mode = mode
		opts.MaxConcurrency = c.MaxConcurrency
	}), bar.Close(); err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return nil, nil, err
	}

	return f, aws.String("application/zip"), nil
}
