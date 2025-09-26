package upload

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/internal"
	"github.com/nguyengg/xy3/util"
)

// compressDir creates a new archive and compresses all files recursively starting at root.
//
// On success, return the name of the archive.
func (c *Command) compressDir(ctx context.Context, dir string) (name string, size int64, contentType *string, checksum string, err error) {
	alg := xy3.DefaultAlgorithmName
	comp := xy3.NewCompressorFromName(alg)
	ext := comp.ArchiveExt()

	dst, err := util.OpenExclFile(".", filepath.Base(dir), ext, 0666)
	if err != nil {
		return "", 0, nil, "", fmt.Errorf("create archive error: %w", err)
	}
	defer dst.Close()

	sizer := &util.Sizer{}
	checksummer := internal.DefaultChecksum()

	if err = xy3.CompressDir(ctx, dir, io.MultiWriter(dst, sizer, checksummer), func(opts *xy3.CompressOptions) {
		opts.Algorithm = alg
		if c.MaxConcurrency > 0 {
			opts.MaxConcurrency = c.MaxConcurrency
		}
	}); err != nil {
		_, _ = dst.Close(), os.Remove(dst.Name())
		return "", 0, nil, "", err
	}

	if err = dst.Close(); err != nil {
		_ = os.Remove(dst.Name())
		return "", 0, nil, "", fmt.Errorf("close archive error: %w", err)
	}

	return dst.Name(), sizer.Size, aws.String(comp.ContentType()), checksummer.SumToString(nil), nil
}
