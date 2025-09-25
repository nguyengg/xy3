package upload

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/nguyengg/xy3"
	"github.com/nguyengg/xy3/codec"
	"github.com/nguyengg/xy3/util"
)

// compressDir creates a new archive and compresses all files recursively starting at root.
//
// On success, the returned file is ready for read at offset 0.
func (c *Command) compressDir(ctx context.Context, root string) (f *os.File, size int64, contentType *string, checksum string, err error) {
	alg := codec.DefaultAlgorithmName
	comp, _ := codec.NewCompressorFromAlgorithm(alg)
	ext := comp.Ext(true)

	f, err = util.OpenExclFile(".", filepath.Base(root), ext, 0666)
	if err != nil {
		err = fmt.Errorf("create archive error: %w", err)
		return
	}

	sizer := &util.Sizer{}
	checksummer := util.DefaultChecksum()

	if err = xy3.Compress(ctx, root, io.MultiWriter(f, sizer, checksummer), func(opts *xy3.CompressOptions) {
		opts.Algorithm = alg
		if c.MaxConcurrency > 0 {
			opts.MaxConcurrency = c.MaxConcurrency
		}
	}); err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return nil, 0, nil, "", err
	}

	return f, sizer.Size, aws.String(comp.ContentType()), checksummer.SumToString(nil), nil
}
