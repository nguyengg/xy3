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
// On success, return the name of the archive as well as additional metadata.
func (c *Command) compressDir(ctx context.Context, dir string) (name string, contentType *string, size int64, checksum string, err error) {
	alg := xy3.DefaultAlgorithmName
	comp := xy3.NewCompressorFromName(alg)
	ext := comp.ArchiveExt()

	f, err := util.OpenExclFile(".", filepath.Base(dir), ext, 0666)
	if err != nil {
		return "", nil, 0, "", fmt.Errorf("create archive error: %w", err)
	}
	defer f.Close()

	var (
		sizer       = &util.Sizer{}
		checksummer = internal.DefaultChecksum()
	)

	if err = xy3.CompressDir(ctx, dir, io.MultiWriter(f, sizer, checksummer), func(opts *xy3.CompressOptions) {
		opts.Algorithm = alg
	}); err != nil {
		_, _ = f.Close(), os.Remove(f.Name())
		return "", nil, 0, "", err
	}

	if err = f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", nil, 0, "", fmt.Errorf("close archive error: %w", err)
	}

	return f.Name(), aws.String(comp.ContentType()), sizer.Size, checksummer.SumToString(nil), nil
}
